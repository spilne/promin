// ---------------------------------------------------------------------------
// WorkerWakeups — tells connected workers that starts they can run were
// queued, so they claim at once instead of at their next poll.
//
// `QueuedWorkflows` reports every enqueue that made a start claimable now
// (a new start, or a delayed one made claimable at once). The notice names
// the run, its workflow and its version; this service sends a small
// `{ kind: "work" }` over the `/ws/worker` control socket to workers that
// can run it. The worker then runs a claim.
//
//   - who: only workers whose socket identified as claiming starts (a
//     socket opened for agent or query work, or a worker with claiming
//     off, is never woken). One matches when its advertisement covers the
//     run's (name, version) — the claim filter's rule (`startMatchesSpec`).
//     A claiming worker the advertisement index doesn't know yet (it
//     advertised after the last read, or the registry forgot it) matches
//     too, and the index is re-read on the next send — once per newly
//     unknown worker, not on every send
//   - how many: each send wakes about as many matching workers as it has
//     starts (plus `WAKE_MARGIN`), taken round-robin, not every matching
//     worker — a fleet of idle workers would otherwise answer every trigger
//     with a claim each. A start not claimed within `followUpMs` (the woken
//     workers were busy, a message was lost) wakes every matching worker
//     once more; after that the workers' polls cover it
//   - coalesced: the first notice after a quiet spell is sent at once;
//     notices within `coalesceMs` of a send merge into one trailing send
//   - bounded: at most `MAX_PENDING_STARTS` notices wait between sends and
//     `MAX_TRACKED_STARTS` await their claim; past either, every matching
//     worker is woken instead. The advertisements are read at most once per
//     `advertisementCacheMs`, into a compact worker → (name, versions) index
//   - a claiming worker that connects (identifies) is woken at once: a start queued
//     while its socket was not yet registered was told to nobody
//   - best effort: a socket on another Zorya instance or a failed
//     advertisement read only delays the claim to the worker's poll
//   - every timer runs on the injected `WallClock`
// ---------------------------------------------------------------------------

import { SystemWallClock, type TimerHandle, type WallClock } from "@promin/workflow";
import {
  startMatchesSpec,
  type WorkerWorkflowSpec,
  type WorkflowAdvertisementRegistry,
} from "@promin/workflow/distributed";
import type { WorkerConnectionEvent, WorkerWsOutbound } from "./worker-ws-server.ts";

/** A start that became claimable, as far as routing a wake-up goes. */
export interface QueuedStartNotice {
  readonly workflowId: string;
  readonly workflowName: string;
  /** The run's version; absent (or "") for a run without one: any worker of the name. */
  readonly version?: string;
}

/** The parts of `WorkerWebSocketServer` the wake-ups use. */
export interface WakeupSockets {
  /** Connected workers that identified as claiming queued starts. */
  claimingWorkers(): string[];
  send(params: { workerId: string; message: WorkerWsOutbound }): boolean;
  /** Connect / disconnect of identified workers. */
  onConnection(handler: (event: WorkerConnectionEvent) => void): () => void;
}

export interface WorkerWakeupsConfig {
  readonly sockets: WakeupSockets;
  /** Which worker can run what. */
  readonly advertisements: WorkflowAdvertisementRegistry;
  /** Time source for the windows, the follow-up and the cache. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
  /**
   * Notices within this long of a send are merged into one trailing send
   * (ms). Default: `DEFAULT_WAKEUP_COALESCE_MS`.
   */
  readonly coalesceMs?: number;
  /**
   * A start not claimed this long after its wake-up wakes every matching
   * worker (ms). Default: `DEFAULT_WAKEUP_FOLLOW_UP_MS`.
   */
  readonly followUpMs?: number;
  /** How long one read of the advertisements is reused (ms). Default 1000. */
  readonly advertisementCacheMs?: number;
  /** Background failures (an advertisement read). Default: `console.error`. */
  readonly onError?: (error: unknown) => void;
}

/** Default coalescing window of wake-up sends (ms). */
export const DEFAULT_WAKEUP_COALESCE_MS = 20;
/** Default wait for a woken start's claim before every matching worker is woken (ms). */
export const DEFAULT_WAKEUP_FOLLOW_UP_MS = 250;

/** Extra workers woken per send beyond its number of starts. */
const WAKE_MARGIN = 1;
/** Notices held between sends; past it, the next send wakes every connected worker. */
const MAX_PENDING_STARTS = 1_024;
/** Starts awaiting their claim; past it, a send wakes every matching worker untracked. */
const MAX_TRACKED_STARTS = 4_096;

/** Shortest gap between advertisement reads caused by an unknown connected worker (ms). */
const MISS_REREAD_MS = 100;

const WORK: WorkerWsOutbound = { kind: "work" };

/** Workflow name → the versions a worker serves (`[]`: any version). */
type WorkerCoverage = ReadonlyMap<string, WorkerWorkflowSpec>;

interface PendingGroup {
  readonly notice: QueuedStartNotice;
  readonly workflowIds: string[];
}

interface TrackedStart {
  readonly notice: QueuedStartNotice;
  readonly dueAt: number;
}

export class WorkerWakeups {
  private readonly sockets: WakeupSockets;
  private readonly advertisements: WorkflowAdvertisementRegistry;
  private readonly clock: WallClock;
  private readonly coalesceMs: number;
  private readonly followUpMs: number;
  private readonly cacheMs: number;
  private readonly onError: (error: unknown) => void;
  private readonly unsubscribe: () => void;
  /** Notices since the last send, by (name, version). */
  private pending = new Map<string, PendingGroup>();
  private pendingStarts = 0;
  /** More notices than the cap arrived: wake every connected worker. */
  private overflow = false;
  private timer?: TimerHandle;
  private flushing?: Promise<void>;
  private lastSendAt = Number.NEGATIVE_INFINITY;
  private coverage?: {
    readonly at: number;
    readonly byWorker: Map<string, WorkerCoverage>;
    /** Claiming workers the read did not cover: no re-read on their account. */
    readonly unknown: ReadonlySet<string>;
  };
  /** The advertisement read in progress: concurrent callers share it. */
  private reading?: Promise<Map<string, WorkerCoverage>>;
  /** Woken starts not claimed yet, by workflow id, in wake order. */
  private readonly tracked = new Map<string, TrackedStart>();
  private followUpTimer?: { readonly handle: TimerHandle; readonly dueAt: number };
  /** Round-robin position over matching workers. */
  private cursor = 0;
  private stopped = false;
  /** Messages sent (tests, diagnostics). */
  sent = 0;

  constructor(config: WorkerWakeupsConfig) {
    this.sockets = config.sockets;
    this.advertisements = config.advertisements;
    this.clock = config.clock ?? SystemWallClock;
    this.coalesceMs = Math.max(0, config.coalesceMs ?? DEFAULT_WAKEUP_COALESCE_MS);
    this.followUpMs = Math.max(0, config.followUpMs ?? DEFAULT_WAKEUP_FOLLOW_UP_MS);
    this.cacheMs = Math.max(0, config.advertisementCacheMs ?? 1_000);
    this.onError =
      config.onError ?? ((error) => console.error("[zorya] worker wake-up failed:", error));
    // A worker that just identified missed whatever was told before it
    // was registered: wake it once.
    this.unsubscribe = this.sockets.onConnection(({ workerId, connected, claimsStarts }) => {
      if (connected && claimsStarts && !this.stopped) this.wake(workerId);
    });
  }

  /** A start became claimable: wake workers that can run it. */
  notify(notice: QueuedStartNotice): void {
    if (this.stopped || this.sockets.claimingWorkers().length === 0) return;
    if (!this.overflow) {
      if (this.pendingStarts >= MAX_PENDING_STARTS) {
        this.overflow = true;
        this.pending.clear();
        this.pendingStarts = 0;
      } else {
        const key = routeKey(notice);
        let group = this.pending.get(key);
        if (!group) {
          group = { notice: routeOf(notice), workflowIds: [] };
          this.pending.set(key, group);
        }
        group.workflowIds.push(notice.workflowId);
        this.pendingStarts++;
      }
    }
    this.schedule();
  }

  /** Starts were claimed (the claim route): they need no follow-up wake. */
  claimed(starts: ReadonlyArray<{ readonly workflowId: string }>): void {
    for (const start of starts) this.tracked.delete(start.workflowId);
  }

  /** Resolves once queued notices are sent (tests). */
  async idle(): Promise<void> {
    while (this.flushing) await this.flushing;
  }

  /** Drop pending notices and cancel the timers. Idempotent. */
  stop(): void {
    this.stopped = true;
    this.unsubscribe();
    this.timer?.clear();
    this.timer = undefined;
    this.followUpTimer?.handle.clear();
    this.followUpTimer = undefined;
    this.pending.clear();
    this.pendingStarts = 0;
    this.overflow = false;
    this.tracked.clear();
  }

  private schedule(): void {
    if (this.timer || this.flushing || (!this.overflow && this.pending.size === 0)) return;
    const waitMs = this.lastSendAt + this.coalesceMs - this.clock.currentTimeMs();
    if (waitMs <= 0) {
      this.flush();
      return;
    }
    this.timer = this.clock.setTimeout(() => {
      this.timer = undefined;
      this.flush();
    }, waitMs);
  }

  private flush(): void {
    const groups = [...this.pending.values()];
    const everyone = this.overflow;
    this.pending = new Map();
    this.pendingStarts = 0;
    this.overflow = false;
    this.lastSendAt = this.clock.currentTimeMs();
    const run = this.send({ groups, everyone })
      .catch((error: unknown) => this.report(error))
      .finally(() => {
        if (this.flushing === run) this.flushing = undefined;
        if (!this.stopped) this.schedule();
      });
    this.flushing = run;
  }

  private async send(params: {
    groups: readonly PendingGroup[];
    everyone: boolean;
  }): Promise<void> {
    const connected = this.sockets.claimingWorkers();
    if (connected.length === 0) return;
    if (params.everyone) {
      for (const workerId of connected) this.wake(workerId);
      return;
    }
    const byWorker = await this.coverageByWorker();
    if (this.stopped) return;
    const targets = new Set<string>();
    const now = this.clock.currentTimeMs();
    for (const group of params.groups) {
      const matching = connected.filter((workerId) =>
        covers({ coverage: byWorker.get(workerId), notice: group.notice }),
      );
      if (matching.length === 0) continue;
      const trackable = this.tracked.size + group.workflowIds.length <= MAX_TRACKED_STARTS;
      if (!trackable) {
        for (const workerId of matching) targets.add(workerId);
        continue;
      }
      for (const workerId of this.rotate({
        workers: matching,
        count: group.workflowIds.length + WAKE_MARGIN,
      })) {
        targets.add(workerId);
      }
      for (const workflowId of group.workflowIds) {
        this.tracked.delete(workflowId); // re-insert: keeps the map in due order
        this.tracked.set(workflowId, { notice: group.notice, dueAt: now + this.followUpMs });
      }
    }
    for (const workerId of targets) this.wake(workerId);
    this.armFollowUp();
  }

  /** `count` of `workers`, round-robin across sends. */
  private rotate(params: { workers: readonly string[]; count: number }): string[] {
    const { workers } = params;
    const count = Math.min(params.count, workers.length);
    const start = this.cursor % workers.length;
    this.cursor = (this.cursor + count) % 1_000_000_007;
    const out: string[] = [];
    for (let i = 0; i < count; i++) out.push(workers[(start + i) % workers.length]!);
    return out;
  }

  private armFollowUp(): void {
    const first = this.tracked.values().next();
    if (first.done || this.stopped) return;
    const dueAt = first.value.dueAt;
    if (this.followUpTimer && this.followUpTimer.dueAt <= dueAt) return;
    this.followUpTimer?.handle.clear();
    const handle = this.clock.setTimeout(
      () => {
        this.followUpTimer = undefined;
        void this.followUp();
      },
      Math.max(0, dueAt - this.clock.currentTimeMs()),
    );
    this.followUpTimer = { handle, dueAt };
  }

  /** Starts still unclaimed past their follow-up: wake every matching worker, once. */
  private async followUp(): Promise<void> {
    const now = this.clock.currentTimeMs();
    const due: QueuedStartNotice[] = [];
    for (const [workflowId, start] of this.tracked) {
      if (start.dueAt > now) break;
      this.tracked.delete(workflowId);
      due.push(start.notice);
    }
    try {
      if (due.length > 0) {
        const connected = this.sockets.claimingWorkers();
        const byWorker = await this.coverageByWorker();
        if (this.stopped) return;
        for (const workerId of connected) {
          const coverage = byWorker.get(workerId);
          if (due.some((notice) => covers({ coverage, notice }))) this.wake(workerId);
        }
      }
    } catch (error) {
      this.report(error);
    } finally {
      this.armFollowUp();
    }
  }

  private wake(workerId: string): void {
    if (this.sockets.send({ workerId, message: WORK })) this.sent++;
  }

  /**
   * Worker id → the (name, versions) it advertises, read at most once per
   * cache window — sooner when a claiming worker the last read did not
   * cover shows up. A worker still missing after a re-read (it never
   * advertises, or the registry dropped it) costs no further reads until
   * the window ends.
   */
  private async coverageByWorker(): Promise<Map<string, WorkerCoverage>> {
    const now = this.clock.currentTimeMs();
    const claiming = this.sockets.claimingWorkers();
    if (this.coverage && now - this.coverage.at < this.cacheMs) {
      const { byWorker: known, unknown } = this.coverage;
      const fresh = now - this.coverage.at < Math.min(this.cacheMs, MISS_REREAD_MS);
      const newMiss = claiming.some((id) => !known.has(id) && !unknown.has(id));
      if (fresh || !newMiss) return known;
    }
    this.reading ??= this.readCoverage({ at: now, claiming }).finally(() => {
      this.reading = undefined;
    });
    return this.reading;
  }

  private async readCoverage(params: {
    at: number;
    claiming: readonly string[];
  }): Promise<Map<string, WorkerCoverage>> {
    const entries = await this.advertisements.list();
    const byWorker = new Map<string, WorkerCoverage>();
    for (const entry of entries) {
      const specs = new Map<string, { name: string; versions: string[]; any: boolean }>();
      for (const w of entry.workflows) {
        let spec = specs.get(w.name);
        if (!spec) {
          spec = { name: w.name, versions: [], any: false };
          specs.set(w.name, spec);
        }
        if (!w.version) spec.any = true;
        else spec.versions.push(w.version);
      }
      byWorker.set(
        entry.workerId,
        new Map(
          [...specs.values()].map((s) => [
            s.name,
            { name: s.name, versions: s.any ? [] : s.versions },
          ]),
        ),
      );
    }
    const unknown = new Set(params.claiming.filter((id) => !byWorker.has(id)));
    this.coverage = { at: params.at, byWorker, unknown };
    return byWorker;
  }

  private report(error: unknown): void {
    try {
      this.onError(error);
    } catch {
      // A throwing reporter must not break the wake-ups.
    }
  }
}

function routeKey(notice: QueuedStartNotice): string {
  return `${notice.workflowName}\u0000${notice.version ?? ""}`;
}

function routeOf(notice: QueuedStartNotice): QueuedStartNotice {
  return {
    workflowId: notice.workflowId,
    workflowName: notice.workflowName,
    ...(notice.version && { version: notice.version }),
  };
}

/**
 * True when a worker with `coverage` can claim a start of `notice` — the
 * claim filter's rule. A worker with unknown coverage (not advertised yet,
 * or forgotten) counts as able: its own claim filter decides.
 */
function covers(params: {
  coverage: WorkerCoverage | undefined;
  notice: QueuedStartNotice;
}): boolean {
  const { coverage, notice } = params;
  if (!coverage) return true;
  const spec = coverage.get(notice.workflowName);
  return spec !== undefined && startMatchesSpec({ version: notice.version, spec });
}
