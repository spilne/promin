// ---------------------------------------------------------------------------
// RunPollWatcher — polls workflow storage and publishes diffs to RunEventBus.
//
// One watcher per watched run, shared by every viewer through
// `RunWatcherPool`. Each viewer gets its own snapshot on subscribe; the
// watcher then publishes { type: "step" } when a step changes or is added
// and { type: "status" } on a status change. It ends ({ type: "end" }) when
// the run reaches any terminal status or disappears, and stops polling when
// its last viewer leaves.
//
// Polls never overlap: each tick schedules the next one on the clock after
// it finishes. A failed poll is reported and retried with backoff; after
// `maxConsecutiveFailures` failures in a row the watcher publishes
// { type: "error" } then { type: "end" } and stops.
// ---------------------------------------------------------------------------

import {
  isTerminalWorkflowStatus,
  SystemWallClock,
  type TimerHandle,
  type WallClock,
  type WorkflowState,
  type WorkflowStorage,
} from "@promin/workflow";
import { runToDto, stepToDto } from "./serialize.ts";
import type { RunEventBus } from "./run-event-bus.ts";
import type { RunDto, RunEvent, StepDto } from "./api-types.ts";

/** Consecutive failed polls after which a watcher gives up. */
export const DEFAULT_MAX_CONSECUTIVE_POLL_FAILURES = 5;

/** Longest delay between retries of a failing poll (ms). */
const MAX_BACKOFF_MS = 30_000;

export type RunWatchErrorReporter = (params: { workflowId: string; error: unknown }) => void;

export interface RunPollWatcherConfig {
  storage: WorkflowStorage;
  bus: RunEventBus;
  workflowId: string;
  /** Poll interval in ms. Default 1000. */
  intervalMs?: number;
  /** Failed polls in a row before the watcher ends the stream. Default 5. */
  maxConsecutiveFailures?: number;
  /** Reports a failed poll. Default: `console.error`. */
  onError?: RunWatchErrorReporter;
  /** Called once when the watcher stops for any reason. */
  onStop?: () => void;
  clock?: WallClock;
}

export class RunPollWatcher {
  private readonly storage: WorkflowStorage;
  private readonly bus: RunEventBus;
  private readonly workflowId: string;
  private readonly intervalMs: number;
  private readonly maxConsecutiveFailures: number;
  private readonly onError: RunWatchErrorReporter;
  private readonly onStop?: () => void;
  private readonly clock: WallClock;
  private handle?: TimerHandle;
  /**
   * Each step's last published change signature — not the step itself, so
   * a watched run's step results are not held between polls.
   */
  private lastSteps = new Map<string, string>();
  private lastStatus?: string;
  private failures = 0;
  private running = false;
  private stopped = false;

  constructor(config: RunPollWatcherConfig) {
    this.storage = config.storage;
    this.bus = config.bus;
    this.workflowId = config.workflowId;
    this.intervalMs = config.intervalMs ?? 1000;
    this.maxConsecutiveFailures =
      config.maxConsecutiveFailures ?? DEFAULT_MAX_CONSECUTIVE_POLL_FAILURES;
    this.onError =
      config.onError ??
      (({ workflowId, error }) => console.error(`[zorya] run watch ${workflowId} failed`, error));
    this.onStop = config.onStop;
    this.clock = config.clock ?? SystemWallClock;
  }

  /**
   * Start polling from a baseline snapshot the caller already loaded.
   * Idempotent; a stopped watcher stays stopped.
   */
  start(baseline: RunDto): void {
    if (this.running || this.stopped) return;
    this.running = true;
    for (const s of baseline.steps) this.lastSteps.set(s.stepName, stepSignature(s));
    this.lastStatus = baseline.status;
    this.schedule(this.intervalMs);
  }

  stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    this.handle?.clear();
    this.handle = undefined;
    this.onStop?.();
  }

  private schedule(delayMs: number): void {
    if (this.stopped) return;
    this.handle = this.clock.setTimeout(() => {
      this.handle = undefined;
      void this.poll();
    }, delayMs);
  }

  /** One guarded poll. Never rejects. */
  private async poll(): Promise<void> {
    if (this.stopped) return;
    if (!this.bus.hasListeners(this.workflowId)) {
      this.stop();
      return;
    }
    let state: WorkflowState | null;
    try {
      state = await this.storage.loadWorkflow(this.workflowId);
    } catch (error) {
      this.failures++;
      try {
        this.onError({ workflowId: this.workflowId, error });
      } catch {
        // a broken reporter must not break the loop
      }
      if (this.failures >= this.maxConsecutiveFailures) {
        this.publish({ type: "error", error: "watch_failed" });
        this.end();
        return;
      }
      this.schedule(Math.min(this.intervalMs * 2 ** this.failures, MAX_BACKOFF_MS));
      return;
    }
    this.failures = 0;
    if (this.stopped) return;
    if (!state) {
      // Run deleted while watched.
      this.end();
      return;
    }
    this.publishDiff(state);
    if (isTerminalWorkflowStatus(state.status)) {
      this.end();
      return;
    }
    this.schedule(this.intervalMs);
  }

  private publishDiff(state: WorkflowState): void {
    for (const s of Object.values(state.steps)) {
      const dto = stepToDto(s);
      const signature = stepSignature(dto);
      if (this.lastSteps.get(dto.stepName) !== signature) {
        this.publish({ type: "step", stepName: dto.stepName, step: dto });
        this.lastSteps.set(dto.stepName, signature);
      }
    }
    if (state.status !== this.lastStatus) {
      this.lastStatus = state.status;
      this.publish({ type: "status", status: state.status });
    }
  }

  private end(): void {
    this.publish({ type: "end" });
    this.stop();
  }

  private publish(event: RunEvent): void {
    this.bus.publish(this.workflowId, event);
  }
}

export interface RunWatcherPoolConfig {
  storage: WorkflowStorage;
  bus: RunEventBus;
  /** Poll interval in ms. Default 1000. */
  intervalMs?: number;
  /** Failed polls in a row before a watcher ends its streams. Default 5. */
  maxConsecutiveFailures?: number;
  /** Reports a failed poll. Default: `console.error`. */
  onError?: RunWatchErrorReporter;
  clock?: WallClock;
}

/**
 * Hands out run-event subscriptions backed by one shared `RunPollWatcher`
 * per run, however many viewers watch it.
 */
export class RunWatcherPool {
  private readonly config: RunWatcherPoolConfig;
  private readonly watchers = new Map<string, RunPollWatcher>();

  constructor(config: RunWatcherPoolConfig) {
    this.config = config;
  }

  /**
   * Subscribe `listener` to `state`'s run. The listener first receives a
   * snapshot of `state`, then the shared watcher's diffs; a run that is
   * already terminal gets `end` straight after the snapshot. Returns the
   * unsubscribe function.
   */
  subscribe(params: { state: WorkflowState; listener: (event: RunEvent) => void }): () => void {
    const { state, listener } = params;
    const workflowId = state.workflowId;
    const snapshot = runToDto(state);
    listener({ type: "snapshot", run: snapshot });
    if (isTerminalWorkflowStatus(snapshot.status)) {
      listener({ type: "end" });
      return () => {};
    }

    const unsubscribe = this.config.bus.subscribe(workflowId, listener);
    if (!this.watchers.has(workflowId)) {
      const watcher = new RunPollWatcher({
        storage: this.config.storage,
        bus: this.config.bus,
        workflowId,
        ...(this.config.intervalMs !== undefined && { intervalMs: this.config.intervalMs }),
        ...(this.config.maxConsecutiveFailures !== undefined && {
          maxConsecutiveFailures: this.config.maxConsecutiveFailures,
        }),
        ...(this.config.onError !== undefined && { onError: this.config.onError }),
        ...(this.config.clock !== undefined && { clock: this.config.clock }),
        onStop: () => {
          if (this.watchers.get(workflowId) === watcher) this.watchers.delete(workflowId);
        },
      });
      this.watchers.set(workflowId, watcher);
      watcher.start(snapshot);
    }

    let active = true;
    return () => {
      if (!active) return;
      active = false;
      unsubscribe();
      if (!this.config.bus.hasListeners(workflowId)) this.watchers.get(workflowId)?.stop();
    };
  }

  /** Number of runs with a live watcher. */
  activeWatchers(): number {
    return this.watchers.size;
  }
}

/** The fields whose change publishes a step event, as one comparable string. */
function stepSignature(s: StepDto): string {
  return JSON.stringify([s.status, s.attempt, s.durationMs, s.completedAt, s.wakeAt, s.signalName]);
}
