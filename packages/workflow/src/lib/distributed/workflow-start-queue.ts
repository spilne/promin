// ---------------------------------------------------------------------------
// WorkflowStartQueue — coordination primitive for "please-start" requests.
//
// In split mode the dashboard server doesn't run workflows itself, so when
// a trigger comes in there's no in-process runner to dispatch to. The
// auto-trigger pre-creates the storage row (so the dashboard sees a
// `pending` run immediately) and enqueues a start here. Workflow-mode
// workers poll, claim, and execute against the existing row.
//
// Persistence: in-memory dies with the process. SQLite/Postgres backends
// survive restarts, so a trigger from the dashboard with no worker
// connected stays pending until one shows up.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

export interface WorkflowStartRecord {
  /** Queue-local id (used for `complete`). Distinct from `workflowId`. */
  readonly id: string;
  readonly workflowId: string;
  readonly workflowName: string;
  readonly input: unknown;
  readonly metadata?: Record<string, unknown>;
  /** Workflow version requested at trigger time, if any. */
  readonly version?: string;
  /** Epoch ms. */
  readonly enqueuedAt: number;
  /** Epoch ms. Set when a worker claims the record. */
  readonly claimedAt?: number;
  /** Worker that claimed the record. */
  readonly claimedBy?: string;
  /**
   * Fencing token of the current claim, set on every claimed record.
   * `heartbeat` and `complete` only act when they carry it.
   */
  readonly claimToken?: string;
  /** Epoch ms. Last heartbeat of the current claim (the claim time until the first one). */
  readonly heartbeatAt?: number;
}

/** One claim of a start: the record id plus its claim token. */
export interface WorkflowStartClaimRef {
  readonly id: string;
  readonly claimToken: string;
}

/** Per-name set of versions a worker can run. */
export interface WorkerWorkflowSpec {
  readonly name: string;
  /** Versions the worker can serve. Empty means "any version". */
  readonly versions: readonly string[];
}

export interface WorkflowStartQueue {
  enqueue(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ id: string }>;
  /**
   * Claim up to `limit` pending starts the worker can run. A start matches
   * a spec when the names match AND (the start has no version, the spec
   * advertises no specific versions, or the start's version is in the
   * spec's version set). Each claimed record carries a fresh `claimToken`.
   *
   * A claim whose last heartbeat (or the claim itself, before the first
   * heartbeat) is older than the queue's reclaim window goes back to
   * pending and can be claimed again under a new token, so a dead worker
   * doesn't strand a start. A worker running a start for longer than that
   * window must `heartbeat` it.
   */
  claim(params: {
    workflowSpecs: readonly WorkerWorkflowSpec[];
    workerId?: string;
    limit: number;
  }): Promise<WorkflowStartRecord[]>;
  /**
   * Keep a claim alive. Returns false when the claim is no longer current
   * (it went stale and was reclaimed, or was completed): the worker has
   * lost the start.
   */
  heartbeat(params: WorkflowStartClaimRef): Promise<boolean>;
  /**
   * Mark a claimed start as done (removes it). Only the current claim can
   * complete it: with a stale token or an unknown id it changes nothing
   * and returns false.
   */
  complete(params: WorkflowStartClaimRef): Promise<boolean>;
  /** Snapshot — used by `GET /api/worker-protocol/starts` for debugging. */
  list(): Promise<WorkflowStartRecord[]>;
}

export interface InMemoryWorkflowStartQueueConfig {
  /** A claim with no heartbeat for this many ms is re-claimable. Default 60s. */
  reclaimAfterMs?: number;
  /** Time source for enqueue / claim stamps and the reclaim cutoff. Default: `SystemWallClock`. */
  clock?: WallClock;
}

export class InMemoryWorkflowStartQueue implements WorkflowStartQueue {
  private readonly pending: WorkflowStartRecord[] = [];
  private readonly inflight = new Map<string, WorkflowStartRecord>();
  private readonly reclaimAfterMs: number;
  private readonly clock: WallClock;

  constructor(config: InMemoryWorkflowStartQueueConfig = {}) {
    this.reclaimAfterMs = config.reclaimAfterMs ?? 60_000;
    this.clock = config.clock ?? SystemWallClock;
  }

  async enqueue(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ id: string }> {
    const now = this.clock.currentTimeMs();
    const id = `start-${now.toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    this.pending.push({
      id,
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      input: params.input,
      ...(params.metadata !== undefined && { metadata: params.metadata }),
      ...(params.version !== undefined && { version: params.version }),
      enqueuedAt: now,
    });
    return { id };
  }

  async claim(params: {
    workflowSpecs: readonly WorkerWorkflowSpec[];
    workerId?: string;
    limit: number;
  }): Promise<WorkflowStartRecord[]> {
    this.reclaimStale();
    const specByName = new Map<string, WorkerWorkflowSpec>();
    for (const spec of params.workflowSpecs) specByName.set(spec.name, spec);
    const claimed: WorkflowStartRecord[] = [];
    for (let i = 0; i < this.pending.length && claimed.length < params.limit; ) {
      const rec = this.pending[i]!;
      const spec = specByName.get(rec.workflowName);
      if (!spec) {
        i += 1;
        continue;
      }
      // A version-pinned start only matches a worker that advertises that
      // exact version. Versionless starts go to anyone advertising the
      // workflow name.
      if (rec.version && spec.versions.length > 0 && !spec.versions.includes(rec.version)) {
        i += 1;
        continue;
      }
      this.pending.splice(i, 1);
      const now = this.clock.currentTimeMs();
      const stamped: WorkflowStartRecord = {
        ...rec,
        claimedAt: now,
        heartbeatAt: now,
        claimToken: crypto.randomUUID(),
        ...(params.workerId !== undefined && { claimedBy: params.workerId }),
      };
      this.inflight.set(rec.id, stamped);
      claimed.push(stamped);
    }
    return claimed;
  }

  async heartbeat(params: WorkflowStartClaimRef): Promise<boolean> {
    this.reclaimStale();
    const rec = this.inflight.get(params.id);
    if (!rec || rec.claimToken !== params.claimToken) return false;
    this.inflight.set(params.id, { ...rec, heartbeatAt: this.clock.currentTimeMs() });
    return true;
  }

  async complete(params: WorkflowStartClaimRef): Promise<boolean> {
    const rec = this.inflight.get(params.id);
    if (!rec || rec.claimToken !== params.claimToken) return false;
    this.inflight.delete(params.id);
    return true;
  }

  async list(): Promise<WorkflowStartRecord[]> {
    this.reclaimStale();
    return [...this.pending, ...this.inflight.values()];
  }

  /** Move stale claims back to pending so a dead worker doesn't strand a start. */
  private reclaimStale(): void {
    const cutoff = this.clock.currentTimeMs() - this.reclaimAfterMs;
    for (const [id, rec] of this.inflight) {
      if ((rec.heartbeatAt ?? rec.claimedAt ?? 0) < cutoff) {
        this.inflight.delete(id);
        const requeued: WorkflowStartRecord = {
          id: rec.id,
          workflowId: rec.workflowId,
          workflowName: rec.workflowName,
          input: rec.input,
          enqueuedAt: rec.enqueuedAt,
          ...(rec.metadata !== undefined && { metadata: rec.metadata }),
          ...(rec.version !== undefined && { version: rec.version }),
        };
        this.pending.push(requeued);
      }
    }
  }
}
