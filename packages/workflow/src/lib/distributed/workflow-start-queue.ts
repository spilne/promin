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

export interface WorkflowStartRecord {
  /** Queue-local id (used for `complete`). Distinct from `workflowId`. */
  readonly id: string;
  readonly workflowId: string;
  readonly workflowName: string;
  readonly namespace?: string;
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
}

/** Per-name set of versions a worker can run. */
export interface WorkerWorkflowSpec {
  readonly name: string;
  /** Versions the worker can serve. Empty means "any version". */
  readonly versions: readonly string[];
}

export interface WorkflowStartQueue {
  /**
   * Enqueue a workflow start. Idempotent on active `workflowId`: while an
   * existing start is pending or claimed, implementations return that start's
   * id instead of creating a duplicate.
   */
  enqueue(params: {
    workflowId: string;
    workflowName: string;
    namespace?: string;
    input: unknown;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ id: string }>;
  /**
   * Claim up to `limit` pending starts the worker can run. A start matches
   * a spec when the names match AND (the start has no version, the spec
   * advertises no specific versions, or the start's version is in the
   * spec's version set).
   */
  claim(params: {
    workflowSpecs: readonly WorkerWorkflowSpec[];
    workerId?: string;
    limit: number;
  }): Promise<WorkflowStartRecord[]>;
  /** Mark a claimed start as done. No-op if id is unknown. */
  complete(id: string): Promise<void>;
  /** Snapshot — used by `GET /api/worker-protocol/starts` for debugging. */
  list(): Promise<WorkflowStartRecord[]>;
}

export class InMemoryWorkflowStartQueue implements WorkflowStartQueue {
  private readonly pending: WorkflowStartRecord[] = [];
  private readonly inflight = new Map<string, WorkflowStartRecord>();

  /** Worker stuck mid-execution: re-claimable after this many ms. Default 60s. */
  constructor(private readonly reclaimAfterMs = 60_000) {}

  async enqueue(params: {
    workflowId: string;
    workflowName: string;
    namespace?: string;
    input: unknown;
    metadata?: Record<string, unknown>;
    version?: string;
  }): Promise<{ id: string }> {
    const existing =
      this.pending.find((rec) => rec.workflowId === params.workflowId) ??
      [...this.inflight.values()].find((rec) => rec.workflowId === params.workflowId);
    if (existing) return { id: existing.id };

    const id = `start-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    this.pending.push({
      id,
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      ...(params.namespace !== undefined && { namespace: params.namespace }),
      input: params.input,
      ...(params.metadata !== undefined && { metadata: params.metadata }),
      ...(params.version !== undefined && { version: params.version }),
      enqueuedAt: Date.now(),
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
      const stamped: WorkflowStartRecord = {
        ...rec,
        claimedAt: Date.now(),
        ...(params.workerId !== undefined && { claimedBy: params.workerId }),
      };
      this.inflight.set(rec.id, stamped);
      claimed.push(stamped);
    }
    return claimed;
  }

  async complete(id: string): Promise<void> {
    this.inflight.delete(id);
  }

  async list(): Promise<WorkflowStartRecord[]> {
    this.reclaimStale();
    return [...this.pending, ...this.inflight.values()];
  }

  /** Move stale claims back to pending so a dead worker doesn't strand a start. */
  private reclaimStale(): void {
    const cutoff = Date.now() - this.reclaimAfterMs;
    for (const [id, rec] of this.inflight) {
      if ((rec.claimedAt ?? 0) < cutoff) {
        this.inflight.delete(id);
        const requeued: WorkflowStartRecord = {
          id: rec.id,
          workflowId: rec.workflowId,
          workflowName: rec.workflowName,
          ...(rec.namespace !== undefined && { namespace: rec.namespace }),
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
