// ---------------------------------------------------------------------------
// WorkflowStartQueue — server-side queue of "please-start" requests.
//
// In split mode the dashboard server doesn't run workflows itself, so when
// the UI hits POST /api/runs/trigger/:name there's no in-process runner to
// dispatch to. The server's auto-trigger pre-creates the storage row (so
// the dashboard sees a `pending` run immediately) and enqueues a start
// here. Workers poll, claim, and execute against the existing row.
//
// In-memory only today; survives the server process. For HA setups,
// implement WorkflowStartQueue on top of Postgres NOTIFY or a real queue.
// ---------------------------------------------------------------------------

export interface WorkflowStartRecord {
  /** Queue-local id (used for `complete`). Distinct from `workflowId`. */
  readonly id: string;
  readonly workflowId: string;
  readonly workflowName: string;
  readonly input: unknown;
  readonly metadata?: Record<string, unknown>;
  /** Epoch ms. */
  readonly enqueuedAt: number;
  /** Epoch ms. Set when a worker claims the record. */
  readonly claimedAt?: number;
  /** Worker that claimed the record. */
  readonly claimedBy?: string;
}

export interface WorkflowStartQueue {
  enqueue(params: {
    workflowId: string;
    workflowName: string;
    input: unknown;
    metadata?: Record<string, unknown>;
  }): Promise<{ id: string }>;
  /**
   * Claim up to `limit` pending starts whose `workflowName` is in
   * `workflowNames`. Records become invisible to subsequent claims until
   * `complete` is called or the claim times out (`reclaimAfterMs`).
   */
  claim(params: {
    workflowNames: readonly string[];
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
    input: unknown;
    metadata?: Record<string, unknown>;
  }): Promise<{ id: string }> {
    const id = `start-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    this.pending.push({
      id,
      workflowId: params.workflowId,
      workflowName: params.workflowName,
      input: params.input,
      metadata: params.metadata,
      enqueuedAt: Date.now(),
    });
    return { id };
  }

  async claim(params: {
    workflowNames: readonly string[];
    workerId?: string;
    limit: number;
  }): Promise<WorkflowStartRecord[]> {
    this.reclaimStale();
    const accept = new Set(params.workflowNames);
    const claimed: WorkflowStartRecord[] = [];
    for (let i = 0; i < this.pending.length && claimed.length < params.limit; ) {
      const rec = this.pending[i]!;
      if (!accept.has(rec.workflowName)) {
        i += 1;
        continue;
      }
      this.pending.splice(i, 1);
      const stamped: WorkflowStartRecord = {
        ...rec,
        claimedAt: Date.now(),
        claimedBy: params.workerId,
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
        this.pending.push({ ...rec, claimedAt: undefined, claimedBy: undefined });
      }
    }
  }
}
