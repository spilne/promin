// ---------------------------------------------------------------------------
// WorkerRegistry — tracks workers, heartbeats, dead detection, retirement
// ---------------------------------------------------------------------------

/**
 * Worker lifecycle status.
 *  - `active`   — registered, heartbeating, accepting work.
 *  - `draining` — finishing current work, not accepting new tasks.
 *  - `dead`     — `detectDead` flipped it after its heartbeat went stale.
 *  - `retired`  — `deregister` retired it on a graceful stop. The row is
 *                 kept (for forensics / the dashboard) until `gc` reaps it.
 */
export type WorkerStatus = "active" | "draining" | "dead" | "retired";

export interface WorkerInfo {
  readonly workerId: string;
  readonly capabilities: readonly string[];
  readonly concurrency: number;
  readonly status: WorkerStatus;
  readonly lastHeartbeat: Date;
  readonly startedAt: Date;
  /**
   * When `deregister` retired the worker. Set only while `status` is
   * `retired`; absent for active / draining / dead rows.
   */
  readonly retiredAt?: Date;
  readonly metadata?: Record<string, unknown>;
}

export interface WorkerRegistry {
  /** Register a worker as active. */
  register(params: {
    workerId: string;
    capabilities: readonly string[];
    concurrency: number;
    metadata?: Record<string, unknown>;
  }): Promise<void>;

  /** Update heartbeat timestamp for a worker. */
  heartbeat(workerId: string): Promise<void>;

  /** Mark worker as draining (finishing current work, not accepting new tasks). */
  drain(workerId: string): Promise<void>;

  /**
   * Retire a worker on a graceful stop — sets `status: 'retired'` and a
   * `retiredAt` timestamp but KEEPS the row, so the dashboard and run
   * forensics can still resolve it. Reaped later by `gc`. No-ops on an
   * unknown worker.
   */
  deregister(workerId: string): Promise<void>;

  /** List all workers, optionally filtered to a single status. */
  list(params?: { status?: WorkerStatus }): Promise<WorkerInfo[]>;

  /**
   * Detect workers whose heartbeat is older than `timeoutMs`, flip them
   * to `dead`, and return them. Skips workers already `dead` or `retired`
   * — a retired worker stopped heartbeating on purpose and must not be
   * mislabelled as a crash.
   */
  detectDead(timeoutMs: number): Promise<WorkerInfo[]>;

  /**
   * Reap long-gone rows: delete every worker whose `retiredAt` (or, when
   * it never retired, `lastHeartbeat`) is older than `retainMs`. Returns
   * the number of rows deleted. Runs from the coordinator's sweep loop so
   * retired / dead rows stay visible for a retention window, then go.
   */
  gc(params: { retainMs: number }): Promise<number>;
}

export class InMemoryWorkerRegistry implements WorkerRegistry {
  private workers = new Map<string, WorkerInfo>();

  async register(params: {
    workerId: string;
    capabilities: readonly string[];
    concurrency: number;
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    const now = new Date();
    this.workers.set(params.workerId, {
      workerId: params.workerId,
      capabilities: params.capabilities,
      concurrency: params.concurrency,
      status: "active",
      lastHeartbeat: now,
      startedAt: now,
      metadata: params.metadata,
    });
  }

  async heartbeat(workerId: string): Promise<void> {
    const w = this.workers.get(workerId);
    if (w) {
      this.workers.set(workerId, { ...w, lastHeartbeat: new Date() });
    }
  }

  async drain(workerId: string): Promise<void> {
    const w = this.workers.get(workerId);
    if (w) {
      this.workers.set(workerId, { ...w, status: "draining" });
    }
  }

  async deregister(workerId: string): Promise<void> {
    // Retire, don't delete — the row stays for forensics until `gc`.
    const w = this.workers.get(workerId);
    if (w) {
      this.workers.set(workerId, { ...w, status: "retired", retiredAt: new Date() });
    }
  }

  async list(params?: { status?: WorkerStatus }): Promise<WorkerInfo[]> {
    const all = [...this.workers.values()];
    if (params?.status) return all.filter((w) => w.status === params.status);
    return all;
  }

  async detectDead(timeoutMs: number): Promise<WorkerInfo[]> {
    const cutoff = Date.now() - timeoutMs;
    const dead: WorkerInfo[] = [];

    for (const [id, w] of this.workers) {
      // A retired worker stopped on purpose — never relabel it `dead`.
      if (w.status === "dead" || w.status === "retired") continue;
      if (w.lastHeartbeat.getTime() < cutoff) {
        const updated: WorkerInfo = { ...w, status: "dead" };
        this.workers.set(id, updated);
        dead.push(updated);
      }
    }

    return dead;
  }

  async gc(params: { retainMs: number }): Promise<number> {
    const cutoff = Date.now() - params.retainMs;
    let reaped = 0;
    for (const [id, w] of this.workers) {
      // Reap on the most-recent activity: when the worker retired, that;
      // otherwise its last heartbeat.
      const lastActive = (w.retiredAt ?? w.lastHeartbeat).getTime();
      if (lastActive < cutoff) {
        this.workers.delete(id);
        reaped += 1;
      }
    }
    return reaped;
  }
}
