// ---------------------------------------------------------------------------
// WorkerRegistry — tracks active workers, heartbeats, dead detection
// ---------------------------------------------------------------------------

export interface WorkerInfo {
  readonly workerId: string;
  readonly queues: string[];
  readonly concurrency: number;
  readonly status: "active" | "draining" | "dead";
  readonly lastHeartbeat: Date;
  readonly startedAt: Date;
  readonly metadata?: Record<string, unknown>;
}

export interface WorkerRegistry {
  /** Register a worker as active. */
  register(params: {
    workerId: string;
    queues: string[];
    concurrency: number;
    metadata?: Record<string, unknown>;
  }): Promise<void>;

  /** Update heartbeat timestamp for a worker. */
  heartbeat(workerId: string): Promise<void>;

  /** Mark worker as draining (finishing current work, not accepting new tasks). */
  drain(workerId: string): Promise<void>;

  /** Remove a worker from the registry. */
  deregister(workerId: string): Promise<void>;

  /** List all workers with a given status. Default: active. */
  list(params?: { status?: "active" | "draining" | "dead" }): Promise<WorkerInfo[]>;

  /**
   * Detect workers whose heartbeat is older than timeoutMs.
   * Marks them as dead and returns them.
   */
  detectDead(timeoutMs: number): Promise<WorkerInfo[]>;
}

export class InMemoryWorkerRegistry implements WorkerRegistry {
  private workers = new Map<string, WorkerInfo & { status: "active" | "draining" | "dead" }>();

  async register(params: {
    workerId: string;
    queues: string[];
    concurrency: number;
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    const now = new Date();
    this.workers.set(params.workerId, {
      workerId: params.workerId,
      queues: params.queues,
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
    this.workers.delete(workerId);
  }

  async list(params?: { status?: "active" | "draining" | "dead" }): Promise<WorkerInfo[]> {
    const all = [...this.workers.values()];
    if (params?.status) return all.filter((w) => w.status === params.status);
    return all;
  }

  async detectDead(timeoutMs: number): Promise<WorkerInfo[]> {
    const cutoff = Date.now() - timeoutMs;
    const dead: WorkerInfo[] = [];

    for (const [id, w] of this.workers) {
      if (w.status !== "dead" && w.lastHeartbeat.getTime() < cutoff) {
        const updated = { ...w, status: "dead" as const };
        this.workers.set(id, updated);
        dead.push(updated);
      }
    }

    return dead;
  }
}
