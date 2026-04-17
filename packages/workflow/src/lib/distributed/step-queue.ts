// ---------------------------------------------------------------------------
// StepQueue — interface for dispatching and claiming step tasks
//
// The coordinator enqueues steps; workers dequeue and execute them.
// Postgres implementation uses SKIP LOCKED for exactly-once delivery.
// ---------------------------------------------------------------------------

export interface StepTask {
  readonly id: string;
  readonly workflowId: string;
  readonly stepName: string;
  readonly queue: string;
  readonly priority: number;
  readonly input: unknown;
  readonly prevResults: Record<string, unknown>;
  readonly attempt: number;
  readonly status: "pending" | "running" | "completed" | "failed";
  readonly createdAt: Date;
  /**
   * Workflow version that enqueued this task, if any. Enables rolling deploys
   * where v1 and v2 workflows share a queue but workers filter by the versions
   * they support. Undefined for unversioned workflows (backward compatible).
   */
  readonly version?: string;
}

/**
 * Fairness policy for task dequeue ordering.
 * - `strict-priority` — always dequeue highest priority first, FIFO within same priority (default)
 * - `round-robin` — cycle across workflows/namespaces, prevents one workflow from starving others
 * - `weighted` — dequeue proportionally to priority (priority 10 gets ~2x tasks vs priority 5)
 */
export type FairnessPolicy = "strict-priority" | "round-robin" | "weighted";

export interface StepQueue {
  /** Enqueue a step for execution on a named queue. Higher priority number = runs first. */
  enqueue(params: {
    workflowId: string;
    stepName: string;
    queue: string;
    input: unknown;
    prevResults: Record<string, unknown>;
    /** Priority — higher number runs first. Default: 5. Range: 0-10. */
    priority?: number;
    /** Namespace for task isolation. Falls back to queue-level default. */
    namespace?: string;
    /**
     * Workflow version this step belongs to. Stored on the task so workers
     * can filter by supported versions during rolling deploys.
     */
    version?: string;
  }): Promise<string>;

  /** Claim up to `limit` pending tasks from the given queues (SKIP LOCKED). */
  claim(params: {
    queues: string[];
    limit: number;
    /** Fairness policy for dequeue ordering. Default: strict-priority. */
    fairness?: FairnessPolicy;
    /**
     * Optional predicate — tasks where `filter(task)` returns false are left
     * in the queue for other workers. Used by workers that only support a
     * subset of step names or workflow versions. Evaluated AFTER SKIP LOCKED
     * selects the task: implementations should release the lock on rejected
     * tasks so other workers can claim them. Default: accept all.
     */
    filter?: (task: StepTask) => boolean;
  }): Promise<StepTask[]>;

  /** Mark a task as completed with a result. */
  complete(params: { taskId: string; result: unknown; durationMs: number }): Promise<void>;

  /** Mark a task as failed with an error. */
  fail(params: { taskId: string; error: string; durationMs: number }): Promise<void>;

  /**
   * Re-enqueue tasks stuck in "running" state. Returns count re-enqueued.
   *
   * Two modes:
   * - `claimedBy` — requeue all tasks claimed by a specific (dead) worker
   * - `staleTimeoutMs` — requeue any task in 'running' longer than this timeout,
   *    regardless of which worker claimed it (catches orphaned tasks from
   *    workers that crashed before heartbeating)
   */
  requeueStuck(params: { claimedBy?: string; staleTimeoutMs?: number }): Promise<number>;

  /** Get pending task count per queue. */
  metrics(): Promise<
    Record<string, { pending: number; running: number; completed: number; failed: number }>
  >;
}
