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
  /**
   * Capabilities this task requires from a worker. A worker claims the task
   * only when `needs ⊆ worker.capabilities`. Empty means "unrestricted" —
   * any worker (even one with no declared capabilities) can claim it.
   * Replaces the old `queue: string` routing primitive; capabilities model
   * heterogeneous clusters more faithfully (multi-value, composable,
   * declared at the step rather than mapped on the coordinator).
   */
  readonly needs: readonly string[];
  readonly priority: number;
  readonly input: unknown;
  readonly prevResults: Record<string, unknown>;
  readonly attempt: number;
  readonly status: "pending" | "running" | "completed" | "failed";
  readonly createdAt: Date;
  /**
   * Workflow version that enqueued this task, if any. Enables rolling deploys
   * where v1 and v2 workflows run concurrently but workers filter by the
   * versions they support. Undefined for unversioned workflows.
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
  /**
   * Enqueue a step for execution.
   *
   * **Idempotent on `(namespace, workflowId, stepName)`.** While a prior
   * task for the same triple is still `pending` or `running`, re-calling
   * `enqueue()` is a no-op: it returns the existing task's id instead of
   * creating a second row. Keeps things sane when multiple coordinators
   * (or a coordinator + an SDK client) both conclude the step is ready —
   * without it, a worker could claim two tasks and execute the step twice
   * (see promin-k6mk).
   *
   * Once the prior task reaches `completed` or `failed`, the next
   * `enqueue()` IS allowed to create a fresh pending task (needed for
   * step-level retry and `startFreshRun()`'s per-step re-execution).
   *
   * **Routing via `needs`**: each task declares the capabilities it
   * requires; workers claim tasks whose `needs` are a subset of their
   * declared `capabilities`. Replaces the old named-queue routing
   * primitive.
   */
  enqueue(params: {
    workflowId: string;
    stepName: string;
    input: unknown;
    prevResults: Record<string, unknown>;
    /**
     * Capabilities this task requires. Empty / omitted = any worker can
     * claim it.
     */
    needs?: readonly string[];
    /** Priority — higher number runs first. Default: 5. Range: 0-10. */
    priority?: number;
    /** Namespace for task isolation. Falls back to backend-level default. */
    namespace?: string;
    /**
     * Workflow version this step belongs to. Stored on the task so workers
     * can filter by supported versions during rolling deploys.
     */
    version?: string;
  }): Promise<string>;

  /**
   * Claim up to `limit` pending tasks the worker can handle. A task is
   * claimable when `task.needs ⊆ capabilities`. A worker with empty
   * capabilities can only claim unrestricted tasks (those with empty
   * `needs`). Uses SKIP LOCKED / equivalent atomic primitives per backend.
   */
  claim(params: {
    /**
     * What this worker can do. Empty = generalist that only claims tasks
     * with no `needs` declared.
     */
    capabilities?: readonly string[];
    limit: number;
    /** Fairness policy for dequeue ordering. Default: strict-priority. */
    fairness?: FairnessPolicy;
    /**
     * Optional predicate — tasks where `filter(task)` returns false are left
     * in the queue for other workers. Used by workers that only support a
     * subset of step names or workflow versions. Evaluated AFTER SKIP LOCKED
     * selects the task: implementations release the lock on rejected tasks
     * so other workers can claim them. Default: accept all.
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

  /** Current task counts by status. Flat — no per-queue breakdown now that queues are gone. */
  metrics(): Promise<{
    pending: number;
    running: number;
    completed: number;
    failed: number;
  }>;
}
