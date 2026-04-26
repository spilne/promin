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
  /**
   * Search-attribute payload set at enqueue time. See `enqueue` for the
   * convention. Round-trips unchanged; never read by the platform.
   */
  readonly metadata?: Record<string, unknown>;
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
    /**
     * Arbitrary search-attribute payload — mirrors `wf_workflows.metadata`.
     * The platform never reads keys here for control flow; it's a generic
     * place for callers (agents, scheduling, custom workloads) to attach
     * scope / labels / tags / experiment IDs that downstream observability
     * + queries can filter by. Conventions (documented, not enforced):
     *   `metadata.userId`     — end-user actor (agent dispatch sets this)
     *   `metadata.subject`    — generic actor when not a user
     *   `metadata.tags`       — string[] for free-form labeling
     *   `metadata.experiment` — A/B / rollout flag
     * Round-trips through `claim` unchanged.
     */
    metadata?: Record<string, unknown>;
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
   * Extend the running lease on a task. Workers call this periodically while
   * executing a long step so `requeueStuck` doesn't reclaim it prematurely.
   * No-op if the task is not in `running` state.
   */
  heartbeat(params: { taskId: string }): Promise<void>;

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

  /**
   * Queue metrics over a time window, with wait / exec latency stats.
   *
   * **Why the window is mandatory.** A running queue accumulates terminal
   * rows forever (until retention kicks in). `metrics()` without a window
   * meant scanning the entire table and returning a useless historical
   * total. Every count is now bounded:
   *
   * - `pending` — tasks in `status='pending'` whose `createdAt ∈ [since, until]`.
   *   Old createdAt + still pending = stuck; the window catches that.
   * - `running` — tasks in `status='running'` whose `claimedAt ∈ [since, until]`.
   *   Old claimedAt + still running = dead worker; window catches that too.
   * - `completed` / `failed` — terminal tasks whose `completedAt ∈ [since, until]`.
   *   Rate/throughput over the window, not lifetime.
   *
   * **Latency stats** — computed over terminal tasks (`completed` + `failed`)
   * in the window:
   *
   * - `avgWaitMs` — mean `claimedAt - createdAt` (queue time).
   * - `avgExecMs` — mean `completedAt - claimedAt` (actual step body time).
   * - `p95ExecMs` — 95th-percentile exec time.
   *
   * All latency fields return `0` when no terminal tasks exist in the
   * window — avoids threading nullable numbers through dashboards.
   */
  metrics(params: {
    /** Inclusive lower bound of the window. Required — no unbounded scans. */
    since: Date;
    /** Inclusive upper bound. Default: now. */
    until?: Date;
  }): Promise<{
    pending: number;
    running: number;
    completed: number;
    failed: number;
    avgWaitMs: number;
    avgExecMs: number;
    p95ExecMs: number;
  }>;
}
