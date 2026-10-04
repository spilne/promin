// ---------------------------------------------------------------------------
// StepQueue — interface for dispatching and claiming step tasks
//
// The coordinator enqueues steps; workers claim and execute them. Delivery
// is at-least-once: a claimed task whose worker dies (or stops heartbeating)
// is put back by `requeueStuck` and claimed again, so step handlers must be
// idempotent. `claimToken` fences every write, so only the current claim
// can settle a task.
// ---------------------------------------------------------------------------

import type { LeaderLease } from "../scheduler/leader-lease.ts";

export type StepTaskStatus = "pending" | "running" | "completed" | "failed";

export interface StepTask {
  readonly id: string;
  readonly workflowId: string;
  readonly stepName: string;
  /**
   * Capabilities this task requires from a worker. A worker claims the task
   * only when `needs ⊆ worker.capabilities`. Empty means "unrestricted" —
   * any worker (even one with no declared capabilities) can claim it.
   */
  readonly needs: readonly string[];
  readonly priority: number;
  readonly input: unknown;
  readonly prevResults: Record<string, unknown>;
  /**
   * The runner's attempt number for this step (1 for the first run, 2 for
   * the first retry, …), as passed to `enqueue`. Redelivery of the same
   * task does not change it — see `deliveries` for that.
   */
  readonly attempt: number;
  /**
   * How many times this task has been handed to a worker and not given
   * back with `release()`. 1 on the first claim; each claim after a
   * `requeueStuck` adds one. Once it reaches the queue's `maxDeliveries`,
   * the next `requeueStuck` dead-letters the task instead of requeueing.
   */
  readonly deliveries: number;
  readonly status: StepTaskStatus;
  readonly createdAt: Date;
  /**
   * Opaque token minted by claim(). Workers pass it to heartbeat,
   * complete, fail and release so a stale worker cannot commit after the
   * task has been requeued and claimed by someone else.
   */
  readonly claimToken?: string;
  /**
   * Workflow version that enqueued this task, if any. Enables rolling deploys
   * where v1 and v2 workflows run concurrently but workers claim only the
   * versions they support. Undefined for unversioned workflows.
   */
  readonly version?: string;
  /**
   * Search-attribute payload set at enqueue time. See `enqueue` for the
   * convention. Round-trips unchanged; never read by the platform.
   */
  readonly metadata?: Record<string, unknown>;
  /**
   * Per-task concurrency cap. When set, `claim()` only claims this task
   * when fewer than `concurrencyLimit` tasks with the same
   * `(concurrencyScope, concurrencyKey)` are currently `running` — across
   * every concurrent claimer, whatever its capabilities.
   *
   * Scope conventions:
   *   `<workflowName>`              — workflow-level (caps any step).
   *   `<workflowName>::<stepName>`  — step-level (caps just one step).
   *
   * `concurrencyKey` is the user-evaluated string (e.g. `payload.tenantId`).
   * Null on any of the three disables enforcement for this task.
   */
  readonly concurrencyKey?: string;
  readonly concurrencyScope?: string;
  readonly concurrencyLimit?: number;
}

/**
 * A task as `StepQueue.get()` reports it: the claimed shape plus the claim
 * and outcome fields a worker never needs.
 */
export interface StepTaskRecord extends StepTask {
  /** Worker id passed to the claim that currently holds (or last held) the task. */
  readonly claimedBy?: string;
  readonly claimedAt?: Date;
  readonly heartbeatAt?: Date;
  readonly completedAt?: Date;
  readonly result?: unknown;
  /** Failure message, including the dead-letter reason for poisoned tasks. */
  readonly error?: string;
  readonly durationMs?: number;
}

/** Parameters for `StepQueue.enqueue`. */
export interface StepQueueEnqueueParams {
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
   * claim only the versions they support during rolling deploys.
   */
  version?: string;
  /** The runner's attempt number for this step. Default: 1. */
  attempt?: number;
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
  /**
   * Per-task concurrency cap — see `StepTask.concurrencyKey` for the
   * shape. Resolved by the coordinator from the workflow / step queue
   * config (step-level overrides workflow-level). Stored on the row
   * verbatim; `claim()` does the count check.
   */
  concurrencyKey?: string;
  concurrencyScope?: string;
  concurrencyLimit?: number;
}

/** Parameters for `StepQueue.claim`. */
export interface StepQueueClaimParams {
  /**
   * Identity of the claiming worker, recorded on every claimed task.
   * `requeueStuck({ mode: "worker", workerId })` reclaims exactly the tasks
   * claimed under this id, so pass the id the worker registers with.
   */
  workerId: string;
  /** Maximum number of tasks to claim. */
  limit: number;
  /**
   * What this worker can do. A task is claimable when
   * `task.needs ⊆ capabilities`. Empty / omitted = generalist that only
   * claims tasks with no `needs` declared.
   */
  capabilities?: readonly string[];
  /**
   * Step names this worker can run. Omitted = any step. The filter is
   * applied inside the backend's claim, so tasks for steps the worker
   * doesn't host are never claimed and never block the tasks behind them.
   */
  stepNames?: readonly string[];
  /**
   * Workflow versions this worker can run. Omitted = any version.
   * Unversioned tasks are always claimable.
   */
  versions?: readonly string[];
}

/**
 * Which running tasks `requeueStuck` puts back:
 * - `worker` — every task claimed under `workerId` (a worker known to be
 *   dead), whatever its last heartbeat.
 * - `stale` — every task whose last activity (heartbeat, else claim) is
 *   more than `olderThanMs` ago, whoever claimed it.
 */
export type StepQueueRequeueParams = (
  | { readonly mode: "worker"; readonly workerId: string }
  | { readonly mode: "stale"; readonly olderThanMs: number }
) & {
  /**
   * Fence the sweep with the caller's leader lease. When set, the sweep
   * changes nothing and rejects with `StaleLeaseError` unless the lease is
   * still the current lease of its key; the check runs in the same
   * transaction (or script) as the writes. Queues fence against the lease
   * store they were configured with (see each implementation's config); a
   * queue with no lease store configured ignores it.
   */
  readonly lease?: LeaderLease;
};

/** What one `requeueStuck` sweep did. */
export interface StepQueueRequeueResult {
  /** Tasks returned to `pending` for another delivery. */
  readonly requeued: number;
  /**
   * Tasks that had used up `maxDeliveries` and were marked `failed`
   * (dead-lettered) instead of being requeued.
   */
  readonly deadLettered: number;
}

/** Default `maxDeliveries` for the bundled queue implementations. */
export const DEFAULT_MAX_DELIVERIES = 10;

/** The error a dead-lettered task is failed with. */
export function deadLetterError(maxDeliveries: number): string {
  return `poisoned: exceeded ${maxDeliveries} deliveries`;
}

export interface StepQueue {
  /**
   * Enqueue a step for execution.
   *
   * **Idempotent on `(workflowId, stepName)`.** While a prior task for the
   * pair is still `pending` or `running`, re-calling `enqueue()` is a
   * no-op: it returns the existing task's id instead of creating a second
   * row. Workflow ids are globally unique, so the namespace is not part of
   * the key. Keeps things sane when multiple coordinators (or a
   * coordinator + an SDK client) both conclude the step is ready.
   *
   * Once the prior task reaches `completed` or `failed`, the next
   * `enqueue()` IS allowed to create a fresh pending task (needed for
   * step-level retry and `startFreshRun()`'s per-step re-execution).
   */
  enqueue(params: StepQueueEnqueueParams): Promise<string>;

  /**
   * Claim up to `limit` pending tasks the worker can run — matching
   * `capabilities`, `stepNames` and `versions` — in priority order (higher
   * first, FIFO within a priority). Each claimed task is `running`, carries
   * a fresh `claimToken`, records `workerId`, and has its `deliveries`
   * incremented. Tasks whose concurrency key is at its limit are skipped.
   */
  claim(params: StepQueueClaimParams): Promise<StepTask[]>;

  /**
   * Give a claimed task back without counting a delivery: it returns to
   * `pending` with its priority and position intact. For workers that
   * claimed something they won't run (a post-claim filter, a shutdown
   * before the task started). Returns false when the claim is no longer
   * current.
   */
  release(params: { taskId: string; claimToken: string }): Promise<boolean>;

  /** Read one task, or `undefined` when it doesn't exist (or was purged). */
  get(taskId: string): Promise<StepTaskRecord | undefined>;

  /**
   * Mark a task as completed with a result. Returns false when the task is no
   * longer held by this claim, letting workers skip stale workflow checkpoints.
   */
  complete(params: {
    taskId: string;
    claimToken?: string;
    result: unknown;
    durationMs: number;
  }): Promise<boolean>;

  /**
   * Mark a task as failed with an error. Returns false when the task is no
   * longer held by this claim.
   */
  fail(params: {
    taskId: string;
    claimToken?: string;
    error: string;
    durationMs: number;
  }): Promise<boolean>;

  /**
   * Extend the running lease on a task. Workers call this periodically while
   * executing a long step so `requeueStuck` doesn't reclaim it prematurely.
   * Returns false when the task is not running under this claim.
   */
  heartbeat(params: { taskId: string; claimToken?: string }): Promise<boolean>;

  /**
   * Put stuck `running` tasks back to `pending` (see
   * `StepQueueRequeueParams` for the two modes). A task that has already
   * been delivered `maxDeliveries` times is dead-lettered instead: marked
   * `failed` with `deadLetterError(maxDeliveries)`, so a task that crashes
   * every worker it lands on stops being redelivered.
   */
  requeueStuck(params: StepQueueRequeueParams): Promise<StepQueueRequeueResult>;

  /**
   * Delete terminal (`completed` / `failed`) tasks that finished before
   * `completedBefore`. Pending and running tasks are never touched. Returns
   * the number of tasks deleted.
   */
  purge(params: { completedBefore: Date }): Promise<number>;

  /**
   * Queue metrics over a time window, with wait / exec latency stats.
   *
   * **Why the window is mandatory.** A running queue accumulates terminal
   * rows until `purge()`. Every count is bounded:
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

/**
 * Linear-interpolation percentile (matches SQL `PERCENTILE_CONT`), shared by
 * the queue implementations that compute `metrics()` latency in process.
 * Sorts a copy. `values` must be non-empty.
 */
export function percentileCont(params: { values: readonly number[]; p: number }): number {
  const sorted = [...params.values].sort((a, b) => a - b);
  const rank = params.p * (sorted.length - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  if (lo === hi) return sorted[lo]!;
  return sorted[lo]! + (sorted[hi]! - sorted[lo]!) * (rank - lo);
}
