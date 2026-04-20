// ---------------------------------------------------------------------------
// withLock — workflow lock acquisition with automatic heartbeat + fencing
// ---------------------------------------------------------------------------
//
// Why this exists:
//
// Without heartbeat, start() with idempotency cannot distinguish a crashed
// workflow from a slow one. Both show status=running with an expired lock.
// The heartbeat is the liveness signal: if the lock is expired, the process
// stopped heartbeating, so it's safe for another caller to re-execute.
//
// How it works:
//
//   1. tryLock() — acquire the lock (or throw WorkflowLockError). The
//      returned fence token is handed to `fn` via `ctx.fenceToken` and
//      threaded to every subsequent mutating call, so a stale holder
//      that wakes up after its lock expired can't corrupt fresh state.
//   2. setInterval — background heartbeat extends the lock every 10s,
//      carrying the fence token so only the current holder can extend.
//   3. fn(ctx) — execute the workflow steps, passing `ctx.fenceToken`
//      down into every storage mutation.
//   4. finally — clear heartbeat timer, release the lock (fenced).
//
// The heartbeat runs independently of the main execution. If a heartbeat
// call fails (e.g. storage is temporarily unavailable), it is silently
// swallowed — the lock will expire naturally at its last-extended time,
// which is the correct behavior (it means we can't prove liveness).
//
// Edge cases:
//   - Heartbeat failure: swallowed, lock expires naturally
//   - fn() throws: lock is released in finally block
//   - Suspended workflow: throw propagates up, lock released
//   - Advisory locks (Postgres): heartbeat is a no-op (see storage impl)
// ---------------------------------------------------------------------------

import type { FenceToken, WorkflowStorage } from "./workflow-storage.ts";
import { WorkflowLockError } from "./durable-pipeline-error.ts";

/**
 * Heartbeat every 30s by default. The previous 10s cadence amplified into
 * N heartbeats per step for remote WorkflowStorage; coarser keeps locks
 * fresh without flooding the wire on long-running workflows.
 */
const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;

/**
 * Each heartbeat extends the lock by 120s (4x the interval). The difference
 * between `lockDurationMs` and `heartbeatIntervalMs` is the implicit grace
 * period: if a heartbeat is delayed by a transient network hiccup, the lock
 * still has ~90s of runway before another worker can steal it. Tune both
 * via `WithLockOptions` when a workflow has different latency/contention
 * characteristics.
 */
const DEFAULT_LOCK_EXTENSION_MS = 120_000;

export interface WithLockOptions {
  /** How often to heartbeat (ms). Default: 10_000 */
  heartbeatIntervalMs?: number;
  /** How long to extend the lock on each heartbeat (ms). Default: 30_000 */
  lockDurationMs?: number;
}

/** Context passed to `withLock`'s callback. */
export interface LockContext {
  /**
   * Fence token for the lock this callback holds, if the backend supports
   * fencing. Thread into every mutating call (`storage.saveStepResult(..., { fenceToken })`)
   * so a stale holder that wakes up after its lock expired is rejected.
   * `undefined` when the backend doesn't issue tokens.
   */
  readonly fenceToken?: FenceToken;
}

/**
 * Acquire a lock, run `fn` with a background heartbeat, then release.
 *
 * The heartbeat extends the lock every `heartbeatIntervalMs` so that
 * long-running steps don't lose their lock. On completion (success or
 * error) the heartbeat is stopped and the lock is released.
 *
 * @throws WorkflowLockError if the lock cannot be acquired
 *
 * @example
 * ```ts
 * const result = await withLock({
 *   storage,
 *   workflowId: "order-123",
 *   fn: async ({ fenceToken }) => {
 *     // storage mutations carry { fenceToken } so a stale holder that
 *     // wakes up after the lock expired is rejected by the backend.
 *     return await executeSteps(fenceToken);
 *   },
 * });
 * ```
 */
export async function withLock<T>(params: {
  storage: WorkflowStorage;
  workflowId: string;
  fn: (ctx: LockContext) => Promise<T>;
  options?: WithLockOptions;
}): Promise<T> {
  const { storage, workflowId, fn } = params;
  const heartbeatMs = params.options?.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
  const lockDurationMs = params.options?.lockDurationMs ?? DEFAULT_LOCK_EXTENSION_MS;

  const { acquired, token } = await storage.tryLock(workflowId, lockDurationMs);
  if (!acquired) {
    throw new WorkflowLockError({
      workflowId,
      message: `Could not acquire lock on workflow "${workflowId}" — already running`,
    });
  }

  const guard = token ? { fenceToken: token } : undefined;

  const heartbeatTimer = setInterval(async () => {
    try {
      await storage.heartbeat(workflowId, lockDurationMs, guard);
    } catch {
      // Heartbeat failure is swallowed — lock expires naturally
    }
  }, heartbeatMs);

  try {
    return await fn({ fenceToken: token });
  } finally {
    clearInterval(heartbeatTimer);
    await storage.releaseLock(workflowId, guard);
  }
}
