// ---------------------------------------------------------------------------
// withLock — workflow lock acquisition with automatic heartbeat
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
//   1. tryLock() — acquire the lock (or throw WorkflowLockError)
//   2. setInterval — background heartbeat extends the lock every 10s
//   3. fn() — execute the workflow steps
//   4. finally — clear heartbeat timer, release the lock
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

import type { WorkflowStorage } from "./workflow-storage.ts";
import { WorkflowLockError } from "./durable-pipeline-error.ts";

/** Heartbeat every 10s by default */
const DEFAULT_HEARTBEAT_INTERVAL_MS = 10_000;

/** Each heartbeat extends the lock by 30s (3x the interval = safe margin) */
const DEFAULT_LOCK_EXTENSION_MS = 30_000;

export interface WithLockOptions {
  /** How often to heartbeat (ms). Default: 10_000 */
  heartbeatIntervalMs?: number;
  /** How long to extend the lock on each heartbeat (ms). Default: 30_000 */
  lockDurationMs?: number;
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
 *   fn: async () => {
 *     // steps execute here — lock stays alive via heartbeat
 *     return await executeSteps();
 *   },
 * });
 * ```
 */
export async function withLock<T>(params: {
  storage: WorkflowStorage;
  workflowId: string;
  fn: () => Promise<T>;
  options?: WithLockOptions;
}): Promise<T> {
  const { storage, workflowId, fn } = params;
  const heartbeatMs = params.options?.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
  const lockDurationMs = params.options?.lockDurationMs ?? DEFAULT_LOCK_EXTENSION_MS;

  const locked = await storage.tryLock(workflowId, lockDurationMs);
  if (!locked) {
    throw new WorkflowLockError({
      workflowId,
      message: `Could not acquire lock on workflow "${workflowId}" — already running`,
    });
  }

  const heartbeatTimer = setInterval(async () => {
    try {
      await storage.heartbeat(workflowId, lockDurationMs);
    } catch {
      // Heartbeat failure is swallowed — lock expires naturally
    }
  }, heartbeatMs);

  try {
    return await fn();
  } finally {
    clearInterval(heartbeatTimer);
    await storage.releaseLock(workflowId);
  }
}
