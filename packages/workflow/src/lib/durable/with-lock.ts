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
//      With `loadState`, `tryLockAndLoad()` takes the lock and reads the
//      workflow's state in one call.
//   2. setInterval — background heartbeat extends the lock every
//      `heartbeatIntervalMs`, carrying the fence token so only the current
//      holder can extend.
//   3. fn(ctx) — execute the workflow steps, passing `ctx.fenceToken`
//      down into every storage mutation and watching `ctx.signal`.
//   4. finally — clear heartbeat timer, release the lock (fenced).
//
// Losing the lock:
//
//   - A heartbeat rejected with `FenceTokenMismatchError` means another
//     holder has the lock: `ctx.signal` aborts at once.
//   - Other heartbeat failures (storage briefly unavailable) are tolerated
//     until no heartbeat has succeeded for a whole `lockDurationMs`; by
//     then the lock has expired and may be taken, so `ctx.signal` aborts.
//
//   The abort reason is a `WorkflowLockLostError`. `fn` decides where to
//   stop (the runner checks between waves); `withLock` never interrupts it.
//
// Releasing: a `releaseLock` failure never replaces `fn`'s outcome — its
// result or its error (including `WorkflowSuspendedError`) is what the
// caller sees. The lock then expires on its own.
//
// Advisory locks (Postgres): the heartbeat is a no-op (see storage impl).
// ---------------------------------------------------------------------------

import type { FenceToken, WorkflowStorage } from "./workflow-storage.ts";
import type { WorkflowState } from "./workflow-state.ts";
import { WorkflowLockError, WorkflowLockLostError } from "./durable-pipeline-error.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

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
  /** How often to heartbeat (ms). Default: 30_000 */
  heartbeatIntervalMs?: number;
  /** How long to extend the lock on each heartbeat (ms). Default: 120_000 */
  lockDurationMs?: number;
  /**
   * Time source. Drives the heartbeat interval via `clock.setInterval`.
   * Default: real system clock. Tests pass a `FakeWallClock` so advancing
   * time fires heartbeats deterministically without real waits.
   */
  clock?: WallClock;
  /**
   * Also load the workflow's state as the lock is taken, through
   * `storage.tryLockAndLoad` (one round trip on backends that implement
   * it), and hand it to the callback as `LockContext.state`. Falls back to
   * `tryLock` then `loadWorkflow` on a storage without `tryLockAndLoad`.
   * Default: false.
   */
  loadState?: boolean;
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
  /**
   * Aborted, with a `WorkflowLockLostError` reason, once the lock is known
   * or presumed lost. Check it at safe points and stop there.
   */
  readonly signal: AbortSignal;
  /**
   * The workflow's state as of taking the lock (`null` when it has no
   * record yet). Set only with `WithLockOptions.loadState`.
   */
  readonly state?: WorkflowState | null;
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
 *   fn: async ({ fenceToken, signal }) => {
 *     // storage mutations carry { fenceToken } so a stale holder that
 *     // wakes up after the lock expired is rejected by the backend.
 *     return await executeSteps({ fenceToken, signal });
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
  const clock = params.options?.clock ?? SystemWallClock;

  const { acquired, token, state } = await acquireLock({
    storage,
    workflowId,
    lockDurationMs,
    loadState: params.options?.loadState === true,
  });
  if (!acquired) {
    throw new WorkflowLockError({
      workflowId,
      message: `Could not acquire lock on workflow "${workflowId}" — already running`,
    });
  }

  const guard = token ? { fenceToken: token } : undefined;
  const lost = new AbortController();
  let lastExtendedMs = clock.currentTimeMs();
  let released = false;

  const heartbeatHandle = clock.setInterval(() => {
    storage.heartbeat(workflowId, lockDurationMs, guard).then(
      () => {
        lastExtendedMs = clock.currentTimeMs();
      },
      (error: unknown) => {
        if (released || lost.signal.aborted) return;
        const tag = (error as { _tag?: unknown } | null | undefined)?._tag;
        if (tag === "FenceTokenMismatchError") {
          lost.abort(
            new WorkflowLockLostError({
              workflowId,
              message: `Lost the lock on workflow "${workflowId}": it is held under another fence token`,
              cause: error,
            }),
          );
          return;
        }
        if (clock.currentTimeMs() - lastExtendedMs >= lockDurationMs) {
          lost.abort(
            new WorkflowLockLostError({
              workflowId,
              message:
                `Lost the lock on workflow "${workflowId}": no heartbeat succeeded ` +
                `for ${lockDurationMs}ms`,
              cause: error,
            }),
          );
        }
      },
    );
  }, heartbeatMs);

  try {
    return await fn({
      fenceToken: token,
      signal: lost.signal,
      ...(state !== undefined && { state }),
    });
  } finally {
    released = true;
    heartbeatHandle.clear();
    try {
      await storage.releaseLock(workflowId, guard);
    } catch {
      // Never let a failed release replace fn's result or error; the lock
      // expires on its own `lockDurationMs` after the last heartbeat.
    }
  }
}

/**
 * Take the lock, with the state when `loadState` is set and the storage
 * has `tryLockAndLoad`. Otherwise `state` is left out and the callback
 * loads it under the lock itself. Not `async`: the plain `tryLock` path
 * hands back the storage's own promise, adding no extra ticks.
 */
function acquireLock(params: {
  storage: WorkflowStorage;
  workflowId: string;
  lockDurationMs: number;
  loadState: boolean;
}): Promise<{ acquired: boolean; token?: FenceToken; state?: WorkflowState | null }> {
  const { storage, workflowId, lockDurationMs } = params;
  if (params.loadState && typeof storage.tryLockAndLoad === "function") {
    return storage
      .tryLockAndLoad(workflowId, lockDurationMs)
      .then(({ locked, token, state }) =>
        locked ? { acquired: true, token, state } : { acquired: false },
      );
  }
  return storage.tryLock(workflowId, lockDurationMs);
}
