// ---------------------------------------------------------------------------
// WorkflowLockStore — the per-run lock (lease) and its fence token.
// ---------------------------------------------------------------------------

import type { WorkflowState } from "../workflow-state.ts";
import type { FencedWrite, FenceToken } from "./fencing.ts";

/** Params of `tryLock` and `tryLockAndLoad`. */
export interface TryLockParams {
  readonly workflowId: string;
  /** Lease length; the lock expires this long after the acquire. */
  readonly lockDurationMs: number;
}

/** Answer of `tryLock`. */
export interface TryLockResult {
  readonly acquired: boolean;
  /** Fence token of the new lease; absent on backends without fencing. */
  readonly token?: FenceToken;
}

/** Answer of `tryLockAndLoad`. */
export interface TryLockAndLoadResult {
  readonly locked: boolean;
  readonly token?: FenceToken;
  readonly state: WorkflowState | null;
}

/** Params of `releaseLock`. */
export interface ReleaseLockParams extends FencedWrite {
  readonly workflowId: string;
}

/** Params of `heartbeat`. */
export interface HeartbeatParams extends FencedWrite {
  readonly workflowId: string;
  /** New lease length, counted from now. */
  readonly lockDurationMs: number;
}

/**
 * The run lock. A lock is a lease: it expires `lockDurationMs` after the
 * acquire (or the last heartbeat), and the fence token it hands out is what
 * every write of the holder carries in its `guard` (see `FenceGuard`).
 */
export interface WorkflowLockStore {
  /**
   * Acquire a lock on a workflow. On success returns `{ acquired: true,
   * token }` — hand the token to every subsequent mutating call so the
   * backend can reject stale writes after the lock expires + someone else
   * picks it up. Backends that don't support fencing omit the `token`
   * (callers treat that as "no fencing", same as passing no token).
   *
   * The lock is exclusive and not re-entrant: while it is held and
   * unexpired, every other `tryLock` for the same workflow fails — from
   * this storage instance or any other instance on the same backend.
   */
  tryLock(params: TryLockParams): Promise<TryLockResult>;

  /**
   * Acquire the lock AND load the current workflow state in one round trip.
   *
   * Exists for the HTTP / remote-storage path, where the typical
   * `tryLock` → check → `loadWorkflow` sequence is two network calls per
   * step invocation. Backends SHOULD do this atomically (Postgres: one
   * transaction; Redis: Lua script) so the load reflects the state as-of
   * the moment the lock was acquired — no window where another actor
   * commits writes between the two observations.
   *
   * Return contract:
   * - `locked: true`  — caller holds the lock; `state` is the current
   *   state or null if the workflow record doesn't exist yet; `token` is
   *   the fence token to pass to subsequent writes (omitted on backends
   *   without fencing support).
   * - `locked: false` — someone else holds the lock; `state` is still
   *   returned for diagnostic use (idempotency joins, "already running"
   *   branches), or null if absent; `token` is always absent.
   *
   * A storage that can't do an atomic read-lock can delegate to
   * `tryLockAndLoadDefault` from `@promin/workflow/storage-kit`.
   */
  tryLockAndLoad(params: TryLockParams): Promise<TryLockAndLoadResult>;

  /**
   * Release a workflow lock. When fencing is in play, only the token
   * holder releases — a stale holder whose lock already expired silently
   * no-ops. Callers usually pass the token they got from `tryLock`.
   */
  releaseLock(params: ReleaseLockParams): Promise<void>;

  /**
   * Heartbeat to extend a lock (for long-running steps). With a fence
   * token in `guard`, only the token holder extends a live lock: when the
   * lock is gone, expired or held under another token, the call rejects
   * with `FenceTokenMismatchError`, which tells the holder it lost the run.
   * Without a token, a lock not held by this instance is left alone.
   */
  heartbeat(params: HeartbeatParams): Promise<void>;
}
