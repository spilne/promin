// ---------------------------------------------------------------------------
// Fencing — the lock token every write of a run's lock holder carries.
// ---------------------------------------------------------------------------

/**
 * Opaque monotonic token handed back by `tryLock` / `tryLockAndLoad` and
 * passed to every subsequent mutating call for the same workflow. Backends
 * validate that the token matches the current lock holder before accepting
 * the write. Protects against the classic split-brain window: worker A's
 * lock expires while it's mid-step, worker B acquires a fresh lock, then
 * worker A wakes up and tries to commit stale state.
 *
 * A string (rather than number) so backends can choose their own monotonic
 * source — Postgres bigserial ("42"), a UUID+counter composite, a redis
 * INCR result, etc. Clients treat the value as opaque.
 */
export type FenceToken = string;

/**
 * Fencing argument of every write a run's lock holder makes. Each fenced
 * method takes one params object, and the fence travels in its `guard`
 * field (see `FencedWrite`).
 *
 * **Fenced write.** With `fenceToken` set, the backend accepts the write
 * only while the token is the workflow's current lock token and that lock
 * has not expired. The check runs atomically with the write: in the same
 * SQL transaction that holds the lock row, in the same Lua script, or in
 * the same synchronous step. A rejected write throws
 * `FenceTokenMismatchError` and changes nothing, so the lock cannot move
 * between the check and the write.
 *
 * **Expired locks.** A lock past its expiry fences nothing, whether or not
 * another worker has taken it over yet. Every fenced write carrying its
 * token is rejected, `heartbeat` included, so a holder that stalled past
 * its lease learns it lost the run at its next write. A `releaseLock` with
 * a stale token never frees a newer holder's lock.
 *
 * **Unfenced write.** Without a token (or on a backend that issues none)
 * the write is accepted as before. Operator actions, external signal
 * delivery and the scanners write unfenced.
 *
 * **Fenced methods.** `saveStepResult`, `batchSaveStepResults`,
 * `saveStepFailure`, `saveTaskResult`, `saveTaskFailure`,
 * `completeWorkflow`, `failWorkflow`, `tripwireWorkflow`, `cancelWorkflow`,
 * `suspendWorkflow`, `setWorkflowMetadata`, `startFreshRun`,
 * `appendStreamChunk`, `createWorkflow` (fenced on the parent's lock),
 * `heartbeat`, `releaseLock` (a stale token releases nothing),
 * `saveStepAttempt`, `checkpointStep`, `beginCompensation`,
 * `saveStepCompensation`, and the journal writes `appendEntry`,
 * `appendPendingEntry`, `completePendingEntry` and `discardJournalEntries`.
 */
export interface FenceGuard {
  readonly fenceToken?: FenceToken;
}

/**
 * The fence field of a fenced write's params object. Every fenced method's
 * params type extends it; pass the lock holder's `{ fenceToken }` as
 * `guard`, or leave it out for an unfenced write.
 */
export interface FencedWrite {
  readonly guard?: FenceGuard;
}
