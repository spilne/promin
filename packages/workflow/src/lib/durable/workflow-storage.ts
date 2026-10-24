// ---------------------------------------------------------------------------
// WorkflowStorage — pluggable persistence interface
// ---------------------------------------------------------------------------

import type {
  WorkflowState,
  WorkflowSummary,
  WorkflowStatus,
  WorkflowRunSummary,
  WorkflowRunEvent,
  WorkflowStatusSnapshot,
  SignalState,
  StepAttemptRecord,
} from "./workflow-state.ts";

/**
 * Opaque monotonic token handed back by `tryLock` / `tryLockAndLoad` and
 * passed to every subsequent mutating call for the same workflow. Backends
 * validate that the token matches the current lock holder (or is higher
 * than the last-observed one, depending on monotonicity) before accepting
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
 * Fencing argument of every write a run's lock holder makes. It is the
 * trailing `guard` parameter of each fenced method.
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
 * `heartbeat`, `StepAttemptStorage.saveStepAttempt`, and the journal writes
 * `appendEntry`, `appendPendingEntry`, `completePendingEntry` and
 * `discardJournalEntries`.
 */
export interface FenceGuard {
  readonly fenceToken?: FenceToken;
}

/**
 * Sortable columns on `WorkflowStorage.listWorkflows`. `duration` is
 * computed as `completedAt - createdAt` and sorts NULL-last for runs that
 * haven't finished yet.
 */
export type WorkflowOrderBy =
  | "createdAt"
  | "startedAt"
  | "completedAt"
  | "duration"
  | "status"
  | "name";

/**
 * A single row from the public-bearer signal-token table. Issued via
 * `createSignalToken`, consumed via the public completion endpoint, and
 * surfaced in the dashboard via `listSignalTokensForWorkflow`.
 *
 * `bearer` is the plaintext credential — short-lived, single-use, bounded
 * by `expiresAt`. Compared with constant-time equality at completion time.
 */
/**
 * One chunk of a workflow stream. Returned by `readStreamChunks`. The
 * `payload` is whatever the appender wrote — the type comes from the
 * caller's `defineStream<T>` declaration; storage stores it as JSONB.
 */
export interface StreamChunk {
  readonly chunkIndex: number;
  readonly payload: unknown;
  readonly appendedBy: "workflow" | "external";
  readonly appendedAt: Date;
}

export interface SignalTokenRecord {
  readonly tokenId: string;
  readonly workflowId: string;
  readonly signalName: string;
  readonly bearer: string;
  readonly tags: ReadonlyArray<string>;
  readonly idempotencyKey: string | null;
  readonly expiresAt: Date;
  readonly completedAt: Date | null;
  readonly completedValue: unknown;
  readonly createdAt: Date;
}

/**
 * A suspended run that is due to be resumed, returned by the scanner
 * queries (`listDueTimers`, `listSignalWakeups`). One row per run.
 */
export interface WorkflowWakeup {
  readonly workflowId: string;
  readonly workflowName: string;
  /** Definition version the run was created with. */
  readonly version?: string;
  readonly input: unknown;
  /** The suspended step that is due. */
  readonly stepName: string;
  /**
   * Why the run is due:
   * - `sleep` — a sleeping step's `wakeAt` has passed.
   * - `signal-timeout` — a signal wait's `signalTimeoutAt` has passed.
   * - `signal` — the signal a waiting step waits on has been delivered.
   */
  readonly reason: "sleep" | "signal-timeout" | "signal";
  /** `signal` / `signal-timeout`: the awaited signal name. */
  readonly signalName?: string;
  /** `signal`: the delivered payload (the latest delivery under the name). */
  readonly signalPayload?: unknown;
}

/**
 * A `pending` / `running` / `compensating` run whose lock is free or
 * expired, returned by `listOrphanedRuns`: nobody is driving it, so a
 * coordinator may adopt it. Adopting a `compensating` run finishes its
 * rollback.
 */
export interface OrphanedRun {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly version?: string;
  readonly status: "pending" | "running" | "compensating";
  readonly input: unknown;
  readonly metadata?: Record<string, unknown>;
}

export interface WorkflowStorage {
  /** Load the full workflow state. Returns null if workflow doesn't exist. */
  loadWorkflow(workflowId: string): Promise<WorkflowState | null>;

  /**
   * Load only the run's status, error and error tag — no step rows. The
   * runner calls it between waves to notice a cancel (unless every step of
   * the last wave was written by `checkpointStep`, which reports the status
   * itself), so backends should answer it from the workflow row alone.
   * Returns null if the workflow doesn't exist.
   */
  loadWorkflowStatus(workflowId: string): Promise<WorkflowStatusSnapshot | null>;

  /**
   * List workflows, optionally filtered by status, name, type, or namespace.
   *
   * `orderBy` defaults to `startedAt`, `orderDir` defaults to `desc` —
   * the most-recently-started run is what dashboards usually want, even
   * when some pending runs were created later but haven't picked up a
   * worker yet. Sort fields with NULL values (e.g. `startedAt` on a
   * still-pending row, `duration` on a still-running row) sort last
   * regardless of direction so the most-relevant rows surface first in
   * both views.
   *
   * `status` orders by the underlying enum/id ordering — not alphabetical —
   * to keep the cost a single column read across backends. Callers that
   * need alphabetical can sort the returned page client-side.
   */
  listWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    /** Workflow definition version the run was created with. */
    version?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    /**
     * Filter by what kicked the run off (`"schedule"`, `"manual"`, …).
     * Stored as an integer column, so this is a single index lookup —
     * unlike the previous `metadata.scheduleId` heuristic, which had to
     * scan/JSON-extract.
     */
    runSource?: import("./workflow-state.ts").RunSource;
    /** Optional producer id; only meaningful with `runSource`. */
    runSourceId?: string;
    /**
     * Filter by metadata key/value pairs. A row matches when its metadata
     * contains every supplied key with a deep-equal value. Backends with
     * native JSON support (Postgres `@>`) push the filter to the database;
     * others apply it after loading. Index strategy is the user's call —
     * Postgres ships with no metadata index by default.
     */
    metadata?: Record<string, unknown>;
    limit?: number;
    offset?: number;
    orderBy?: WorkflowOrderBy;
    orderDir?: "asc" | "desc";
  }): Promise<WorkflowState[]>;

  /**
   * Lean variant of `listWorkflows` that skips the heavy blob columns
   * (`steps`, `input`, `result`, `error`). Returns `WorkflowSummary` —
   * everything dashboards need for list-view rows without deserialising
   * step JSON on every poll.
   *
   * Optional — backends that don't implement this fall back to
   * `listWorkflows`. Callers should prefer it wherever `steps`/`input`/
   * `result` are not needed (e.g. the runs list, sparklines).
   */
  listWorkflowSummaries?(
    params?: Parameters<WorkflowStorage["listWorkflows"]>[0],
  ): Promise<WorkflowSummary[]>;

  /**
   * Count workflows matching the given filters without loading rows. Backends
   * that support this skip all blob deserialization and return a single integer
   * from a `SELECT COUNT(*)` (or equivalent) query.
   *
   * Optional — falls back to `listWorkflows` counting in JS when absent.
   */
  countWorkflows?(params?: {
    status?: WorkflowStatus;
    name?: string;
    /** Workflow definition version the run was created with. */
    version?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    runSource?: import("./workflow-state.ts").RunSource;
    runSourceId?: string;
    metadata?: Record<string, unknown>;
  }): Promise<number>;

  /**
   * Distinct workflow names ever observed in storage, optionally scoped to
   * a namespace. Returned alphabetically sorted. Used by dashboard filter
   * dropdowns so a workflow that hasn't run recently still appears.
   */
  distinctWorkflowNames(params?: { namespace?: string }): Promise<string[]>;

  /**
   * Distinct workflow types ever observed in storage, optionally scoped to
   * a namespace. Returned alphabetically sorted; null/undefined types are
   * excluded.
   */
  distinctWorkflowTypes(params?: { namespace?: string }): Promise<string[]>;

  /**
   * Distinct namespaces ever observed in storage. Returned alphabetically
   * sorted; null/undefined namespaces are excluded.
   */
  distinctNamespaces(): Promise<string[]>;

  /**
   * Cancel a pending, running or suspended workflow: status becomes
   * `failed` with error `"Cancelled"` and `errorTag`
   * `"WorkflowCancelledError"` (`CANCELLED_ERROR` / `CANCELLED_ERROR_TAG`).
   * A terminal run is left untouched, and so is every later terminal write
   * of the run that was executing (they are conditional), so the cancel
   * wins over a completion that lands after it.
   * With `cascade`, every descendant created with `parentWorkflowId`
   * pointing at this run (transitively) is cancelled the same way.
   */
  cancelWorkflow(
    workflowId: string,
    options?: { cascade?: boolean },
    guard?: FenceGuard,
  ): Promise<void>;

  /**
   * Create a new workflow record. Returns `{ created: false, existing }` on
   * conflict. `parentWorkflowId`, `runSource` and `runSourceId` are persisted
   * and round-trip through `loadWorkflow` and the list filters.
   *
   * `guard` fences the create on the parent's lock: a parent run creating
   * a child passes its own token, and the row is created only while that
   * token holds `parentWorkflowId`'s live lock. A stale token creates
   * nothing; whether it rejects or answers `created: false` when the child
   * already exists is backend-specific. A guard with a token requires
   * `parentWorkflowId`.
   */
  createWorkflow(
    params: {
      workflowId: string;
      workflowName: string;
      input: unknown;
      workflowType?: string;
      parentWorkflowId?: string;
      namespace?: string;
      metadata?: Record<string, unknown>;
      version?: string;
      /**
       * What kicked this run off — stored as a small int column so backends
       * can filter / sort by source efficiently. See `RunSource`.
       */
      runSource?: import("./workflow-state.ts").RunSource;
      /** Producer id corresponding to `runSource` (e.g. `scheduleId`). */
      runSourceId?: string;
      /**
       * Per-call idempotency key + expiry. Lets the auto-mint path
       * (`workflows.trigger()` minting workflowId via `crypto.randomUUID`)
       * dedup without the caller knowing the workflowId ahead of time. The
       * partial-unique index on `(namespace, workflowName, idempotencyKey)` guarantees
       * concurrent creates with the same key resolve to the same row —
       * `created: false; existing` returns the canonical workflowId.
       *
       * Once a key's `idempotencyExpiresAt` has passed it no longer claims the
       * slot: a create with the same key succeeds and the new row owns it.
       */
      idempotencyKey?: string;
      idempotencyExpiresAt?: Date;
    },
    guard?: FenceGuard,
  ): Promise<{ created: true } | { created: false; existing: WorkflowState }>;

  /**
   * Resolve a `(namespace, workflowName, idempotencyKey)` tuple to its workflow row,
   * if still unexpired. Returns null when no matching key exists or the
   * key's TTL has passed (expired keys are reclaimable by future creates).
   *
   * Used by the runner before the main start path: a hit means the run
   * redirects to the existing workflowId rather than creating a new one,
   * even if the caller passed a different (e.g. auto-minted) workflowId.
   */
  findWorkflowByIdempotencyKey(params: {
    readonly workflowName: string;
    readonly namespace?: string;
    readonly idempotencyKey: string;
    readonly now: Date;
  }): Promise<{ readonly workflowId: string } | null>;

  /** Save a completed step result. */
  saveStepResult(
    params: {
      workflowId: string;
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      /**
       * Step-kind-specific audit data (e.g. `.match()` writes the chosen
       * case). Stored verbatim on the step row. Omitted / `undefined` for
       * steps that don't produce audit data, which must round-trip as
       * `undefined` on load — not an empty object.
       */
      metadata?: Record<string, unknown>;
    },
    guard?: FenceGuard,
  ): Promise<void>;

  /**
   * Save many step results in a single round trip. Exists for the HTTP /
   * remote-storage path, where looping over `saveStepResult` turns into
   * one network call per step and dominates wall-clock for workflows with
   * tall map steps or fan-out DAGs.
   *
   * **Atomicity.** Backends SHOULD persist the batch atomically (Postgres
   * via `INSERT ... VALUES (...), (...), ...`; Redis via a pipeline/Lua
   * script; in-memory trivially). On partial failure the backend MAY
   * roll back — callers MUST NOT assume any subset survived.
   *
   * **Cross-workflow batches.** Each record carries its own `workflowId`,
   * so callers can batch across workflows. In practice the engine always
   * sends records for a single workflow at a time, but the signature stays
   * generic so cron / migration tooling can reuse the same primitive.
   *
   * Default is provided via the `batchSaveStepResultsDefault` helper —
   * custom storages that only override `saveStepResult` can point this
   * at it and opt into the interface without a perf gain.
   */
  batchSaveStepResults(
    records: ReadonlyArray<{
      workflowId: string;
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    }>,
    guard?: FenceGuard,
  ): Promise<void>;

  /** Mark a step as failed. `errorTag` is stored on the step row as given. */
  saveStepFailure(
    params: {
      workflowId: string;
      stepName: string;
      error: string;
      /** `_tag` of the error that failed the step, if it had one. */
      errorTag?: string;
      durationMs: number;
      startedAt: Date;
      /**
       * Step-kind-specific audit data set before the step ran. Preserved on
       * the failure row so ops can still see "which case fired" when a
       * `.match()` branch threw.
       */
      metadata?: Record<string, unknown>;
    },
    guard?: FenceGuard,
  ): Promise<void>;

  /** Save a completed task result within a map step. */
  saveTaskResult(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      result: unknown;
    },
    guard?: FenceGuard,
  ): Promise<void>;

  /** Mark a task within a map step as failed. */
  saveTaskFailure(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      error: string;
    },
    guard?: FenceGuard,
  ): Promise<void>;

  /**
   * Mark the entire workflow as completed. Terminal transitions only apply
   * to a non-terminal run: when the run is already `completed`, `failed`
   * (including cancelled) or `tripwire`, the call is a silent no-op, so a
   * cancel is never overwritten by a late completion.
   */
  completeWorkflow(workflowId: string, result: unknown, guard?: FenceGuard): Promise<void>;

  /**
   * Mark the entire workflow as failed, storing `error` and, when given,
   * `details.errorTag` (the failing error's `_tag`) on the run. No-op on a
   * terminal run (see `completeWorkflow`).
   */
  failWorkflow(
    workflowId: string,
    error: string,
    guard?: FenceGuard,
    details?: { readonly errorTag?: string },
  ): Promise<void>;

  /**
   * Mark the entire workflow as ended by a tripwire — an intentional early
   * exit distinct from `failed`. `reason` is the opaque payload returned by
   * the firing `.tripwire()` step's `reason(prev)`; backends persist it
   * verbatim alongside `status = "tripwire"` so callers can inspect why.
   *
   * No-op on a terminal run (see `completeWorkflow`).
   *
   * Optional. Storages that don't implement this don't support the
   * `.tripwire()` builder primitive — the runner raises
   * `TripwireStorageMissingError` at the fire site rather than silently
   * falling back to `failed`.
   */
  tripwireWorkflow?(workflowId: string, reason: unknown, guard?: FenceGuard): Promise<void>;

  /** Suspend the workflow (sleeping or waiting for signal). Fenced by `guard`. */
  suspendWorkflow(
    workflowId: string,
    stepName: string,
    stepUpdate: Record<string, unknown>,
    guard?: FenceGuard,
  ): Promise<void>;

  /**
   * Notify subscribers that a step is about to execute. Fired by the
   * runner before each local step body runs so subscribers can observe
   * `step-started` events. Optional — storages without subscription
   * support or without this method are skipped silently by the runner.
   * Persists nothing; this is purely an event-bus hook.
   */
  notifyStepStarted?(workflowId: string, stepName: string): Promise<void> | void;

  /**
   * Subscribe to step/workflow-lifecycle events for a single workflow run.
   * Returns an async iterable; the stream closes on the first terminal
   * event (`workflow-completed`, `workflow-failed`, `workflow-tripwire`) or
   * when the caller aborts via `options.signal`.
   *
   * Optional. Storages that don't implement this don't support live
   * subscriptions — callers must fall back to polling `loadWorkflow`.
   * Backends typically implement this by tapping the same code paths that
   * write the state transitions (in-memory: an in-process EventBus;
   * Postgres: `pg_notify` on relevant tables).
   *
   * Subscribers that attach before the workflow starts receive every event;
   * subscribers that attach mid-execution receive from-now onwards.
   */
  subscribeToWorkflow?(
    workflowId: string,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<WorkflowRunEvent>;

  /**
   * Deliver a signal to a workflow. Signals are scoped to the current run
   * and keyed by name: a second delivery under the same name replaces the
   * first (last delivery wins), and `startFreshRun` drops every delivered
   * signal so a new run never sees the previous run's deliveries.
   *
   * A delivery is a value, not a message: reading it never consumes it.
   * It stays visible until a later delivery under the same name replaces
   * it or a fresh run drops it — so a delivery that lands before its
   * waiter suspends still satisfies the wait, and a wait that runs again
   * (after `resetSteps`) sees the latest delivery. Child-ended wakeups
   * (`workflow.child-ended:<id>#<run>`) rely on this: the child may end
   * before its parent has parked. A workflow that needs one wake per
   * message uses a distinct signal name per message, or a stream.
   */
  deliverSignal(workflowId: string, signalName: string, payload: unknown): Promise<void>;

  /**
   * Load the signals delivered to the current run — at most one per name,
   * the latest delivery. Reading does not consume them.
   */
  loadSignals(workflowId: string): Promise<SignalState[]>;

  /**
   * Merge `patch` into the workflow row's metadata column. Used by
   * `ctx.metadata.set/merge` from inside a journaled body to surface live
   * progress/state to the dashboard. Shallow merge: top-level keys in
   * `patch` overwrite the same keys on the existing metadata; keys not in
   * `patch` are left untouched. Pass `null` for a key to remove it.
   *
   * Idempotent on identical patches — replay safely re-applies the same
   * writes without journaling. Cheap to call frequently (one row update).
   * Atomic per call: concurrent patches touching different keys all land.
   * Fenced by `guard` (the body's run passes its lock token).
   */
  setWorkflowMetadata(
    workflowId: string,
    patch: Record<string, unknown>,
    guard?: FenceGuard,
  ): Promise<void>;

  // -------------------------------------------------------------------------
  // Signal tokens — public-bearer authorization for `deliverSignal`.
  //
  // A signal token grants one-shot delivery rights to an external completer
  // (no Zorya auth) for a specific (workflowId, signalName). The completion
  // route validates the bearer, then calls `deliverSignal` to resume the
  // workflow through the existing path. Tokens don't change suspend
  // semantics — they're an authz sidecar, not a new primitive.
  //
  // Stored bearer is plaintext (short-lived, single-use, bounded by
  // `expiresAt`), compared with constant-time equality on completion.
  // -------------------------------------------------------------------------

  /**
   * Insert a signal token or return the existing row when an idempotency
   * key matches. `isCached: true` indicates the caller hit a dedup —
   * the original `(tokenId, bearer)` pair is reused so retries see the
   * same credentials.
   */
  createSignalToken(params: {
    readonly tokenId: string;
    readonly workflowId: string;
    readonly signalName: string;
    readonly bearer: string;
    readonly tags: ReadonlyArray<string>;
    readonly idempotencyKey?: string | null;
    readonly expiresAt: Date;
  }): Promise<{ readonly record: SignalTokenRecord; readonly isCached: boolean }>;

  /** Lookup by token id — used by the public completion endpoint. */
  findSignalTokenById(tokenId: string): Promise<SignalTokenRecord | null>;

  /**
   * Atomic completion: marks the token as completed (`completedAt` +
   * `completedValue`) only if it was still pending. Returns the prior
   * state so the caller can decide between 200 (delivered) and 410
   * (already completed). Doesn't itself call `deliverSignal` — the
   * route does that after a successful claim.
   */
  markSignalTokenCompleted(params: {
    readonly tokenId: string;
    readonly value: unknown;
    readonly now: Date;
  }): Promise<
    | { readonly outcome: "delivered"; readonly record: SignalTokenRecord }
    | { readonly outcome: "already_completed"; readonly record: SignalTokenRecord }
  >;

  /**
   * List every token issued for one workflow — drives
   * `runs.retrieve(workflowId).signalTokens[]` in the dashboard.
   * Ordered by `createdAt DESC`.
   */
  listSignalTokensForWorkflow(workflowId: string): Promise<ReadonlyArray<SignalTokenRecord>>;

  // -------------------------------------------------------------------------
  // Generic typed streams — bidirectional append-only channels per workflow.
  //
  // Output streams: workflow appends, external subscribers read.
  // Input streams: external subscribers append, workflow peeks/waits.
  // Storage doesn't distinguish — `appendedBy` records direction so the
  // SSE dashboard / consumer can render workflow-vs-external chunks
  // differently.
  // -------------------------------------------------------------------------

  /**
   * Append one chunk to a workflow's stream. Returns the assigned
   * `chunkIndex` (monotonic per `(workflowId, streamId)`). Atomic against
   * concurrent appends: every call gets a distinct index and the indices
   * of a stream stay gap-free (0, 1, 2, …).
   *
   * A step appending on behalf of its run passes the run's `guard`;
   * external appends are unfenced.
   */
  appendStreamChunk(
    params: {
      readonly workflowId: string;
      readonly streamId: string;
      readonly payload: unknown;
      readonly appendedBy: "workflow" | "external";
    },
    guard?: FenceGuard,
  ): Promise<{ readonly chunkIndex: number }>;

  /**
   * Read chunks from a stream. Pass `since` (exclusive) to replay from
   * the last index observed (SSE reconnect). `limit` caps the page;
   * default limits per backend (in-memory: 1000, postgres: 1000).
   */
  readStreamChunks(params: {
    readonly workflowId: string;
    readonly streamId: string;
    readonly since?: number;
    readonly limit?: number;
  }): Promise<ReadonlyArray<StreamChunk>>;

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
  tryLock(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ acquired: boolean; token?: FenceToken }>;

  /**
   * Acquire the lock AND load the current workflow state in one round trip.
   *
   * Exists for the HTTP / remote-storage path, where the typical
   * `tryLock` → check → `loadWorkflow` sequence is two network calls per
   * step invocation. Backends SHOULD persist this atomically
   * (Postgres: same transaction; Redis: Lua script) so the load reflects
   * the state as-of the moment the lock was acquired — no window where
   * another actor commits writes between the two observations.
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
   * Default is provided via `tryLockAndLoadDefault` — custom storages
   * that can't do an atomic read-lock can point this at it and still
   * satisfy the interface.
   */
  tryLockAndLoad(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ locked: boolean; token?: FenceToken; state: WorkflowState | null }>;

  /**
   * Release a workflow lock. When fencing is in play, only the token
   * holder releases — a stale holder whose lock already expired silently
   * no-ops. Callers usually pass the token they got from `tryLock`.
   */
  releaseLock(workflowId: string, guard?: FenceGuard): Promise<void>;

  /**
   * Heartbeat to extend a lock (for long-running steps). With a fence
   * token in `guard`, only the token holder extends a live lock: when the
   * lock is gone, expired or held under another token, the call rejects
   * with `FenceTokenMismatchError`, which tells the holder it lost the run.
   * Without a token, a lock not held by this instance is left alone.
   */
  heartbeat(workflowId: string, lockDurationMs: number, guard?: FenceGuard): Promise<void>;

  /**
   * Reset a workflow for a fresh re-execution (continue-as-new, idempotency
   * `onExpiry: "fresh-run"`). Archives the current run, increments the `run`
   * counter, resets status to `pending` and clears result/error/timestamps.
   *
   * Everything the new run replays from is cleared in the same atomic
   * operation: the activity journal (every step) and the delivered signals.
   * Otherwise run N+1 would replay run N's activity results or consume its
   * signals. Old step results remain in run history. Signal tokens and
   * streams are workflow-scoped and survive.
   *
   * Fenced by `guard` when the lock holder restarts its own run
   * (continue-as-new, a forced re-run); operator re-runs are unfenced.
   *
   * Returns the new run number.
   */
  startFreshRun(workflowId: string, guard?: FenceGuard): Promise<number>;

  /**
   * Reset specific step rows back to `pending`, clearing their result /
   * error / completedAt. Also clears any journal entries for those steps
   * so journaled bodies re-execute from zero. The workflow's overall
   * status flips back to `running` so the runner picks it up, and the
   * compensation ledger (`CompensationLedgerStorage`) is cleared on every
   * step of the run, so a later failure rolls the kept steps back again.
   * Delivered signals are kept: the run continues, and a re-run signal wait
   * sees the latest delivery.
   *
   * Used by `WorkflowRunner.resume(workflowId, fromStep)` for the
   * "rewind to step N and continue" debugging primitive — storage is the
   * primitive, runner walks the DAG to compute the downstream set.
   *
   * Optional: backends opt in by implementing it. The runner type-guards
   * at first use and throws a clear error if the configured storage
   * doesn't support reset. Steps not present on the workflow row are
   * silently skipped (idempotent on missing names).
   *
   * @param workflowId — the workflow whose steps to reset.
   * @param stepNames — explicit set to reset (caller computes downstream
   *   from the DAG; storage doesn't know topology).
   */
  resetSteps?(workflowId: string, stepNames: readonly string[]): Promise<void>;

  /**
   * Bulk-fail every workflow in `statuses` (default: pending, running,
   * suspended) whose `createdAt` is older than `olderThanMs`, recording
   * `error` on each. Returns the number of rows updated.
   *
   * Optional: `WorkflowRunner.recover()` uses it as a single-statement fast
   * path for stale-run termination and otherwise pages through
   * `listWorkflows` and cancels rows one by one.
   */
  cancelStaleWorkflows?(params: {
    olderThanMs: number;
    error?: string;
    statuses?: Array<"pending" | "running" | "suspended">;
  }): number | Promise<number>;

  /**
   * Scanner query: suspended runs (current run only) with a due timer — a
   * `sleeping` step whose `wakeAt <= now` (`reason: "sleep"`), or a
   * `waiting_for_signal` step whose `signalTimeoutAt <= now`
   * (`reason: "signal-timeout"`). One row per run (its due step with the
   * smallest name), ordered by `workflowId` ascending, at most `limit`.
   *
   * Keyset pagination: pass the last row's `workflowId` as
   * `afterWorkflowId` for the next page. Resuming rows between pages never
   * makes a later page skip one, unlike offset paging over a status filter.
   *
   * Scoped to the storage's namespace, like listWorkflows.
   *
   * Optional: without it the sleep scanner pages through `listWorkflows`.
   * Backends index `wakeAt` / `signalTimeoutAt` on suspended steps.
   */
  listDueTimers?(params: {
    now: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<WorkflowWakeup[]>;

  /**
   * Scanner query: suspended runs (current run only) with a
   * `waiting_for_signal` step whose `signalName` has a delivered signal
   * (`reason: "signal"`, `signalPayload` = the delivered payload). One row
   * per run (its matching step with the smallest name), ordered by
   * `workflowId` ascending, keyset-paginated like `listDueTimers`.
   *
   * Scoped to the storage's namespace, like listWorkflows.
   *
   * Optional: without it the signal scanner pages through `listWorkflows`
   * and `loadSignals`.
   */
  listSignalWakeups?(params: {
    limit: number;
    afterWorkflowId?: string;
  }): Promise<WorkflowWakeup[]>;

  /**
   * Recovery query: `pending` / `running` / `compensating` runs that
   * nobody is driving —
   * no lock, or a lock that expired at or before `now` — and that were last
   * updated before `updatedBefore` (so a run a live coordinator has just
   * created and not locked yet is left alone). Never returns `suspended`
   * runs: the scanners own those. Ordered by `workflowId` ascending,
   * keyset-paginated with `afterWorkflowId`, at most `limit` rows.
   *
   * Scoped to the storage's namespace, like listWorkflows.
   *
   * Optional: without it the coordinator pages through `listWorkflows`
   * and lets the run lock turn away runs that are still owned.
   */
  listOrphanedRuns?(params: {
    now: Date;
    updatedBefore: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<OrphanedRun[]>;

  /**
   * Load run history for a workflow — all runs with their step results.
   * Ordered by run number descending (newest first).
   */
  loadRunHistory(
    workflowId: string,
    params?: { limit?: number; offset?: number },
  ): Promise<WorkflowRunSummary[]>;

  /**
   * Delete completed/failed workflows matching the given time window.
   * Returns the number of workflows deleted.
   * Never deletes running or suspended workflows.
   *
   * Overloads:
   * - `olderThanMs` — relative: delete workflows completed more than N ms ago
   * - `from` / `to` — absolute: delete workflows with completedAt in [from, to)
   */
  purgeCompleted(
    params: { olderThanMs: number; limit: number } | { from: Date; to: Date; limit: number },
  ): Promise<number>;
}

/**
 * Deep-equality predicate matching Postgres jsonb `@>` containment for the
 * `listWorkflows({ metadata })` filter. Returns true when, for every key/
 * value pair in `filter`, `actual?.[key]` deep-equals the filter value.
 * Missing or undefined `actual` matches an empty filter only.
 */
export function workflowMetadataMatches(
  actual: Record<string, unknown> | undefined | null,
  filter: Record<string, unknown>,
): boolean {
  const keys = Object.keys(filter);
  if (keys.length === 0) return true;
  if (!actual) return false;
  for (const k of keys) {
    if (!deepEqual(actual[k], filter[k])) return false;
  }
  return true;
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return false;
  if (typeof a !== "object") return false;
  if (Array.isArray(a)) {
    if (!Array.isArray(b) || a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (!deepEqual(a[i], b[i])) return false;
    }
    return true;
  }
  if (Array.isArray(b)) return false;
  const ar = a as Record<string, unknown>;
  const br = b as Record<string, unknown>;
  const ak = Object.keys(ar);
  const bk = Object.keys(br);
  if (ak.length !== bk.length) return false;
  for (const k of ak) {
    if (!deepEqual(ar[k], br[k])) return false;
  }
  return true;
}

/**
 * Fallback `tryLockAndLoad` that sequences tryLock + loadWorkflow.
 * Atomic across the pair only if the underlying storage serializes both
 * calls against the same transaction — most local-process backends
 * (in-memory, same-connection Postgres) are fine, but a distributed
 * storage with no coupling between the two primitives may see another
 * writer commit between them. Backends that can do better should
 * override.
 */
export async function tryLockAndLoadDefault(
  storage: Pick<WorkflowStorage, "tryLock" | "loadWorkflow">,
  workflowId: string,
  lockDurationMs: number,
): Promise<{
  locked: boolean;
  token?: FenceToken;
  state: import("./workflow-state.ts").WorkflowState | null;
}> {
  const { acquired, token } = await storage.tryLock(workflowId, lockDurationMs);
  const state = await storage.loadWorkflow(workflowId);
  return { locked: acquired, token, state };
}

/**
 * Fallback `batchSaveStepResults` implementation for backends that can't do
 * a real multi-row write. Just loops `saveStepResult` in order. Intentionally
 * not concurrent — callers rely on the batch staying ordered so that step
 * rows created later in the batch sort after earlier ones on the receiving
 * side's `created_at` / insertion order.
 */
export async function batchSaveStepResultsDefault(
  storage: Pick<WorkflowStorage, "saveStepResult">,
  records: ReadonlyArray<{
    workflowId: string;
    stepName: string;
    result: unknown;
    durationMs: number;
    startedAt: Date;
    metadata?: Record<string, unknown>;
  }>,
  guard?: FenceGuard,
): Promise<void> {
  for (const r of records) {
    await storage.saveStepResult(r, guard);
  }
}

// ---------------------------------------------------------------------------
// StepAttemptStorage — optional interface for recording attempt history
// ---------------------------------------------------------------------------

/**
 * Optional storage extension for recording step attempt history.
 * Implementations that support this append a record for every execution
 * and compensation attempt, enabling audit trails and retry analysis.
 *
 * The engine detects this at runtime via `isStepAttemptStorage()`.
 */
export interface StepAttemptStorage {
  /** Append a step attempt record (execution or compensation). Fenced by `guard`. */
  saveStepAttempt(record: StepAttemptRecord, guard?: FenceGuard): Promise<void>;

  /** Load attempt history for a workflow, optionally filtered by step name. */
  loadStepAttempts(workflowId: string, stepName?: string): Promise<StepAttemptRecord[]>;
}

/** Runtime check for whether a storage implementation supports attempt history. */
export function isStepAttemptStorage(
  storage: WorkflowStorage,
): storage is WorkflowStorage & StepAttemptStorage {
  return "saveStepAttempt" in storage && typeof (storage as any).saveStepAttempt === "function";
}

// ---------------------------------------------------------------------------
// StepCheckpointStorage — optional one-call checkpoint of a settled step
// ---------------------------------------------------------------------------

/** A settled step as `checkpointStep` writes it: its row and its attempt rows. */
export interface StepCheckpoint {
  readonly workflowId: string;
  readonly stepName: string;
  /**
   * The step row to write: a `completed` outcome has `saveStepResult`
   * semantics, a `failed` one `saveStepFailure` semantics (pending →
   * running, attempt counter bumped, metadata kept when absent).
   */
  readonly outcome:
    | {
        readonly kind: "completed";
        readonly result: unknown;
        readonly durationMs: number;
        readonly startedAt: Date;
        readonly metadata?: Record<string, unknown>;
      }
    | {
        readonly kind: "failed";
        readonly error: string;
        readonly errorTag?: string;
        readonly durationMs: number;
        readonly startedAt: Date;
        readonly metadata?: Record<string, unknown>;
      };
  /**
   * Attempt rows to append with `saveStepAttempt` semantics, oldest first.
   * A backend that does not record attempts (or has them switched off)
   * ignores them.
   */
  readonly attempts: readonly StepAttemptRecord[];
}

/**
 * Optional storage extension: checkpoint a settled step in one call. Writes
 * the step row and its attempt rows atomically, fenced by `guard` like each
 * of the separate writes it replaces, and answers with the run's status as
 * of the write, so the runner learns about a cancel without a separate
 * `loadWorkflowStatus` read.
 *
 * Equivalent to `saveStepAttempt` for every attempt plus `saveStepResult`
 * / `saveStepFailure`, except that it is one round trip and lands whole or
 * not at all. The runner detects it with `isStepCheckpointStorage()` and
 * falls back to the separate calls without it.
 */
export interface StepCheckpointStorage {
  /**
   * Write `checkpoint`. Resolves with the run's status, error and error tag
   * read in the same atomic write, or `null` when the workflow does not
   * exist (nothing is written then). A rejected fence throws
   * `FenceTokenMismatchError` and writes nothing.
   */
  checkpointStep(
    checkpoint: StepCheckpoint,
    guard?: FenceGuard,
  ): Promise<WorkflowStatusSnapshot | null>;
}

/** Runtime check for whether a storage implementation has `checkpointStep`. */
export function isStepCheckpointStorage(
  storage: WorkflowStorage,
): storage is WorkflowStorage & StepCheckpointStorage {
  return typeof (storage as Partial<StepCheckpointStorage>).checkpointStep === "function";
}

/**
 * What a run's compensation ledger records for one step: its rollback ran
 * (`compensated`) or failed after its retries (`compensation_failed`).
 */
export type StepCompensationOutcome = "compensated" | "compensation_failed";

/**
 * Optional storage extension that makes saga compensation durable: the
 * run's `compensating` phase and a per-step ledger of the rollbacks that
 * already ran. A run that crashes mid-rollback is found `compensating` by
 * its next driver, which finishes the rollback instead of re-running the
 * workflow, and skips every step the ledger already lists.
 *
 * The ledger lives on the step rows of the current run
 * (`StepState.compensationStatus` / `compensationError` /
 * `compensatedAt`), so `loadWorkflow` returns it. `startFreshRun` starts
 * an empty ledger (new step rows), and `resetSteps` clears it on every step
 * of the run, so a run re-driven after a rollback can be rolled back again.
 *
 * Both writes are fenced by `guard`, atomically with the write, like every
 * other write of the run's lock holder.
 *
 * The engine detects this at runtime via `isCompensationLedgerStorage()`.
 * Without it, compensation still runs, but a crash mid-rollback is not
 * resumable.
 */
export interface CompensationLedgerStorage {
  /**
   * Enter the compensation phase: a `pending`, `running` or `suspended` run
   * becomes `compensating`, storing `error` / `errorTag` (the failure that
   * triggered the rollback; the run ends `failed` with them). A run that is
   * already `compensating` is left as it is, ledger and error included.
   *
   * Returns `true` when the run is `compensating` after the call, `false`
   * when it is missing or has ended (completed, failed, cancelled,
   * tripwire): an ended run is never rolled back by this call.
   */
  beginCompensation(
    params: { readonly workflowId: string; readonly error: string; readonly errorTag?: string },
    guard?: FenceGuard,
  ): Promise<boolean>;

  /**
   * Record one step's rollback in the ledger of the current run: sets the
   * step row's `compensationStatus` to `status`, `compensationError` to
   * `error` (cleared when absent) and `compensatedAt` to now. A step without
   * a row in the current run is left alone.
   */
  saveStepCompensation(
    params: {
      readonly workflowId: string;
      readonly stepName: string;
      readonly status: StepCompensationOutcome;
      readonly error?: string;
    },
    guard?: FenceGuard,
  ): Promise<void>;
}

/** Runtime check for whether a storage implementation keeps a compensation ledger. */
export function isCompensationLedgerStorage(
  storage: WorkflowStorage,
): storage is WorkflowStorage & CompensationLedgerStorage {
  const s = storage as Partial<CompensationLedgerStorage>;
  return typeof s.beginCompensation === "function" && typeof s.saveStepCompensation === "function";
}

/**
 * Storage with tripwire support — the optional `tripwireWorkflow` method
 * is present. Use the type guard to narrow before calling from the runner.
 */
export type TripwireCapableStorage = WorkflowStorage &
  Required<Pick<WorkflowStorage, "tripwireWorkflow">>;

/** Runtime check for whether a storage implementation supports tripwire termination. */
export function isTripwireCapableStorage(
  storage: WorkflowStorage,
): storage is TripwireCapableStorage {
  return "tripwireWorkflow" in storage && typeof (storage as any).tripwireWorkflow === "function";
}

/** Storage with `subscribeToWorkflow` — supports live per-run event streams. */
export type SubscribableStorage = WorkflowStorage &
  Required<Pick<WorkflowStorage, "subscribeToWorkflow">>;

/** Runtime check for whether a storage implementation supports run subscriptions. */
export function isSubscribableStorage(storage: WorkflowStorage): storage is SubscribableStorage {
  return (
    "subscribeToWorkflow" in storage && typeof (storage as any).subscribeToWorkflow === "function"
  );
}
