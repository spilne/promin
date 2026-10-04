// ---------------------------------------------------------------------------
// WorkflowRunStore — a run's row, its step and task rows, and its lifecycle
// transitions.
// ---------------------------------------------------------------------------

import type { RunSource, WorkflowState, WorkflowStatusSnapshot } from "../workflow-state.ts";
import type { FencedWrite } from "./fencing.ts";

/** Params of `createWorkflow`. */
export interface CreateWorkflowParams extends FencedWrite {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly input: unknown;
  readonly workflowType?: string;
  readonly parentWorkflowId?: string;
  readonly namespace?: string;
  readonly metadata?: Record<string, unknown>;
  readonly version?: string;
  /**
   * What kicked this run off — stored as a small int column so backends
   * can filter / sort by source efficiently. See `RunSource`.
   */
  readonly runSource?: RunSource;
  /** Producer id corresponding to `runSource` (e.g. `scheduleId`). */
  readonly runSourceId?: string;
  /**
   * Per-call idempotency key + expiry. Lets the auto-mint path
   * (`workflows.trigger()` minting workflowId via `crypto.randomUUID`)
   * dedup without the caller knowing the workflowId ahead of time. The
   * partial-unique index on `(namespace, workflowName, idempotencyKey)`
   * guarantees concurrent creates with the same key resolve to the same
   * row — `created: false; existing` returns the canonical workflowId.
   *
   * Once a key's `idempotencyExpiresAt` has passed it no longer claims the
   * slot: a create with the same key succeeds and the new row owns it.
   */
  readonly idempotencyKey?: string;
  readonly idempotencyExpiresAt?: Date;
}

/** Answer of `createWorkflow`. */
export type CreateWorkflowResult =
  | { readonly created: true }
  | { readonly created: false; readonly existing: WorkflowState };

/** Params of `findWorkflowByIdempotencyKey`. */
export interface FindWorkflowByIdempotencyKeyParams {
  readonly workflowName: string;
  readonly namespace?: string;
  readonly idempotencyKey: string;
  readonly now: Date;
}

/** One completed step row, as `saveStepResult` and `batchSaveStepResults` write it. */
export interface StepResultRecord {
  readonly workflowId: string;
  readonly stepName: string;
  readonly result: unknown;
  readonly durationMs: number;
  readonly startedAt: Date;
  /**
   * Step-kind-specific audit data (e.g. `.match()` writes the chosen
   * case). Stored verbatim on the step row. Omitted / `undefined` for
   * steps that don't produce audit data, which must round-trip as
   * `undefined` on load — not an empty object.
   */
  readonly metadata?: Record<string, unknown>;
}

/** Params of `saveStepResult`. */
export interface SaveStepResultParams extends StepResultRecord, FencedWrite {}

/** Params of `batchSaveStepResults`. */
export interface BatchSaveStepResultsParams extends FencedWrite {
  readonly records: ReadonlyArray<StepResultRecord>;
}

/** Params of `saveStepFailure`. */
export interface SaveStepFailureParams extends FencedWrite {
  readonly workflowId: string;
  readonly stepName: string;
  readonly error: string;
  /** `_tag` of the error that failed the step, if it had one. */
  readonly errorTag?: string;
  readonly durationMs: number;
  readonly startedAt: Date;
  /**
   * Step-kind-specific audit data set before the step ran. Preserved on
   * the failure row so ops can still see "which case fired" when a
   * `.match()` branch threw.
   */
  readonly metadata?: Record<string, unknown>;
}

/** Params of `saveTaskResult`. */
export interface SaveTaskResultParams extends FencedWrite {
  readonly workflowId: string;
  readonly stepName: string;
  readonly taskIndex: number;
  readonly result: unknown;
}

/** Params of `saveTaskFailure`. */
export interface SaveTaskFailureParams extends FencedWrite {
  readonly workflowId: string;
  readonly stepName: string;
  readonly taskIndex: number;
  readonly error: string;
}

/** Params of `completeWorkflow`. */
export interface CompleteWorkflowParams extends FencedWrite {
  readonly workflowId: string;
  readonly result: unknown;
}

/** Params of `failWorkflow`. */
export interface FailWorkflowParams extends FencedWrite {
  readonly workflowId: string;
  readonly error: string;
  /** `_tag` of the error that failed the run, stored on the run row. */
  readonly errorTag?: string;
}

/** Params of `tripwireWorkflow`. */
export interface TripwireWorkflowParams extends FencedWrite {
  readonly workflowId: string;
  /** Opaque payload returned by the firing `.tripwire()` step's `reason(prev)`. */
  readonly reason: unknown;
}

/** Params of `cancelWorkflow`. */
export interface CancelWorkflowParams extends FencedWrite {
  readonly workflowId: string;
  /** Also cancel every descendant created with `parentWorkflowId` pointing here. */
  readonly cascade?: boolean;
}

/** Params of `suspendWorkflow`. */
export interface SuspendWorkflowParams extends FencedWrite {
  readonly workflowId: string;
  /** The step that suspends the run. */
  readonly stepName: string;
  /** Fields merged into the step row (`status`, `wakeAt`, `signalName`, ...). */
  readonly stepUpdate: Record<string, unknown>;
}

/** Params of `setWorkflowMetadata`. */
export interface SetWorkflowMetadataParams extends FencedWrite {
  readonly workflowId: string;
  /** Shallow patch; a `null` value removes the key. */
  readonly patch: Record<string, unknown>;
}

/** Params of `startFreshRun`. */
export interface StartFreshRunParams extends FencedWrite {
  readonly workflowId: string;
}

/** Params of `resetSteps`. */
export interface ResetStepsParams {
  readonly workflowId: string;
  /**
   * Explicit set to reset. The caller computes the downstream set from the
   * DAG; storage doesn't know topology.
   */
  readonly stepNames: readonly string[];
}

/**
 * The run's row and its step rows: create and load a run, write its step
 * and task outcomes, and move it through its lifecycle.
 *
 * **Terminal states.** `completed`, `failed` (cancelled included) and
 * `tripwire` are terminal. Every terminal transition is conditional on a
 * non-terminal run, so a cancel is never overwritten by a late completion.
 *
 * **Atomicity.** Each method is one atomic write unless its doc says
 * otherwise; a fenced write is rejected whole (see `FenceGuard`).
 */
export interface WorkflowRunStore {
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
   * Create a new workflow record (status `pending`, run 1). Returns
   * `{ created: false, existing }` on conflict. `parentWorkflowId`,
   * `runSource` and `runSourceId` are persisted and round-trip through
   * `loadWorkflow` and the list filters.
   *
   * `guard` fences the create on the parent's lock: a parent run creating
   * a child passes its own token, and the row is created only while that
   * token holds `parentWorkflowId`'s live lock. A stale token creates
   * nothing; whether it rejects or answers `created: false` when the child
   * already exists is backend-specific. A guard with a token requires
   * `parentWorkflowId`.
   */
  createWorkflow(params: CreateWorkflowParams): Promise<CreateWorkflowResult>;

  /**
   * Resolve a `(namespace, workflowName, idempotencyKey)` tuple to its
   * workflow row, if still unexpired. Returns null when no matching key
   * exists or the key's TTL has passed (expired keys are reclaimable by
   * future creates).
   *
   * Used by the runner before the main start path: a hit means the run
   * redirects to the existing workflowId rather than creating a new one,
   * even if the caller passed a different (e.g. auto-minted) workflowId.
   */
  findWorkflowByIdempotencyKey(
    params: FindWorkflowByIdempotencyKeyParams,
  ): Promise<{ readonly workflowId: string } | null>;

  /**
   * Save a completed step result. A `pending` run becomes `running` and the
   * step's attempt counter is bumped.
   */
  saveStepResult(params: SaveStepResultParams): Promise<void>;

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
   * A storage that can't do better can delegate to
   * `batchSaveStepResultsDefault` from `@promin/workflow/storage-kit`.
   */
  batchSaveStepResults(params: BatchSaveStepResultsParams): Promise<void>;

  /** Mark a step as failed. `errorTag` is stored on the step row as given. */
  saveStepFailure(params: SaveStepFailureParams): Promise<void>;

  /** Save a completed task result within a map step. */
  saveTaskResult(params: SaveTaskResultParams): Promise<void>;

  /** Mark a task within a map step as failed. */
  saveTaskFailure(params: SaveTaskFailureParams): Promise<void>;

  /**
   * Mark the entire workflow as completed. Terminal transitions only apply
   * to a non-terminal run: when the run is already `completed`, `failed`
   * (including cancelled) or `tripwire`, the call is a silent no-op, so a
   * cancel is never overwritten by a late completion.
   */
  completeWorkflow(params: CompleteWorkflowParams): Promise<void>;

  /**
   * Mark the entire workflow as failed, storing `error` and, when given,
   * `errorTag` (the failing error's `_tag`) on the run. No-op on a terminal
   * run (see `completeWorkflow`).
   */
  failWorkflow(params: FailWorkflowParams): Promise<void>;

  /**
   * Mark the entire workflow as ended by a tripwire — an intentional early
   * exit distinct from `failed`. Backends persist `reason` verbatim
   * alongside `status = "tripwire"` so callers can inspect why.
   *
   * No-op on a terminal run (see `completeWorkflow`).
   *
   * Optional (capability `tripwire`). Storages that don't implement this
   * don't support the `.tripwire()` builder primitive — the runner raises
   * `TripwireStorageMissingError` at the fire site rather than silently
   * falling back to `failed`.
   */
  tripwireWorkflow?(params: TripwireWorkflowParams): Promise<void>;

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
  cancelWorkflow(params: CancelWorkflowParams): Promise<void>;

  /** Suspend the workflow (sleeping or waiting for signal). */
  suspendWorkflow(params: SuspendWorkflowParams): Promise<void>;

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
   * A missing workflow is a silent no-op.
   */
  setWorkflowMetadata(params: SetWorkflowMetadataParams): Promise<void>;

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
   * Fenced when the lock holder restarts its own run (continue-as-new, a
   * forced re-run); operator re-runs are unfenced.
   *
   * Returns the new run number.
   */
  startFreshRun(params: StartFreshRunParams): Promise<number>;

  /**
   * Reset specific step rows back to `pending`, clearing their result /
   * error / completedAt. Also clears any journal entries for those steps
   * so journaled bodies re-execute from zero. The workflow's overall
   * status flips back to `running` so the runner picks it up, and the
   * compensation ledger (`CompensationLedgerStore`) is cleared on every
   * step of the run, so a later failure rolls the kept steps back again.
   * Delivered signals are kept: the run continues, and a re-run signal wait
   * sees the latest delivery.
   *
   * Used by `WorkflowRunner.resume(workflowId, fromStep)` for the
   * "rewind to step N and continue" debugging primitive — storage is the
   * primitive, runner walks the DAG to compute the downstream set.
   *
   * Optional (capability `resetSteps`): the runner throws a clear error at
   * first use when the configured storage doesn't support it. Steps not
   * present on the workflow row are silently skipped (idempotent on
   * missing names).
   */
  resetSteps?(params: ResetStepsParams): Promise<void>;
}
