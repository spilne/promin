// ---------------------------------------------------------------------------
// RetentionStore — the two primitives behind `RetentionSweeper`: a keyset page
// of ended runs old enough to be deleted, and a guarded delete of chosen runs.
//
// The sweeper decides what is safe to delete (per-status ages, parent / child
// relations, owed rollbacks, keep-at-least); the store only pages and deletes.
// The delete re-checks each run's own state atomically, so a run that was
// resumed, or that ended again since the sweeper looked, is never deleted on
// a stale decision.
// ---------------------------------------------------------------------------

import type { CompensationOrigin, CompensationStatus } from "../workflow-state.ts";

/** Statuses a run can be purged in: the ended ones. */
export type RetentionStatus = "completed" | "failed" | "tripwire";

/** Position in the `(completedAt, workflowId)` order `listRetentionCandidates` pages by. */
export interface RetentionCursor {
  readonly completedAt: Date;
  readonly workflowId: string;
}

/** An ended run, as the sweeper needs to see it to decide whether to delete it. */
export interface RetentionCandidate {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly status: RetentionStatus;
  /** `"WorkflowCancelledError"` on a cancelled run. */
  readonly errorTag?: string;
  readonly parentWorkflowId?: string;
  readonly compensationOrigin?: CompensationOrigin;
  readonly compensationStatus?: CompensationStatus;
  readonly completedAt: Date;
}

/** Params of `listRetentionCandidates`. */
export interface ListRetentionCandidatesParams {
  /** Only runs that completed strictly before this. */
  readonly completedBefore: Date;
  /** Only runs strictly after this position, in `(completedAt, workflowId)` order. */
  readonly after?: RetentionCursor;
  readonly limit: number;
  /** Only runs of this workflow name. */
  readonly name?: string;
  /** Only runs in these statuses. Default: all ended ones. */
  readonly statuses?: readonly RetentionStatus[];
}

/** A run to delete, with the `completedAt` the sweeper saw it end at. */
export interface PurgeRunRef {
  readonly workflowId: string;
  readonly completedAt: Date;
}

/** Retention primitives of a storage (capability `retention`). */
export interface RetentionStore {
  /**
   * Ended runs (`completed`, `failed`, `tripwire`) that completed before
   * `completedBefore`, ordered by `(completedAt, workflowId)` ascending,
   * strictly after `after`, at most `limit`. A storage built with a default
   * `namespace` lists only runs of that namespace.
   *
   * Optional (capability `retention`).
   */
  listRetentionCandidates?(params: ListRetentionCandidatesParams): Promise<RetentionCandidate[]>;

  /**
   * Delete each run together with everything stored for it (steps, step
   * progress, signals, signal tokens, journal, step attempts, compensation
   * ledger, run history, streams, events), and return the ids this call
   * deleted. A run is deleted only when, at the moment of the delete, it is
   * still ended, its `completedAt` still equals the ref's (to the
   * millisecond), and it owes no cancel rollback; any other run is left
   * alone and left out of the result. Deleting a run that is already gone is
   * not an error, so concurrent sweepers never double count.
   *
   * Optional (capability `retention`).
   */
  purgeRuns?(params: { readonly runs: readonly PurgeRunRef[] }): Promise<string[]>;
}
