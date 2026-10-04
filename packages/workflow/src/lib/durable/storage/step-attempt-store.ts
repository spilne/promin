// ---------------------------------------------------------------------------
// StepAttemptStore and StepCheckpointStore — the attempt audit trail and the
// one-call checkpoint of a settled step.
// ---------------------------------------------------------------------------

import type { StepAttemptRecord, WorkflowStatusSnapshot } from "../workflow-state.ts";
import type { FencedWrite } from "./fencing.ts";

/** Params of `saveStepAttempt`. */
export interface SaveStepAttemptParams extends FencedWrite {
  readonly record: StepAttemptRecord;
}

/** Params of `loadStepAttempts`. */
export interface LoadStepAttemptsParams {
  readonly workflowId: string;
  /** Only this step's attempts; every step's when absent. */
  readonly stepName?: string;
}

/**
 * Optional storage capability (`stepAttempts`) for recording step attempt
 * history. Implementations append a record for every execution and
 * compensation attempt, enabling audit trails and retry analysis.
 */
export interface StepAttemptStore {
  /** Append a step attempt record (execution or compensation). */
  saveStepAttempt(params: SaveStepAttemptParams): Promise<void>;

  /** Load attempt history for a workflow, oldest first. */
  loadStepAttempts(params: LoadStepAttemptsParams): Promise<StepAttemptRecord[]>;
}

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

/** Params of `checkpointStep`. */
export interface CheckpointStepParams extends StepCheckpoint, FencedWrite {}

/**
 * Optional storage capability (`stepCheckpoint`): checkpoint a settled step
 * in one call. Writes the step row and its attempt rows atomically, fenced
 * like each of the separate writes it replaces, and answers with the run's
 * status as of the write, so the runner learns about a cancel without a
 * separate `loadWorkflowStatus` read.
 *
 * Equivalent to `saveStepAttempt` for every attempt plus `saveStepResult`
 * / `saveStepFailure`, except that it is one round trip and lands whole or
 * not at all. The runner falls back to the separate calls without it.
 */
export interface StepCheckpointStore {
  /**
   * Write the checkpoint. Resolves with the run's status, error and error
   * tag read in the same atomic write, or `null` when the workflow does not
   * exist (nothing is written then). A rejected fence throws
   * `FenceTokenMismatchError` and writes nothing.
   */
  checkpointStep(params: CheckpointStepParams): Promise<WorkflowStatusSnapshot | null>;
}
