// ---------------------------------------------------------------------------
// CompensationLedgerStore — durable saga rollback.
// ---------------------------------------------------------------------------

import type { FencedWrite } from "./fencing.ts";

/**
 * What a run's compensation ledger records for one step: its rollback ran
 * (`compensated`) or failed after its retries (`compensation_failed`).
 */
export type StepCompensationOutcome = "compensated" | "compensation_failed";

/** Params of `beginCompensation`. */
export interface BeginCompensationParams extends FencedWrite {
  readonly workflowId: string;
  /**
   * The failure that triggered the rollback; the run ends `failed` with it.
   * `errorTag: CANCELLED_ERROR_TAG` marks a cancel rollback: it is also what
   * allows an ended cancelled run to enter the phase (see `beginCompensation`).
   */
  readonly error: string;
  readonly errorTag?: string;
}

/** Params of `saveStepCompensation`. */
export interface SaveStepCompensationParams extends FencedWrite {
  readonly workflowId: string;
  readonly stepName: string;
  readonly status: StepCompensationOutcome;
  readonly error?: string;
}

/**
 * Optional storage capability (`compensationLedger`) that makes saga
 * compensation durable: the run's `compensating` phase and a per-step
 * ledger of the rollbacks that already ran. A run that crashes mid-rollback
 * is found `compensating` by its next driver, which finishes the rollback
 * instead of re-running the workflow, and skips every step the ledger
 * already lists.
 *
 * The ledger lives on the step rows of the current run
 * (`StepState.compensationStatus` / `compensationError` /
 * `compensatedAt`), so `loadWorkflow` returns it. `startFreshRun` starts
 * an empty ledger (new step rows), and `resetSteps` clears it on every step
 * of the run, so a run re-driven after a rollback can be rolled back again.
 *
 * Without it, compensation still runs, but a crash mid-rollback is not
 * resumable.
 */
export interface CompensationLedgerStore {
  /**
   * Enter the compensation phase: a `pending`, `running` or `suspended` run
   * becomes `compensating`, storing `error` / `errorTag`. A run that is
   * already `compensating` is left as it is, ledger and error included.
   *
   * A cancelled run (`failed` with `errorTag` `CANCELLED_ERROR_TAG`) is
   * the one ended run this call may reopen, and only when `errorTag` is
   * `CANCELLED_ERROR_TAG` too: it becomes `compensating` again with
   * `completedAt` cleared, so a driver that sees a cancel can roll the run
   * back durably (a crash leaves it `compensating`, for recovery to finish).
   * The run's final `failWorkflow` with the same tag ends it cancelled again.
   *
   * Returns `true` when the run is `compensating` after the call, `false`
   * when it is missing or has ended otherwise (completed, failed, tripwire,
   * or cancelled when `errorTag` is not the cancelled tag): such a run is
   * never rolled back by this call.
   */
  beginCompensation(params: BeginCompensationParams): Promise<boolean>;

  /**
   * Record one step's rollback in the ledger of the current run: sets the
   * step row's `compensationStatus` to `status`, `compensationError` to
   * `error` (cleared when absent) and `compensatedAt` to now. A step without
   * a row in the current run is left alone.
   */
  saveStepCompensation(params: SaveStepCompensationParams): Promise<void>;
}
