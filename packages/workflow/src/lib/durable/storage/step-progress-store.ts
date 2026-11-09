// ---------------------------------------------------------------------------
// StepProgressStore — what a step is doing between its settled writes.
// ---------------------------------------------------------------------------

import type { StepProgress } from "../workflow-state.ts";
import type { FencedWrite } from "./fencing.ts";

/** Params of `saveStepProgress`. */
export interface SaveStepProgressParams extends FencedWrite {
  readonly workflowId: string;
  readonly stepName: string;
  readonly progress: StepProgress;
}

/**
 * Optional storage capability (`stepProgress`): record that a step attempt is
 * running or backing off before its next attempt, so a status call can say
 * "step X is running attempt 2 on worker w" or "step X retries at 12:00"
 * without a scan. The records are advisory and read back through
 * `WorkflowState.stepProgress`.
 */
export interface StepProgressStore {
  /**
   * Upsert `progress` as the latest record of `stepName`, one record per step.
   * Fenced. Idempotent: writing the same record again changes nothing.
   *
   * A no-op when the workflow does not exist or is not `pending` / `running`
   * (a parked or ended run runs nothing, so a record that arrives late would
   * only mislead). Never touches step rows, the run's status or `updatedAt`.
   * The stored record carries the run it was written for
   * (`StoredStepProgress.run`), so one left by an earlier run is recognisable;
   * `resetSteps` discards the reset steps' records.
   */
  saveStepProgress(params: SaveStepProgressParams): Promise<void>;
}
