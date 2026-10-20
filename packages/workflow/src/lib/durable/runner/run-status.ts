// ---------------------------------------------------------------------------
// Run status — what the orchestration reads back about a run it is driving:
// the entry gate for a stored terminal run, the between-wave check for a
// cancel or a lost lock, and the run's deadline from its persisted start.
// ---------------------------------------------------------------------------

import type { WallClock } from "../../shared/wall-clock.ts";
import {
  WorkflowCancelledError,
  WorkflowFailedError,
  WorkflowTripwireError,
} from "../durable-pipeline-error.ts";
import { isCancelledRun, type StepState, type WorkflowState } from "../workflow-state.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";

/**
 * The step whose row carries `metadata.tripwireFired = true` — the
 * `.tripwire()` step that ended a run with status `tripwire`.
 */
export function findTripwireStep(steps: Record<string, StepState>): StepState | undefined {
  return Object.values(steps).find(
    (s) => (s.metadata as { tripwireFired?: boolean } | undefined)?.tripwireFired === true,
  );
}

/** The error a cancelled run surfaces with. */
export function cancelledError(workflowId: string): WorkflowCancelledError {
  return new WorkflowCancelledError({
    workflowId,
    message: `Workflow "${workflowId}" was cancelled`,
  });
}

/**
 * The step whose row is `failed` — the one that failed the run. With
 * several, the latest to finish.
 */
export function findFailedStep(steps: Record<string, StepState>): StepState | undefined {
  let latest: StepState | undefined;
  for (const step of Object.values(steps)) {
    if (step.status !== "failed") continue;
    const at = step.completedAt?.getTime() ?? 0;
    if (latest === undefined || at > (latest.completedAt?.getTime() ?? 0)) latest = step;
  }
  return latest;
}

/**
 * The error a stored `failed` / `tripwire` run is reported with: a cancel
 * as `WorkflowCancelledError`, a tripwire as `WorkflowTripwireError`, any
 * other failure as `WorkflowFailedError` carrying the failed step and the
 * stored `errorTag`. `undefined` for a run that did not end that way.
 */
export function storedRunError(state: WorkflowState): Error | undefined {
  const { workflowId } = state;
  if (state.status === "tripwire") {
    const firedStep = findTripwireStep(state.steps);
    return new WorkflowTripwireError({
      workflowId,
      stepName: firedStep?.stepName ?? "unknown",
      reason: state.tripwire,
      message: `Workflow "${workflowId}" ended via tripwire`,
    });
  }
  if (state.status !== "failed") return undefined;
  if (isCancelledRun(state)) return cancelledError(workflowId);
  const failedStep = findFailedStep(state.steps);
  const errorTag = state.errorTag ?? failedStep?.errorTag;
  return new WorkflowFailedError({
    workflowId,
    ...(failedStep !== undefined && { stepName: failedStep.stepName }),
    message: state.error ?? `Workflow ${workflowId} failed`,
    ...(errorTag !== undefined && { errorTag }),
  });
}

/**
 * Entry gate for a run found in storage: a `failed`, cancelled or
 * `tripwire` run rejects with `storedRunError`. Any other run returns
 * (a completed one answers with its result, the rest proceed).
 */
export function rejectEndedRun(state: WorkflowState): void {
  const error = storedRunError(state);
  if (error !== undefined) throw error;
}

/**
 * Stop a run that should no longer go on: rejects with the lock-loss reason
 * once `signal` is aborted, and with `WorkflowCancelledError` once the
 * stored run reads as cancelled.
 */
export async function assertRunActive(params: {
  readonly storage: WorkflowStorage;
  readonly workflowId: string;
  readonly signal?: AbortSignal;
}): Promise<void> {
  const { storage, workflowId, signal } = params;
  if (signal?.aborted) throw signal.reason;
  const status = await storage.loadWorkflowStatus(workflowId);
  if (status !== null && isCancelledRun(status)) throw cancelledError(workflowId);
}

/**
 * When the run started, for its deadline: the persisted `startedAt`, so a
 * resume after a sleep or a signal wait keeps the original deadline. A run
 * that has not started yet (`pending`) starts now; a started run without a
 * recorded start falls back to its creation time.
 */
export function runStartMs(params: {
  readonly state: WorkflowState;
  readonly clock: WallClock;
}): number {
  const { state, clock } = params;
  if (state.startedAt !== undefined) return state.startedAt.getTime();
  if (state.status === "pending") return clock.currentTimeMs();
  return state.createdAt.getTime();
}
