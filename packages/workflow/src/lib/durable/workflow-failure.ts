// ---------------------------------------------------------------------------
// WorkflowFailure — the one structured shape of a failed run.
//
// A failed run surfaces in several places: the error `run` throws and
// `runSafe` returns, `handle.result()` / `handle.status()`, the stored
// `error` + `errorTag` of the `WorkflowState`, the DLQ record and the
// `onWorkflowFailure` hook. Each carries the same failure; `WorkflowFailure`
// is the shape they share. It is derived, never stored: a state read from
// any backend (including the HTTP remote storage) yields it with
// `workflowFailure(state)`, so no column or migration is involved.
// ---------------------------------------------------------------------------

import {
  ChildWorkflowGoneError,
  StepError,
  WorkflowCancelledError,
  WorkflowError,
  WorkflowFailedError,
  WorkflowTimeoutError,
} from "./durable-pipeline-error.ts";
import {
  CANCELLED_ERROR_TAG,
  type StepState,
  type WorkflowFailure,
  type WorkflowState,
} from "./workflow-state.ts";

/** `tag` of a failure whose error carried no `_tag` (a defect). */
export const UNTAGGED_FAILURE_TAG = "UnknownError";

export type { WorkflowFailure };

/**
 * The step whose row is `failed`: the one that failed the run. With
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
 * The failure of a stored run: for a `failed` run, and for a `compensating`
 * one (whose stored error is the failure its rollback answers). `undefined`
 * for a run in any other status. `cause` is the live error, when the caller
 * has it.
 */
export function workflowFailure(
  state: Pick<WorkflowState, "workflowId" | "status" | "error" | "errorTag" | "steps">,
  cause?: unknown,
): WorkflowFailure | undefined {
  if (state.status !== "failed" && state.status !== "compensating") return undefined;
  const step = findFailedStep(state.steps);
  const tag = state.errorTag ?? step?.errorTag;
  return {
    tag: tag ?? (cause !== undefined ? tagOfError(cause) : undefined) ?? UNTAGGED_FAILURE_TAG,
    message: state.error ?? `Workflow ${state.workflowId} failed`,
    ...(step !== undefined && { stepName: step.stepName, attempt: step.attempt }),
    ...(cause !== undefined && { cause }),
  };
}

/** The failure of a live error: what `runSafe` and `failureOf` report for it. */
export function failureFromError(error: unknown): WorkflowFailure {
  const e = (typeof error === "object" && error !== null ? error : {}) as {
    stepName?: unknown;
    attempt?: unknown;
    message?: unknown;
  };
  return {
    tag: tagOfError(error) ?? UNTAGGED_FAILURE_TAG,
    message: error instanceof Error ? error.message : String(error),
    ...(typeof e.stepName === "string" && { stepName: e.stepName }),
    ...(typeof e.attempt === "number" && { attempt: e.attempt }),
    cause: error,
  };
}

/** `_tag` to store for an error: a `StepError`'s reported `errorTag`, else its own `_tag`. */
function tagOfError(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  const { _tag, errorTag } = error as { _tag?: unknown; errorTag?: unknown };
  if (_tag === "StepError" && typeof errorTag === "string") return errorTag;
  return typeof _tag === "string" ? _tag : undefined;
}

/**
 * The failure derived from an error alone (no failed step or attempt unless
 * the error carries them). `runSafe` reports the engine-recorded failure of
 * its own run instead, which is never looked up by error identity.
 */
export function failureOf(error: unknown): WorkflowFailure {
  return failureFromError(error);
}

/** Builds the typed error for a failure with a known `tag`. */
export type FailureRehydrator = (params: {
  readonly workflowId: string;
  readonly failure: WorkflowFailure;
}) => Error;

const BUILT_IN_REHYDRATORS: Record<string, FailureRehydrator> = {
  [CANCELLED_ERROR_TAG]: ({ workflowId, failure }) =>
    new WorkflowCancelledError({ workflowId, message: failure.message }),
  WorkflowError: ({ workflowId, failure }) =>
    new WorkflowError({ workflowId, message: failure.message }),
  ChildWorkflowGoneError: ({ failure }) => new ChildWorkflowGoneError({ message: failure.message }),
  WorkflowTimeoutError: ({ workflowId, failure }) =>
    new WorkflowTimeoutError({
      workflowId,
      stepName: failure.stepName ?? "unknown",
      message: failure.message,
    }),
  StepError: ({ workflowId, failure }) =>
    new StepError({
      workflowId,
      stepName: failure.stepName ?? "unknown",
      message: failure.message,
    }),
};

/**
 * The typed error for a stored failure. A `tag` with a rehydrator (built in
 * for the engine errors whose fields a failure holds, or given in `errors`
 * for the workflow's own typed errors) gives that error; any other tag gives
 * `WorkflowFailedError`, which carries the tag, step and attempt. Opt-in:
 * `handle.result()` keeps rejecting with `WorkflowFailedError`.
 */
export function rehydrateFailure(params: {
  readonly workflowId: string;
  readonly failure: WorkflowFailure;
  readonly errors?: Readonly<Record<string, FailureRehydrator>>;
}): Error {
  const { workflowId, failure } = params;
  const build = params.errors?.[failure.tag] ?? BUILT_IN_REHYDRATORS[failure.tag];
  if (build !== undefined) return build({ workflowId, failure });
  return new WorkflowFailedError({
    workflowId,
    message: failure.message,
    errorTag: failure.tag,
    ...(failure.stepName !== undefined && { stepName: failure.stepName }),
    ...(failure.attempt !== undefined && { attempt: failure.attempt }),
  });
}
