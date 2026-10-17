// ---------------------------------------------------------------------------
// Step body — runs one step's body under its policies and settles it into a
// `StepBodyOutcome`: completed, failed, suspended or continue-as-new. Shared
// by the inline wave and `InProcessStepExecutor`, so both number attempts,
// report failed retry attempts and classify control flow the same way.
// ---------------------------------------------------------------------------

import type { Eff, Throws } from "@spilne/perfect-core";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { runEffSafe } from "../../shared/eff.ts";
import type { WallClock } from "../../shared/wall-clock.ts";
import type { StepDefinition } from "../durable-pipeline.ts";
import { applyStepPolicies } from "./step-policies.ts";

/** One attempt of a step body that failed with a typed error. */
export interface StepAttemptFailure {
  readonly attempt: number;
  readonly error: string;
  readonly startedAt: Date;
  readonly durationMs: number;
}

/** How a step body settled. `result` is codec-encoded. */
export type StepBodyOutcome =
  | {
      readonly kind: "completed";
      readonly result: unknown;
      readonly metadata?: Record<string, unknown>;
      /** Attempt number of the last invocation. */
      readonly attempt: number;
      /** Attempts that failed with a typed error, oldest first. */
      readonly failedAttempts: readonly StepAttemptFailure[];
    }
  | {
      readonly kind: "failed";
      /** The original error: a typed failure or a defect. */
      readonly error: unknown;
      readonly metadata?: Record<string, unknown>;
      readonly attempt: number;
      readonly failedAttempts: readonly StepAttemptFailure[];
    }
  | { readonly kind: "suspended"; readonly error: unknown; readonly attempt: number }
  | { readonly kind: "continue-as-new"; readonly error: unknown; readonly attempt: number };

const SUSPENDED_TAG = "WorkflowSuspendedError";
const CONTINUE_AS_NEW_TAG = "WorkflowContinueAsNewError";

/** True for the errors that carry control flow rather than a failure. */
export function isControlFlowError(error: unknown): boolean {
  const tag = (error as { _tag?: unknown } | null | undefined)?._tag;
  return tag === SUSPENDED_TAG || tag === CONTINUE_AS_NEW_TAG;
}

/** The message of an error, or its string form. */
export function errorMessage(error: unknown): string {
  return error instanceof globalThis.Error ? error.message : String(error);
}

/**
 * Run `stepDef`'s body under its timeout, retry and `onFailure` policies.
 * Attempts are numbered from `firstAttempt`, one per invocation. Every
 * attempt that fails with a typed error (other than suspension or
 * continue-as-new) is reported in `failedAttempts`, including a last
 * attempt that `onFailure` then absorbed. Defects (a rejected
 * `.stepAsync()` body, a synchronous throw) settle as `failed` too.
 */
export async function runStepBody(params: {
  readonly stepDef: StepDefinition;
  readonly workflowId: string;
  readonly clock: WallClock;
  readonly firstAttempt: number;
  /** Invoke the step's `execute` for one attempt. */
  readonly execute: (attempt: {
    readonly attempt: number;
    readonly metadataRef: { current?: Record<string, unknown> };
  }) => Eff<unknown, Throws<TaggedError>>;
}): Promise<StepBodyOutcome> {
  const { stepDef, clock } = params;
  // Shared audit-metadata slot. `.match()` fills it at selector time, so it
  // is set even when the chosen branch then fails. One per step, not per
  // attempt: a retry overwrites it.
  const metadataRef: { current?: Record<string, unknown> } = { current: undefined };
  const failedAttempts: StepAttemptFailure[] = [];
  let attempt = params.firstAttempt - 1;
  let attemptStartedAt = clock.now();

  const raw = applyStepPolicies({
    stepDef,
    workflowId: params.workflowId,
    clock,
    invoke: () => {
      attempt++;
      attemptStartedAt = clock.now();
      return params.execute({ attempt, metadataRef });
    },
    onAttemptFailed: (error) => {
      if (isControlFlowError(error)) return;
      failedAttempts.push({
        attempt,
        error: errorMessage(error),
        startedAt: attemptStartedAt,
        durationMs: clock.currentTimeMs() - attemptStartedAt.getTime(),
      });
    },
  });

  const { data, error } = await runEffSafe(
    raw.map((result) => stepDef.codec.encode(result)),
    { catchDefects: true },
  );
  // `invoke` always runs at least once; the floor only guards the type.
  const lastAttempt = Math.max(attempt, params.firstAttempt);

  if (error === null) {
    return {
      kind: "completed",
      result: data,
      metadata: metadataRef.current,
      attempt: lastAttempt,
      failedAttempts,
    };
  }
  const tag = (error as { _tag?: unknown } | null | undefined)?._tag;
  if (tag === SUSPENDED_TAG) return { kind: "suspended", error, attempt: lastAttempt };
  if (tag === CONTINUE_AS_NEW_TAG) {
    return { kind: "continue-as-new", error, attempt: lastAttempt };
  }
  return {
    kind: "failed",
    error,
    metadata: metadataRef.current,
    attempt: lastAttempt,
    failedAttempts,
  };
}
