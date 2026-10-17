// ---------------------------------------------------------------------------
// Step policy — the one mapping from a step kind's options to the policy
// fields the runner applies (timeout, retry, onFailure, compensation, skip,
// dispatch hints), plus the timeout / retry / onFailure combinators shared
// by the runner (whole step) and `.mapOver()` (per element).
// ---------------------------------------------------------------------------

import { fail, race, succeed, sync, type Eff, type Throws } from "@spilne/perfect-core";
import type { TaggedError } from "../shared/tagged-error.ts";
import { sleepOn } from "../shared/eff.ts";
import { retryWithPolicy, type RetryPolicy } from "../shared/retry-policy.ts";
import type { WallClock } from "../shared/wall-clock.ts";
import { StepTimeoutError } from "./durable-pipeline-error.ts";
import type { StepDefinition, StepOptions } from "./durable-pipeline.ts";

/** The policy fields of a `StepDefinition` that come from step options. */
export type StepPolicy = Pick<
  StepDefinition,
  | "timeoutMs"
  | "retry"
  | "onFailure"
  | "compensate"
  | "skipWhen"
  | "skipValue"
  | "needs"
  | "priority"
  | "queue"
>;

/**
 * Map step options onto the policy fields of a `StepDefinition`. Every step
 * kind builds its definition through this, so an option a kind's options
 * type admits is an option the runner applies. `codec` and `cache` are not
 * policy fields: each kind installs the codec and wraps its body with the
 * cache itself.
 */
export function toStepPolicy<T>(options: Partial<StepOptions<T>> | undefined): StepPolicy {
  if (!options) return {};
  return {
    ...(options.timeoutMs !== undefined && { timeoutMs: options.timeoutMs }),
    ...(options.retry !== undefined && { retry: options.retry }),
    ...(options.onFailure !== undefined && {
      onFailure: options.onFailure as StepDefinition["onFailure"],
    }),
    ...(options.compensate !== undefined && {
      compensate: options.compensate as StepDefinition["compensate"],
    }),
    ...(options.skipWhen !== undefined && { skipWhen: options.skipWhen }),
    ...(options.skipValue !== undefined && {
      skipValue: options.skipValue as StepDefinition["skipValue"],
    }),
    ...(options.needs !== undefined && { needs: options.needs }),
    ...(options.priority !== undefined && { priority: options.priority }),
    ...(options.queue !== undefined && { queue: options.queue }),
  };
}

/**
 * Exits that are engine control flow, not step failures: step-level retry
 * and `onFailure` must let them through untouched. Matched by `_tag` so an
 * error raised by another copy of the module is still recognised.
 *
 *  - `WorkflowSuspendedError`: a sleep / signal wait parked the run.
 *  - `WorkflowContinueAsNewError`: a clean restart of the run.
 *  - `WorkflowTripwireError`: an intentional early end.
 *  - `JournalNonDeterminismError`: code drifted from the journal.
 *  - `AmbiguousActivityOutcome`: halt for an operator.
 *  - `WorkflowLockError` / `FenceTokenMismatchError`: this worker lost the
 *    run; the new owner re-drives it from storage.
 */
const CONTROL_FLOW_EXITS: ReadonlySet<string> = new Set([
  "WorkflowSuspendedError",
  "WorkflowContinueAsNewError",
  "WorkflowTripwireError",
  "JournalNonDeterminismError",
  "AmbiguousActivityOutcome",
  "WorkflowLockError",
  "FenceTokenMismatchError",
]);

/** Whether `error` is an engine control-flow exit rather than a step failure. */
export function isControlFlowExit(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const tag = (error as { readonly _tag?: unknown })._tag;
  return typeof tag === "string" && CONTROL_FLOW_EXITS.has(tag);
}

/**
 * Fail `eff` with `StepTimeoutError` when it does not settle within `ms` on
 * `clock`. The loser of the race is interrupted, so a body that settles
 * first clears the timer.
 */
export function withStepTimeout<A>(params: {
  readonly eff: Eff<A, Throws<TaggedError>>;
  readonly clock: WallClock;
  readonly ms: number;
  readonly workflowId: string;
  /** Step the error is attributed to. */
  readonly stepName: string;
  /** What timed out, for the message (default `stepName`; `step[3]` for a map element). */
  readonly subject?: string;
}): Eff<A, Throws<TaggedError>> {
  const { ms, workflowId, stepName } = params;
  const subject = params.subject ?? stepName;
  return race([
    params.eff,
    sleepOn(params.clock, ms).flatMap(() =>
      fail(
        new StepTimeoutError({
          workflowId,
          stepName,
          timeoutMs: ms,
          message: `Step "${subject}" timed out after ${ms}ms`,
        }),
      ),
    ),
  ]) as Eff<A, Throws<TaggedError>>;
}

/**
 * Retry `eff` on typed failures per `policy`, never on control-flow exits
 * (suspension, continue-as-new, ...). Backoff sleeps run on `clock`.
 */
export function withStepRetry<A>(params: {
  readonly eff: Eff<A, Throws<TaggedError>>;
  readonly policy: RetryPolicy<TaggedError>;
  readonly clock: WallClock;
}): Eff<A, Throws<TaggedError>> {
  const { policy } = params;
  const userWhen = policy.when;
  return retryWithPolicy({
    eff: params.eff,
    policy: {
      ...policy,
      when: (error: TaggedError) =>
        !isControlFlowExit(error) && (userWhen === undefined || userWhen(error)),
    },
    clock: params.clock,
  });
}

/**
 * Apply a step's `onFailure` strategy to typed failures. Control-flow exits
 * and defects pass through, so `"skip"` cannot swallow a suspension.
 */
export function withFailureStrategy(params: {
  readonly eff: Eff<unknown, Throws<TaggedError>>;
  readonly strategy: StepDefinition["onFailure"];
}): Eff<unknown, Throws<TaggedError>> {
  const strategy = params.strategy ?? "fail";
  if (strategy === "fail") return params.eff;
  return params.eff.catch((error): Eff<unknown, Throws<TaggedError>> => {
    if (isControlFlowExit(error)) return fail(error);
    if (strategy === "skip") return succeed(undefined);
    return sync(() => strategy.fallback(error));
  });
}
