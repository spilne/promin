// ---------------------------------------------------------------------------
// Step policies — wraps one step invocation with the step's own timeout,
// retry and `onFailure` strategy. Shared by the inline wave and
// `InProcessStepExecutor` so both paths apply the same semantics.
// ---------------------------------------------------------------------------

import { succeed, suspend, sync, type Eff, type Throws } from "@spilne/perfect-core";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { retryWithPolicy } from "../../shared/retry-policy.ts";
import type { WallClock } from "../../shared/wall-clock.ts";
import type { StepDefinition } from "../durable-pipeline.ts";
import { StepTimeoutError } from "../durable-pipeline-error.ts";

/**
 * Wrap one step invocation with the step's own policies, in order:
 * per-attempt timeout, retry on typed failures, then the `onFailure`
 * strategy. `invoke` runs again on every retry. A step that does not settle
 * within `timeoutMs` fails with `StepTimeoutError`; the abandoned attempt is
 * interrupted. Retry backoff sleeps on `clock`.
 */
export function applyStepPolicies(params: {
  stepDef: StepDefinition;
  workflowId: string;
  clock: WallClock;
  invoke: () => Eff<unknown, Throws<TaggedError>>;
}): Eff<unknown, Throws<TaggedError>> {
  const { stepDef, workflowId, clock } = params;
  let raw: Eff<unknown, Throws<TaggedError>> = suspend(params.invoke);

  if (stepDef.timeoutMs != null) {
    const stepTimeoutMs = stepDef.timeoutMs;
    const stepName = stepDef.name;
    raw = raw.timeoutFail(
      stepTimeoutMs,
      () =>
        new StepTimeoutError({
          workflowId,
          stepName,
          timeoutMs: stepTimeoutMs,
          message: `Step "${stepName}" timed out after ${stepTimeoutMs}ms`,
        }),
    );
  }

  if (stepDef.retry) {
    raw = retryWithPolicy({ eff: raw, policy: stepDef.retry, clock });
  }

  // `onFailure` handles typed failures only; defects still fail the step.
  const strategy = stepDef.onFailure ?? "fail";
  if (strategy === "skip") {
    raw = raw.catch(() => succeed(undefined));
  } else if (strategy !== "fail" && "fallback" in strategy) {
    const fallbackFn = strategy.fallback;
    raw = raw.catch((err) => sync(() => fallbackFn(err)));
  }

  return raw;
}
