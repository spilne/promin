// ---------------------------------------------------------------------------
// Step policies — wraps one step invocation with the step's own timeout,
// retry and `onFailure` strategy. Shared by the inline wave and
// `InProcessStepExecutor` so both paths apply the same semantics.
// ---------------------------------------------------------------------------

import { fail, suspend, type Eff, type Throws } from "@spilne/perfect-core";
import type { TaggedError } from "../../shared/tagged-error.ts";
import type { WallClock } from "../../shared/wall-clock.ts";
import type { StepDefinition } from "../durable-pipeline.ts";
import {
  isControlFlowExit,
  withFailureStrategy,
  withStepRetry,
  withStepTimeout,
} from "../step-policy.ts";

/**
 * Wrap one step invocation with the step's own policies, in order:
 * per-attempt timeout, retry on typed failures, then the `onFailure`
 * strategy. `invoke` runs again on every retry. A step that does not settle
 * within `timeoutMs` (measured on `clock`) fails with `StepTimeoutError`;
 * the abandoned attempt is interrupted. Retry backoff sleeps on `clock`.
 * `onAttemptFailed` sees each attempt's typed failure before retry does
 * (engine control flow is not an attempt failure and skips it).
 *
 * Retry and `onFailure` act on typed failures only. Defects (a throw from a
 * step's synchronous callback, a rejected `.stepAsync()` body) and engine
 * control flow (suspension, continue-as-new, tripwire, lost lock) pass
 * through untouched.
 */
export function applyStepPolicies(params: {
  stepDef: StepDefinition;
  workflowId: string;
  clock: WallClock;
  invoke: () => Eff<unknown, Throws<TaggedError>>;
  /**
   * Called once per attempt that fails with a typed error (a timeout
   * included), before retry decides whether to run another. Defects do not
   * reach it.
   */
  onAttemptFailed?: (error: unknown) => void;
}): Eff<unknown, Throws<TaggedError>> {
  const { stepDef, workflowId, clock } = params;
  let raw: Eff<unknown, Throws<TaggedError>> = suspend(params.invoke);

  if (stepDef.timeoutMs != null) {
    // Raced against a timer on the injected WallClock (not perfect's own
    // Clock service), so a FakeWallClock drives step timeouts too.
    raw = withStepTimeout({
      eff: raw,
      clock,
      ms: stepDef.timeoutMs,
      workflowId,
      stepName: stepDef.name,
    });
  }

  const onAttemptFailed = params.onAttemptFailed;
  if (onAttemptFailed) {
    raw = raw.catch((err) => {
      if (!isControlFlowExit(err)) onAttemptFailed(err);
      return fail(err) as Eff<never, Throws<TaggedError>>;
    });
  }

  if (stepDef.retry) {
    raw = withStepRetry({ eff: raw, policy: stepDef.retry, clock });
  }

  return withFailureStrategy({ eff: raw, strategy: stepDef.onFailure });
}
