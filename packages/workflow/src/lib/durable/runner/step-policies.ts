// ---------------------------------------------------------------------------
// Step policies — wraps one step invocation with the step's own timeout,
// retry and `onFailure` strategy. Shared by the inline wave and
// `InProcessStepExecutor` so both paths apply the same semantics.
// ---------------------------------------------------------------------------

import {
  async,
  fail,
  race,
  succeed,
  suspend,
  sync,
  type Eff,
  type Throws,
} from "@spilne/perfect-core";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { retryWithPolicy } from "../../shared/retry-policy.ts";
import type { WallClock } from "../../shared/wall-clock.ts";
import type { StepDefinition } from "../durable-pipeline.ts";
import { StepTimeoutError } from "../durable-pipeline-error.ts";

/**
 * Wrap one step invocation with the step's own policies, in order:
 * per-attempt timeout, retry on typed failures, then the `onFailure`
 * strategy. `invoke` runs again on every retry. A step that does not settle
 * within `timeoutMs` (measured on `clock`) fails with `StepTimeoutError`;
 * the abandoned attempt is interrupted. Retry backoff sleeps on `clock`.
 * `onAttemptFailed` sees each attempt's typed failure before retry does.
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
    const stepTimeoutMs = stepDef.timeoutMs;
    const stepName = stepDef.name;
    // Raced against a timer on the injected WallClock (not perfect's own
    // Clock service), so a FakeWallClock drives step timeouts too. The loser
    // is interrupted: a body that settles first clears the timer.
    raw = race([
      raw,
      wallClockSleep({ clock, ms: stepTimeoutMs }).flatMap(() =>
        fail(
          new StepTimeoutError({
            workflowId,
            stepName,
            timeoutMs: stepTimeoutMs,
            message: `Step "${stepName}" timed out after ${stepTimeoutMs}ms`,
          }),
        ),
      ),
    ]);
  }

  const onAttemptFailed = params.onAttemptFailed;
  if (onAttemptFailed) {
    raw = raw.catch((err) => {
      onAttemptFailed(err);
      return fail(err) as Eff<never, Throws<TaggedError>>;
    });
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

/**
 * Wait `ms` on a `WallClock` timer. Interruption clears the timer, so a
 * step that settles first leaves nothing pending on the clock.
 */
function wallClockSleep(params: { clock: WallClock; ms: number }): Eff<void> {
  return async<void>((resume) => {
    const handle = params.clock.setTimeout(() => resume(succeed(undefined)), params.ms);
    return () => handle.clear();
  }).orDie();
}
