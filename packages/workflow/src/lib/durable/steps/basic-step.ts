// ---------------------------------------------------------------------------
// `.step()` / `.stepAsync()` — one user function as one DAG node.
// ---------------------------------------------------------------------------

import type { Codec } from "@spilne/perfect-core/connect";
import { withOptionalStepCache } from "../step-cache.ts";
import {
  asStepEff,
  linearStepContext,
  type DagStepContext,
  type StepContext,
  type StepDefinition,
  type StepOptions,
} from "../step-definition.ts";
import { toStepPolicy } from "../step-policy.ts";

export function createBasicStep(params: {
  readonly name: string;
  readonly dependsOn: string[];
  /**
   * A linear step (no explicit `dependsOn`) gets a `StepContext` with
   * `prev`; a DAG step gets a `DagStepContext` with the named `deps`.
   */
  readonly isLinear: boolean;
  readonly fn: (ctx: never) => unknown;
  readonly options: StepOptions<unknown> | undefined;
  readonly codec: Codec<unknown>;
  /** Default step-cache namespace (the workflow name). */
  readonly cacheNamespace: string;
}): StepDefinition {
  const { name, dependsOn, isLinear, options, codec } = params;
  const fn = params.fn as (ctx: unknown) => unknown;
  return {
    name,
    dependsOn,
    kind: "normal",
    codec,
    ...toStepPolicy(options),
    execute: (exec) => {
      let ctx: StepContext<unknown, unknown> | DagStepContext<unknown, Record<string, unknown>>;
      if (isLinear) {
        ctx = linearStepContext({ dependsOn, exec });
      } else {
        const deps: Record<string, unknown> = {};
        for (const dep of dependsOn) deps[dep] = exec.results[dep];
        ctx = {
          input: exec.input,
          deps,
          workflowId: exec.workflowId,
          attempt: exec.attemptRef.current,
        };
      }
      return withOptionalStepCache({
        cache: options?.cache,
        ctx: ctx as StepContext<unknown, unknown>,
        runBody: () => asStepEff({ result: fn(ctx), stepName: name }),
        stepName: name,
        namespace: params.cacheNamespace,
        codec,
      });
    },
  };
}

/**
 * `def` with `fn` applied to its result (the builder's `.map()`). The
 * transform runs inside the step body, so the checkpointed result is the
 * mapped value.
 */
export function mapStepResult(params: {
  readonly def: StepDefinition;
  readonly fn: (value: unknown) => unknown;
}): StepDefinition {
  const { def, fn } = params;
  const originalExecute = def.execute;
  return { ...def, execute: (exec) => originalExecute(exec).map(fn) };
}
