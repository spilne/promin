// ---------------------------------------------------------------------------
// `.step()` / `.stepAsync()` — one user function as one DAG node.
// ---------------------------------------------------------------------------

import { sync } from "@spilne/perfect-core";
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
        // `cache.key` sees `prev` for a DAG step too (its first dependency).
        ctx: isLinear
          ? (ctx as StepContext<unknown, unknown>)
          : linearStepContext({ dependsOn, exec }),
        runBody: () => asStepEff({ result: fn(ctx), stepName: name }),
        stepName: name,
        namespace: params.cacheNamespace,
        codec,
      });
    },
  };
}

/**
 * The pure step `.map()` adds after `head`: it applies `fn` to the head's
 * result and checkpoints the mapped value with `codec` (the workflow
 * codec). The head step keeps its own result, codec and options, so its
 * `skipValue`, `onFailure` fallback and cache hits are mapped as well, and
 * its `compensate` receives the unmapped result. A throw from `fn` is a
 * defect.
 */
export function createTransformStep(params: {
  readonly name: string;
  readonly head: string;
  readonly fn: (value: unknown) => unknown;
  readonly codec: Codec<unknown>;
}): StepDefinition {
  const { head, fn } = params;
  return {
    name: params.name,
    dependsOn: [head],
    kind: "transform",
    codec: params.codec,
    execute: (exec) => sync(() => fn(exec.results[head])),
  };
}
