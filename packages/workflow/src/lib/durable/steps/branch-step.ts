// ---------------------------------------------------------------------------
// `.branch()` — two-way conditional: one DAG node that runs `ifTrue` or
// `ifFalse` depending on `condition(prev)`.
// ---------------------------------------------------------------------------

import type { Codec } from "@spilne/perfect-core/connect";
import { withOptionalStepCache } from "../step-cache.ts";
import {
  asStepEff,
  linearStepContext,
  type StepDefinition,
  type StepOptions,
} from "../step-definition.ts";
import { toStepPolicy } from "../step-policy.ts";

export function createBranchStep(params: {
  readonly name: string;
  readonly dependsOn: string[];
  readonly condition: (value: never) => boolean;
  readonly ifTrue: (ctx: never) => unknown;
  readonly ifFalse: (ctx: never) => unknown;
  readonly options: StepOptions<unknown> | undefined;
  readonly codec: Codec<unknown>;
  /** Default step-cache namespace (the workflow name). */
  readonly cacheNamespace: string;
}): StepDefinition {
  const { name, dependsOn, options, codec } = params;
  const condition = params.condition as (value: unknown) => boolean;
  const ifTrue = params.ifTrue as (ctx: unknown) => unknown;
  const ifFalse = params.ifFalse as (ctx: unknown) => unknown;
  return {
    name,
    dependsOn,
    kind: "branch",
    codec,
    ...toStepPolicy(options),
    execute: (exec) => {
      const ctx = linearStepContext({ dependsOn, exec });
      return withOptionalStepCache({
        cache: options?.cache,
        ctx,
        // `condition` runs on every attempt (and only on a cache miss).
        runBody: () => {
          const branch = condition(ctx.prev) ? ifTrue : ifFalse;
          return asStepEff({ result: branch(ctx), stepName: name });
        },
        stepName: name,
        namespace: params.cacheNamespace,
        codec,
      });
    },
  };
}
