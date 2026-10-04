// ---------------------------------------------------------------------------
// `.mapOver()` — fan out over an array produced by an earlier step, one task
// row per element, results joined in order.
// ---------------------------------------------------------------------------

import { fail, forEachPar, suspend } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import { codecArray } from "@spilne/perfect-core/connect";
import { promiseOrDie } from "../../shared/eff.ts";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { SystemWallClock } from "../../shared/wall-clock.ts";
import { StepError } from "../durable-pipeline-error.ts";
import { withOptionalStepCache } from "../step-cache.ts";
import {
  asStepEff,
  type MapOverOptions,
  type MapStepContext,
  type StepDefinition,
  type StepEff,
} from "../step-definition.ts";
import { toStepPolicy, withStepRetry, withStepTimeout } from "../step-policy.ts";

export function createMapOverStep(params: {
  readonly name: string;
  /** The step whose result is the array to map over. */
  readonly array: string;
  readonly concurrency: number | undefined;
  readonly fn: (element: never, ctx: MapStepContext<never>) => unknown;
  readonly options: MapOverOptions<unknown> | undefined;
  /** The workflow's default codec. */
  readonly defaultCodec: Codec<unknown>;
  /** Default step-cache namespace (the workflow name). */
  readonly cacheNamespace: string;
}): StepDefinition {
  const { name, options } = params;
  const fn = params.fn as (element: unknown, ctx: MapStepContext<unknown>) => unknown;
  const elementOptions = options?.element;
  const elementCodec = (elementOptions?.codec ?? params.defaultCodec) as Codec<unknown>;
  const codec = (options?.codec ??
    (elementOptions?.codec
      ? codecArray(elementOptions.codec)
      : params.defaultCodec)) as Codec<unknown>;
  const concurrency = params.concurrency ?? Infinity;

  return {
    name,
    dependsOn: [params.array],
    kind: "map",
    codec,
    ...toStepPolicy(options),
    execute: (exec) => {
      const sourceArray = exec.results[params.array] as unknown[];
      if (!Array.isArray(sourceArray)) {
        return fail(
          new StepError({
            workflowId: exec.workflowId,
            stepName: name,
            message: `mapOver source "${params.array}" is not an array`,
          }),
        );
      }
      const clock = exec.clock ?? SystemWallClock;
      const stepAttempt = exec.attemptRef.current;

      const runElement = (element: unknown, taskIndex: number): StepEff<unknown, TaggedError> => {
        // Element retries re-enter this thunk; each entry is one attempt.
        let elementRetries = -1;
        let attemptEff: StepEff<unknown, TaggedError> = suspend(() => {
          elementRetries++;
          const ctx: MapStepContext<unknown> = {
            input: exec.input,
            workflowId: exec.workflowId,
            taskIndex,
            attempt: stepAttempt + elementRetries,
          };
          return asStepEff({ result: fn(element, ctx), stepName: name });
        });
        if (elementOptions?.timeoutMs != null) {
          attemptEff = withStepTimeout({
            eff: attemptEff,
            clock,
            ms: elementOptions.timeoutMs,
            workflowId: exec.workflowId,
            stepName: name,
            subject: `${name}[${taskIndex}]`,
          });
        }
        if (elementOptions?.retry) {
          attemptEff = withStepRetry({ eff: attemptEff, policy: elementOptions.retry, clock });
        }
        return attemptEff.flatMap((result) =>
          promiseOrDie(() =>
            exec.storage.saveTaskResult(
              {
                workflowId: exec.workflowId,
                stepName: name,
                taskIndex,
                result: elementCodec.encode(result),
              },
              exec.guard,
            ),
          ).as(result),
        );
      };

      const runAll = () =>
        forEachPar(sourceArray, runElement, {
          concurrency: Number.isFinite(concurrency) ? concurrency : "unbounded",
        }) as StepEff<unknown, TaggedError>;
      return withOptionalStepCache({
        cache: options?.cache,
        ctx: {
          input: exec.input,
          prev: sourceArray,
          workflowId: exec.workflowId,
          attempt: stepAttempt,
        },
        runBody: runAll,
        stepName: name,
        namespace: params.cacheNamespace,
        codec,
      });
    },
  };
}
