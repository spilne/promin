// ---------------------------------------------------------------------------
// `.mapOver()` — fan out over an array produced by an earlier step, one task
// row per element, results joined in order.
//
// Resume: every element that completes writes its task row (encoded with
// the element codec) before the map step settles. When the step runs again
// (a crash-resume, a workflow retry or a step-level retry), the elements
// that already have a completed task row are decoded from it and are not
// run again; only the missing or failed elements run.
// ---------------------------------------------------------------------------

import { fail, forEachPar, succeed, suspend, type Eff } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import { codecArray } from "@spilne/perfect-core/connect";
import { promiseOrDie } from "../../shared/eff.ts";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { SystemWallClock } from "../../shared/wall-clock.ts";
import { StepError } from "../durable-pipeline-error.ts";
import { withOptionalStepCache } from "../step-cache.ts";
import {
  asStepEff,
  currentStepState,
  type ExecuteParams,
  type MapOverOptions,
  type MapStepContext,
  type StepDefinition,
  type StepEff,
} from "../step-definition.ts";
import { isControlFlowExit, toStepPolicy, withStepRetry, withStepTimeout } from "../step-policy.ts";
import type { StepState } from "../workflow-state.ts";

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
        return attemptEff
          .catch((error: TaggedError): StepEff<unknown, TaggedError> => {
            // A typed failure that outlived the element's retries is recorded
            // on its task row; control flow passes through untouched.
            if (isControlFlowExit(error)) return fail(error);
            return promiseOrDie(() =>
              exec.storage.saveTaskFailure(
                {
                  workflowId: exec.workflowId,
                  stepName: name,
                  taskIndex,
                  error: failureMessage(error),
                },
                exec.guard,
              ),
            ).flatMap(() => fail(error));
          })
          .flatMap((result) =>
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

      const runAll = (): StepEff<unknown, TaggedError> =>
        savedElementResults({
          exec,
          stepName: name,
          length: sourceArray.length,
          codec: elementCodec,
        }).flatMap((saved): StepEff<unknown, TaggedError> => {
          const missing: number[] = [];
          for (let i = 0; i < sourceArray.length; i++) if (!saved.has(i)) missing.push(i);
          if (missing.length === 0) return succeed(sourceArray.map((_, i) => saved.get(i)));
          return forEachPar(missing, (i) => runElement(sourceArray[i], i), {
            concurrency: Number.isFinite(concurrency) ? concurrency : "unbounded",
          }).map((fresh) => {
            if (saved.size === 0) return fresh;
            const ran = new Map<number, unknown>();
            for (let k = 0; k < missing.length; k++) ran.set(missing[k]!, fresh[k]);
            return sourceArray.map((_, i) => (saved.has(i) ? saved.get(i) : ran.get(i)));
          }) as StepEff<unknown, TaggedError>;
        });

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

/**
 * Decoded results of the elements that already have a completed task row,
 * by task index. The first attempt reads the row the runner loaded with the
 * run; a later attempt (a step-level retry re-invokes `execute` with that
 * same row) reloads it so the elements finished by earlier attempts are
 * seen. A row that no longer decodes with the element codec counts as
 * missing, so its element runs again.
 */
function savedElementResults(params: {
  readonly exec: ExecuteParams;
  readonly stepName: string;
  readonly length: number;
  readonly codec: Codec<unknown>;
}): Eff<Map<number, unknown>> {
  const { exec, length, codec } = params;
  return currentStepState({
    exec,
    stepName: params.stepName,
    reload: exec.attemptRef.current > 1,
  }).map((row) => decodeCompletedTasks({ row, length, codec }));
}

function decodeCompletedTasks(params: {
  readonly row: StepState | undefined;
  readonly length: number;
  readonly codec: Codec<unknown>;
}): Map<number, unknown> {
  const saved = new Map<number, unknown>();
  for (const task of params.row?.tasks ?? []) {
    if (task.status !== "completed") continue;
    if (task.taskIndex < 0 || task.taskIndex >= params.length) continue;
    try {
      saved.set(task.taskIndex, params.codec.decode(task.result));
    } catch {
      // Undecodable (the element codec changed): run the element again.
    }
  }
  return saved;
}

function failureMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  const tag = (error as { readonly _tag?: unknown } | null | undefined)?._tag;
  return typeof tag === "string" ? tag : String(error);
}
