// ---------------------------------------------------------------------------
// `.dowhile()` / `.dountil()` — a durable loop. Each iteration is its own
// step row (`<name>.iter.<n>`); on resume, completed iteration rows are
// replayed and the loop continues at the first missing one.
// ---------------------------------------------------------------------------

import { Cause, eff, fail, succeed, suspend, type Eff } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import { promiseOrDie } from "../../shared/eff.ts";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { SystemWallClock } from "../../shared/wall-clock.ts";
import { LoopLimitExceededError, WorkflowError } from "../durable-pipeline-error.ts";
import {
  asStepEff,
  linearStepContext,
  type StepContext,
  type StepDefinition,
  type StepEff,
  type StepOptions,
} from "../step-definition.ts";
import { toStepPolicy } from "../step-policy.ts";
import { hasCapability } from "../workflow-storage.ts";

/**
 * Options for `.dowhile()` / `.dountil()` loops. The step-level fields
 * apply to the loop step as a whole (not per iteration): a `retry` restarts
 * the loop, which replays completed iterations from their rows and resumes
 * at the first missing one. No `cache` (iteration rows are the loop's memo).
 */
export interface LoopOptions<T, Input = unknown, Prev = unknown> extends Omit<
  StepOptions<T, Input, Prev>,
  "cache"
> {
  /**
   * Safety cap on the number of iterations. When the loop runs this many
   * times without the exit condition being satisfied, the loop step fails
   * with `LoopLimitExceededError`. Default: 100.
   */
  readonly maxIterations?: number;
}

/** Default `LoopOptions.maxIterations`. */
const DEFAULT_MAX_ITERATIONS = 100;

/**
 * The loop runs as one `Eff`: each iteration's body and its row writes are
 * steps of that effect, so interrupting the loop step (its `timeoutMs`, or
 * the run being abandoned) stops it before the next iteration and no
 * iteration row is written afterwards. A typed failure of the body stays
 * typed (step `retry` / `onFailure` apply); a throw or rejection is a
 * defect. Either way the iteration's failure row is written before the loop
 * step fails.
 */
export function createLoopStep(params: {
  readonly name: string;
  readonly dependsOn: string[];
  /** Returns the iteration's `Eff` (or, via `asStepEff`, a Promise of its value). */
  readonly body: (ctx: StepContext<never, never>, iter: number) => unknown;
  /** `true` runs another iteration. */
  readonly keepGoing: (result: never, iter: number) => boolean;
  readonly options: LoopOptions<unknown> | undefined;
  readonly codec: Codec<unknown>;
  /** The Promise-body method named in the defect for a non-Eff result. */
  readonly asyncVariant: string;
}): StepDefinition {
  const { name, dependsOn, codec, options } = params;
  const body = params.body as (ctx: StepContext<unknown, unknown>, iter: number) => unknown;
  const keepGoing = params.keepGoing as (result: unknown, iter: number) => boolean;
  const maxIterations = options?.maxIterations ?? DEFAULT_MAX_ITERATIONS;
  if (maxIterations < 1) {
    throw new WorkflowError({
      workflowId: "",
      message: `.dowhile / .dountil("${name}", ...): maxIterations must be >= 1`,
    });
  }

  return {
    name,
    dependsOn,
    kind: "loop",
    codec,
    ...toStepPolicy(options),
    execute: (exec) => {
      const ctx = linearStepContext({ dependsOn, exec });
      const { storage, guard, workflowId } = exec;
      const attemptStorage = hasCapability(storage, "stepAttempts") ? storage : undefined;
      const clock = exec.clock ?? SystemWallClock;

      /** Write an iteration's step row and (when supported) its attempt row. */
      const recordIteration = (row: {
        readonly stepName: string;
        readonly startedAt: Date;
        readonly outcome:
          | { readonly status: "completed"; readonly result: unknown }
          | { readonly status: "failed"; readonly error: string };
      }): Eff<void> =>
        promiseOrDie(async () => {
          const { stepName, startedAt, outcome } = row;
          const durationMs = clock.currentTimeMs() - startedAt.getTime();
          if (outcome.status === "completed") {
            await storage.saveStepResult({
              workflowId,
              stepName,
              result: outcome.result,
              durationMs,
              startedAt,
              guard,
            });
          } else {
            await storage.saveStepFailure({
              workflowId,
              stepName,
              error: outcome.error,
              durationMs,
              startedAt,
              guard,
            });
          }
          await attemptStorage?.saveStepAttempt({
            record: {
              workflowId,
              stepName,
              attempt: 1,
              type: "execution",
              ...outcome,
              durationMs,
              startedAt,
              completedAt: clock.now(),
            },
            guard,
          });
        });

      const runIteration = (iter: number): StepEff<unknown, TaggedError> =>
        suspend(() => {
          const stepName = `${name}.iter.${iter}`;
          const startedAt = clock.now();
          return suspend(() =>
            asStepEff({
              result: body(ctx, iter),
              stepName: name,
              asyncVariant: params.asyncVariant,
            }),
          )
            .tapErrorCause((cause) => {
              // Interrupted (step timeout, abandoned run): write nothing more.
              if (Cause.isInterruptedOnly(cause)) return succeed(undefined);
              const error = Cause.squash(cause);
              return recordIteration({
                stepName,
                startedAt,
                outcome: {
                  status: "failed",
                  error: error instanceof Error ? error.message : String(error),
                },
              });
            })
            .flatMap((result) =>
              recordIteration({
                stepName,
                startedAt,
                outcome: { status: "completed", result: codec.encode(result) },
              }).as(result),
            );
        });

      return eff(function* () {
        let result: unknown = undefined;
        let iter = 0;

        // Crash resume: replay completed `<name>.iter.<n>` rows in order,
        // re-evaluating the condition against each persisted result, and
        // resume from the first missing iteration. Already-run iterations
        // are never re-executed.
        const state = yield* promiseOrDie(() => storage.loadWorkflow(workflowId));
        if (state) {
          while (iter < maxIterations) {
            const row = state.steps[`${name}.iter.${iter}`];
            if (!row || row.status !== "completed") break;
            result = codec.decode(row.result);
            iter++;
            // The previous run already decided to stop here.
            if (!keepGoing(result, iter - 1)) return result;
          }
        }

        while (true) {
          if (iter >= maxIterations) {
            return yield* fail(
              new LoopLimitExceededError({
                workflowId,
                stepName: name,
                maxIterations,
                message: `Loop "${name}" exceeded ${maxIterations} iterations without converging`,
              }),
            );
          }
          const current = iter;
          result = yield* runIteration(current);
          iter++;
          if (!keepGoing(result, current)) return result;
        }
      }) as StepEff<unknown, TaggedError>;
    },
  };
}
