// ---------------------------------------------------------------------------
// Executor wave — runs one wave of ready steps through the configured
// `StepExecutor`, concurrently. The runner keeps `skipWhen`, attempt
// counting, concurrency resolution and the per-step checkpoint; the
// executor runs the body.
// ---------------------------------------------------------------------------

import { StepError } from "../durable-pipeline-error.ts";
import { isStepAttemptStorage } from "../workflow-storage.ts";
import type { LocalStepResult, WaveOutcome, WaveParams } from "./dag-context.ts";
import { resolveStepConcurrency } from "./step-concurrency.ts";
import type { StepExecutionRequest } from "./step-executor.ts";

/**
 * Run `readySteps` through `ctx.stepExecutor`. Each step saves its result
 * (and attempt row) the moment its body resolves, unless the executor
 * reports it already checkpointed the step. Skipped steps are returned
 * unsaved and persisted by the DAG executor after the wave.
 */
export async function runExecutorWave(params: WaveParams): Promise<WaveOutcome> {
  const { ctx, workflowId, input, readySteps, results, clock } = params;
  let batchResults: LocalStepResult[] | null = null;
  let batchError: unknown = null;

  try {
    batchResults = await Promise.all(
      readySteps.map(async (stepDef): Promise<LocalStepResult> => {
        if (stepDef.skipWhen) {
          const prevStepName = stepDef.dependsOn[0];
          const prev = prevStepName != null ? results[prevStepName] : input;
          if (stepDef.skipWhen(prev)) {
            const skipResult = stepDef.skipValue ? stepDef.skipValue(prev) : prev;
            return {
              name: stepDef.name,
              result: stepDef.codec.encode(skipResult),
              durationMs: 0,
              startedAt: clock.now(),
              skipped: true,
            };
          }
        }

        const startedAt = clock.now();
        const startTime = startedAt.getTime();
        const currentAttempt = (params.stepAttempts.get(stepDef.name) ?? 0) + 1;
        params.stepAttempts.set(stepDef.name, currentAttempt);

        // Resolve per-task concurrency cap. Step-level wins over the
        // workflow-level default. The key fn is evaluated against the
        // step's input ctx; the resolved string + scope + limit are
        // stamped on the dispatched task so workers don't re-evaluate.
        const concurrency = resolveStepConcurrency({
          workflowName: ctx.workflowName,
          workflowQueue: ctx.workflowQueue,
          stepDef,
          workflowInput: input,
          stepInput: (() => {
            const prevStepName = stepDef.dependsOn[0];
            return prevStepName != null ? results[prevStepName] : input;
          })(),
          workflowId,
          attempt: currentAttempt,
          results,
        });

        const req: StepExecutionRequest = {
          workflowId,
          stepName: stepDef.name,
          input,
          prevResults: { ...results },
          attempt: currentAttempt,
          needs: stepDef.needs,
          priority: stepDef.priority,
          ...(concurrency
            ? {
                concurrencyKey: concurrency.key,
                concurrencyScope: concurrency.scope,
                concurrencyLimit: concurrency.limit,
              }
            : {}),
        };
        const res = await ctx.stepExecutor!.executeStep(req);
        if (!res.ok) {
          throw new StepError({ workflowId, stepName: stepDef.name, message: res.error });
        }
        const durationMs = clock.currentTimeMs() - startTime;

        // Save here unless the executor already wrote the step row
        // (Postgres step-queue / coordinator path sets
        // `storageAlreadyCheckpointed`). Either way the returned result
        // is flagged as checkpointed so the DAG executor does not save it
        // again.
        if (!res.storageAlreadyCheckpointed) {
          await ctx.storage.saveStepResult(
            {
              workflowId,
              stepName: stepDef.name,
              result: res.result,
              metadata: res.metadata,
              durationMs,
              startedAt,
            },
            ctx.guard,
          );
          if (isStepAttemptStorage(ctx.storage)) {
            await ctx.storage.saveStepAttempt(
              {
                workflowId,
                stepName: stepDef.name,
                attempt: currentAttempt,
                type: "execution",
                status: "completed",
                result: res.result,
                durationMs,
                startedAt,
                completedAt: clock.now(),
                ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
              },
              ctx.guard,
            );
          }
        }
        return {
          name: stepDef.name,
          result: res.result,
          metadata: res.metadata,
          storageAlreadyCheckpointed: true,
          durationMs,
          startedAt,
        };
      }),
    );
  } catch (err) {
    batchError = err;
  }

  return { batchResults, batchError };
}
