// ---------------------------------------------------------------------------
// Inline wave — runs one wave of ready steps in-process, concurrently,
// applying each step's policies and checkpointing each step's outcome as
// soon as that step settles (and its `step-started` notice has gone out).
// ---------------------------------------------------------------------------

import { stepRuntimeFor, type WaveOutcome, type WaveParams } from "./dag-context.ts";
import { runStepBody } from "./step-body.ts";
import { createStepProgressReporter } from "./step-progress.ts";
import { checkpointStepOutcome } from "./step-checkpoint.ts";
import { fireHook } from "./hooks.ts";
import { emitTrace } from "../workflow-tracer.ts";
import { traceStepAttemptStart } from "./step-tracing.ts";
import { outcomeOfBody, settleWave, skippedOutcome } from "./wave.ts";

/**
 * Run `readySteps` inline and wait for all of them. Typed failures and
 * defects (a rejected step Promise, a synchronous throw) both settle as
 * that step's `failed` outcome; siblings run to completion and keep their
 * own rows.
 */
export async function runInlineWave(params: WaveParams): Promise<WaveOutcome> {
  const { ctx, workflowId, input, readySteps, results, clock, stepStates, workflowMetadata } =
    params;

  return settleWave({
    readySteps,
    runStep: async (stepDef) => {
      const skipped = skippedOutcome({
        stepDef,
        input,
        results,
        clock,
        attempt: params.stepAttempts.get(stepDef.name) ?? 1,
      });
      if (skipped) {
        await params.stepStarted?.get(stepDef.name);
        return checkpointStepOutcome({ ctx, clock, workflowId, outcome: skipped });
      }

      const startedAt = clock.now();
      const progress = createStepProgressReporter({
        storage: ctx.storage,
        workflowId,
        stepName: stepDef.name,
        ...(ctx.guard !== undefined && { guard: ctx.guard }),
        clock,
      });
      // Attempt numbers continue across workflow retries.
      const body = await runStepBody({
        ...((ctx.tracer !== undefined || progress !== undefined) && {
          onAttemptStart: ({ attempt, startedAt: attemptStartedAt }) => {
            progress?.report({
              phase: "running",
              attempt,
              at: attemptStartedAt,
              ...(ctx.executorId !== undefined && { holder: ctx.executorId }),
            });
            if (ctx.tracer !== undefined) {
              traceStepAttemptStart({
                ctx,
                stepName: stepDef.name,
                attempt,
                startedAt: attemptStartedAt,
              });
            }
          },
        }),
        ...(progress !== undefined && {
          onRetryScheduled: ({ attempt, at }) =>
            progress.report({
              phase: "backoff",
              attempt,
              at: clock.now(),
              nextAttemptAt: at,
            }),
        }),
        stepDef,
        workflowId,
        clock,
        firstAttempt: (params.stepAttempts.get(stepDef.name) ?? 0) + 1,
        ...(ctx.signal !== undefined && { signal: ctx.signal }),
        execute: ({ attempt, metadataRef, signal }) =>
          stepDef.execute({
            ...stepRuntimeFor({
              ctx,
              clock,
              stepStates,
              stepName: stepDef.name,
              usageByStep: params.usageByStep,
              workflowMetadata,
            }),
            // Inline only: a function does not cross to a queue worker, which
            // falls back to the default `console.warn`.
            ...(ctx.hooks?.onMetadataWriteError !== undefined && {
              onMetadataWriteError: ({ error, keys }) =>
                void fireHook({
                  hooks: ctx.hooks,
                  name: "onMetadataWriteError",
                  event: { workflowId, stepName: stepDef.name, error, keys },
                }),
            }),
            ...(ctx.tracer !== undefined &&
              ctx.traceRef !== undefined && {
                onUpdate: (event) =>
                  emitTrace(ctx.tracer!, "update", { ...ctx.traceRef!, ...event, at: clock.now() }),
              }),
            input,
            results,
            workflowId,
            storage: ctx.storage,
            attemptRef: { current: attempt },
            metadataRef,
            signal,
          }),
      });
      params.stepAttempts.set(stepDef.name, body.attempt);
      // No progress record lands after the step's own write.
      await progress?.drain();
      const outcome = outcomeOfBody({ name: stepDef.name, body, startedAt, clock });

      // The step's `step-started` notice goes out before its outcome row.
      await params.stepStarted?.get(stepDef.name);
      return checkpointStepOutcome({
        ctx,
        clock,
        workflowId,
        outcome,
        failedAttempts:
          body.kind === "completed" || body.kind === "failed" ? body.failedAttempts : [],
      });
    },
  });
}
