// ---------------------------------------------------------------------------
// Inline wave — runs one wave of ready steps in-process, concurrently,
// applying each step's policies and checkpointing each step's outcome as
// soon as that step settles.
// ---------------------------------------------------------------------------

import { stepRuntimeFor, type WaveOutcome, type WaveParams } from "./dag-context.ts";
import { runStepBody } from "./step-body.ts";
import { checkpointStepOutcome } from "./step-checkpoint.ts";
import { outcomeOfBody, settleWave, skippedOutcome } from "./wave.ts";

/**
 * Run `readySteps` inline and wait for all of them. Typed failures and
 * defects (a `.stepAsync()` rejection, a synchronous throw) both settle as
 * that step's `failed` outcome; siblings run to completion and keep their
 * own rows.
 */
export async function runInlineWave(params: WaveParams): Promise<WaveOutcome> {
  const { ctx, workflowId, input, readySteps, results, clock, stepStates } = params;

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
      if (skipped) return checkpointStepOutcome({ ctx, clock, workflowId, outcome: skipped });

      const startedAt = clock.now();
      // Attempt numbers continue across workflow retries.
      const body = await runStepBody({
        stepDef,
        workflowId,
        clock,
        firstAttempt: (params.stepAttempts.get(stepDef.name) ?? 0) + 1,
        execute: ({ attempt, metadataRef }) =>
          stepDef.execute({
            ...stepRuntimeFor({ ctx, clock, stepStates, stepName: stepDef.name }),
            input,
            results,
            workflowId,
            storage: ctx.storage,
            attemptRef: { current: attempt },
            metadataRef,
          }),
      });
      params.stepAttempts.set(stepDef.name, body.attempt);

      return checkpointStepOutcome({
        ctx,
        clock,
        workflowId,
        outcome: outcomeOfBody({ name: stepDef.name, body, startedAt, clock }),
        failedAttempts:
          body.kind === "completed" || body.kind === "failed" ? body.failedAttempts : [],
      });
    },
  });
}
