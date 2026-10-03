// ---------------------------------------------------------------------------
// Inline wave — runs one wave of ready steps in-process as a single
// concurrent `Eff`, applying each step's policies and saving each step's
// result (and attempt row) as soon as that step finishes.
// ---------------------------------------------------------------------------

import { all, succeed, type Eff, type Throws } from "@spilne/perfect-core";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { promiseOrDie, runEffSafe } from "../../shared/eff.ts";
import { isStepAttemptStorage } from "../workflow-storage.ts";
import {
  stepRuntimeFor,
  type LocalStepResult,
  type WaveOutcome,
  type WaveParams,
} from "./dag-context.ts";
import { applyStepPolicies } from "./step-policies.ts";

/**
 * Run `readySteps` inline. Defects (a `.stepAsync()` rejection, a
 * synchronous throw) land in `batchError` next to typed failures. Skipped
 * steps are returned unsaved and persisted by the DAG executor after the
 * wave.
 *
 * `stepMetadata` receives the audit metadata a step kind sets before its
 * body runs (e.g. `.match()`'s chosen case), so the failure path can
 * persist it even when that body throws.
 */
export async function runInlineWave(
  params: WaveParams & { readonly stepMetadata: Map<string, Record<string, unknown>> },
): Promise<WaveOutcome> {
  const { ctx, workflowId, input, readySteps, results, clock, stepMetadata, stepStates } = params;

  const batch = all(
    readySteps.map((stepDef): Eff<LocalStepResult, Throws<TaggedError>> => {
      // Evaluate skipWhen before entering the step execution Eff
      if (stepDef.skipWhen) {
        const prevStepName = stepDef.dependsOn[0];
        const prev = prevStepName != null ? results[prevStepName] : input;
        if (stepDef.skipWhen(prev)) {
          const skipResult = stepDef.skipValue ? stepDef.skipValue(prev) : prev;
          const encoded = stepDef.codec.encode(skipResult);
          return succeed({
            name: stepDef.name,
            result: encoded,
            durationMs: 0,
            startedAt: clock.now(),
            skipped: true as const,
          });
        }
      }

      const startedAt = clock.now();
      const startTime = startedAt.getTime();

      // Get or initialize attempt counter for this step (persists across workflow retries)
      const currentAttemptForStep = params.stepAttempts.get(stepDef.name) ?? 0;
      const attemptRef = { current: currentAttemptForStep + 1 };
      // Shared audit-metadata slot — `.match()` fills it at selector time;
      // the map() below forwards it onto the stepResult shape. Fresh per
      // step (not per attempt) so a retry overwrites rather than appends.
      const metadataRef: { current?: Record<string, unknown> } = { current: undefined };

      // Raw step execution — wrapped in suspend so retry re-invokes the step fn.
      // attemptRef tracks the attempt number; incremented each invocation so
      // step retries and workflow retries both see monotonically increasing attempts.
      const raw = applyStepPolicies({
        stepDef,
        workflowId,
        clock,
        invoke: () => {
          const currentAttempt = attemptRef.current;
          attemptRef.current = currentAttempt + 1;
          // Write back to shared map so workflow retries pick up the right count
          params.stepAttempts.set(stepDef.name, currentAttempt);
          const executed = stepDef.execute({
            ...stepRuntimeFor({ ctx, clock, stepStates, stepName: stepDef.name }),
            input,
            results,
            workflowId,
            storage: ctx.storage,
            attemptRef: { current: currentAttempt },
            metadataRef,
          });
          // Kinds that set metadata synchronously in their execute (e.g.
          // `.match()` after selector resolution) surface it here BEFORE
          // the branch Eff runs. The failure path can then read the
          // map by step name even when the branch throws.
          if (metadataRef.current) {
            stepMetadata.set(stepDef.name, metadataRef.current);
          }
          return executed;
        },
      });

      // Map to step result, then save inside the step's own Eff so each
      // step's `completedAt` reflects when that step finished rather than
      // when the slowest sibling did.
      return raw
        .map((result) => {
          const encoded = stepDef.codec.encode(result);
          return {
            name: stepDef.name,
            result: encoded,
            metadata: metadataRef.current,
            durationMs: clock.currentTimeMs() - startTime,
            startedAt,
          };
        })
        .flatMap((stepResult) =>
          promiseOrDie(async () => {
            await ctx.storage.saveStepResult(
              {
                workflowId,
                stepName: stepResult.name,
                result: stepResult.result,
                metadata: stepResult.metadata,
                durationMs: stepResult.durationMs,
                startedAt: stepResult.startedAt,
              },
              ctx.guard,
            );
            if (isStepAttemptStorage(ctx.storage)) {
              await ctx.storage.saveStepAttempt(
                {
                  workflowId,
                  stepName: stepResult.name,
                  attempt: params.stepAttempts.get(stepResult.name) ?? 1,
                  type: "execution",
                  status: "completed",
                  result: stepResult.result,
                  durationMs: stepResult.durationMs,
                  startedAt: stepResult.startedAt,
                  completedAt: clock.now(),
                  ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
                },
                ctx.guard,
              );
            }
            return { ...stepResult, storageAlreadyCheckpointed: true };
          }),
        );
    }),
  );

  // `catchDefects` lands defects (a `.stepAsync()` rejection, a
  // synchronous throw in a step body) in the `error` channel next to
  // typed failures. Without it they would escape unobserved, skip the
  // `saveStepFailure` path, and leave the workflow stuck in `pending`.
  // The failure path already handles arbitrary Error instances, so both
  // kinds share one failure path.
  const { data, error } = await runEffSafe(batch, { catchDefects: true });
  return { batchResults: data as LocalStepResult[] | null, batchError: error ?? null };
}
