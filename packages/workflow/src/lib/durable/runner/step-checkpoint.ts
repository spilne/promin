// ---------------------------------------------------------------------------
// Step checkpoint — the one place a wave's step outcome reaches storage: the
// step row (result or failure) and one `execution` attempt row per attempt.
// Both waves call it as each step settles, so a step's row and timings
// reflect that step, not the wave.
// ---------------------------------------------------------------------------

import type { WallClock } from "../../shared/wall-clock.ts";
import { isStepAttemptStorage } from "../workflow-storage.ts";
import type { DagExecutionContext } from "./dag-context.ts";
import { errorMessage, type StepAttemptFailure } from "./step-body.ts";

/**
 * What one step of a wave came to. `result` is codec-encoded. A `failed`
 * outcome always names the step that failed; `error` is the original error
 * (a typed failure, a defect, or a `StepError` built from an executor's
 * report).
 */
export type StepOutcome =
  | {
      readonly kind: "completed";
      readonly name: string;
      readonly result: unknown;
      readonly metadata?: Record<string, unknown>;
      readonly startedAt: Date;
      readonly durationMs: number;
      readonly attempt: number;
      /** `skipWhen` matched: the body never ran. */
      readonly skipped?: true;
    }
  | {
      readonly kind: "failed";
      readonly name: string;
      readonly error: unknown;
      readonly metadata?: Record<string, unknown>;
      readonly startedAt: Date;
      readonly durationMs: number;
      readonly attempt: number;
    }
  | { readonly kind: "suspended"; readonly name: string; readonly error: unknown }
  | { readonly kind: "continue-as-new"; readonly name: string; readonly error: unknown };

/**
 * Persist a settled step and return its final outcome.
 *
 * - `completed`: one failed attempt row per `failedAttempts` entry, the step
 *   result, then a completed attempt row for the last attempt (unless that
 *   attempt already failed and `onFailure` absorbed it). If any of these
 *   writes fails, the step is recorded as failed with the storage error,
 *   which the workflow-level retry then sees.
 * - `failed`: the failed attempt rows, the step failure, and a failed
 *   attempt row for the last attempt unless `failedAttempts` already has it.
 *   A write that fails here rejects.
 * - `suspended` / `continue-as-new`: nothing; the step kind wrote its own
 *   rows.
 *
 * `checkpointed` skips every write: the executor already persisted the step
 * (the step-queue worker writes the step row and its attempt row).
 */
export async function checkpointStepOutcome(params: {
  readonly ctx: DagExecutionContext;
  readonly clock: WallClock;
  readonly workflowId: string;
  readonly outcome: StepOutcome;
  readonly failedAttempts?: readonly StepAttemptFailure[];
  readonly checkpointed?: boolean;
}): Promise<StepOutcome> {
  const { ctx, clock, workflowId, outcome } = params;
  if (params.checkpointed === true) return outcome;
  if (outcome.kind === "suspended" || outcome.kind === "continue-as-new") return outcome;
  const failedAttempts = params.failedAttempts ?? [];
  const lastAttemptRecorded = failedAttempts.some((a) => a.attempt === outcome.attempt);

  if (outcome.kind === "completed") {
    try {
      await saveFailedAttempts({ ctx, clock, workflowId, stepName: outcome.name, failedAttempts });
      await ctx.storage.saveStepResult(
        {
          workflowId,
          stepName: outcome.name,
          result: outcome.result,
          metadata: outcome.metadata,
          durationMs: outcome.durationMs,
          startedAt: outcome.startedAt,
        },
        ctx.guard,
      );
      if (!lastAttemptRecorded && isStepAttemptStorage(ctx.storage)) {
        await ctx.storage.saveStepAttempt(
          {
            workflowId,
            stepName: outcome.name,
            attempt: outcome.attempt,
            type: "execution",
            status: "completed",
            result: outcome.result,
            durationMs: outcome.durationMs,
            startedAt: outcome.startedAt,
            completedAt: clock.now(),
            ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
          },
          ctx.guard,
        );
      }
      return outcome;
    } catch (storageError) {
      // The body succeeded but its checkpoint did not. Recorded as a step
      // failure so the run fails (or retries) instead of continuing past an
      // unsaved step.
      const failed: StepOutcome = {
        kind: "failed",
        name: outcome.name,
        error: storageError,
        metadata: outcome.metadata,
        startedAt: outcome.startedAt,
        durationMs: outcome.durationMs,
        attempt: outcome.attempt,
      };
      await saveFailure({
        ctx,
        clock,
        workflowId,
        outcome: failed,
        saveAttempt: !lastAttemptRecorded,
      });
      return failed;
    }
  }

  await saveFailedAttempts({ ctx, clock, workflowId, stepName: outcome.name, failedAttempts });
  await saveFailure({ ctx, clock, workflowId, outcome, saveAttempt: !lastAttemptRecorded });
  return outcome;
}

async function saveFailure(params: {
  ctx: DagExecutionContext;
  clock: WallClock;
  workflowId: string;
  outcome: Extract<StepOutcome, { kind: "failed" }>;
  saveAttempt: boolean;
}): Promise<void> {
  const { ctx, clock, workflowId, outcome } = params;
  const error = errorMessage(outcome.error);
  await ctx.storage.saveStepFailure(
    {
      workflowId,
      stepName: outcome.name,
      error,
      durationMs: outcome.durationMs,
      startedAt: outcome.startedAt,
      metadata: outcome.metadata,
    },
    ctx.guard,
  );
  if (params.saveAttempt && isStepAttemptStorage(ctx.storage)) {
    await ctx.storage.saveStepAttempt(
      {
        workflowId,
        stepName: outcome.name,
        attempt: outcome.attempt,
        type: "execution",
        status: "failed",
        error,
        durationMs: outcome.durationMs,
        startedAt: outcome.startedAt,
        completedAt: clock.now(),
        ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
      },
      ctx.guard,
    );
  }
}

async function saveFailedAttempts(params: {
  ctx: DagExecutionContext;
  clock: WallClock;
  workflowId: string;
  stepName: string;
  failedAttempts: readonly StepAttemptFailure[];
}): Promise<void> {
  const { ctx, workflowId, stepName } = params;
  if (!isStepAttemptStorage(ctx.storage)) return;
  for (const failed of params.failedAttempts) {
    await ctx.storage.saveStepAttempt(
      {
        workflowId,
        stepName,
        attempt: failed.attempt,
        type: "execution",
        status: "failed",
        error: failed.error,
        durationMs: failed.durationMs,
        startedAt: failed.startedAt,
        completedAt: new Date(failed.startedAt.getTime() + failed.durationMs),
        ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
      },
      ctx.guard,
    );
  }
}
