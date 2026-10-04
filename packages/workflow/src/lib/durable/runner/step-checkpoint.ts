// ---------------------------------------------------------------------------
// Step checkpoint — the one place a wave's step outcome reaches storage: the
// step row (result or failure) and one `execution` attempt row per attempt.
// Both waves call it as each step settles, so a step's row and timings
// reflect that step, not the wave. Every durable write here (and the run's
// terminal writes) goes through `checkpointWrite`: retried, then classified
// as a `CheckpointError`, never as a step failure.
// ---------------------------------------------------------------------------

import { retryAsync, type RetryPolicy } from "../../shared/retry-policy.ts";
import type { WallClock } from "../../shared/wall-clock.ts";
import { CheckpointError } from "../durable-pipeline-error.ts";
import { isControlFlowExit } from "../step-policy.ts";
import { isStepAttemptStorage } from "../workflow-storage.ts";
import type { DagExecutionContext } from "./dag-context.ts";
import { errorMessage, errorTagOf, type StepAttemptFailure } from "./step-body.ts";

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
 * Retry policy for a durable write the run depends on: 3 retries, 250ms
 * base backoff (250, 500, 1000ms) on the run's clock. A fence rejection is
 * never retried: the lock moved on, and retrying cannot win it back.
 */
export const CHECKPOINT_RETRY_POLICY: RetryPolicy<unknown> = {
  maxRetries: 3,
  baseDelayMs: 250,
  when: (error) => !isControlFlowExit(error),
};

/**
 * Run one durable write under `CHECKPOINT_RETRY_POLICY`. A write that still
 * fails rejects with `CheckpointError` (the storage error as `cause`); a
 * fence rejection or other control-flow exit rejects as itself.
 */
export async function checkpointWrite(params: {
  readonly clock: WallClock;
  readonly workflowId: string;
  /** The storage call, for the error (`saveStepResult`, `completeWorkflow`, ...). */
  readonly operation: string;
  readonly stepName?: string;
  readonly write: () => Promise<void>;
}): Promise<void> {
  const { workflowId, operation, stepName } = params;
  try {
    await retryAsync({ policy: CHECKPOINT_RETRY_POLICY, clock: params.clock, run: params.write });
  } catch (error) {
    if (isControlFlowExit(error)) throw error;
    const subject = stepName !== undefined ? ` for step "${stepName}"` : "";
    throw new CheckpointError({
      workflowId,
      operation,
      ...(stepName !== undefined && { stepName }),
      message:
        `Workflow "${workflowId}": ${operation}${subject} failed after ` +
        `${CHECKPOINT_RETRY_POLICY.maxRetries} retries: ${errorMessage(error)}`,
      cause: error,
    });
  }
}

/**
 * Persist a settled step and return its outcome.
 *
 * - `completed`: one failed attempt row per `failedAttempts` entry, the step
 *   result, then a completed attempt row for the last attempt (unless that
 *   attempt already failed and `onFailure` absorbed it).
 * - `failed`: the failed attempt rows, the step failure (with the error's
 *   `_tag` as `errorTag`), and a failed attempt row for the last attempt
 *   unless `failedAttempts` already has it.
 * - `suspended` / `continue-as-new`: nothing; the step kind wrote its own
 *   rows.
 *
 * Each write is retried; one that still fails rejects with
 * `CheckpointError`. The step's outcome is never rewritten into a failure
 * because its checkpoint failed: the body's side effects happened, and
 * recovery re-drives the run from what was saved.
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
  const write = (operation: string, fn: () => Promise<void>) =>
    checkpointWrite({ clock, workflowId, operation, stepName: outcome.name, write: fn });

  await saveFailedAttempts({ ctx, workflowId, stepName: outcome.name, failedAttempts, write });

  if (outcome.kind === "completed") {
    await write("saveStepResult", () =>
      ctx.storage.saveStepResult(
        {
          workflowId,
          stepName: outcome.name,
          result: outcome.result,
          metadata: outcome.metadata,
          durationMs: outcome.durationMs,
          startedAt: outcome.startedAt,
        },
        ctx.guard,
      ),
    );
    const storage = ctx.storage;
    if (!lastAttemptRecorded && isStepAttemptStorage(storage)) {
      const completedAt = clock.now();
      await write("saveStepAttempt", () =>
        storage.saveStepAttempt(
          {
            workflowId,
            stepName: outcome.name,
            attempt: outcome.attempt,
            type: "execution",
            status: "completed",
            result: outcome.result,
            durationMs: outcome.durationMs,
            startedAt: outcome.startedAt,
            completedAt,
            ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
          },
          ctx.guard,
        ),
      );
    }
    return outcome;
  }

  const error = errorMessage(outcome.error);
  const errorTag = errorTagOf(outcome.error);
  await write("saveStepFailure", () =>
    ctx.storage.saveStepFailure(
      {
        workflowId,
        stepName: outcome.name,
        error,
        ...(errorTag !== undefined && { errorTag }),
        durationMs: outcome.durationMs,
        startedAt: outcome.startedAt,
        metadata: outcome.metadata,
      },
      ctx.guard,
    ),
  );
  const storage = ctx.storage;
  if (!lastAttemptRecorded && isStepAttemptStorage(storage)) {
    const completedAt = clock.now();
    await write("saveStepAttempt", () =>
      storage.saveStepAttempt(
        {
          workflowId,
          stepName: outcome.name,
          attempt: outcome.attempt,
          type: "execution",
          status: "failed",
          error,
          durationMs: outcome.durationMs,
          startedAt: outcome.startedAt,
          completedAt,
          ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
        },
        ctx.guard,
      ),
    );
  }
  return outcome;
}

async function saveFailedAttempts(params: {
  ctx: DagExecutionContext;
  workflowId: string;
  stepName: string;
  failedAttempts: readonly StepAttemptFailure[];
  write: (operation: string, fn: () => Promise<void>) => Promise<void>;
}): Promise<void> {
  const { ctx, workflowId, stepName } = params;
  const storage = ctx.storage;
  if (!isStepAttemptStorage(storage)) return;
  for (const failed of params.failedAttempts) {
    await params.write("saveStepAttempt", () =>
      storage.saveStepAttempt(
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
      ),
    );
  }
}
