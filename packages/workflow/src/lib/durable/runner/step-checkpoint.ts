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
import type { StepAttemptRecord, WorkflowStatusSnapshot } from "../workflow-state.ts";
import { hasCapability } from "../workflow-storage.ts";
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
      /** Who ran the last attempt, when not this runner (a worker id). */
      readonly executorId?: string;
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
      /** Who ran the last attempt, when not this runner (a worker id). */
      readonly executorId?: string;
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
 * A step outcome after its checkpoint, with the run's status as the
 * checkpoint read it. `runStatus` is `undefined` when the checkpoint did
 * not read it (nothing was written, or the storage has no
 * `checkpointStep`), and `null` when the run was not found.
 */
export interface CheckpointedStep {
  readonly outcome: StepOutcome;
  readonly runStatus?: WorkflowStatusSnapshot | null;
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
 * A storage with `checkpointStep` gets all of a step's rows in that one
 * call, which also reports the run's status (`runStatus`); any other
 * storage gets the separate writes, in the order above.
 *
 * Each write is retried; one that still fails rejects with
 * `CheckpointError`. The step's outcome is never rewritten into a failure
 * because its checkpoint failed: the body's side effects happened, and
 * recovery re-drives the run from what was saved.
 */
export async function checkpointStepOutcome(params: {
  readonly ctx: DagExecutionContext;
  readonly clock: WallClock;
  readonly workflowId: string;
  readonly outcome: StepOutcome;
  readonly failedAttempts?: readonly StepAttemptFailure[];
}): Promise<CheckpointedStep> {
  const { ctx, clock, workflowId, outcome } = params;
  if (outcome.kind === "suspended" || outcome.kind === "continue-as-new") return { outcome };
  const failedAttempts = params.failedAttempts ?? [];
  const lastAttemptRecorded = failedAttempts.some((a) => a.attempt === outcome.attempt);
  const write = (operation: string, fn: () => Promise<void>) =>
    checkpointWrite({ clock, workflowId, operation, stepName: outcome.name, write: fn });
  const error = outcome.kind === "failed" ? errorMessage(outcome.error) : undefined;
  const errorTag = outcome.kind === "failed" ? errorTagOf(outcome.error) : undefined;
  /** The attempt row of the step's last attempt, stamped now. */
  const lastAttempt = (): StepAttemptRecord => ({
    workflowId,
    stepName: outcome.name,
    attempt: outcome.attempt,
    type: "execution",
    ...(outcome.kind === "completed"
      ? { status: "completed" as const, result: outcome.result }
      : { status: "failed" as const, error }),
    durationMs: outcome.durationMs,
    startedAt: outcome.startedAt,
    completedAt: clock.now(),
    ...executorOf({ ctx, executorId: outcome.executorId }),
  });

  const storage = ctx.storage;
  if (hasCapability(storage, "stepCheckpoint")) {
    const attempts = failedAttempts.map((failed) =>
      failedAttemptRecord({ ctx, workflowId, stepName: outcome.name, failed }),
    );
    if (!lastAttemptRecorded) attempts.push(lastAttempt());
    let runStatus: WorkflowStatusSnapshot | null = null;
    await write("checkpointStep", async () => {
      runStatus = await storage.checkpointStep({
        workflowId,
        stepName: outcome.name,
        outcome:
          outcome.kind === "completed"
            ? {
                kind: "completed",
                result: outcome.result,
                durationMs: outcome.durationMs,
                startedAt: outcome.startedAt,
                ...(outcome.metadata !== undefined && { metadata: outcome.metadata }),
              }
            : {
                kind: "failed",
                error: error!,
                ...(errorTag !== undefined && { errorTag }),
                durationMs: outcome.durationMs,
                startedAt: outcome.startedAt,
                ...(outcome.metadata !== undefined && { metadata: outcome.metadata }),
              },
        attempts,
        guard: ctx.guard,
      });
    });
    return { outcome, runStatus };
  }

  await saveFailedAttempts({ ctx, workflowId, stepName: outcome.name, failedAttempts, write });

  if (outcome.kind === "completed") {
    await write("saveStepResult", () =>
      storage.saveStepResult({
        workflowId,
        stepName: outcome.name,
        result: outcome.result,
        metadata: outcome.metadata,
        durationMs: outcome.durationMs,
        startedAt: outcome.startedAt,
        guard: ctx.guard,
      }),
    );
  } else {
    await write("saveStepFailure", () =>
      storage.saveStepFailure({
        workflowId,
        stepName: outcome.name,
        error: error!,
        ...(errorTag !== undefined && { errorTag }),
        durationMs: outcome.durationMs,
        startedAt: outcome.startedAt,
        metadata: outcome.metadata,
        guard: ctx.guard,
      }),
    );
  }
  if (!lastAttemptRecorded && hasCapability(storage, "stepAttempts")) {
    const record = lastAttempt();
    await write("saveStepAttempt", () => storage.saveStepAttempt({ record, guard: ctx.guard }));
  }
  return { outcome };
}

/** The attempt row of an attempt that failed before the step settled. */
function failedAttemptRecord(params: {
  ctx: DagExecutionContext;
  workflowId: string;
  stepName: string;
  failed: StepAttemptFailure;
}): StepAttemptRecord {
  const { ctx, failed } = params;
  return {
    workflowId: params.workflowId,
    stepName: params.stepName,
    attempt: failed.attempt,
    type: "execution",
    status: "failed",
    error: failed.error,
    durationMs: failed.durationMs,
    startedAt: failed.startedAt,
    completedAt: new Date(failed.startedAt.getTime() + failed.durationMs),
    ...executorOf({ ctx, executorId: failed.executorId }),
  };
}

/** The attempt row's `executorId`: who ran the attempt, else this runner. */
function executorOf(params: { ctx: DagExecutionContext; executorId: string | undefined }): {
  executorId?: string;
} {
  const executorId = params.executorId ?? params.ctx.executorId;
  return executorId !== undefined ? { executorId } : {};
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
  if (!hasCapability(storage, "stepAttempts")) return;
  for (const failed of params.failedAttempts) {
    const record = failedAttemptRecord({ ctx, workflowId, stepName, failed });
    await params.write("saveStepAttempt", () =>
      storage.saveStepAttempt({ record, guard: ctx.guard }),
    );
  }
}
