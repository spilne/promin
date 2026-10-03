// ---------------------------------------------------------------------------
// Step checkpoint — the step-row writes the DAG executor issues after a
// wave settles: a step's result (with its attempt row) when a wave did not
// already save it, and a failed step's row (with its attempt row).
// ---------------------------------------------------------------------------

import type { WallClock } from "../../shared/wall-clock.ts";
import { isStepAttemptStorage } from "../workflow-storage.ts";
import type { DagExecutionContext } from "./dag-context.ts";

/** Save a completed step's encoded result plus an `execution` attempt row. */
export async function persistStepResult(params: {
  ctx: DagExecutionContext;
  clock: WallClock;
  workflowId: string;
  name: string;
  /** Codec-encoded result. */
  result: unknown;
  metadata: Record<string, unknown> | undefined;
  durationMs: number;
  startedAt: Date;
  attempt: number;
}): Promise<void> {
  const { ctx, clock, workflowId, name, result, metadata, durationMs, startedAt } = params;
  await ctx.storage.saveStepResult(
    {
      workflowId,
      stepName: name,
      result,
      metadata,
      durationMs,
      startedAt,
    },
    ctx.guard,
  );
  if (isStepAttemptStorage(ctx.storage)) {
    await ctx.storage.saveStepAttempt(
      {
        workflowId,
        stepName: name,
        attempt: params.attempt,
        type: "execution",
        status: "completed",
        result,
        durationMs,
        startedAt,
        completedAt: clock.now(),
        ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
      },
      ctx.guard,
    );
  }
}

/** Save a failed step's row plus a failed `execution` attempt row. */
export async function persistStepFailure(params: {
  ctx: DagExecutionContext;
  clock: WallClock;
  workflowId: string;
  stepName: string;
  errorMsg: string;
  metadata: Record<string, unknown> | undefined;
  attempt: number;
}): Promise<void> {
  const { ctx, clock, workflowId, stepName, errorMsg } = params;
  const failStartedAt = clock.now();
  await ctx.storage.saveStepFailure(
    {
      workflowId,
      stepName,
      error: errorMsg,
      durationMs: 0,
      startedAt: failStartedAt,
      metadata: params.metadata,
    },
    ctx.guard,
  );
  if (isStepAttemptStorage(ctx.storage)) {
    await ctx.storage.saveStepAttempt(
      {
        workflowId,
        stepName,
        attempt: params.attempt,
        type: "execution",
        status: "failed",
        error: errorMsg,
        durationMs: 0,
        startedAt: failStartedAt,
        completedAt: clock.now(),
        ...(ctx.executorId !== undefined && { executorId: ctx.executorId }),
      },
      ctx.guard,
    );
  }
}
