// ---------------------------------------------------------------------------
// Executor wave — runs one wave of ready steps through the configured
// `StepExecutor`, concurrently. The runner keeps `skipWhen`, attempt
// counting, concurrency resolution and the per-step checkpoint; the
// executor runs the body.
// ---------------------------------------------------------------------------

import {
  StepError,
  WorkflowContinueAsNewError,
  WorkflowSuspendedError,
} from "../durable-pipeline-error.ts";
import { stepRuntimeFor, type WaveOutcome, type WaveParams } from "./dag-context.ts";
import { errorMessage } from "./step-body.ts";
import { checkpointStepOutcome, type StepOutcome } from "./step-checkpoint.ts";
import { resolveStepConcurrency } from "./step-concurrency.ts";
import type { StepExecutionRequest, StepExecutionResult } from "./step-executor.ts";
import { settleWave, skippedOutcome } from "./wave.ts";

/**
 * Run `readySteps` through `ctx.stepExecutor` and wait for every one of
 * them. Each step is checkpointed as soon as its executor reports, unless
 * the executor says it already persisted the step. The wave's
 * `AbortSignal` is aborted when the first step fails.
 */
export async function runExecutorWave(params: WaveParams): Promise<WaveOutcome> {
  const { ctx, workflowId, input, readySteps, results, clock, stepStates } = params;
  const abort = new AbortController();

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
      const currentAttempt = (params.stepAttempts.get(stepDef.name) ?? 0) + 1;
      params.stepAttempts.set(stepDef.name, currentAttempt);

      // Resolve per-task concurrency cap. Step-level wins over the
      // workflow-level default. The key fn is evaluated against the
      // step's input ctx; the resolved string + scope + limit are
      // stamped on the dispatched task so workers don't re-evaluate.
      const prevStepName = stepDef.dependsOn[0];
      const concurrency = resolveStepConcurrency({
        workflowName: ctx.workflowName,
        workflowQueue: ctx.workflowQueue,
        stepDef,
        workflowInput: input,
        stepInput: prevStepName != null ? results[prevStepName] : input,
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
        signal: abort.signal,
        ...(ctx.workflowVersion !== undefined && { version: ctx.workflowVersion }),
        runtime: stepRuntimeFor({ ctx, clock, stepStates, stepName: stepDef.name }),
        ...(concurrency
          ? {
              concurrencyKey: concurrency.key,
              concurrencyScope: concurrency.scope,
              concurrencyLimit: concurrency.limit,
            }
          : {}),
      };

      let res: StepExecutionResult;
      try {
        res = await ctx.stepExecutor!.executeStep(req);
      } catch (thrown) {
        res = resultOfThrown(thrown);
      }

      const reported = res.ok || res.kind === undefined || res.kind === "failed" ? res : undefined;
      if (reported?.attempt !== undefined) {
        params.stepAttempts.set(stepDef.name, reported.attempt);
      }
      const outcome = outcomeOfResult({
        workflowId,
        name: stepDef.name,
        res,
        startedAt,
        durationMs: clock.currentTimeMs() - startedAt.getTime(),
        attempt: reported?.attempt ?? currentAttempt,
      });
      if (outcome.kind === "failed") abort.abort(outcome.error);

      return checkpointStepOutcome({
        ctx,
        clock,
        workflowId,
        outcome,
        failedAttempts: reported?.failedAttempts ?? [],
        checkpointed: reported?.storageAlreadyCheckpointed === true,
      });
    },
  });
}

/**
 * An executor that throws instead of reporting: a suspension or
 * continue-as-new keeps its meaning, anything else is the step's failure.
 */
function resultOfThrown(thrown: unknown): StepExecutionResult {
  const tag = (thrown as { _tag?: unknown } | null | undefined)?._tag;
  if (tag === "WorkflowSuspendedError") {
    const suspended = thrown as WorkflowSuspendedError;
    return {
      ok: false,
      kind: "suspended",
      reason: suspended.reason,
      message: suspended.message,
      cause: thrown,
    };
  }
  if (tag === "WorkflowContinueAsNewError") {
    const next = thrown as WorkflowContinueAsNewError;
    return {
      ok: false,
      kind: "continue-as-new",
      nextInput: next.nextInput,
      message: next.message,
      cause: thrown,
    };
  }
  return { ok: false, kind: "failed", error: errorMessage(thrown), cause: thrown };
}

/**
 * The step's outcome from its executor's report. In-process executors hand
 * back the original error as `cause`, so the run sees the same error the
 * inline path would; a remote report becomes a `StepError` carrying the
 * reported `errorTag`.
 */
function outcomeOfResult(params: {
  workflowId: string;
  name: string;
  res: StepExecutionResult;
  startedAt: Date;
  durationMs: number;
  attempt: number;
}): StepOutcome {
  const { workflowId, name, res, startedAt, durationMs, attempt } = params;
  if (res.ok) {
    return {
      kind: "completed",
      name,
      result: res.result,
      metadata: res.metadata,
      startedAt,
      durationMs,
      attempt,
    };
  }
  if (res.kind === "suspended") {
    return {
      kind: "suspended",
      name,
      error:
        res.cause ??
        new WorkflowSuspendedError({
          workflowId,
          stepName: name,
          reason: res.reason,
          message: res.message ?? `Workflow "${workflowId}" suspended at step "${name}"`,
        }),
    };
  }
  if (res.kind === "continue-as-new") {
    return {
      kind: "continue-as-new",
      name,
      error:
        res.cause ??
        new WorkflowContinueAsNewError({
          workflowId,
          nextInput: res.nextInput,
          message: res.message ?? `Workflow "${workflowId}" requested continue-as-new`,
        }),
    };
  }
  return {
    kind: "failed",
    name,
    error:
      res.cause ??
      new StepError({
        workflowId,
        stepName: name,
        message: res.error,
        ...(res.errorTag !== undefined && { errorTag: res.errorTag }),
      }),
    metadata: res.metadata,
    startedAt,
    durationMs,
    attempt,
  };
}
