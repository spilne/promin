// ---------------------------------------------------------------------------
// Step tracing — turns a settled step into the tracer's step-attempt events.
// Starts are emitted live where an attempt begins; ends are emitted here,
// when the step settles, from the attempts the step reports (each carries
// its own start time and duration, so a trace is accurate even though the
// end of a retried attempt is delivered late).
// ---------------------------------------------------------------------------

import { stepUsage } from "../budget.ts";
import { SystemWallClock } from "../../shared/wall-clock.ts";
import {
  emitTrace,
  traceErrorOf,
  type WorkflowTraceError,
  type WorkflowTraceStepOutcome,
} from "../workflow-tracer.ts";
import type { DagExecutionContext } from "./dag-context.ts";
import type { StepAttemptFailure } from "./step-body.ts";
import type { StepOutcome } from "./step-checkpoint.ts";

/** Emit `stepAttemptStart` for the attempt a step is about to run. */
export function traceStepAttemptStart(params: {
  readonly ctx: DagExecutionContext;
  readonly stepName: string;
  readonly attempt: number;
  readonly startedAt: Date;
}): void {
  const { ctx } = params;
  if (ctx.tracer === undefined || ctx.traceRef === undefined) return;
  emitTrace(ctx.tracer, "stepAttemptStart", {
    ...ctx.traceRef,
    stepName: params.stepName,
    attempt: params.attempt,
    startedAt: params.startedAt,
  });
}

/**
 * Emit `stepAttemptEnd` for every attempt of a settled step: the failed
 * ones it reports (retried, except the step's last) and the last attempt
 * itself unless that is one of them. A step that suspended or continued
 * ends its one attempt with that outcome, and a suspension also opens a
 * wait (`waitStart`).
 */
export function traceStepSettled(params: {
  readonly ctx: DagExecutionContext;
  readonly outcome: StepOutcome;
  readonly failedAttempts: readonly StepAttemptFailure[];
}): void {
  const { ctx, outcome, failedAttempts } = params;
  const tracer = ctx.tracer;
  const ref = ctx.traceRef;
  if (tracer === undefined || ref === undefined) return;
  const clock = ctx.clock ?? SystemWallClock;
  const now = clock.now();

  if (outcome.kind === "suspended" || outcome.kind === "continue-as-new") {
    if (outcome.attempt !== undefined && outcome.startedAt !== undefined) {
      emitTrace(tracer, "stepAttemptEnd", {
        ...ref,
        stepName: outcome.name,
        attempt: outcome.attempt,
        startedAt: outcome.startedAt,
        endedAt: now,
        durationMs: now.getTime() - outcome.startedAt.getTime(),
        outcome: outcome.kind === "suspended" ? "suspended" : "continued",
        willRetry: false,
      });
    }
    if (outcome.kind === "suspended") {
      const reason = (outcome.error as { reason?: unknown } | null)?.reason;
      if (reason === "sleep" || reason === "signal") {
        emitTrace(tracer, "waitStart", { ...ref, stepName: outcome.name, kind: reason, at: now });
      }
    }
    return;
  }

  const usage =
    outcome.metadata === undefined ? undefined : stepUsage({ metadata: outcome.metadata });
  const lastRecorded = failedAttempts.some((a) => a.attempt === outcome.attempt);
  for (const failed of failedAttempts) {
    const isLast = failed.attempt === outcome.attempt;
    const error: WorkflowTraceError = {
      tag: failed.errorTag ?? "UnknownError",
      message: failed.error,
    };
    emitTrace(tracer, "stepAttemptEnd", {
      ...ref,
      stepName: outcome.name,
      attempt: failed.attempt,
      startedAt: failed.startedAt,
      endedAt: new Date(failed.startedAt.getTime() + failed.durationMs),
      durationMs: failed.durationMs,
      outcome: "failed",
      error,
      willRetry: !isLast,
      ...(isLast && usage !== undefined && { usage }),
      ...(failed.executorId !== undefined && { executorId: failed.executorId }),
    });
  }
  if (lastRecorded) return;

  // The last attempt began after the previous one ended (or with the step).
  const previous = failedAttempts[failedAttempts.length - 1];
  const startedAt =
    previous !== undefined
      ? new Date(previous.startedAt.getTime() + previous.durationMs)
      : outcome.startedAt;
  const kind: WorkflowTraceStepOutcome =
    outcome.kind === "failed" ? "failed" : outcome.skipped === true ? "skipped" : "completed";
  emitTrace(tracer, "stepAttemptEnd", {
    ...ref,
    stepName: outcome.name,
    attempt: outcome.attempt,
    startedAt,
    endedAt: now,
    durationMs: Math.max(0, now.getTime() - startedAt.getTime()),
    outcome: kind,
    ...(outcome.kind === "failed" && { error: traceErrorOf(outcome.error) }),
    willRetry: false,
    ...(usage !== undefined && { usage }),
    ...(outcome.executorId !== undefined && { executorId: outcome.executorId }),
  });
}
