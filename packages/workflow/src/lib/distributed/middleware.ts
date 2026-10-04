// ---------------------------------------------------------------------------
// Worker middleware — composable wrappers around step execution
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import type { StepTask } from "./step-queue.ts";
import type { WorkerStepContext } from "./step-registry.ts";
import { retryAsync } from "../shared/retry-policy.ts";

/** The next function in the middleware chain. Call it to proceed. */
export type NextFn = (ctx: WorkerStepContext) => Promise<unknown>;

/**
 * Worker middleware wraps step execution. Each middleware receives the
 * task, context, and a `next` function to call the next middleware
 * (or the actual step handler).
 *
 * Return the step result, or throw to fail the step.
 */
export type WorkerMiddleware = (params: {
  task: StepTask;
  ctx: WorkerStepContext;
  next: NextFn;
}) => Promise<unknown>;

// ---------------------------------------------------------------------------
// Built-in middleware
// ---------------------------------------------------------------------------

/**
 * Timeout middleware — fails the step if it takes longer than `ms`.
 *
 * Pass `clock` to drive the deadline off an injected `WallClock` — tests can
 * advance a `FakeWallClock` to trigger the timeout without a real wait.
 *
 * @example
 * ```ts
 * createWorker({ middleware: [timeoutMiddleware({ ms: 30_000 })] })
 * ```
 */
export function timeoutMiddleware(params: {
  ms: number;
  /** Time source for the deadline. Default: `SystemWallClock`. */
  clock?: WallClock;
}): WorkerMiddleware {
  const { ms, clock = SystemWallClock } = params;
  return async ({ ctx, next }) => {
    return Promise.race([
      next(ctx),
      new Promise<never>((_, reject) =>
        clock.setTimeout(() => reject(new Error(`Step timed out after ${ms}ms`)), ms),
      ),
    ]);
  };
}

/**
 * Retry middleware — retries the step on failure.
 *
 * @example
 * ```ts
 * createWorker({ middleware: [retryMiddleware({ maxRetries: 3, baseDelayMs: 500 })] })
 * ```
 */
export function retryMiddleware(params: {
  maxRetries: number;
  baseDelayMs?: number;
  when?: (error: unknown) => boolean;
  /** Time source for backoff waits. Default: `SystemWallClock`. */
  clock?: WallClock;
}): WorkerMiddleware {
  const { maxRetries, baseDelayMs = 500, when, clock = SystemWallClock } = params;
  return ({ ctx, next }) =>
    retryAsync({
      policy: { maxRetries, baseDelayMs, ...(when !== undefined && { when }) },
      clock,
      signal: ctx.signal,
      run: () => next(ctx),
    });
}

/**
 * Logging middleware — logs step start, completion, and failure.
 *
 * @example
 * ```ts
 * createWorker({ middleware: [loggingMiddleware({ log: console.log })] })
 * ```
 */
export function loggingMiddleware(
  params: {
    /** Log sink. Default: `console.log`. */
    log?: (message: string, meta?: Record<string, unknown>) => void;
    /** Time source for durations. Default: `SystemWallClock`. */
    clock?: WallClock;
  } = {},
): WorkerMiddleware {
  const { log = console.log, clock = SystemWallClock } = params;
  return async ({ task, ctx, next }) => {
    const start = clock.currentTimeMs();
    log("step:start", {
      workflowId: task.workflowId,
      stepName: task.stepName,
      attempt: task.attempt,
    });
    try {
      const result = await next(ctx);
      log("step:complete", {
        workflowId: task.workflowId,
        stepName: task.stepName,
        durationMs: clock.currentTimeMs() - start,
      });
      return result;
    } catch (err) {
      log("step:error", {
        workflowId: task.workflowId,
        stepName: task.stepName,
        error: err instanceof Error ? err.message : String(err),
        durationMs: clock.currentTimeMs() - start,
      });
      throw err;
    }
  };
}

/**
 * Metrics middleware — records step execution metrics via a callback.
 *
 * @example
 * ```ts
 * createWorker({
 *   middleware: [metricsMiddleware({ record: (m) => prometheus.observe(m) })]
 * })
 * ```
 */
export function metricsMiddleware(params: {
  record: (metric: {
    workflowId: string;
    stepName: string;
    needs: readonly string[];
    status: "completed" | "failed";
    durationMs: number;
  }) => void;
  /** Time source for durations. Default: `SystemWallClock`. */
  clock?: WallClock;
}): WorkerMiddleware {
  const { record, clock = SystemWallClock } = params;
  return async ({ task, ctx, next }) => {
    const start = clock.currentTimeMs();
    try {
      const result = await next(ctx);
      record({
        workflowId: task.workflowId,
        stepName: task.stepName,
        needs: task.needs,
        status: "completed",
        durationMs: clock.currentTimeMs() - start,
      });
      return result;
    } catch (err) {
      record({
        workflowId: task.workflowId,
        stepName: task.stepName,
        needs: task.needs,
        status: "failed",
        durationMs: clock.currentTimeMs() - start,
      });
      throw err;
    }
  };
}
