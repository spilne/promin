// ---------------------------------------------------------------------------
// Worker middleware — composable wrappers around step execution
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import type { StepTask } from "./step-queue.ts";
import type { StepContext } from "./step-registry.ts";

/** The next function in the middleware chain. Call it to proceed. */
export type NextFn = (ctx: StepContext) => Promise<unknown>;

/**
 * Worker middleware wraps step execution. Each middleware receives the
 * task, context, and a `next` function to call the next middleware
 * (or the actual step handler).
 *
 * Return the step result, or throw to fail the step.
 */
export type WorkerMiddleware = (params: {
  task: StepTask;
  ctx: StepContext;
  next: NextFn;
}) => Promise<unknown>;

/** Compose an array of middleware into a single function that wraps a handler. */
export function composeMiddleware(middleware: WorkerMiddleware[], handler: NextFn): NextFn {
  return middleware.reduceRight<NextFn>(
    (next, mw) => (ctx) => mw({ task: ctx as any, ctx, next }),
    handler,
  );
}

// ---------------------------------------------------------------------------
// Built-in middleware
// ---------------------------------------------------------------------------

/**
 * Timeout middleware — fails the step if it takes longer than `ms`.
 *
 * Pass `clock` to drive the deadline off an injected `Clock` — tests can
 * advance a `FakeClock` to trigger the timeout without a real wait.
 *
 * @example
 * ```ts
 * createWorker({ middleware: [timeoutMiddleware(30_000)] })
 * ```
 */
export function timeoutMiddleware(ms: number, clock: Clock = SystemClock): WorkerMiddleware {
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
  /** Time source for backoff waits. Default: `SystemClock`. */
  clock?: Clock;
}): WorkerMiddleware {
  return async ({ ctx, next }) => {
    const { maxRetries, baseDelayMs = 500, when, clock = SystemClock } = params;
    let lastError: unknown;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        if (attempt > 0) {
          await new Promise<void>((r) =>
            clock.setTimeout(() => r(), baseDelayMs * Math.pow(2, attempt - 1)),
          );
        }
        return await next(ctx);
      } catch (err) {
        lastError = err;
        if (when && !when(err)) throw err; // not retryable
      }
    }
    throw lastError;
  };
}

/**
 * Logging middleware — logs step start, completion, and failure.
 *
 * @example
 * ```ts
 * createWorker({ middleware: [loggingMiddleware(console.log)] })
 * ```
 */
export function loggingMiddleware(
  log: (message: string, meta?: Record<string, unknown>) => void = console.log,
  clock: Clock = SystemClock,
): WorkerMiddleware {
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
 *   middleware: [metricsMiddleware((m) => prometheus.observe(m))]
 * })
 * ```
 */
export function metricsMiddleware(
  record: (metric: {
    workflowId: string;
    stepName: string;
    needs: readonly string[];
    status: "completed" | "failed";
    durationMs: number;
  }) => void,
  clock: Clock = SystemClock,
): WorkerMiddleware {
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
