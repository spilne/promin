// ---------------------------------------------------------------------------
// retryAsync — the one retry loop of the distributed worker. Step-level
// `WorkerStepOptions.retry` and `retryMiddleware` both run on it, with the
// same backoff as the engine's `RetryPolicy` (`retryDelayMs`: exponential
// from `baseDelayMs`, `maxDelayMs` cap, optional jitter, `timeBudgetMs`).
// ---------------------------------------------------------------------------

import { RETRY_POLICY_DEFAULTS, retryDelayMs, type RetryPolicy } from "../shared/retry-policy.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

/**
 * Run `run` and retry it on failure per `policy`. `run` receives the retry
 * number (0 for the first try). Backoff waits run on `clock`. An aborted
 * `signal` stops retrying: a pending wait ends at once and the call rejects
 * with the signal's reason.
 */
export async function retryAsync<T>(params: {
  readonly policy: RetryPolicy<unknown>;
  readonly run: (retry: number) => Promise<T>;
  /** Time source for backoff waits and the time budget. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
  readonly signal?: AbortSignal;
  /** Uniform `[0, 1)` source for jitter. Default: `Math.random`. */
  readonly random?: () => number;
}): Promise<T> {
  const { policy, run, signal } = params;
  const clock = params.clock ?? SystemWallClock;
  const maxRetries = policy.maxRetries ?? RETRY_POLICY_DEFAULTS.maxRetries;
  let firstFailureMs: number | undefined;

  for (let retry = 0; ; retry++) {
    try {
      return await run(retry);
    } catch (error) {
      const now = clock.currentTimeMs();
      firstFailureMs ??= now;
      const budgetSpent =
        policy.timeBudgetMs !== undefined &&
        policy.timeBudgetMs > 0 &&
        now - firstFailureMs >= policy.timeBudgetMs;
      if (retry >= maxRetries || budgetSpent || signal?.aborted) throw error;
      if (policy.when && !policy.when(error)) throw error;
      const delay = retryDelayMs({
        policy: policy as RetryPolicy<never>,
        retry,
        ...(params.random !== undefined && { random: params.random }),
      });
      await waitOn({ clock, ms: delay, signal });
    }
  }
}

function waitOn(params: {
  readonly clock: WallClock;
  readonly ms: number;
  readonly signal?: AbortSignal;
}): Promise<void> {
  const { clock, ms, signal } = params;
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const onAbort = () => {
      timer.clear();
      reject(signal!.reason);
    };
    const timer = clock.setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
