// ---------------------------------------------------------------------------
// RetryPolicy — typed-error-only retry, as persisted on step definitions
// ---------------------------------------------------------------------------

import { fail, suspend, type Eff, type ErrorsOf } from "@spilne/perfect-core";
import { sleepOn } from "./eff.ts";
import { SystemWallClock, type WallClock } from "./wall-clock.ts";

/**
 * Plain-data retry configuration for a step or activity. It is stored with
 * workflow definitions, so the shape (and its defaults) must stay stable.
 */
export interface RetryPolicy<E> {
  /** Max number of retries. Defaults to 3. */
  readonly maxRetries?: number;
  /** Base delay for exponential backoff. Defaults to 250ms. */
  readonly baseDelayMs?: number;
  /** Filter which errors are retryable. Defaults to all typed errors. */
  readonly when?: (error: E) => boolean;
  /** Add ±25% randomness to delays (prevents thundering herd). */
  readonly jitter?: boolean;
  /** Cap the maximum delay between retries. */
  readonly maxDelayMs?: number;
  /** Total time budget for all retries combined. */
  readonly timeBudgetMs?: number;
}

/** Defaults applied to a `RetryPolicy` field that is left unset. */
export const RETRY_POLICY_DEFAULTS = {
  maxRetries: 3,
  baseDelayMs: 250,
  /** Jitter spreads each delay uniformly over `delay * (1 ± JITTER_RATIO)`. */
  jitterRatio: 0.25,
} as const;

/**
 * Delay before retry number `retry` (0 = the first retry): exponential from
 * `baseDelayMs`, doubled each time, capped by `maxDelayMs`, then jittered.
 */
export function retryDelayMs(params: {
  policy: RetryPolicy<never>;
  retry: number;
  /** Uniform `[0, 1)` source. Default: `Math.random`. */
  random?: () => number;
}): number {
  const { policy, retry } = params;
  const base = policy.baseDelayMs ?? RETRY_POLICY_DEFAULTS.baseDelayMs;
  let delay = base * Math.pow(2, retry);
  if (policy.maxDelayMs) delay = Math.min(delay, policy.maxDelayMs);
  if (policy.jitter) {
    const random = params.random ?? Math.random;
    const ratio = RETRY_POLICY_DEFAULTS.jitterRatio;
    delay = delay * (1 - ratio + random() * 2 * ratio);
  }
  return delay;
}

/**
 * Retry `eff` on typed failures according to a persisted `RetryPolicy`.
 * Defects and interruptions are never retried. Backoff sleeps run on the
 * given `WallClock` so a `FakeWallClock` drives them in tests.
 *
 * `timeBudgetMs` is measured from the first failure: a retry is only
 * scheduled while less than the budget has elapsed since then.
 */
export function retryWithPolicy<A, S>(params: {
  eff: Eff<A, S>;
  policy: RetryPolicy<ErrorsOf<S>>;
  /** Time source for backoff sleeps and the time budget. Default: `SystemWallClock`. */
  clock?: WallClock;
  /** Uniform `[0, 1)` source for jitter. Default: `Math.random`. */
  random?: () => number;
}): Eff<A, S> {
  const { eff, policy } = params;
  const clock = params.clock ?? SystemWallClock;
  const maxRetries = policy.maxRetries ?? RETRY_POLICY_DEFAULTS.maxRetries;
  const when = policy.when ?? (() => true);

  return suspend(() => {
    let firstFailureMs: number | undefined;
    const attempt = (retry: number): Eff<A, S> =>
      eff.catch((error): Eff<A, S> => {
        const now = clock.currentTimeMs();
        firstFailureMs ??= now;
        const budgetSpent =
          policy.timeBudgetMs !== undefined &&
          policy.timeBudgetMs > 0 &&
          now - firstFailureMs >= policy.timeBudgetMs;
        // Re-raising the caught error keeps the original `S`.
        if (retry >= maxRetries || budgetSpent || !when(error)) {
          return fail(error) as unknown as Eff<A, S>;
        }
        const delay = retryDelayMs({
          policy: policy as RetryPolicy<never>,
          retry,
          random: params.random,
        });
        return sleepOn(clock, delay).flatMap(() => attempt(retry + 1));
      }) as Eff<A, S>;
    return attempt(0);
  });
}
