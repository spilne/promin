// ---------------------------------------------------------------------------
// RetryPolicy — the one retry policy and the one retry loop of the package.
//
// Every retry in the engine runs on the decision below, in one of two
// shapes: `retryWithPolicy` for an `Eff` (step-level `retry`), and
// `retryAsync` for a Promise (journaled activities, the workflow-level
// retry, compensation, checkpoint writes, the state machine's
// `retryMiddleware`, the distributed worker). Backoff waits run on the
// injected `WallClock`, so a `FakeWallClock` drives them in tests.
//
// Defaults (a field left unset):
//
// | field          | default                                              |
// |----------------|------------------------------------------------------|
// | `maxRetries`   | 3 (the workflow-level and compensation retries use 0  |
// |                | when no policy is configured at all)                 |
// | `baseDelayMs`  | 250                                                  |
// | backoff        | `baseDelayMs * 2^retry`                              |
// | `maxDelayMs`   | no cap (`0` also means no cap)                       |
// | `jitter`       | off; on, each delay is spread over `delay * (1±0.25)`|
// | `timeBudgetMs` | none; measured from the first failure                |
// | `when`         | every error the caller hands to the loop             |
// ---------------------------------------------------------------------------

import { fail, suspend, type Eff, type ErrorsOf } from "@spilne/perfect-core";
import { sleepOn } from "./eff.ts";
import type { TaggedError } from "./tagged-error.ts";
import { SystemWallClock, type WallClock } from "./wall-clock.ts";

/**
 * Plain-data retry configuration for a step or activity. It is stored with
 * workflow definitions, so the shape (and its defaults) must stay stable.
 * Defaults are listed in `RETRY_POLICY_DEFAULTS`.
 */
export interface RetryPolicy<E> {
  /** Max number of retries. Defaults to 3. */
  readonly maxRetries?: number;
  /** Base delay for exponential backoff. Defaults to 250ms. */
  readonly baseDelayMs?: number;
  /** Filter which errors are retryable. Defaults to all errors the loop sees. */
  readonly when?: (error: E) => boolean;
  /** Add ±25% randomness to delays (prevents thundering herd). */
  readonly jitter?: boolean;
  /** Cap the maximum delay between retries. Unset or `0`: no cap. */
  readonly maxDelayMs?: number;
  /** Total time budget for all retries, measured from the first failure. */
  readonly timeBudgetMs?: number;
}

/**
 * Workflow-level retry (`workflow({ retry })`): re-runs the DAG from the
 * failed step, with the `RetryPolicy` defaults once a policy is set (no
 * policy, no retry). Retried by default: typed failures. Never retried:
 * engine control flow, `WorkflowDeadlineError` (the deadline is already
 * spent), a cancel. Defects (untagged throws, a rejected `.stepAsync()`)
 * only with `retryDefects: true`; `when` then sees them too.
 */
export interface WorkflowRetryPolicy<E = TaggedError> extends RetryPolicy<E> {
  /** Retry defects as well as typed failures. Default: `false`. */
  readonly retryDefects?: boolean;
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
 * The retry decision shared by both loops: the delay before retry number
 * `retry` after `error`, or `undefined` when the loop must give up (retries
 * spent, time budget spent, or `when` rejects the error).
 */
export function nextRetryDelayMs<E>(params: {
  readonly policy: RetryPolicy<E>;
  readonly retry: number;
  readonly error: E;
  /** When the first failure happened, on the loop's clock. */
  readonly firstFailureMs: number;
  readonly nowMs: number;
  readonly random?: () => number;
}): number | undefined {
  const { policy, retry } = params;
  const maxRetries = policy.maxRetries ?? RETRY_POLICY_DEFAULTS.maxRetries;
  if (retry >= maxRetries) return undefined;
  const budget = policy.timeBudgetMs;
  if (budget !== undefined && budget > 0 && params.nowMs - params.firstFailureMs >= budget) {
    return undefined;
  }
  if (policy.when !== undefined && !policy.when(params.error)) return undefined;
  return retryDelayMs({
    policy: policy as RetryPolicy<never>,
    retry,
    ...(params.random !== undefined && { random: params.random }),
  });
}

/**
 * Retry `eff` on typed failures according to a persisted `RetryPolicy`.
 * Defects and interruptions are never retried. Backoff sleeps run on the
 * given `WallClock` so a `FakeWallClock` drives them in tests.
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

  return suspend(() => {
    let firstFailureMs: number | undefined;
    const attempt = (retry: number): Eff<A, S> =>
      eff.catch((error): Eff<A, S> => {
        const nowMs = clock.currentTimeMs();
        firstFailureMs ??= nowMs;
        const delay = nextRetryDelayMs({
          policy,
          retry,
          error,
          firstFailureMs,
          nowMs,
          ...(params.random !== undefined && { random: params.random }),
        });
        // Re-raising the caught error keeps the original `S`.
        if (delay === undefined) return fail(error) as unknown as Eff<A, S>;
        return sleepOn(clock, delay).flatMap(() => attempt(retry + 1));
      }) as Eff<A, S>;
    return attempt(0);
  });
}

/**
 * Run `run` and retry it on rejection per `policy`. `run` receives the retry
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
  let firstFailureMs: number | undefined;

  for (let retry = 0; ; retry++) {
    try {
      return await run(retry);
    } catch (error) {
      if (signal?.aborted) throw error;
      const nowMs = clock.currentTimeMs();
      firstFailureMs ??= nowMs;
      const delay = nextRetryDelayMs({
        policy,
        retry,
        error,
        firstFailureMs,
        nowMs,
        ...(params.random !== undefined && { random: params.random }),
      });
      if (delay === undefined) throw error;
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
