import { Effect, Duration, Fiber, type Either } from "effect";
import { HttpTimeoutError, PollTimeoutError, type HttpClientError } from "./http-client-error.ts";

// ---------------------------------------------------------------------------
// Parallel execution helpers
// ---------------------------------------------------------------------------

/**
 * Run multiple HTTP effects in parallel and collect all results.
 * Equivalent to `Effect.all(effects, { concurrency: "unbounded" })`.
 *
 * @example
 * ```ts
 * const [users, posts, comments] = await Effect.runPromise(
 *   parallel([getUsers, getPosts, getComments]),
 * );
 * ```
 */
export function parallel<Effects extends readonly Effect.Effect<any, HttpClientError>[]>(
  effects: [...Effects],
): Effect.Effect<{ [K in keyof Effects]: Effect.Effect.Success<Effects[K]> }, HttpClientError> {
  return Effect.all(effects, { concurrency: "unbounded" }) as any;
}

/**
 * Run multiple HTTP effects in parallel, settling all (never short-circuits).
 * Returns an array of `Either<T, HttpClientError>` so you can inspect each result.
 *
 * @example
 * ```ts
 * const results = await Effect.runPromise(allSettled([getUserA, getUserB]));
 * for (const r of results) {
 *   Either.match(r, { onLeft: (err) => console.warn(err), onRight: (val) => use(val) });
 * }
 * ```
 */
export function allSettled<Effects extends readonly Effect.Effect<any, HttpClientError>[]>(
  effects: [...Effects],
): Effect.Effect<
  {
    [K in keyof Effects]: Either.Either<Effect.Effect.Success<Effects[K]>, HttpClientError>;
  },
  never
> {
  return Effect.all(
    effects.map((e) => Effect.either(e)),
    { concurrency: "unbounded" },
  ) as any;
}

// ---------------------------------------------------------------------------
// Racing / first-to-succeed
// ---------------------------------------------------------------------------

/**
 * Race multiple HTTP effects — returns the first one to succeed.
 * If all fail, returns the error from whichever fiber failed first.
 *
 * @example
 * ```ts
 * const fastest = race([
 *   httpRequest({ url: "https://primary.api/data", schema }),
 *   httpRequest({ url: "https://fallback.api/data", schema }),
 * ]);
 * ```
 */
export function race<T>(
  effects: readonly Effect.Effect<T, HttpClientError>[],
): Effect.Effect<T, HttpClientError> {
  if (effects.length === 0) {
    return Effect.die(new Error("race() requires at least one effect"));
  }
  if (effects.length === 1) return effects[0];
  return effects.reduce((acc, e) => Effect.race(acc, e));
}

// ---------------------------------------------------------------------------
// Fallback chain
// ---------------------------------------------------------------------------

/**
 * Try effects in sequence — if one fails, try the next.
 * Returns the first successful result. If all fail, returns the last error.
 *
 * @example
 * ```ts
 * const data = fallbackChain([
 *   httpRequest({ url: "https://primary.api/data", schema }),
 *   httpRequest({ url: "https://secondary.api/data", schema }),
 *   Effect.succeed(defaultData), // last-resort fallback
 * ]);
 * ```
 */
export function fallbackChain<T>(
  effects: readonly Effect.Effect<T, HttpClientError>[],
): Effect.Effect<T, HttpClientError> {
  if (effects.length === 0) {
    return Effect.die(new Error("fallbackChain() requires at least one effect"));
  }
  return effects.reduce((acc, e) => Effect.orElse(acc, () => e));
}

// ---------------------------------------------------------------------------
// Hedged request (start backup after delay)
// ---------------------------------------------------------------------------

/**
 * Hedged request pattern: start the primary request, and if it hasn't completed
 * after `hedgeDelayMs`, fire a second attempt in parallel. Returns whichever finishes first.
 *
 * Useful for tail-latency-sensitive calls where you'd rather pay for an extra request
 * than wait for a slow one.
 *
 * @example
 * ```ts
 * const fast = hedged(
 *   httpRequest({ url: "https://api.example.com/data", schema }),
 *   { hedgeDelayMs: 200 },
 * );
 * ```
 */
export function hedged<T>(
  effect: Effect.Effect<T, HttpClientError>,
  options: { readonly hedgeDelayMs: number },
): Effect.Effect<T, HttpClientError> {
  return Effect.gen(function* () {
    const primary = yield* Effect.fork(effect);
    const hedgeTimer = yield* Effect.fork(
      Effect.sleep(Duration.millis(options.hedgeDelayMs)).pipe(Effect.flatMap(() => effect)),
    );

    const winner = yield* Fiber.join(
      yield* Effect.fork(Effect.race(Fiber.join(primary), Fiber.join(hedgeTimer))),
    );

    return winner;
  });
}

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

export interface PollOptions<T> {
  /** The effect to execute on each poll tick. */
  readonly request: Effect.Effect<T, HttpClientError>;
  /** Return `true` when the desired condition is met and polling should stop. */
  readonly until: (result: T) => boolean;
  /** Interval between polls. Defaults to 1 000ms. */
  readonly intervalMs?: number;
  /** Maximum number of poll attempts. Defaults to 60. */
  readonly maxAttempts?: number;
  /** Maximum total time for all polls combined. Defaults to 5 minutes. */
  readonly maxDurationMs?: number;
}

// PollTimeoutError is re-exported from http-client-error.ts
export { PollTimeoutError } from "./http-client-error.ts";

/**
 * Poll an HTTP endpoint until a condition is met.
 *
 * Returns the first result that satisfies `until`, or fails with `PollTimeoutError`.
 *
 * @example
 * ```ts
 * const completedJob = await Effect.runPromise(
 *   poll({
 *     request: httpRequest({ url: `https://api.example.com/jobs/${id}`, schema: JobSchema }),
 *     until: (job) => job.status === "completed" || job.status === "failed",
 *     intervalMs: 2_000,
 *     maxAttempts: 30,
 *   }),
 * );
 * ```
 */
export function poll<T>(options: PollOptions<T>): Effect.Effect<T, HttpClientError> {
  const {
    request,
    until,
    intervalMs = 1_000,
    maxAttempts = 60,
    maxDurationMs = 5 * 60 * 1_000,
  } = options;

  const loop = Effect.gen(function* () {
    let lastResult: T | undefined;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      lastResult = yield* request;

      if (until(lastResult)) {
        return lastResult;
      }

      if (attempt < maxAttempts) {
        yield* Effect.sleep(Duration.millis(intervalMs));
      }
    }

    return yield* Effect.fail(
      new PollTimeoutError({
        attempts: maxAttempts,
        lastResult,
        message: `Polling exhausted ${maxAttempts} attempts without satisfying condition`,
      }),
    );
  });

  return loop.pipe(
    Effect.timeoutFail({
      duration: Duration.millis(maxDurationMs),
      onTimeout: () =>
        new PollTimeoutError({
          attempts: maxAttempts,
          lastResult: undefined,
          message: `Polling exceeded max duration of ${maxDurationMs}ms`,
        }),
    }),
  );
}

/**
 * Poll with exponential backoff between attempts.
 * The interval starts at `initialIntervalMs` and doubles each attempt, capped at `maxIntervalMs`.
 *
 * @example
 * ```ts
 * const result = await Effect.runPromise(
 *   pollWithBackoff({
 *     request: httpRequest({ url: `https://api.example.com/jobs/${id}`, schema: JobSchema }),
 *     until: (job) => job.status === "completed",
 *     initialIntervalMs: 500,
 *     maxIntervalMs: 10_000,
 *     maxAttempts: 20,
 *   }),
 * );
 * ```
 */
export function pollWithBackoff<T>(
  options: PollOptions<T> & {
    readonly initialIntervalMs?: number;
    readonly maxIntervalMs?: number;
  },
): Effect.Effect<T, HttpClientError> {
  const {
    request,
    until,
    initialIntervalMs = 500,
    maxIntervalMs = 30_000,
    maxAttempts = 60,
    maxDurationMs = 5 * 60 * 1_000,
  } = options;

  const loop = Effect.gen(function* () {
    let lastResult: T | undefined;
    let currentInterval = initialIntervalMs;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      lastResult = yield* request;

      if (until(lastResult)) {
        return lastResult;
      }

      if (attempt < maxAttempts) {
        yield* Effect.sleep(Duration.millis(currentInterval));
        currentInterval = Math.min(currentInterval * 2, maxIntervalMs);
      }
    }

    return yield* Effect.fail(
      new PollTimeoutError({
        attempts: maxAttempts,
        lastResult,
        message: `Polling with backoff exhausted ${maxAttempts} attempts without satisfying condition`,
      }),
    );
  });

  return loop.pipe(
    Effect.timeoutFail({
      duration: Duration.millis(maxDurationMs),
      onTimeout: () =>
        new PollTimeoutError({
          attempts: maxAttempts,
          lastResult: undefined,
          message: `Polling with backoff exceeded max duration of ${maxDurationMs}ms`,
        }),
    }),
  );
}

// ---------------------------------------------------------------------------
// Timeout wrapper
// ---------------------------------------------------------------------------

/**
 * Apply a timeout to any effect, converting to `HttpTimeoutError` on expiry.
 *
 * @example
 * ```ts
 * const fast = withTimeout(someSlowEffect, { timeoutMs: 5_000 });
 * ```
 */
export function withTimeout<T>(
  effect: Effect.Effect<T, HttpClientError>,
  options: { readonly timeoutMs: number },
): Effect.Effect<T, HttpClientError> {
  return effect.pipe(
    Effect.timeoutFail({
      duration: Duration.millis(options.timeoutMs),
      onTimeout: () =>
        new HttpTimeoutError({
          url: "pipeline",
          timeoutMs: options.timeoutMs,
          message: `Pipeline timed out after ${options.timeoutMs}ms`,
        }),
    }),
  );
}
