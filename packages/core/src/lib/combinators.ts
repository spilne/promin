import { Effect, Duration, Fiber, type Either } from "effect";
import { TimeoutError, PollTimeoutError } from "./pipeline-error.ts";

// ---------------------------------------------------------------------------
// Parallel execution helpers
// ---------------------------------------------------------------------------

/**
 * Run multiple effects in parallel and collect all results.
 * Short-circuits on first error.
 *
 * @example
 * ```ts
 * const [a, b, c] = await Effect.runPromise(
 *   parallel([effectA, effectB, effectC]),
 * );
 * ```
 */
export function parallel<Effects extends readonly Effect.Effect<any, any>[]>(
  effects: [...Effects],
): Effect.Effect<
  { [K in keyof Effects]: Effect.Effect.Success<Effects[K]> },
  Effects[number] extends Effect.Effect<any, infer E> ? E : never
> {
  return Effect.all(effects, { concurrency: "unbounded" }) as any;
}

/**
 * Run multiple effects in parallel, settling all (never short-circuits).
 * Returns an array of `Either<T, E>` so you can inspect each result.
 */
export function allSettled<Effects extends readonly Effect.Effect<any, any>[]>(
  effects: [...Effects],
): Effect.Effect<
  {
    [K in keyof Effects]: Either.Either<
      Effect.Effect.Success<Effects[K]>,
      Effects[number] extends Effect.Effect<any, infer E> ? E : never
    >;
  },
  never
> {
  return Effect.all(
    effects.map((e) => Effect.either(e)),
    { concurrency: "unbounded" },
  ) as any;
}

// ---------------------------------------------------------------------------
// Validate — parallel, accumulate all errors
// ---------------------------------------------------------------------------

/**
 * Run multiple effects in parallel, accumulating ALL errors instead of short-circuiting.
 * Useful for validation scenarios.
 */
export function validate<Effects extends readonly Effect.Effect<any, any>[]>(
  effects: [...Effects],
): Effect.Effect<
  { [K in keyof Effects]: Effect.Effect.Success<Effects[K]> },
  Effects[number] extends Effect.Effect<any, infer E> ? E : never
> {
  return Effect.all(effects, { concurrency: "unbounded", mode: "validate" }) as any;
}

// ---------------------------------------------------------------------------
// Racing / first-to-succeed
// ---------------------------------------------------------------------------

/**
 * Race multiple effects — returns the first one to succeed.
 * If all fail, returns the error from whichever fiber failed first.
 */
export function race<T, E>(effects: readonly Effect.Effect<T, E>[]): Effect.Effect<T, E> {
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
 */
export function fallbackChain<T, E>(effects: readonly Effect.Effect<T, E>[]): Effect.Effect<T, E> {
  if (effects.length === 0) {
    return Effect.die(new Error("fallbackChain() requires at least one effect"));
  }
  return effects.reduce((acc, e) => Effect.orElse(acc, () => e));
}

// ---------------------------------------------------------------------------
// Hedged request (start backup after delay)
// ---------------------------------------------------------------------------

/**
 * Hedged execution pattern: start the primary effect, and if it hasn't completed
 * after `hedgeDelayMs`, fire a second attempt in parallel. Returns whichever finishes first.
 *
 * Useful for tail-latency-sensitive operations.
 */
export function hedged<T, E>(
  effect: Effect.Effect<T, E>,
  options: { readonly hedgeDelayMs: number },
): Effect.Effect<T, E> {
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

export interface PollOptions<T, E> {
  /** The effect to execute on each poll tick. */
  readonly request: Effect.Effect<T, E>;
  /** Return `true` when the desired condition is met and polling should stop. */
  readonly until: (result: T) => boolean;
  /** Interval between polls. Defaults to 1 000ms. */
  readonly intervalMs?: number;
  /** Maximum number of poll attempts. Defaults to 60. */
  readonly maxAttempts?: number;
  /** Maximum total time for all polls combined. Defaults to 5 minutes. */
  readonly maxDurationMs?: number;
}

/**
 * Poll an effect until a condition is met.
 * Returns the first result that satisfies `until`, or fails with `PollTimeoutError`.
 */
export function poll<T, E>(options: PollOptions<T, E>): Effect.Effect<T, E | PollTimeoutError> {
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
 */
export function pollWithBackoff<T, E>(
  options: PollOptions<T, E> & {
    readonly initialIntervalMs?: number;
    readonly maxIntervalMs?: number;
  },
): Effect.Effect<T, E | PollTimeoutError> {
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
 * Apply a timeout to any effect, converting to `TimeoutError` on expiry.
 */
export function withTimeout<T, E>(
  effect: Effect.Effect<T, E>,
  options: { readonly timeoutMs: number },
): Effect.Effect<T, E | TimeoutError> {
  return effect.pipe(
    Effect.timeoutFail({
      duration: Duration.millis(options.timeoutMs),
      onTimeout: () =>
        new TimeoutError({
          timeoutMs: options.timeoutMs,
          message: `Pipeline timed out after ${options.timeoutMs}ms`,
        }),
    }),
  );
}

// ---------------------------------------------------------------------------
// forEach with bounded concurrency
// ---------------------------------------------------------------------------

/**
 * Execute an effect-returning function for each item with bounded concurrency.
 *
 * @example
 * ```ts
 * const results = await Effect.runPromise(
 *   forEach(userIds, (id) => fetchUser(id), { concurrency: 5 }),
 * );
 * ```
 */
export function forEach<T, U, E>(
  items: readonly T[],
  fn: (item: T) => Effect.Effect<U, E>,
  options: { readonly concurrency: number },
): Effect.Effect<U[], E> {
  return Effect.all(
    items.map((item) => fn(item)),
    { concurrency: options.concurrency },
  ) as Effect.Effect<U[], E>;
}
