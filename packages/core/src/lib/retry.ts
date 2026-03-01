import { Effect, Duration, Schedule } from "effect";

// ---------------------------------------------------------------------------
// PipelineResult — ADT for retry outcome discrimination
// ---------------------------------------------------------------------------

/** Result of a pipeline execution — success, typed error, or thrown error. */
export type PipelineResult<T, E> =
  | PipelineResult.Success<T>
  | PipelineResult.TypedError<E>
  | PipelineResult.Defect;

export namespace PipelineResult {
  export interface Success<T> {
    readonly _tag: "success";
    readonly value: T;
  }

  export interface TypedError<E> {
    readonly _tag: "typedError";
    readonly error: E;
  }

  export interface Defect {
    readonly _tag: "defect";
    readonly error: globalThis.Error;
  }

  export const success = <T>(value: T): Success<T> => ({ _tag: "success", value });
  export const typedError = <E>(error: E): TypedError<E> => ({ _tag: "typedError", error });
  export const defect = (error: globalThis.Error): Defect => ({ _tag: "defect", error });

  export const isSuccess = <T, E>(outcome: PipelineResult<T, E>): outcome is Success<T> =>
    outcome._tag === "success";
  export const isTypedError = <T, E>(outcome: PipelineResult<T, E>): outcome is TypedError<E> =>
    outcome._tag === "typedError";
  export const isDefect = <T, E>(outcome: PipelineResult<T, E>): outcome is Defect =>
    outcome._tag === "defect";
}

// ---------------------------------------------------------------------------
// RetryPolicy — typed-error-only retry
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// RetryAllPolicy — full-spectrum retry
// ---------------------------------------------------------------------------

export interface RetryAllPolicy<T, E> {
  /** Max number of retries. Defaults to 3. */
  readonly maxRetries?: number;
  /** Base delay for exponential backoff. Defaults to 250ms. */
  readonly baseDelayMs?: number;
  /** Add ±25% randomness to delays (prevents thundering herd). */
  readonly jitter?: boolean;
  /** Cap the maximum delay between retries. */
  readonly maxDelayMs?: number;
  /** Total time budget for all retries combined. */
  readonly timeBudgetMs?: number;
  /**
   * Should we retry this result? Receives the full result — success, typed error, or defect.
   * Defaults to: retry errors, don't retry success.
   */
  readonly shouldRetry?: (result: PipelineResult<T, E>) => boolean;
}

// ---------------------------------------------------------------------------
// Schedule builder
// ---------------------------------------------------------------------------

function buildSchedule(params: {
  maxRetries: number;
  baseDelayMs: number;
  jitter?: boolean;
  maxDelayMs?: number;
  timeBudgetMs?: number;
}) {
  let delays: Schedule.Schedule<unknown> = Schedule.exponential(
    Duration.millis(params.baseDelayMs),
    2,
  );

  if (params.maxDelayMs) {
    const maxDelay = params.maxDelayMs;
    // Cap individual delays — once exponential exceeds cap, use fixed spacing
    delays = Schedule.union(
      Schedule.exponential(Duration.millis(params.baseDelayMs), 2).pipe(
        Schedule.whileOutput((d) => Duration.toMillis(d) <= maxDelay),
      ),
      Schedule.spaced(Duration.millis(maxDelay)),
    );
  }

  if (params.jitter) {
    delays = Schedule.jittered(delays);
  }

  let combined: Schedule.Schedule<unknown> = Schedule.intersect(
    delays,
    Schedule.recurs(params.maxRetries),
  );

  if (params.timeBudgetMs) {
    combined = combined.pipe(Schedule.upTo(Duration.millis(params.timeBudgetMs)));
  }

  return combined;
}

// ---------------------------------------------------------------------------
// withRetryAll — full-spectrum retry implementation
// ---------------------------------------------------------------------------

const DEFAULT_SHOULD_RETRY_ALL = <T, E>(result: PipelineResult<T, E>): boolean =>
  !PipelineResult.isSuccess(result);

/**
 * Retry that sees ALL results — typed errors, thrown errors from transforms, and success values.
 * The `shouldRetry` predicate decides what triggers a retry.
 *
 * The entire effect (action + transforms) is re-executed on each retry.
 */
export function withRetryAll<T, E>(
  effect: Effect.Effect<T, E>,
  policy: RetryAllPolicy<T, E> = {},
): Effect.Effect<T, E> {
  const { maxRetries = 3, baseDelayMs = 250, shouldRetry = DEFAULT_SHOULD_RETRY_ALL } = policy;

  const schedule = buildSchedule({
    maxRetries,
    baseDelayMs,
    jitter: policy.jitter,
    maxDelayMs: policy.maxDelayMs,
    timeBudgetMs: policy.timeBudgetMs,
  });

  // Normalize everything into the error channel as PipelineResult so retry can see it all
  const normalized: Effect.Effect<T, PipelineResult<T, E>> = effect.pipe(
    // Map typed errors to result
    Effect.mapError((e) => PipelineResult.typedError(e)),
    // Absorb thrown errors into error channel.
    // If the defect is already an Error, pass it through unchanged.
    // Otherwise wrap it — String() gives a readable message for primitives (e.g. throw "oops"),
    // and { cause } preserves the original value for objects (e.g. throw { code: "FAIL" }).
    Effect.catchAllDefect((defect) => {
      const err = defect instanceof Error ? defect : new Error(String(defect), { cause: defect });
      return Effect.fail<PipelineResult<T, E>>(PipelineResult.defect(err));
    }),
    // Check success value — if shouldRetry says retry, push it to error channel
    Effect.flatMap((value) => {
      const result = PipelineResult.success<T>(value);
      if (shouldRetry(result)) return Effect.fail<PipelineResult<T, E>>(result);
      return Effect.succeed(value);
    }),
  );

  // Retry while shouldRetry returns true
  const retried = normalized.pipe(Effect.retry(schedule.pipe(Schedule.whileInput(shouldRetry))));

  // Unwrap: convert PipelineResult errors back to their proper channels
  return retried.pipe(
    Effect.catchAll((result) => {
      switch (result._tag) {
        case "success":
          return Effect.succeed(result.value);
        case "typedError":
          return Effect.fail(result.error);
        case "defect":
          return Effect.die(result.error);
      }
    }),
  );
}

// ---------------------------------------------------------------------------
// withRetry — typed-error-only retry (delegates to retryAll)
// ---------------------------------------------------------------------------

/**
 * Retry on typed errors only. Delegates to {@link withRetryAll} with
 * `shouldRetry` scoped to typed errors matching the `when` predicate.
 */
export function withRetry<T, E>(
  effect: Effect.Effect<T, E>,
  policy: RetryPolicy<E> = {},
): Effect.Effect<T, E> {
  const { when = () => true } = policy;

  return withRetryAll(effect, {
    maxRetries: policy.maxRetries,
    baseDelayMs: policy.baseDelayMs,
    jitter: policy.jitter,
    maxDelayMs: policy.maxDelayMs,
    timeBudgetMs: policy.timeBudgetMs,
    shouldRetry: (result) => PipelineResult.isTypedError(result) && when(result.error),
  });
}
