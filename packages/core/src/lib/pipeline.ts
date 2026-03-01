import { Effect, Either, Cause, Duration, Schedule, Stream } from "effect";
import { TimeoutError, PollTimeoutError, CircuitOpenError } from "./pipeline-error.ts";
import { withRetry, withRetryAll, type RetryPolicy, type RetryAllPolicy } from "./retry.ts";
import {
  parallel as parallelFn,
  allSettled as allSettledFn,
  validate as validateFn,
  race as raceFn,
  fallbackChain as fallbackChainFn,
  hedged as hedgedFn,
  poll as pollFn,
  pollWithBackoff as pollWithBackoffFn,
  withTimeout as withTimeoutFn,
  forEach as forEachFn,
} from "./combinators.ts";
import type { PipelineSemaphore } from "./semaphore.ts";
import type { CircuitBreaker } from "./circuit-breaker.ts";
import type { PipelineCache } from "./cache.ts";

// ---------------------------------------------------------------------------
// TaggedError constraint — required for .catch() to work
// ---------------------------------------------------------------------------

/** Constraint for discriminated-union error types. */
export type TaggedError = { readonly _tag: string };

// ---------------------------------------------------------------------------
// PipelineDefaults — configurable behavior injected at construction
// ---------------------------------------------------------------------------

/**
 * Configurable defaults that flow through the pipeline chain.
 * Injected at construction time (e.g., by HttpClient) and propagated
 * through every chained method.
 *
 * @example
 * ```ts
 * const defaults: PipelineDefaults<HttpClientError> = {
 *   retryWhen: (e) => e._tag === "HttpTimeoutError" || e._tag === "HttpNetworkError",
 * };
 * Pipeline.from(effect, { defaults });
 * ```
 */
export interface PipelineDefaults<E> {
  /** Default predicate for `.retry()` when no explicit `when` is provided. */
  readonly retryWhen?: (error: E) => boolean;
}

// ---------------------------------------------------------------------------
// Pipeline<T, E> — chainable wrapper around Effect<T, E>
// ---------------------------------------------------------------------------

/**
 * A chainable, lazily-evaluated pipeline for any async action with structural concurrency.
 *
 * Nothing executes until you call `.runPromise()`, `.runSafe()`, or `.runEither()`.
 * Chain transformations, retries, error handling, and dependent actions
 * with a fluent API — then resolve to a Promise at the boundary.
 *
 * @typeParam T - The success type
 * @typeParam E - The error type (must extend `{ _tag: string }` for `.catch()`)
 *
 * @example
 * ```ts
 * const result = await Pipeline.fromPromise(() => fetchData())
 *   .map((data) => transform(data))
 *   .retry(3)
 *   .timeout(5_000)
 *   .runPromise();
 * ```
 */
export class Pipeline<T, E extends TaggedError> {
  /** @internal Pipeline defaults — typed as `any` to avoid contravariance issues with `never`. */
  private readonly _defaults?: any;

  constructor(
    readonly effect: Effect.Effect<T, E>,
    defaults?: PipelineDefaults<E>,
  ) {
    this._defaults = defaults;
  }

  // -------------------------------------------------------------------------
  // Transform
  // -------------------------------------------------------------------------

  /**
   * Transform the successful value.
   *
   * @example
   * ```ts
   * pipeline.map((user) => user.name.toUpperCase())
   * ```
   */
  map<U>(fn: (value: T) => U): Pipeline<U, E> {
    return new Pipeline(Effect.map(this.effect, fn), this._defaults);
  }

  /**
   * Chain a dependent pipeline — the next action can use the result of this one.
   *
   * @example
   * ```ts
   * api.get("/users/1", UserSchema)
   *   .flatMap((user) => api.get(`/users/${user.id}/posts`, PostsSchema))
   * ```
   */
  flatMap<U, E2 extends TaggedError>(fn: (value: T) => Pipeline<U, E2>): Pipeline<U, E | E2> {
    return new Pipeline(
      Effect.flatMap(this.effect, (value) => fn(value).effect) as Effect.Effect<U, E | E2>,
      this._defaults,
    );
  }

  /**
   * Chain a dependent async action — like `flatMap` but takes a Promise instead of a Pipeline.
   *
   * @example
   * ```ts
   * api.get("/users/1", UserSchema)
   *   .flatMapAsync((user) => db.getDetails(user.id))
   * ```
   */
  flatMapAsync<U>(fn: (value: T) => Promise<U>): Pipeline<U, E> {
    return new Pipeline(
      Effect.flatMap(this.effect, (value) => Effect.promise(() => fn(value))),
      this._defaults,
    );
  }

  /** Run a sync side-effect on success without changing the value. */
  tap(fn: (value: T) => void): Pipeline<T, E> {
    return new Pipeline(
      Effect.tap(this.effect, (value) => Effect.sync(() => fn(value))),
      this._defaults,
    );
  }

  /** Run an async side-effect on success without changing the value. Awaits the Promise before continuing. */
  tapAsync(fn: (value: T) => Promise<void>): Pipeline<T, E> {
    return new Pipeline(
      Effect.tap(this.effect, (value) => Effect.promise(() => fn(value))),
      this._defaults,
    );
  }

  /**
   * Fork an async side-effect into a background fiber — non-blocking, does not delay the pipeline.
   * The fiber is cancelled if the pipeline is interrupted (no orphaned work).
   * Errors in the forked effect are silently discarded.
   *
   * @example
   * ```ts
   * pipeline.tapAsyncFork((value) => analytics.record(value))
   * ```
   */
  tapAsyncFork(fn: (value: T) => Promise<void>): Pipeline<T, E> {
    return new Pipeline(
      Effect.tap(this.effect, (value) =>
        Effect.fork(Effect.promise(() => fn(value)).pipe(Effect.catchAll(() => Effect.void))),
      ),
      this._defaults,
    );
  }

  /**
   * Fork a side-effect pipeline into a background fiber — non-blocking, does not delay the pipeline.
   * The fiber is cancelled if the pipeline is interrupted.
   * Errors in the forked pipeline are silently discarded.
   *
   * @example
   * ```ts
   * pipeline.tapFork((value) =>
   *   Pipeline.fromPromise(() => metrics.record(value)).retry(1)
   * )
   * ```
   */
  tapFork<E2 extends TaggedError>(fn: (value: T) => Pipeline<unknown, E2>): Pipeline<T, E> {
    return new Pipeline(
      Effect.tap(this.effect, (value) =>
        Effect.fork(fn(value).effect.pipe(Effect.catchAll(() => Effect.void))),
      ),
      this._defaults,
    );
  }

  /**
   * Run a side-effect pipeline on success without changing the value.
   * The side-effect can have its own retry/timeout. Awaits completion before continuing.
   *
   * @example
   * ```ts
   * pipeline.tapPipeline((value) =>
   *   Pipeline.fromPromise(() => metrics.record(value)).retry(1)
   * )
   * ```
   */
  tapPipeline<E2 extends TaggedError>(
    fn: (value: T) => Pipeline<unknown, E2>,
  ): Pipeline<T, E | E2> {
    return new Pipeline(
      Effect.tap(this.effect, (value) => fn(value).effect) as Effect.Effect<T, E | E2>,
      this._defaults,
    );
  }

  /**
   * Async transform — takes a function returning a Promise.
   *
   * @example
   * ```ts
   * pipeline.mapAsync((user) => enrichFromDb(user.id))
   * ```
   */
  mapAsync<U>(fn: (value: T) => Promise<U>): Pipeline<U, E> {
    return new Pipeline(
      Effect.flatMap(this.effect, (value) => Effect.promise(() => fn(value))),
      this._defaults,
    );
  }

  /**
   * Transform the error type.
   *
   * @example
   * ```ts
   * httpPipeline.mapError((httpErr) => new AppError({ cause: httpErr }))
   * ```
   */
  mapError<E2 extends TaggedError>(fn: (error: E) => E2): Pipeline<T, E2> {
    return new Pipeline(Effect.mapError(this.effect, fn));
  }

  /**
   * Filter the result — fails with the provided error if the predicate returns false.
   *
   * @example
   * ```ts
   * pipeline.filter({
   *   predicate: (user) => user.isActive,
   *   orFail: (user) => new InactiveError({ userId: user.id }),
   * })
   * ```
   */
  filter<E2 extends TaggedError>(params: {
    predicate: (value: T) => boolean;
    orFail: (value: T) => E2;
  }): Pipeline<T, E | E2> {
    return new Pipeline(
      Effect.flatMap(this.effect, (value) =>
        params.predicate(value) ? Effect.succeed(value) : Effect.fail(params.orFail(value)),
      ) as Effect.Effect<T, E | E2>,
      this._defaults,
    );
  }

  /** Delay execution by a fixed duration before running the pipeline. */
  delay(ms: number): Pipeline<T, E> {
    return new Pipeline(Effect.delay(this.effect, Duration.millis(ms)), this._defaults);
  }

  /** Conditionally execute — returns `undefined` if the condition is false. */
  when(condition: () => boolean): Pipeline<T | undefined, E> {
    return new Pipeline(
      Effect.suspend(() =>
        condition() ? this.effect : Effect.succeed(undefined as T | undefined),
      ),
      this._defaults,
    );
  }

  // -------------------------------------------------------------------------
  // Resilience
  // -------------------------------------------------------------------------

  /**
   * Retry on typed errors. Accepts a number (shorthand for maxRetries) or a full policy.
   *
   * If no `when` is provided, uses the pipeline's defaults (injected at construction).
   * If no defaults exist, retries all typed errors.
   *
   * @example
   * ```ts
   * pipeline.retry(3)                                     // shorthand
   * pipeline.retry({ maxRetries: 3, jitter: true })       // full policy
   * pipeline.retry({ maxRetries: 5, timeBudgetMs: 30_000 }) // time-budgeted
   * ```
   */
  retry(policyOrMaxRetries?: RetryPolicy<E> | number): Pipeline<T, E> {
    const policy: RetryPolicy<E> | undefined =
      typeof policyOrMaxRetries === "number"
        ? { maxRetries: policyOrMaxRetries }
        : policyOrMaxRetries;
    const effectivePolicy: RetryPolicy<E> = {
      ...policy,
      when: policy?.when ?? this._defaults?.retryWhen,
    };
    return new Pipeline(withRetry(this.effect, effectivePolicy), this._defaults);
  }

  /**
   * Retry that sees ALL outcomes — typed errors, defects from transforms, AND success values.
   *
   * @example
   * ```ts
   * // Retry until success value satisfies condition
   * pipeline.retryAll({
   *   maxRetries: 30,
   *   shouldRetry: (r) => r._tag !== "success" || r.value.status !== "completed",
   * })
   * ```
   */
  retryAll(policy?: RetryAllPolicy<T, E>): Pipeline<T, E> {
    return new Pipeline(withRetryAll(this.effect, policy), this._defaults);
  }

  /**
   * Race this pipeline against one or more others — first to succeed wins, losers are interrupted.
   *
   * @example
   * ```ts
   * api.post("/ai/openai", Schema, { json: prompt })
   *   .race(
   *     api.post("/ai/anthropic", Schema, { json: prompt }),
   *     api.post("/ai/gemini", Schema, { json: prompt }),
   *   )
   *   .runPromise();
   * ```
   */
  race(...others: Pipeline<T, E>[]): Pipeline<T, E> {
    const all = [this.effect, ...others.map((p) => p.effect)];
    return new Pipeline(raceFn(all), this._defaults);
  }

  /**
   * Run this pipeline concurrently with one or more others — all execute in parallel, all results returned as a tuple.
   * Like `Pipeline.all` but as an instance method with `this` as the first element.
   *
   * @example
   * ```ts
   * const [user, stats, posts] = await api.get("/users/1", UserSchema)
   *   .concurrently(
   *     api.get("/stats", StatsSchema),
   *     api.get("/posts", PostsSchema),
   *   )
   *   .runPromise();
   * ```
   */
  concurrently<Others extends readonly Pipeline<any, any>[]>(
    ...others: Others
  ): Pipeline<
    [T, ...{ [K in keyof Others]: Others[K] extends Pipeline<infer U, any> ? U : never }],
    E | (Others[number] extends Pipeline<any, infer E2 extends TaggedError> ? E2 : never)
  > {
    const effects = [this.effect, ...others.map((p) => p.effect)];
    return new Pipeline(Effect.all(effects, { concurrency: "unbounded" }) as any, this._defaults);
  }

  /** Apply a total timeout to the entire pipeline (including retries). */
  timeout(timeoutMs: number): Pipeline<T, E | TimeoutError> {
    return new Pipeline(withTimeoutFn(this.effect, { timeoutMs }), this._defaults);
  }

  /**
   * Acquire a permit from a semaphore before running, release after.
   *
   * @example
   * ```ts
   * const apiLimit = PipelineSemaphore.make(10);
   * pipeline.withPermit(apiLimit).runPromise();
   * ```
   */
  withPermit(semaphore: PipelineSemaphore): Pipeline<T, E> {
    return new Pipeline(semaphore.withPermit(this.effect), this._defaults);
  }

  /**
   * Wrap with circuit breaker — fails fast with CircuitOpenError when the circuit is open.
   *
   * @example
   * ```ts
   * const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeoutMs: 30_000 });
   * pipeline.withCircuitBreaker(breaker)
   *   .catch("CircuitOpenError", () => fallbackValue)
   * ```
   */
  withCircuitBreaker(breaker: CircuitBreaker): Pipeline<T, E | CircuitOpenError> {
    return new Pipeline(breaker.protect(this.effect), this._defaults);
  }

  /**
   * Return cached value if fresh, otherwise re-execute and cache.
   *
   * @example
   * ```ts
   * const cache = new PipelineCache<Token>(55 * 60 * 1000);
   * const getToken = api.post("/oauth/token", TokenSchema, { json: creds }).cached(cache);
   * await getToken.runPromise(); // hits API
   * await getToken.runPromise(); // returns cached
   * ```
   */
  cached(cache: PipelineCache<T>): Pipeline<T, E> {
    return new Pipeline(cache.wrap(this.effect), this._defaults);
  }

  /**
   * Repeat this pipeline N times with an interval. Returns the last result.
   *
   * @example
   * ```ts
   * await Pipeline.fromPromise(() => healthCheck())
   *   .repeat({ times: 10, intervalMs: 5_000 })
   *   .runPromise();
   * ```
   */
  repeat(params: { times: number; intervalMs?: number }): Pipeline<T, E> {
    const schedule: Schedule.Schedule<unknown> = params.intervalMs
      ? Schedule.intersect(
          Schedule.recurs(params.times),
          Schedule.spaced(Duration.millis(params.intervalMs)),
        )
      : Schedule.recurs(params.times);
    return new Pipeline(
      Effect.repeat(this.effect, schedule) as unknown as Effect.Effect<T, E>,
      this._defaults,
    );
  }

  /**
   * Run this pipeline as a supervised long-lived process that restarts on failure.
   * Useful for background consumers, webhook listeners, queue processors.
   *
   * @example
   * ```ts
   * await Pipeline.fromPromise(() => consumeQueue())
   *   .supervised({ restart: "on-failure", maxRestarts: 10, intervalMs: 1_000 })
   *   .runPromise(); // runs forever, restarts on crash
   * ```
   */
  supervised(params?: {
    restart?: "on-failure" | "always";
    maxRestarts?: number;
    intervalMs?: number;
  }): Pipeline<void, never> {
    const { restart = "on-failure", maxRestarts, intervalMs = 1_000 } = params ?? {};

    let schedule: Schedule.Schedule<unknown> = Schedule.spaced(Duration.millis(intervalMs));
    if (maxRestarts !== undefined) {
      schedule = Schedule.intersect(schedule, Schedule.recurs(maxRestarts));
    }

    // Absorb all errors (typed + defects) so retry/repeat can see them
    const inner = this.effect.pipe(
      Effect.catchAll(() => Effect.fail("error" as const)),
      Effect.catchAllDefect(() => Effect.fail("error" as const)),
    );

    const looped: Effect.Effect<unknown, string> =
      restart === "always"
        ? Effect.repeat(inner.pipe(Effect.catchAll(() => Effect.void)), schedule)
        : Effect.retry(inner, schedule);

    return new Pipeline(
      looped.pipe(
        Effect.catchAll(() => Effect.void),
        Effect.map(() => undefined as void),
      ) as Effect.Effect<void, never>,
    );
  }

  // -------------------------------------------------------------------------
  // Error handling
  // -------------------------------------------------------------------------

  /** Run a side-effect on error without changing the error. */
  tapError(fn: (error: E) => void): Pipeline<T, E> {
    return new Pipeline(
      Effect.tapError(this.effect, (error) => Effect.sync(() => fn(error))),
      this._defaults,
    );
  }

  /** Run a side-effect on the full Cause (typed errors, defects, interruptions). */
  tapCause(fn: (cause: Cause.Cause<E>) => void): Pipeline<T, E> {
    return new Pipeline(
      Effect.tapErrorCause(this.effect, (cause) => Effect.sync(() => fn(cause))),
      this._defaults,
    );
  }

  /** Recover from errors by providing a fallback value. */
  orElse(fallback: T): Pipeline<T, never> {
    return new Pipeline(
      Effect.orElse(this.effect, () => Effect.succeed(fallback)) as Effect.Effect<T, never>,
    );
  }

  /**
   * Recover from errors by running a different pipeline.
   *
   * @example
   * ```ts
   * primaryApi.get("/data", Schema)
   *   .orElsePipeline(() => backupApi.get("/data", Schema))
   * ```
   */
  orElsePipeline<U, E2 extends TaggedError>(
    fn: (error: E) => Pipeline<U, E2>,
  ): Pipeline<T | U, E2> {
    return new Pipeline(
      Effect.catchAll(this.effect, (error) => fn(error).effect),
      this._defaults,
    );
  }

  /**
   * Catch specific error tags and recover.
   *
   * @example
   * ```ts
   * pipeline
   *   .catch("HttpStatusError", (err) => err.status === 404 ? defaultValue : throw err)
   *   .catch("CircuitOpenError", () => fallbackValue)
   * ```
   */
  catch<Tag extends E["_tag"]>(
    tag: Tag,
    fn: (error: Extract<E, { _tag: Tag }>) => T,
  ): Pipeline<T, Exclude<E, { _tag: Tag }>> {
    return new Pipeline(
      Effect.catchAll(this.effect, (error) =>
        error._tag === tag
          ? Effect.succeed(fn(error as Extract<E, { _tag: Tag }>))
          : Effect.fail(error as Exclude<E, { _tag: Tag }>),
      ),
      this._defaults as PipelineDefaults<Exclude<E, { _tag: Tag }>> | undefined,
    );
  }

  // -------------------------------------------------------------------------
  // Polling (turn this single action into a poller)
  // -------------------------------------------------------------------------

  /**
   * Repeat this action until the condition is met.
   *
   * @example
   * ```ts
   * api.get(`/jobs/${id}`, JobSchema).pollUntil({
   *   until: (job) => job.status === "completed",
   *   intervalMs: 2_000,
   *   maxAttempts: 30,
   * })
   * ```
   */
  pollUntil(params: {
    until: (value: T) => boolean;
    intervalMs?: number;
    maxAttempts?: number;
    maxDurationMs?: number;
  }): Pipeline<T, E | PollTimeoutError> {
    return new Pipeline(
      pollFn({
        request: this.effect,
        until: params.until,
        intervalMs: params.intervalMs,
        maxAttempts: params.maxAttempts,
        maxDurationMs: params.maxDurationMs,
      }),
      this._defaults,
    );
  }

  /** Repeat this action with exponential backoff until the condition is met. */
  pollUntilWithBackoff(params: {
    until: (value: T) => boolean;
    initialIntervalMs?: number;
    maxIntervalMs?: number;
    maxAttempts?: number;
    maxDurationMs?: number;
  }): Pipeline<T, E | PollTimeoutError> {
    return new Pipeline(
      pollWithBackoffFn({
        request: this.effect,
        until: params.until,
        initialIntervalMs: params.initialIntervalMs,
        maxIntervalMs: params.maxIntervalMs,
        maxAttempts: params.maxAttempts,
        maxDurationMs: params.maxDurationMs,
      }),
      this._defaults,
    );
  }

  // -------------------------------------------------------------------------
  // Static constructors
  // -------------------------------------------------------------------------

  /** Create a pipeline from a raw Effect, optionally with defaults. */
  static from<T, E extends TaggedError>(
    effect: Effect.Effect<T, E>,
    options?: { defaults?: PipelineDefaults<E> },
  ): Pipeline<T, E> {
    return new Pipeline(effect, options?.defaults);
  }

  /**
   * Create a pipeline from a Promise-returning function.
   * Rejections become defects (untyped errors) — use `.retryAll()` to handle them.
   *
   * @example
   * ```ts
   * Pipeline.fromPromise(() => fetch("/api").then(r => r.json()))
   * ```
   */
  static fromPromise<T>(fn: () => Promise<T>): Pipeline<T, never> {
    return new Pipeline(Effect.promise(fn));
  }

  /**
   * Shorthand for `Pipeline.fromPromise`. Less ceremony for wrapping async functions.
   *
   * @example
   * ```ts
   * Pipeline.fn(() => fetchUser(id))
   * // instead of: Pipeline.fromPromise(() => fetchUser(id))
   * ```
   */
  static fn<T>(f: () => Promise<T>): Pipeline<T, never> {
    return new Pipeline(Effect.promise(f));
  }

  /**
   * Create a pipeline that sleeps for the given duration, then succeeds with void.
   *
   * @example
   * ```ts
   * await Pipeline.sleep(1_000).runPromise(); // wait 1 second
   * ```
   */
  static sleep(ms: number): Pipeline<void, never> {
    return new Pipeline(
      Effect.sleep(Duration.millis(ms)).pipe(Effect.asVoid) as Effect.Effect<void, never>,
    );
  }

  /** Create a pipeline that succeeds with the given value. */
  static succeed<T>(value: T): Pipeline<T, never> {
    return new Pipeline(Effect.succeed(value));
  }

  /** Create a pipeline that fails with the given error. */
  static fail<E extends TaggedError>(error: E): Pipeline<never, E> {
    return new Pipeline(Effect.fail(error));
  }

  /**
   * Create a pipeline from a scoped resource (acquire/release).
   * The resource is automatically released when the pipeline completes, fails, or is interrupted.
   *
   * @example
   * ```ts
   * Pipeline.scoped({
   *   acquire: () => pool.connect(),
   *   release: (conn) => conn.release(),
   *   use: (conn) => Pipeline.fromPromise(() => conn.query("SELECT 1")),
   * })
   * ```
   */
  static scoped<R, T, E extends TaggedError>(params: {
    acquire: () => R | Promise<R>;
    release: (resource: R) => void | Promise<void>;
    use: (resource: R) => Pipeline<T, E>;
  }): Pipeline<T, E> {
    const acquire = Effect.suspend(() => {
      const result = params.acquire();
      return result instanceof Promise ? Effect.promise(() => result) : Effect.succeed(result as R);
    });

    const scoped = Effect.acquireUseRelease(
      acquire,
      (resource) => params.use(resource).effect,
      (resource) => {
        const result = params.release(resource);
        return result instanceof Promise ? Effect.promise(() => result) : Effect.sync(() => {});
      },
    );

    return new Pipeline(scoped as Effect.Effect<T, E>);
  }

  // -------------------------------------------------------------------------
  // Static combinators
  // -------------------------------------------------------------------------

  /**
   * Run multiple pipelines in parallel — all must succeed (short-circuits on first error).
   *
   * @example
   * ```ts
   * const [users, stats] = await Pipeline.all(
   *   api.get("/users", UsersSchema),
   *   api.get("/stats", StatsSchema),
   * ).runPromise();
   * ```
   */
  static all<Pipelines extends readonly Pipeline<any, any>[]>(
    ...pipelines: Pipelines
  ): Pipeline<
    { [K in keyof Pipelines]: Pipelines[K] extends Pipeline<infer U, any> ? U : never },
    Pipelines[number] extends Pipeline<any, infer E extends TaggedError> ? E : never
  > {
    const effects = pipelines.map((p) => p.effect);
    return new Pipeline(parallelFn(effects as any) as any);
  }

  /** Run multiple pipelines in parallel — never short-circuits, returns Either for each. */
  static allSettled<Pipelines extends readonly Pipeline<any, any>[]>(
    ...pipelines: Pipelines
  ): Pipeline<
    {
      [K in keyof Pipelines]: Either.Either<
        Pipelines[K] extends Pipeline<infer U, any> ? U : never,
        Pipelines[number] extends Pipeline<any, infer E extends TaggedError> ? E : never
      >;
    },
    never
  > {
    const effects = pipelines.map((p) => p.effect);
    return new Pipeline(allSettledFn(effects as any) as any);
  }

  /**
   * Run multiple pipelines in parallel — accumulates ALL errors instead of short-circuiting.
   * Use for validation scenarios where you want to report every failure.
   *
   * @example
   * ```ts
   * const [email, age, name] = await Pipeline.validate(
   *   validateEmail(input.email),
   *   validateAge(input.age),
   *   validateName(input.name),
   * ).runPromise(); // throws with ALL validation errors
   * ```
   */
  static validate<Pipelines extends readonly Pipeline<any, any>[]>(
    ...pipelines: Pipelines
  ): Pipeline<
    { [K in keyof Pipelines]: Pipelines[K] extends Pipeline<infer U, any> ? U : never },
    Pipelines[number] extends Pipeline<any, infer E extends TaggedError> ? E : never
  > {
    const effects = pipelines.map((p) => p.effect);
    return new Pipeline(validateFn(effects as any) as any);
  }

  /** Race multiple pipelines — first to succeed wins. */
  static race<T, E extends TaggedError>(...pipelines: Pipeline<T, E>[]): Pipeline<T, E> {
    return new Pipeline(raceFn(pipelines.map((p) => p.effect)));
  }

  /** Try pipelines in order — first to succeed wins (no parallelism). */
  static fallback<T, E extends TaggedError>(...pipelines: Pipeline<T, E>[]): Pipeline<T, E> {
    return new Pipeline(fallbackChainFn(pipelines.map((p) => p.effect)));
  }

  /** Hedged execution: start primary, fire backup after delay. First to succeed wins. */
  static hedged<T, E extends TaggedError>(
    pipeline: Pipeline<T, E>,
    options: { readonly hedgeDelayMs: number },
  ): Pipeline<T, E> {
    return new Pipeline(hedgedFn(pipeline.effect, options));
  }

  /**
   * Execute a function for each item with bounded concurrency.
   *
   * @example
   * ```ts
   * const users = await Pipeline.forEach(
   *   userIds,
   *   (id) => api.get(`/users/${id}`, UserSchema),
   *   { concurrency: 5 },
   * ).runPromise();
   * ```
   */
  static forEach<T, U, E extends TaggedError>(
    items: readonly T[],
    fn: (item: T) => Pipeline<U, E>,
    options: { readonly concurrency: number },
  ): Pipeline<U[], E> {
    return new Pipeline(forEachFn(items, (item) => fn(item).effect, options));
  }

  // -------------------------------------------------------------------------
  // Observability
  // -------------------------------------------------------------------------

  /**
   * Annotate this pipeline with a tracing span name (Effect's built-in tracing).
   *
   * @example
   * ```ts
   * pipeline.withSpan("fetchUser")
   * ```
   */
  withSpan(name: string): Pipeline<T, E> {
    return new Pipeline(Effect.withSpan(this.effect, name), this._defaults);
  }

  /**
   * Annotate the current span with a key-value attribute.
   *
   * @example
   * ```ts
   * pipeline.withSpan("fetchUser").withTag("userId", id)
   * ```
   */
  withTag(key: string, value: string): Pipeline<T, E> {
    return new Pipeline(
      Effect.tap(this.effect, () => Effect.annotateCurrentSpan(key, value)),
      this._defaults,
    );
  }

  // -------------------------------------------------------------------------
  // Terminals
  // -------------------------------------------------------------------------

  /** Run a cleanup function regardless of success or failure. */
  finally(fn: () => void): Pipeline<T, E> {
    return new Pipeline(Effect.ensuring(this.effect, Effect.sync(fn)), this._defaults);
  }

  /** Run a cleanup function only when the pipeline is interrupted (cancelled). */
  onInterrupt(fn: () => void): Pipeline<T, E> {
    return new Pipeline(
      Effect.onInterrupt(this.effect, () => Effect.sync(fn)),
      this._defaults,
    );
  }

  /** Execute the pipeline and return a Promise. This is where the lazy chain materializes. */
  runPromise(): Promise<T> {
    return Effect.runPromise(this.effect);
  }

  /**
   * Execute and return `{ data, error }` — never throws.
   *
   * @example
   * ```ts
   * const { data, error } = await pipeline.runSafe();
   * if (error) handleError(error);
   * else useData(data);
   * ```
   */
  async runSafe(): Promise<{ data: T; error: null } | { data: null; error: E }> {
    const either = await Effect.runPromise(Effect.either(this.effect));
    return Either.isRight(either)
      ? { data: either.right, error: null }
      : { data: null, error: either.left };
  }

  /** Execute and return an `Either<T, E>` — never throws. */
  async runEither(): Promise<Either.Either<T, E>> {
    return Effect.runPromise(Effect.either(this.effect));
  }

  /** Escape hatch: get the raw Effect for advanced composition. */
  toEffect(): Effect.Effect<T, E> {
    return this.effect;
  }

  /** Escape hatch: get the raw Effect as a single-element Stream. */
  toEffectStream(): Stream.Stream<T, E> {
    return Stream.fromEffect(this.effect);
  }
}
