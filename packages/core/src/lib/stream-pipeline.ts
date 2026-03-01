import { Effect, Stream, Chunk, Duration, Schedule, Ref, Option } from "effect";
import type { TaggedError, Pipeline } from "./pipeline.ts";
import type { PipelineRef } from "./ref.ts";

// ---------------------------------------------------------------------------
// StreamPipeline<T, E> — chainable wrapper around Stream<T, E>
// ---------------------------------------------------------------------------

/**
 * A chainable, lazily-evaluated streaming pipeline for any async source with structural concurrency.
 *
 * Nothing executes until you call a terminal (`.forEach()`, `.collect()`, `.reduce()`, `.drain()`).
 * Handles backpressure naturally — the producer only advances when the consumer is ready.
 *
 * @typeParam T - The item type emitted by the stream
 * @typeParam E - The error type (must extend `{ _tag: string }`)
 */
export class StreamPipeline<T, E extends TaggedError> {
  constructor(readonly stream: Stream.Stream<T, E>) {}

  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  /** Create a StreamPipeline from a raw Effect Stream. */
  static from<T, E extends TaggedError>(stream: Stream.Stream<T, E>): StreamPipeline<T, E> {
    return new StreamPipeline(stream);
  }

  /** Create a StreamPipeline from an iterable. */
  static fromIterable<T>(items: Iterable<T>): StreamPipeline<T, never> {
    return new StreamPipeline(Stream.fromIterable(items));
  }

  /** Create a StreamPipeline from an async iterable (e.g., async generator). */
  static fromAsyncIterable<T, E extends TaggedError>(
    iterable: AsyncIterable<T>,
    onError: (error: unknown) => E,
  ): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.fromAsyncIterable(iterable, onError));
  }

  /** Create an empty StreamPipeline. */
  static empty<T, E extends TaggedError>(): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.empty);
  }

  /**
   * Bridge a Pipeline into a StreamPipeline — emits the Pipeline's result as a single item.
   * Combine with `.flatMap()` to fan out into a stream.
   *
   * @example
   * ```ts
   * // Fetch IDs → stream over them → enrich in parallel
   * StreamPipeline.fromPipeline(api.get("/video-ids", IdsSchema))
   *   .flatMap((ids) => StreamPipeline.fromIterable(ids))
   *   .parAsyncMap(5, (id) => fetchVideo(id))
   *   .drain();
   * ```
   */
  static fromPipeline<T, E extends TaggedError>(pipeline: Pipeline<T, E>): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.fromEffect(pipeline.effect));
  }

  /**
   * Emit a tick at a fixed interval. Like fs2's `Stream.awakeEvery`.
   * Each emission is the tick count (0, 1, 2, ...).
   *
   * @example
   * ```ts
   * // Poll every 5 seconds
   * await StreamPipeline.tick(5_000)
   *   .mapAsync(() => fetchMetrics())
   *   .forEach((m) => gauge.set(m.value));
   *
   * // Combined with zipWith for periodic enrichment
   * await StreamPipeline.tick(1_000)
   *   .take(60)
   *   .mapAsync(() => healthCheck())
   *   .drain();
   * ```
   */
  static tick(intervalMs: number): StreamPipeline<number, never> {
    return new StreamPipeline(Stream.fromSchedule(Schedule.spaced(Duration.millis(intervalMs))));
  }

  /**
   * Generate a stream by repeatedly applying an async function to a seed value.
   * Return `undefined` as next seed to stop. Like fs2's `Stream.unfold`.
   *
   * @example
   * ```ts
   * // Paginated API
   * const allItems = await StreamPipeline.unfold(firstCursor, async (cursor) => {
   *   const page = await fetchPage(cursor);
   *   return page.nextCursor
   *     ? { value: page.items, next: page.nextCursor }
   *     : { value: page.items };
   * })
   *   .flatMap((items) => StreamPipeline.fromIterable(items))
   *   .collect();
   * ```
   */
  static unfold<T, S>(
    seed: S,
    fn: (state: S) => Promise<{ value: T; next?: S }>,
  ): StreamPipeline<T, never> {
    return new StreamPipeline(
      Stream.unfoldEffect(seed as S | undefined, (state) => {
        if (state === undefined) return Effect.succeed(Option.none());
        return Effect.promise(() => fn(state)).pipe(
          Effect.map((result) => Option.some([result.value, result.next] as const)),
        );
      }),
    );
  }

  /**
   * Generate a stream by repeatedly applying a function to the previous value.
   * Like fs2's `Stream.iterate`.
   *
   * @example
   * ```ts
   * // Fibonacci
   * StreamPipeline.iterate([0, 1], ([a, b]) => [b, a + b])
   *   .map(([a]) => a)
   *   .take(10)
   *   .collect(); // [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
   * ```
   */
  static iterate<T>(initial: T, fn: (prev: T) => T): StreamPipeline<T, never> {
    return new StreamPipeline(Stream.iterate(initial, fn));
  }

  // -------------------------------------------------------------------------
  // Transform
  // -------------------------------------------------------------------------

  /** Transform each item. */
  map<U>(fn: (value: T) => U): StreamPipeline<U, E> {
    return new StreamPipeline(Stream.map(this.stream, fn));
  }

  /** Async transform each item — takes a function returning a Promise. The resolved value replaces the item. */
  mapAsync<U>(fn: (value: T) => Promise<U>): StreamPipeline<U, E> {
    return new StreamPipeline(
      Stream.mapEffect(this.stream, (value) => Effect.promise(() => fn(value))),
    );
  }

  /** Transform each item using an Effect. Escape hatch for Effect-native code. */
  mapEffect<U, E2 extends TaggedError>(
    fn: (value: T) => Effect.Effect<U, E2>,
  ): StreamPipeline<U, E | E2> {
    return new StreamPipeline(Stream.mapEffect(this.stream, fn) as Stream.Stream<U, E | E2>);
  }

  /**
   * Pair each item with its zero-based index. Like fs2's `zipWithIndex`.
   *
   * @example
   * ```ts
   * await stream.zipWithIndex().forEach(([item, index]) => console.log(`#${index}: ${item}`));
   * ```
   */
  zipWithIndex(): StreamPipeline<[T, number], E> {
    return new StreamPipeline(Stream.zipWithIndex(this.stream));
  }

  /** Stateful map — carries an accumulator, emits both accumulator and transformed value. */
  mapAccumulate<S, U>(
    initial: S,
    fn: (state: S, value: T) => readonly [S, U],
  ): StreamPipeline<U, E> {
    return new StreamPipeline(
      Stream.mapAccum(this.stream, initial, (state, value) => {
        const [nextState, output] = fn(state, value);
        return [nextState, output];
      }),
    );
  }

  /**
   * Filter items by predicate.
   * Optionally specify `action: "keep"` or `"drop"` for readability.
   * Default: predicate returns true → item is kept.
   *
   * @example
   * ```ts
   * stream.filter((n) => n > 5)                    // keep items > 5
   * stream.filter((n) => n > 5, "keep")             // same, explicit
   * stream.filter((e) => e.type === "internal", "drop") // drop internal events
   * ```
   */
  filter(fn: (value: T) => boolean, action?: "keep" | "drop"): StreamPipeline<T, E> {
    const predicate = action === "drop" ? (value: T) => !fn(value) : fn;
    return new StreamPipeline(Stream.filter(this.stream, predicate));
  }

  /**
   * Filter items by async predicate. Awaits the Promise before deciding.
   * Optionally specify `action: "keep"` or `"drop"` for readability.
   *
   * @example
   * ```ts
   * stream.filterAsync(async (user) => db.isActive(user.id), "keep")
   * stream.filterAsync(async (user) => db.isBanned(user.id), "drop")
   * ```
   */
  filterAsync(fn: (value: T) => Promise<boolean>, action?: "keep" | "drop"): StreamPipeline<T, E> {
    const predicate = action === "drop" ? (value: T) => fn(value).then((r) => !r) : fn;
    return new StreamPipeline(
      Stream.filterEffect(this.stream, (value) => Effect.promise(() => predicate(value))),
    );
  }

  /**
   * Filter and transform in one pass — return `undefined` to drop, return a value to keep.
   * Like fs2's `collect` with a partial function.
   *
   * @example
   * ```ts
   * // Instead of: stream.filter(x => x.status === "ok").map(x => x.data)
   * stream.filterMap((x) => x.status === "ok" ? x.data : undefined)
   * ```
   */
  filterMap<U>(fn: (value: T) => U | undefined): StreamPipeline<U, E> {
    return new StreamPipeline(
      Stream.filterMap(this.stream, (value) => {
        const result = fn(value);
        return result === undefined ? (Option.none() as Option.Option<U>) : Option.some(result);
      }),
    );
  }

  /**
   * Drop `undefined` and `null` values, narrowing the type.
   *
   * @example
   * ```ts
   * stream.mapAsync((id) => db.findUser(id))  // StreamPipeline<User | undefined>
   *   .unNone()                                // StreamPipeline<User>
   * ```
   */
  unNone(): StreamPipeline<NonNullable<T>, E> {
    return new StreamPipeline(
      Stream.filter(this.stream, (value): value is NonNullable<T> => value != null),
    );
  }

  /** Run a sync side-effect for each item without changing it. */
  tap(fn: (value: T) => void): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.tap(this.stream, (value) => Effect.sync(() => fn(value))));
  }

  /** Run an async side-effect for each item without changing it. Awaits the Promise before continuing. */
  tapAsync(fn: (value: T) => Promise<void>): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.tap(this.stream, (value) => Effect.promise(() => fn(value))));
  }

  /**
   * Fork an async side-effect into a background fiber for each item — non-blocking.
   * The fiber is cancelled if the stream is interrupted. Errors are silently discarded.
   *
   * @example
   * ```ts
   * stream.tapAsyncFork((item) => analytics.record(item))
   * ```
   */
  tapAsyncFork(fn: (value: T) => Promise<void>): StreamPipeline<T, E> {
    return new StreamPipeline(
      Stream.tap(this.stream, (value) =>
        Effect.fork(Effect.promise(() => fn(value)).pipe(Effect.catchAll(() => Effect.void))),
      ),
    );
  }

  /** Take the first N items then stop. */
  take(n: number): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.take(this.stream, n));
  }

  /** Take items while predicate is true, then stop. */
  takeWhile(fn: (value: T) => boolean): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.takeWhile(this.stream, fn));
  }

  /** Skip the first N items. */
  drop(n: number): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.drop(this.stream, n));
  }

  /** Skip items while the predicate is true, then emit everything after. */
  dropWhile(fn: (value: T) => boolean): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.dropWhile(this.stream, fn));
  }

  /** Emit only when the value changes (by structural equality). */
  dedupe(): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.changes(this.stream));
  }

  /**
   * Deduplicate by a key function. Keeps the first occurrence of each key.
   *
   * @example
   * ```ts
   * stream.distinctBy((event) => event.id) // dedup by ID
   * ```
   */
  distinctBy<K>(fn: (value: T) => K): StreamPipeline<T, E> {
    const seen = new Set<K>();
    return new StreamPipeline(
      Stream.filter(this.stream, (value) => {
        const key = fn(value);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      }),
    );
  }

  /** Flat-map each item into a sub-stream. */
  flatMap<U, E2 extends TaggedError>(
    fn: (value: T) => StreamPipeline<U, E2>,
  ): StreamPipeline<U, E | E2> {
    return new StreamPipeline(
      Stream.flatMap(this.stream, (value) => fn(value).stream) as Stream.Stream<U, E | E2>,
    );
  }

  /** Flat-map but cancel the previous inner stream when a new item arrives ("latest wins"). */
  switchMap<U, E2 extends TaggedError>(
    fn: (value: T) => StreamPipeline<U, E2>,
  ): StreamPipeline<U, E | E2> {
    return new StreamPipeline(
      Stream.flatMap(this.stream, (value) => fn(value).stream, {
        switch: true,
      }) as Stream.Stream<U, E | E2>,
    );
  }

  /** Append another stream after this one completes. */
  concat(other: StreamPipeline<T, E>): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.concat(this.stream, other.stream));
  }

  // -------------------------------------------------------------------------
  // Parallel & batching
  // -------------------------------------------------------------------------

  /** Async transform with bounded concurrency, preserving input order. */
  parAsyncMap<U>(concurrency: number, fn: (value: T) => Promise<U>): StreamPipeline<U, E> {
    return new StreamPipeline(
      Stream.mapEffect(this.stream, (value) => Effect.promise(() => fn(value)), { concurrency }),
    );
  }

  /** Like `parAsyncMap` but results arrive in completion order, not input order. */
  parAsyncMapUnordered<U>(concurrency: number, fn: (value: T) => Promise<U>): StreamPipeline<U, E> {
    return new StreamPipeline(
      Stream.mapEffect(this.stream, (value) => Effect.promise(() => fn(value)), {
        concurrency,
        unordered: true,
      }),
    );
  }

  /** Async transform with per-item retry. Failed items are retried before moving on. */
  mapAsyncRetry<U>(
    fn: (value: T) => Promise<U>,
    policy?: { maxRetries?: number; baseDelayMs?: number },
  ): StreamPipeline<U, E> {
    const { maxRetries = 3, baseDelayMs = 250 } = policy ?? {};
    const schedule: Schedule.Schedule<unknown> = Schedule.intersect(
      Schedule.exponential(Duration.millis(baseDelayMs), 2),
      Schedule.recurs(maxRetries),
    );
    return new StreamPipeline(
      Stream.mapEffect(this.stream, (value) => {
        // Effect.promise turns rejections into defects. Absorb defects into error channel
        // so Effect.retry can see them.
        const attempt = Effect.catchAllDefect(
          Effect.promise(() => fn(value)),
          (defect) => Effect.fail(defect),
        );
        return Effect.retry(attempt, schedule).pipe(
          // Convert back to defect if exhausted
          Effect.catchAll((defect) => Effect.die(defect)),
        );
      }) as Stream.Stream<U, E>,
    );
  }

  /** Buffer items into batches of up to `maxSize` items or `maxWaitMs` ms, whichever comes first. */
  groupWithin(maxSize: number, maxWaitMs: number): StreamPipeline<T[], E> {
    return new StreamPipeline(
      Stream.groupedWithin(this.stream, maxSize, Duration.millis(maxWaitMs)).pipe(
        Stream.map(Chunk.toArray),
      ),
    );
  }

  /** Buffer items into fixed-size batches. Last batch may be smaller. */
  grouped(size: number): StreamPipeline<T[], E> {
    return new StreamPipeline(Stream.grouped(this.stream, size).pipe(Stream.map(Chunk.toArray)));
  }

  /** Sliding window over stream items. Emits arrays of `size` elements. */
  sliding(size: number): StreamPipeline<T[], E> {
    return new StreamPipeline(Stream.sliding(this.stream, size).pipe(Stream.map(Chunk.toArray)));
  }

  /** Decouple producer/consumer — buffer up to N items ahead. */
  buffer(capacity: number): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.buffer(this.stream, { capacity }));
  }

  /** Running accumulator — like `reduce` but emits every intermediate result. */
  scan<U>(initial: U, fn: (acc: U, value: T) => U): StreamPipeline<U, E> {
    return new StreamPipeline(Stream.scan(this.stream, initial, fn));
  }

  /** Emit only after a quiet period of `ms` milliseconds with no new items. */
  debounce(ms: number): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.debounce(this.stream, Duration.millis(ms)));
  }

  /** Enforce max emission rate — emit at most 1 item per `ms` milliseconds. */
  metered(ms: number): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.schedule(this.stream, Schedule.spaced(Duration.millis(ms))));
  }

  // -------------------------------------------------------------------------
  // Combination
  // -------------------------------------------------------------------------

  /** Merge another stream — interleave items from both as they arrive. */
  merge(other: StreamPipeline<T, E>): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.merge(this.stream, other.stream));
  }

  /** Merge multiple streams — interleave items from all as they arrive. */
  static mergeAll<T, E extends TaggedError>(
    ...streams: StreamPipeline<T, E>[]
  ): StreamPipeline<T, E> {
    if (streams.length === 0) return new StreamPipeline(Stream.empty);
    return streams.reduce((acc, s) => acc.merge(s));
  }

  /** Combine two streams element-by-element. */
  zipWith<U, V, E2 extends TaggedError>(
    other: StreamPipeline<U, E2>,
    fn: (a: T, b: U) => V,
  ): StreamPipeline<V, E | E2> {
    return new StreamPipeline(
      Stream.zipWith(this.stream, other.stream, fn) as Stream.Stream<V, E | E2>,
    );
  }

  /** Alternate items from two streams (round-robin). */
  interleave(other: StreamPipeline<T, E>): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.interleave(this.stream, other.stream));
  }

  /** Reusable stream transformation. */
  through<U, E2 extends TaggedError>(
    pipe: (stream: StreamPipeline<T, E>) => StreamPipeline<U, E2>,
  ): StreamPipeline<U, E2> {
    return pipe(this);
  }

  /**
   * Fan out every item to multiple parallel processing pipelines.
   * Each function receives a clone of the stream; results are merged.
   *
   * @example
   * ```ts
   * await stream.broadcastThrough(
   *   (s) => s.map(toMetric).tapAsync(writeMetrics),
   *   (s) => s.filter(isAnomaly).tapAsync(sendAlert),
   * ).drain();
   * ```
   */
  broadcastThrough(
    ...fns: ((stream: StreamPipeline<T, E>) => StreamPipeline<unknown, E>)[]
  ): StreamPipeline<unknown, E> {
    if (fns.length === 0) return this as StreamPipeline<unknown, E>;
    // Each fn gets the same stream; results are merged
    const streams = fns.map((fn) => fn(this));
    return streams.reduce((acc, s) => acc.merge(s as StreamPipeline<unknown, E>));
  }

  /**
   * Fork a parallel side-effect consumer that does not block the main stream.
   * Every item passes through to the main pipeline unchanged while also being
   * processed by the observer in the background.
   *
   * @example
   * ```ts
   * await stream
   *   .observe((s) => s.groupWithin(100, 5_000).tapAsync(writeToAnalytics))
   *   .parAsyncMap(10, processItem)
   *   .drain();
   * ```
   */
  observe(fn: (stream: StreamPipeline<T, E>) => StreamPipeline<unknown, E>): StreamPipeline<T, E> {
    // Use tap + fork to run the observer in background without blocking
    return new StreamPipeline(
      Stream.tap(this.stream, (value) =>
        Effect.fork(
          Stream.runDrain(fn(new StreamPipeline(Stream.make(value))).stream).pipe(
            Effect.catchAll(() => Effect.void),
          ),
        ),
      ),
    );
  }

  /**
   * Pause/resume the stream based on a boolean ref.
   * When the ref is `true`, the stream pauses. When `false`, it resumes.
   *
   * @example
   * ```ts
   * const paused = PipelineRef.make(false);
   * stream.pauseWhen(paused).forEach(process);
   * // From another fiber:
   * await Effect.runPromise(paused.update(() => true));  // pause
   * await Effect.runPromise(paused.update(() => false)); // resume
   * ```
   */
  pauseWhen(ref: PipelineRef<boolean>): StreamPipeline<T, E> {
    // Check the ref before emitting each item; if paused, wait until unpaused
    return new StreamPipeline(
      Stream.mapEffect(this.stream, (value) =>
        Effect.gen(function* () {
          let isPaused = yield* Ref.get(ref.ref);
          while (isPaused) {
            yield* Effect.sleep(Duration.millis(50));
            isPaused = yield* Ref.get(ref.ref);
          }
          return value;
        }),
      ),
    );
  }

  // -------------------------------------------------------------------------
  // Error handling
  // -------------------------------------------------------------------------

  /** Recover from stream errors with a fallback value and stop. */
  orElse(fallback: T): StreamPipeline<T, never> {
    return new StreamPipeline(
      Stream.orElse(this.stream, () => Stream.make(fallback)) as Stream.Stream<T, never>,
    );
  }

  /** Run a side-effect on error. */
  tapError(fn: (error: E) => void): StreamPipeline<T, E> {
    return new StreamPipeline(
      Stream.tapError(this.stream, (error) => Effect.sync(() => fn(error))),
    );
  }

  /**
   * Retry the entire stream on error with exponential backoff.
   *
   * @example
   * ```ts
   * stream.retry({ maxRetries: 3, baseDelayMs: 1_000 })
   * ```
   */
  retry(policy?: { maxRetries?: number; baseDelayMs?: number }): StreamPipeline<T, E> {
    const { maxRetries = 3, baseDelayMs = 250 } = policy ?? {};
    const schedule: Schedule.Schedule<unknown> = Schedule.intersect(
      Schedule.exponential(Duration.millis(baseDelayMs), 2),
      Schedule.recurs(maxRetries),
    );
    return new StreamPipeline(Stream.retry(this.stream, schedule));
  }

  /**
   * Fail the stream if it doesn't complete within the given duration.
   *
   * @example
   * ```ts
   * stream.timeout(30_000).collect()
   * ```
   */
  timeout(ms: number): StreamPipeline<T, E> {
    return new StreamPipeline(
      Stream.timeoutFail(this.stream, () => "timeout" as never, Duration.millis(ms)),
    );
  }

  // -------------------------------------------------------------------------
  // Interruption
  // -------------------------------------------------------------------------

  /** Stop this stream when an AbortSignal fires. */
  interruptOn(signal: AbortSignal): StreamPipeline<T, E> {
    return new StreamPipeline(
      Stream.interruptWhen(
        this.stream,
        Effect.async<void, never>((resume) => {
          if (signal.aborted) {
            resume(Effect.void);
            return;
          }
          const onAbort = () => resume(Effect.void);
          signal.addEventListener("abort", onAbort, { once: true });
          return Effect.sync(() => signal.removeEventListener("abort", onAbort));
        }),
      ),
    );
  }

  /** Auto-stop stream after a duration. */
  interruptAfter(ms: number): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.interruptAfter(this.stream, Duration.millis(ms)));
  }

  // -------------------------------------------------------------------------
  // Terminals
  // -------------------------------------------------------------------------

  /** Run a sync cleanup function when the stream ends (success, error, or interruption). */
  finally(fn: () => void): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.ensuring(this.stream, Effect.sync(fn)));
  }

  /** Run an async cleanup function when the stream ends. */
  onFinalize(fn: () => Promise<void>): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.ensuring(this.stream, Effect.promise(fn)));
  }

  /** Process each item. Returns a Promise that resolves when the stream ends. */
  async forEach(fn: (value: T) => void): Promise<void> {
    await Effect.runPromise(
      Stream.runForEach(this.stream, (value) => Effect.sync(() => fn(value))),
    );
  }

  /**
   * Find the first item matching the predicate and return it.
   * Stops consuming the stream after the first match.
  /**
   * Get the first item from the stream. Returns `undefined` if the stream is empty.
   *
   * @example
   * ```ts
   * const first = await stream.runFirst(); // T | undefined
   * ```
   */
  async runFirst(): Promise<T | undefined> {
    const chunk = await Effect.runPromise(Stream.runCollect(this.stream.pipe(Stream.take(1))));
    const arr = Chunk.toArray(chunk);
    return arr.length > 0 ? arr[0] : undefined;
  }

  /**
   * Like fs2's `collectFirst`.
   *
   * @example
   * ```ts
   * const completed = await stream.collectFirst(
   *   (job) => job.status === "completed" && job.resultUrl != null,
   * );
   * ```
   */
  async collectFirst(predicate: (value: T) => boolean): Promise<T | undefined> {
    const result = await Effect.runPromise(
      Stream.runCollect(this.stream.pipe(Stream.filter(predicate), Stream.take(1))),
    );
    const arr = Chunk.toArray(result);
    return arr.length > 0 ? arr[0] : undefined;
  }

  /**
   * Collect items while the predicate is true, then stop.
   * Like fs2's `collectWhile`.
   *
   * @example
   * ```ts
   * const pending = await stream.collectWhile((job) => job.status === "pending");
   * ```
   */
  async collectWhile(predicate: (value: T) => boolean): Promise<T[]> {
    const chunk = await Effect.runPromise(
      Stream.runCollect(this.stream.pipe(Stream.takeWhile(predicate))),
    );
    return Chunk.toArray(chunk);
  }

  /** Collect all items into an array. */
  async collect(): Promise<T[]> {
    const chunk = await Effect.runPromise(Stream.runCollect(this.stream));
    return Chunk.toArray(chunk);
  }

  /** Fold over all items to produce a single value. */
  async reduce<U>(initial: U, fn: (acc: U, value: T) => U): Promise<U> {
    return Effect.runPromise(Stream.runFold(this.stream, initial, fn));
  }

  /** Drain the stream (consume all items, discard values). */
  async drain(): Promise<void> {
    await Effect.runPromise(Stream.runDrain(this.stream));
  }

  /** Escape hatch: get the raw Effect Stream for advanced composition. */
  toStream(): Stream.Stream<T, E> {
    return this.stream;
  }
}
