import { Effect, Stream, Chunk, Duration, Schedule, Ref, Option, Cause } from "effect";
import type { TaggedError, Pipeline } from "./pipeline.ts";
import type { PipelineRef } from "./ref.ts";
import { type FusibleOp, fuseOpsToStream } from "./fusion.ts";
import type {
  Streamable,
  Acknowledgeable,
  Envelope,
  Offset,
  Sinkable,
  KeyedSinkable,
} from "./typeclasses/streamable.ts";
import type { StateBackend } from "./typeclasses/state-backend.ts";
import { isPartitionable, isReplayable } from "./typeclasses/streamable.ts";

// ---------------------------------------------------------------------------
// StreamPipeline<T, E> — chainable wrapper around Stream<T, E>
// ---------------------------------------------------------------------------

/**
 * A chainable, lazily-evaluated streaming pipeline for any async source with structural concurrency.
 *
 * Nothing executes until you call a terminal (`.forEach()`, `.collect()`, `.reduce()`, `.drain()`).
 * Handles backpressure naturally — the producer only advances when the consumer is ready.
 *
 * **Automatic operator fusion**: adjacent pure operators (`.map()`, `.filter()`, `.filterMap()`,
 * `.tap()`) are automatically fused into a single `mapChunks` call at execution time.
 * This processes ~4096 elements per Effect runtime step instead of 1 per operator — typically
 * 2-3x faster for chained pure operations. No `.optimized()` call needed.
 *
 * @typeParam T - The item type emitted by the stream
 * @typeParam E - The error type (must extend `{ _tag: string }`)
 */
export class StreamPipeline<T, E extends TaggedError> {
  /** @internal Base stream — use .stream getter which materializes pending ops. */
  private readonly _baseStream: Stream.Stream<any, E>;
  /** @internal Pending pure ops accumulated for fusion. */
  private readonly _ops: FusibleOp[];

  constructor(stream: Stream.Stream<T, E>, ops?: FusibleOp[]) {
    this._baseStream = stream;
    this._ops = ops ?? [];
  }

  /**
   * The underlying Effect Stream. Materializes any pending fused operators.
   *
   * Accessing this property triggers fusion: adjacent pure ops (map/filter/filterMap/tap)
   * are compiled into a single `mapChunks` call before returning the stream.
   */
  get stream(): Stream.Stream<T, E> {
    return this._materialize();
  }

  /** @internal Materialize pending ops into the Effect Stream via mapChunks. */
  private _materialize(): Stream.Stream<T, E> {
    if (this._ops.length === 0) return this._baseStream;
    return fuseOpsToStream(this._baseStream, this._ops) as Stream.Stream<T, E>;
  }

  /** @internal Flush pending ops — returns a new StreamPipeline with ops materialized. */
  private _flush(): StreamPipeline<T, E> {
    if (this._ops.length === 0) return this;
    return new StreamPipeline<T, E>(this._materialize());
  }

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

  /**
   * Repeatedly evaluate an async function and emit its result.
   * Like fs2's `Stream.repeatEval`.
   *
   * @example
   * ```ts
   * // Poll an API forever
   * StreamPipeline.repeatEval(() => fetchMetrics())
   *   .forEach((m) => gauge.set(m.value));
   * ```
   */
  static repeatEval<T>(fn: () => Promise<T>): StreamPipeline<T, never> {
    return new StreamPipeline(Stream.repeatEffect(Effect.promise(fn)));
  }

  /**
   * Emit a range of integers [start, end).
   *
   * @example
   * ```ts
   * StreamPipeline.range(0, 10).collect(); // [0, 1, 2, ..., 9]
   * ```
   */
  static range(start: number, end: number): StreamPipeline<number, never> {
    const items = Array.from({ length: end - start }, (_, i) => start + i);
    return new StreamPipeline(Stream.fromIterable(items));
  }

  /**
   * Emit a tick after a fixed delay between completions.
   * Unlike `tick()` (fixed rate), `fixedDelay` waits `ms` *after* the previous
   * element is consumed before emitting the next.
   *
   * @example
   * ```ts
   * // Process, wait 5s, process, wait 5s...
   * StreamPipeline.fixedDelay(5_000)
   *   .mapAsync(() => heavyWork())
   *   .drain();
   * ```
   */
  static fixedDelay(ms: number): StreamPipeline<number, never> {
    return new StreamPipeline(
      Stream.unfoldEffect(0, (n) =>
        Effect.sleep(Duration.millis(ms)).pipe(Effect.map(() => Option.some([n, n + 1] as const))),
      ),
    );
  }

  // -------------------------------------------------------------------------
  // Typeclass-based construction
  // -------------------------------------------------------------------------

  /**
   * Create a StreamPipeline from any Streamable source.
   * Detects Partitionable and Replayable capabilities automatically.
   *
   * @example
   * ```ts
   * StreamPipeline.fromSource(kafkaTopic)
   * StreamPipeline.fromSource(kafkaTopic, { partitions: [0, 1] })
   * StreamPipeline.fromSource(kafkaTopic, { offset: { type: "earliest" } })
   * ```
   */
  static fromSource<T>(
    source: Streamable<T>,
    params?: { group?: string; partitions?: number[]; offset?: Offset },
  ): StreamPipeline<T, never> {
    if (params?.offset && isReplayable<T>(source)) {
      return source.subscribeFrom({ offset: params.offset, group: params?.group });
    }
    if (params?.partitions && isPartitionable<T>(source)) {
      return source.subscribe({ group: params?.group, partitions: params.partitions });
    }
    return source.subscribe({ group: params?.group });
  }

  /**
   * Create a StreamPipeline with manual acknowledgement from an Acknowledgeable source.
   *
   * @example
   * ```ts
   * StreamPipeline.fromAck(sqsQueue)
   *   .forEach(async (envelope) => {
   *     await process(envelope.value);
   *     await envelope.ack();
   *   });
   * ```
   */
  static fromAck<T>(
    source: Acknowledgeable<T>,
    params?: { group?: string },
  ): StreamPipeline<Envelope<T>, never> {
    return source.subscribeAck(params);
  }

  // -------------------------------------------------------------------------
  // Transform
  // -------------------------------------------------------------------------

  /** Transform each item. Automatically fused with adjacent map/filter/tap. */
  map<U>(fn: (value: T) => U): StreamPipeline<U, E> {
    return new StreamPipeline<U, E>(this._baseStream, [...this._ops, { tag: "map", fn }]);
  }

  /** Async transform each item — takes a function returning a Promise. The resolved value replaces the item. */
  mapAsync<U>(fn: (value: T) => Promise<U>): StreamPipeline<U, E> {
    return new StreamPipeline(
      Stream.mapEffect(this._materialize(), (value) => Effect.promise(() => fn(value))),
    );
  }

  /** Transform each item using an Effect. Escape hatch for Effect-native code. */
  mapEffect<U, E2 extends TaggedError>(
    fn: (value: T) => Effect.Effect<U, E2>,
  ): StreamPipeline<U, E | E2> {
    return new StreamPipeline(
      Stream.mapEffect(this._materialize(), fn) as Stream.Stream<U, E | E2>,
    );
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
    return new StreamPipeline(Stream.zipWithIndex(this._materialize()));
  }

  /** Stateful map — carries an accumulator, emits both accumulator and transformed value. */
  mapAccumulate<S, U>(
    initial: S,
    fn: (state: S, value: T) => readonly [S, U],
  ): StreamPipeline<U, E> {
    return new StreamPipeline(
      Stream.mapAccum(this._materialize(), initial, (state, value) => {
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
  /** Filter items. Automatically fused with adjacent map/filter/tap. */
  filter(fn: (value: T) => boolean, action?: "keep" | "drop"): StreamPipeline<T, E> {
    const predicate = action === "drop" ? (value: T) => !fn(value) : fn;
    return new StreamPipeline<T, E>(this._baseStream, [
      ...this._ops,
      { tag: "filter", fn: predicate },
    ]);
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
      Stream.filterEffect(this._materialize(), (value) => Effect.promise(() => predicate(value))),
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
  /** Filter + map in one pass. Automatically fused with adjacent pure ops. */
  filterMap<U>(fn: (value: T) => U | undefined): StreamPipeline<U, E> {
    return new StreamPipeline<U, E>(this._baseStream, [...this._ops, { tag: "filterMap", fn }]);
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
  /** Drop null/undefined. Automatically fused. */
  unNone(): StreamPipeline<NonNullable<T>, E> {
    return new StreamPipeline<NonNullable<T>, E>(this._baseStream, [
      ...this._ops,
      { tag: "filter", fn: (value: any) => value != null },
    ]);
  }

  /** Run a sync side-effect for each item without changing it. Automatically fused. */
  tap(fn: (value: T) => void): StreamPipeline<T, E> {
    return new StreamPipeline<T, E>(this._baseStream, [...this._ops, { tag: "tap", fn }]);
  }

  /** Run an async side-effect for each item without changing it. Awaits the Promise before continuing. */
  tapAsync(fn: (value: T) => Promise<void>): StreamPipeline<T, E> {
    const base = this._flush();
    return new StreamPipeline(
      Stream.tap(base._baseStream, (value) => Effect.promise(() => fn(value))),
    );
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
      Stream.tap(this._materialize(), (value) =>
        Effect.fork(Effect.promise(() => fn(value)).pipe(Effect.catchAll(() => Effect.void))),
      ),
    );
  }

  /** Take the first N items then stop. */
  take(n: number): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.take(this._materialize(), n));
  }

  /** Take items while predicate is true, then stop. */
  takeWhile(fn: (value: T) => boolean): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.takeWhile(this._materialize(), fn));
  }

  /** Skip the first N items. */
  drop(n: number): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.drop(this._materialize(), n));
  }

  /** Skip items while the predicate is true, then emit everything after. */
  dropWhile(fn: (value: T) => boolean): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.dropWhile(this._materialize(), fn));
  }

  /** Emit only when the value changes (by structural equality). */
  dedupe(): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.changes(this._materialize()));
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
      Stream.filter(this._materialize(), (value) => {
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
      Stream.flatMap(this._materialize(), (value) => fn(value).stream) as Stream.Stream<U, E | E2>,
    );
  }

  /** Flat-map but cancel the previous inner stream when a new item arrives ("latest wins"). */
  switchMap<U, E2 extends TaggedError>(
    fn: (value: T) => StreamPipeline<U, E2>,
  ): StreamPipeline<U, E | E2> {
    return new StreamPipeline(
      Stream.flatMap(this._materialize(), (value) => fn(value).stream, {
        switch: true,
      }) as Stream.Stream<U, E | E2>,
    );
  }

  /** Append another stream after this one completes. */
  concat(other: StreamPipeline<T, E>): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.concat(this._materialize(), other.stream));
  }

  // -------------------------------------------------------------------------
  // Parallel & batching
  // -------------------------------------------------------------------------

  /** Async transform with bounded concurrency, preserving input order. */
  parAsyncMap<U>(concurrency: number, fn: (value: T) => Promise<U>): StreamPipeline<U, E> {
    return new StreamPipeline(
      Stream.mapEffect(this._materialize(), (value) => Effect.promise(() => fn(value)), {
        concurrency,
      }),
    );
  }

  /** Like `parAsyncMap` but results arrive in completion order, not input order. */
  parAsyncMapUnordered<U>(concurrency: number, fn: (value: T) => Promise<U>): StreamPipeline<U, E> {
    return new StreamPipeline(
      Stream.mapEffect(this._materialize(), (value) => Effect.promise(() => fn(value)), {
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
      Stream.mapEffect(this._materialize(), (value) => {
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
      Stream.groupedWithin(this._materialize(), maxSize, Duration.millis(maxWaitMs)).pipe(
        Stream.map(Chunk.toArray),
      ),
    );
  }

  /** Buffer items into fixed-size batches. Last batch may be smaller. */
  grouped(size: number): StreamPipeline<T[], E> {
    return new StreamPipeline(
      Stream.grouped(this._materialize(), size).pipe(Stream.map(Chunk.toArray)),
    );
  }

  /**
   * Transform entire chunks at once for maximum throughput on hot paths.
   * The function receives an array of items (one chunk, typically ~4096 elements)
   * and returns a transformed array. Effect runtime cost is paid once per chunk.
   *
   * @example
   * ```ts
   * stream.mapChunks((batch) =>
   *   batch.map(transform).filter(isValid)
   * )
   * ```
   */
  mapChunks<U>(fn: (chunk: T[]) => U[]): StreamPipeline<U, E> {
    return new StreamPipeline(
      Stream.mapChunks(this._materialize(), (chunk) =>
        Chunk.unsafeFromArray(fn(Chunk.toArray(chunk))),
      ),
    );
  }

  /**
   * Re-chunk the stream into chunks of exactly `size` elements (last chunk may be smaller).
   * Useful for controlling throughput granularity — larger chunks amortize per-chunk overhead,
   * smaller chunks reduce latency.
   *
   * @example
   * ```ts
   * stream.rechunk(10_000).mapChunks((batch) => processBatch(batch))
   * ```
   */
  rechunk(size: number): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.rechunk(this._materialize(), size));
  }

  /** Sliding window over stream items. Emits arrays of `size` elements. */
  sliding(size: number): StreamPipeline<T[], E> {
    return new StreamPipeline(
      Stream.sliding(this._materialize(), size).pipe(Stream.map(Chunk.toArray)),
    );
  }

  /** Decouple producer/consumer — buffer up to N items ahead. */
  buffer(capacity: number): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.buffer(this._materialize(), { capacity }));
  }

  /** Running accumulator — like `reduce` but emits every intermediate result. */
  scan<U>(initial: U, fn: (acc: U, value: T) => U): StreamPipeline<U, E> {
    return new StreamPipeline(Stream.scan(this._materialize(), initial, fn));
  }

  /** Emit only after a quiet period of `ms` milliseconds with no new items. */
  debounce(ms: number): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.debounce(this._materialize(), Duration.millis(ms)));
  }

  /** Enforce max emission rate — emit at most 1 item per `ms` milliseconds. */
  metered(ms: number): StreamPipeline<T, E> {
    return new StreamPipeline(
      Stream.schedule(this._materialize(), Schedule.spaced(Duration.millis(ms))),
    );
  }

  /**
   * Add a fixed delay between each element.
   * Unlike `metered()` which throttles to a max rate, `spaced()` inserts
   * a delay *after* each element is consumed.
   */
  spaced(ms: number): StreamPipeline<T, E> {
    return new StreamPipeline(
      Stream.mapEffect(this._materialize(), (value) =>
        Effect.sleep(Duration.millis(ms)).pipe(Effect.map(() => value)),
      ),
    );
  }

  /**
   * Infinitely repeat this stream's output.
   * fs2: `stream.repeat`
   *
   * @example
   * ```ts
   * StreamPipeline.fromIterable([1, 2, 3]).repeat().take(9).collect();
   * // [1, 2, 3, 1, 2, 3, 1, 2, 3]
   * ```
   */
  repeat(): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.forever(this._materialize()));
  }

  /**
   * Repeat this stream's output exactly `n` times.
   * fs2: `stream.repeatN`
   *
   * @example
   * ```ts
   * StreamPipeline.fromIterable([1, 2]).repeatN(3).collect();
   * // [1, 2, 1, 2, 1, 2]
   * ```
   */
  repeatN(n: number): StreamPipeline<T, E> {
    const streams = Array.from({ length: n }, () => this._materialize());
    return new StreamPipeline(streams.reduce((acc, s) => Stream.concat(acc, s)));
  }

  // -------------------------------------------------------------------------
  // Combination
  // -------------------------------------------------------------------------

  /** Merge another stream — interleave items from both as they arrive. */
  merge(other: StreamPipeline<T, E>): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.merge(this._materialize(), other.stream));
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
      Stream.zipWith(this._materialize(), other.stream, fn) as Stream.Stream<V, E | E2>,
    );
  }

  /** Alternate items from two streams (round-robin). */
  interleave(other: StreamPipeline<T, E>): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.interleave(this._materialize(), other.stream));
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
      Stream.tap(this._materialize(), (value) =>
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
  pauseWhen(ref: PipelineRef<boolean>, pollMs: number = 50): StreamPipeline<T, E> {
    // Check the ref before emitting each item; if paused, sleep and re-check
    return new StreamPipeline(
      Stream.mapEffect(this._materialize(), (value) =>
        Effect.gen(function* () {
          while (yield* Ref.get(ref.ref)) {
            yield* Effect.sleep(Duration.millis(pollMs));
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
      Stream.orElse(this._materialize(), () => Stream.make(fallback)) as Stream.Stream<T, never>,
    );
  }

  /** Run a side-effect on error. */
  tapError(fn: (error: E) => void): StreamPipeline<T, E> {
    return new StreamPipeline(
      Stream.tapError(this._materialize(), (error) => Effect.sync(() => fn(error))),
    );
  }

  /**
   * Observe all failures — typed errors AND defects — without changing them.
   * Useful for logging where you want to see every failure regardless of type.
   */
  tapAnyError(fn: (error: unknown) => void): StreamPipeline<T, E> {
    return new StreamPipeline(
      this._materialize().pipe(
        Stream.tapError((error) => Effect.sync(() => fn(error))),
        Stream.catchAllCause((cause) => {
          const defects = Cause.defects(cause);
          if (defects.length > 0) {
            return Stream.fromEffect(
              Effect.andThen(
                Effect.sync(() => {
                  for (const d of defects) fn(d);
                }),
                Effect.failCause(cause),
              ),
            ) as Stream.Stream<T, E>;
          }
          return Stream.failCause(cause) as Stream.Stream<T, E>;
        }),
      ),
    );
  }

  /**
   * Pull specific defect types into the typed error channel.
   * Thrown errors matching any of the provided classes become typed errors;
   * unmatched defects remain as defects.
   */
  trapError<Classes extends (new (...args: any[]) => TaggedError)[]>(
    ...classes: Classes
  ): StreamPipeline<T, E | InstanceType<Classes[number]>> {
    return new StreamPipeline(
      this._materialize().pipe(
        Stream.catchAllCause((cause) => {
          const defects = Cause.defects(cause);
          for (const defect of defects) {
            for (const cls of classes) {
              if (defect instanceof cls) return Stream.fail(defect as any);
            }
          }
          return Stream.failCause(cause) as Stream.Stream<T, E | InstanceType<Classes[number]>>;
        }),
      ),
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
    return new StreamPipeline(Stream.retry(this._materialize(), schedule));
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
      Stream.timeoutFail(this._materialize(), () => "timeout" as never, Duration.millis(ms)),
    );
  }

  // -------------------------------------------------------------------------
  // Interruption
  // -------------------------------------------------------------------------

  /** Stop this stream when an AbortSignal fires. */
  interruptOn(signal: AbortSignal): StreamPipeline<T, E> {
    return new StreamPipeline(
      Stream.interruptWhen(
        this._materialize(),
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
    return new StreamPipeline(Stream.interruptAfter(this._materialize(), Duration.millis(ms)));
  }

  // -------------------------------------------------------------------------
  // Terminals
  // -------------------------------------------------------------------------

  /** Run a sync cleanup function when the stream ends (success, error, or interruption). */
  finally(fn: () => void): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.ensuring(this._materialize(), Effect.sync(fn)));
  }

  /** Run an async cleanup function when the stream ends. */
  onFinalize(fn: () => Promise<void>): StreamPipeline<T, E> {
    return new StreamPipeline(Stream.ensuring(this._materialize(), Effect.promise(fn)));
  }

  // -------------------------------------------------------------------------
  // Typeclass-based sink & stateful operators
  // -------------------------------------------------------------------------

  /**
   * Publish each stream item to a Sinkable (or KeyedSinkable with key extractor).
   *
   * @example
   * ```ts
   * stream.to(redisSink)
   * stream.to(kafkaTopic, { key: (item) => item.userId })
   * ```
   */
  to(sink: Sinkable<T>, params?: { key?: (value: T) => string }): Promise<void> {
    return this.forEach(async (value) => {
      if (params?.key) {
        await (sink as KeyedSinkable<T>).publish(value, { key: params.key(value) });
      } else {
        await sink.publish(value);
      }
    });
  }

  /**
   * Stateful stream processing — Flink-style keyed state.
   * Each item is routed to a key, and the process function can read/write state.
   *
   * @example
   * ```ts
   * stream.statefulMap({
   *   stateBackend: new InMemoryState<string, number>(),
   *   keyBy: (event) => event.userId,
   *   process: async (event, state) => {
   *     const count = (await state.get(event.userId)) ?? 0;
   *     await state.put(event.userId, count + 1);
   *     return { ...event, visitCount: count + 1 };
   *   },
   * })
   * ```
   */
  statefulMap<K, V, U>(params: {
    stateBackend: StateBackend<K, V>;
    keyBy: (value: T) => K;
    process: (value: T, state: StateBackend<K, V>) => Promise<U>;
  }): StreamPipeline<U, E> {
    return new StreamPipeline(
      Stream.mapEffect(this._materialize(), (value) =>
        Effect.promise(() => params.process(value, params.stateBackend)),
      ),
    );
  }

  /** Process each item (sync or async). Returns a Promise that resolves when the stream ends. */
  async forEach(fn: (value: T) => unknown): Promise<void> {
    await Effect.runPromise(
      Stream.runForEach(this._materialize(), (value) => {
        const result = fn(value);
        return result instanceof Promise ? Effect.promise(() => result) : Effect.sync(() => result);
      }),
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
    const chunk = await Effect.runPromise(
      Stream.runCollect(this._materialize().pipe(Stream.take(1))),
    );
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
      Stream.runCollect(this._materialize().pipe(Stream.filter(predicate), Stream.take(1))),
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
      Stream.runCollect(this._materialize().pipe(Stream.takeWhile(predicate))),
    );
    return Chunk.toArray(chunk);
  }

  /** Collect all items into an array. */
  async collect(): Promise<T[]> {
    const chunk = await Effect.runPromise(Stream.runCollect(this._materialize()));
    return Chunk.toArray(chunk);
  }

  /** Fold over all items to produce a single value. */
  async reduce<U>(initial: U, fn: (acc: U, value: T) => U): Promise<U> {
    return Effect.runPromise(Stream.runFold(this._materialize(), initial, fn));
  }

  /** Drain the stream (consume all items, discard values). */
  async drain(): Promise<void> {
    await Effect.runPromise(Stream.runDrain(this._materialize()));
  }

  /** Escape hatch: get the raw Effect Stream for advanced composition. */
  toStream(): Stream.Stream<T, E> {
    return this._materialize();
  }

  /**
   * **Experimental** — Switch to optimized mode. Fuses adjacent pure operators
   * (map, filter, filterMap, tap) into a single pass per element, eliminating
   * per-operator Effect overhead.
   *
   * @experimental This API may change in future releases.
   *
   * @example
   * ```ts
   * await StreamPipeline.fromIterable(data)
   *   .optimized()
   *   .map(transform)
   *   .filter(isValid)
   *   .map(enrich)
   *   .collect(); // runs fused: 1 Effect call per element, not 3
   * ```
   */
  /**
   * @deprecated Fusion is now automatic. StreamPipeline fuses adjacent pure operators
   * (map, filter, filterMap, tap) by default — no need to call `.optimized()`.
   * Returns `this` for backward compatibility.
   */
  optimized(): StreamPipeline<T, E> {
    return this;
  }
}
