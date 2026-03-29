import { Effect, Stream, Chunk, Duration, Schedule, Ref } from "effect";
import type { TaggedError } from "./pipeline.ts";
import type { PipelineRef } from "./ref.ts";
import type { Sinkable, KeyedSinkable } from "./typeclasses/streamable.ts";
import type { StateBackend } from "./typeclasses/state-backend.ts";

/** Structural type for anything with a `.stream` property (StreamPipeline, OptimizedStreamPipeline, etc.) */
type HasStream<T, E> = { readonly stream: Stream.Stream<T, E> };

// Note: StreamPipeline import is deferred to avoid circular module initialization.
// StreamPipeline imports us, so we lazy-import it only when needed at runtime.

// TODO: This class is temporary. Once validated in production, merge fusion logic
// directly into StreamPipeline's terminals (collect/forEach/drain) so optimization
// happens by default — no .optimized() opt-in needed. Then delete this file.
// See: docs/v1/plan/11-performance.md Phase R.2

// ---------------------------------------------------------------------------
// Fusion internals
// ---------------------------------------------------------------------------

const SKIP: unique symbol = Symbol("SKIP");

type FusibleOp =
  | { readonly tag: "map"; readonly fn: (value: any) => any }
  | { readonly tag: "filter"; readonly fn: (value: any) => boolean }
  | { readonly tag: "filterMap"; readonly fn: (value: any) => any | undefined }
  | { readonly tag: "tap"; readonly fn: (value: any) => void };

function compile(ops: FusibleOp[]): (value: any) => any {
  if (ops.length === 0) return (v: any) => v;

  if (ops.length === 1) {
    const op = ops[0];
    switch (op.tag) {
      case "map":
        return op.fn;
      case "filter":
        return (v: any) => (op.fn(v) ? v : SKIP);
      case "filterMap":
        return (v: any) => {
          const r = op.fn(v);
          return r === undefined ? SKIP : r;
        };
      case "tap":
        return (v: any) => {
          op.fn(v);
          return v;
        };
    }
  }

  return (value: any) => {
    let v: any = value;
    for (let i = 0; i < ops.length; i++) {
      const op = ops[i];
      switch (op.tag) {
        case "map":
          v = op.fn(v);
          break;
        case "filter":
          if (!op.fn(v)) return SKIP;
          break;
        case "filterMap": {
          const r = op.fn(v);
          if (r === undefined) return SKIP;
          v = r;
          break;
        }
        case "tap":
          op.fn(v);
          break;
      }
    }
    return v;
  };
}

// ---------------------------------------------------------------------------
// OptimizedStreamPipeline<T, E>
// ---------------------------------------------------------------------------

export class OptimizedStreamPipeline<T, E extends TaggedError> {
  private readonly _baseStream: Stream.Stream<any, E>;
  private readonly _ops: FusibleOp[];

  constructor(stream: Stream.Stream<any, E>, ops?: FusibleOp[]) {
    this._baseStream = stream;
    this._ops = ops ?? [];
  }

  // -------------------------------------------------------------------------
  // Internal: materialize & flush
  // -------------------------------------------------------------------------

  private _materialize(): Stream.Stream<T, E> {
    if (this._ops.length === 0) return this._baseStream;

    const fused = compile(this._ops);
    const hasFilter = this._ops.some((op) => op.tag === "filter" || op.tag === "filterMap");

    // Use mapChunks for maximum throughput — processes entire chunks in a tight
    // loop, paying the Effect runtime cost once per chunk (~4096 elements) instead
    // of once per element.
    return Stream.mapChunks(this._baseStream, (chunk) => {
      if (hasFilter) {
        const src = Chunk.toArray(chunk);
        const result: any[] = [];
        for (let i = 0; i < src.length; i++) {
          const v = fused(src[i]);
          if (v !== SKIP) result.push(v);
        }
        return Chunk.unsafeFromArray(result);
      }
      return Chunk.map(chunk, fused);
    }) as Stream.Stream<T, E>;
  }

  private _flush(): OptimizedStreamPipeline<T, E> {
    if (this._ops.length === 0) return this;
    return new OptimizedStreamPipeline<T, E>(this._materialize());
  }

  /** Access the underlying Effect Stream (materializes pending ops). */
  get stream(): Stream.Stream<T, E> {
    return this._materialize();
  }

  // -------------------------------------------------------------------------
  // Bridge
  // -------------------------------------------------------------------------

  /** Convert back to a regular StreamPipeline. */
  toStreamPipeline(): HasStream<T, E> {
    // eslint-disable-next-line @typescript-eslint/no-require-imports -- lazy import avoids circular dep
    const { StreamPipeline } =
      require("./stream-pipeline.ts") as typeof import("./stream-pipeline.ts");
    return new StreamPipeline(this._materialize());
  }

  // -------------------------------------------------------------------------
  // Fusible operators
  // -------------------------------------------------------------------------

  map<U>(fn: (value: T) => U): OptimizedStreamPipeline<U, E> {
    return new OptimizedStreamPipeline<U, E>(this._baseStream, [...this._ops, { tag: "map", fn }]);
  }

  filter(fn: (value: T) => boolean, action?: "keep" | "drop"): OptimizedStreamPipeline<T, E> {
    const predicate = action === "drop" ? (value: T) => !fn(value) : fn;
    return new OptimizedStreamPipeline<T, E>(this._baseStream, [
      ...this._ops,
      { tag: "filter", fn: predicate },
    ]);
  }

  filterMap<U>(fn: (value: T) => U | undefined): OptimizedStreamPipeline<U, E> {
    return new OptimizedStreamPipeline<U, E>(this._baseStream, [
      ...this._ops,
      { tag: "filterMap", fn },
    ]);
  }

  tap(fn: (value: T) => void): OptimizedStreamPipeline<T, E> {
    return new OptimizedStreamPipeline<T, E>(this._baseStream, [...this._ops, { tag: "tap", fn }]);
  }

  unNone(): OptimizedStreamPipeline<NonNullable<T>, E> {
    return new OptimizedStreamPipeline<NonNullable<T>, E>(this._baseStream, [
      ...this._ops,
      { tag: "filter", fn: (value: any) => value != null },
    ]);
  }

  // -------------------------------------------------------------------------
  // Non-fusible operators (flush first, then delegate to Effect)
  // -------------------------------------------------------------------------

  mapAsync<U>(fn: (value: T) => Promise<U>): OptimizedStreamPipeline<U, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.mapEffect(base._baseStream, (value) => Effect.promise(() => fn(value))),
    );
  }

  mapEffect<U, E2 extends TaggedError>(
    fn: (value: T) => Effect.Effect<U, E2>,
  ): OptimizedStreamPipeline<U, E | E2> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.mapEffect(base._baseStream, fn) as Stream.Stream<U, E | E2>,
    );
  }

  filterAsync(
    fn: (value: T) => Promise<boolean>,
    action?: "keep" | "drop",
  ): OptimizedStreamPipeline<T, E> {
    const predicate = action === "drop" ? (value: T) => fn(value).then((r) => !r) : fn;
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.filterEffect(base._baseStream, (value) => Effect.promise(() => predicate(value))),
    );
  }

  zipWithIndex(): OptimizedStreamPipeline<[T, number], E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.zipWithIndex(base._baseStream));
  }

  mapAccumulate<S, U>(
    initial: S,
    fn: (state: S, value: T) => readonly [S, U],
  ): OptimizedStreamPipeline<U, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.mapAccum(base._baseStream, initial, (state, value) => {
        const [nextState, output] = fn(state, value);
        return [nextState, output];
      }),
    );
  }

  tapAsync(fn: (value: T) => Promise<void>): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.tap(base._baseStream, (value) => Effect.promise(() => fn(value))),
    );
  }

  tapAsyncFork(fn: (value: T) => Promise<void>): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.tap(base._baseStream, (value) =>
        Effect.fork(Effect.promise(() => fn(value)).pipe(Effect.catchAll(() => Effect.void))),
      ),
    );
  }

  take(n: number): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.take(base._baseStream, n));
  }

  takeWhile(fn: (value: T) => boolean): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.takeWhile(base._baseStream, fn));
  }

  drop(n: number): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.drop(base._baseStream, n));
  }

  dropWhile(fn: (value: T) => boolean): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.dropWhile(base._baseStream, fn));
  }

  dedupe(): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.changes(base._baseStream));
  }

  distinctBy<K>(fn: (value: T) => K): OptimizedStreamPipeline<T, E> {
    const seen = new Set<K>();
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.filter(base._baseStream, (value) => {
        const key = fn(value);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      }),
    );
  }

  flatMap<U, E2 extends TaggedError>(
    fn: (value: T) => OptimizedStreamPipeline<U, E2> | HasStream<U, E2>,
  ): OptimizedStreamPipeline<U, E | E2> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.flatMap(base._baseStream, (value) => fn(value).stream) as Stream.Stream<U, E | E2>,
    );
  }

  switchMap<U, E2 extends TaggedError>(
    fn: (value: T) => OptimizedStreamPipeline<U, E2> | HasStream<U, E2>,
  ): OptimizedStreamPipeline<U, E | E2> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.flatMap(base._baseStream, (value) => fn(value).stream, {
        switch: true,
      }) as Stream.Stream<U, E | E2>,
    );
  }

  concat(other: OptimizedStreamPipeline<T, E> | HasStream<T, E>): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.concat(base._baseStream, other.stream));
  }

  // -------------------------------------------------------------------------
  // Parallel & batching
  // -------------------------------------------------------------------------

  parAsyncMap<U>(concurrency: number, fn: (value: T) => Promise<U>): OptimizedStreamPipeline<U, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.mapEffect(base._baseStream, (value) => Effect.promise(() => fn(value)), {
        concurrency,
      }),
    );
  }

  parAsyncMapUnordered<U>(
    concurrency: number,
    fn: (value: T) => Promise<U>,
  ): OptimizedStreamPipeline<U, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.mapEffect(base._baseStream, (value) => Effect.promise(() => fn(value)), {
        concurrency,
        unordered: true,
      }),
    );
  }

  mapAsyncRetry<U>(
    fn: (value: T) => Promise<U>,
    policy?: { maxRetries?: number; baseDelayMs?: number },
  ): OptimizedStreamPipeline<U, E> {
    const { maxRetries = 3, baseDelayMs = 250 } = policy ?? {};
    const schedule: Schedule.Schedule<unknown> = Schedule.intersect(
      Schedule.exponential(Duration.millis(baseDelayMs), 2),
      Schedule.recurs(maxRetries),
    );
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.mapEffect(base._baseStream, (value) => {
        const attempt = Effect.catchAllDefect(
          Effect.promise(() => fn(value)),
          (defect) => Effect.fail(defect),
        );
        return Effect.retry(attempt, schedule).pipe(
          Effect.catchAll((defect) => Effect.die(defect)),
        );
      }) as Stream.Stream<U, E>,
    );
  }

  groupWithin(maxSize: number, maxWaitMs: number): OptimizedStreamPipeline<T[], E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.groupedWithin(base._baseStream, maxSize, Duration.millis(maxWaitMs)).pipe(
        Stream.map(Chunk.toArray),
      ),
    );
  }

  grouped(size: number): OptimizedStreamPipeline<T[], E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.grouped(base._baseStream, size).pipe(Stream.map(Chunk.toArray)),
    );
  }

  mapChunks<U>(fn: (chunk: T[]) => U[]): OptimizedStreamPipeline<U, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.mapChunks(base._baseStream, (chunk) =>
        Chunk.unsafeFromArray(fn(Chunk.toArray(chunk))),
      ),
    );
  }

  rechunk(size: number): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.rechunk(base._baseStream, size));
  }

  sliding(size: number): OptimizedStreamPipeline<T[], E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.sliding(base._baseStream, size).pipe(Stream.map(Chunk.toArray)),
    );
  }

  buffer(capacity: number): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.buffer(base._baseStream, { capacity }));
  }

  scan<U>(initial: U, fn: (acc: U, value: T) => U): OptimizedStreamPipeline<U, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.scan(base._baseStream, initial, fn));
  }

  debounce(ms: number): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.debounce(base._baseStream, Duration.millis(ms)));
  }

  metered(ms: number): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.schedule(base._baseStream, Schedule.spaced(Duration.millis(ms))),
    );
  }

  spaced(ms: number): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.mapEffect(base._baseStream, (value) =>
        Effect.sleep(Duration.millis(ms)).pipe(Effect.map(() => value)),
      ),
    );
  }

  repeat(): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.forever(base._baseStream));
  }

  repeatN(n: number): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    const streams = Array.from({ length: n }, () => base._baseStream);
    return new OptimizedStreamPipeline(streams.reduce((acc, s) => Stream.concat(acc, s)));
  }

  // -------------------------------------------------------------------------
  // Combination
  // -------------------------------------------------------------------------

  merge(other: OptimizedStreamPipeline<T, E> | HasStream<T, E>): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.merge(base._baseStream, other.stream));
  }

  zipWith<U, V, E2 extends TaggedError>(
    other: OptimizedStreamPipeline<U, E2> | HasStream<U, E2>,
    fn: (a: T, b: U) => V,
  ): OptimizedStreamPipeline<V, E | E2> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.zipWith(base._baseStream, other.stream, fn) as Stream.Stream<V, E | E2>,
    );
  }

  interleave(
    other: OptimizedStreamPipeline<T, E> | HasStream<T, E>,
  ): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.interleave(base._baseStream, other.stream));
  }

  through<U, E2 extends TaggedError>(
    pipe: (stream: OptimizedStreamPipeline<T, E>) => OptimizedStreamPipeline<U, E2>,
  ): OptimizedStreamPipeline<U, E2> {
    return pipe(this);
  }

  // -------------------------------------------------------------------------
  // Error handling
  // -------------------------------------------------------------------------

  orElse(fallback: T): OptimizedStreamPipeline<T, never> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.orElse(base._baseStream, () => Stream.make(fallback)) as Stream.Stream<T, never>,
    );
  }

  tapError(fn: (error: E) => void): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.tapError(base._baseStream, (error) => Effect.sync(() => fn(error))),
    );
  }

  retry(policy?: { maxRetries?: number; baseDelayMs?: number }): OptimizedStreamPipeline<T, E> {
    const { maxRetries = 3, baseDelayMs = 250 } = policy ?? {};
    const schedule: Schedule.Schedule<unknown> = Schedule.intersect(
      Schedule.exponential(Duration.millis(baseDelayMs), 2),
      Schedule.recurs(maxRetries),
    );
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.retry(base._baseStream, schedule));
  }

  timeout(ms: number): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.timeoutFail(base._baseStream, () => "timeout" as never, Duration.millis(ms)),
    );
  }

  // -------------------------------------------------------------------------
  // Interruption
  // -------------------------------------------------------------------------

  interruptOn(signal: AbortSignal): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.interruptWhen(
        base._baseStream,
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

  interruptAfter(ms: number): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.interruptAfter(base._baseStream, Duration.millis(ms)),
    );
  }

  // -------------------------------------------------------------------------
  // Cleanup
  // -------------------------------------------------------------------------

  finally(fn: () => void): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.ensuring(base._baseStream, Effect.sync(fn)));
  }

  onFinalize(fn: () => Promise<void>): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(Stream.ensuring(base._baseStream, Effect.promise(fn)));
  }

  // -------------------------------------------------------------------------
  // Typeclass-based
  // -------------------------------------------------------------------------

  statefulMap<K, V, U>(params: {
    stateBackend: StateBackend<K, V>;
    keyBy: (value: T) => K;
    process: (value: T, state: StateBackend<K, V>) => Promise<U>;
  }): OptimizedStreamPipeline<U, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.mapEffect(base._baseStream, (value) =>
        Effect.promise(() => params.process(value, params.stateBackend)),
      ),
    );
  }

  to(sink: Sinkable<T>, params?: { key?: (value: T) => string }): Promise<void> {
    return this.forEach(async (value) => {
      if (params?.key) {
        await (sink as KeyedSinkable<T>).publish(value, { key: params.key(value) });
      } else {
        await sink.publish(value);
      }
    });
  }

  pauseWhen(ref: PipelineRef<boolean>, pollMs: number = 50): OptimizedStreamPipeline<T, E> {
    const base = this._flush();
    return new OptimizedStreamPipeline(
      Stream.mapEffect(base._baseStream, (value) =>
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
  // Terminals
  // -------------------------------------------------------------------------

  async forEach(fn: (value: T) => unknown): Promise<void> {
    await Effect.runPromise(
      Stream.runForEach(this._materialize(), (value) => {
        const result = fn(value);
        return result instanceof Promise ? Effect.promise(() => result) : Effect.sync(() => result);
      }),
    );
  }

  async runFirst(): Promise<T | undefined> {
    const chunk = await Effect.runPromise(
      Stream.runCollect(this._materialize().pipe(Stream.take(1))),
    );
    const arr = Chunk.toArray(chunk);
    return arr.length > 0 ? arr[0] : undefined;
  }

  async collectFirst(predicate: (value: T) => boolean): Promise<T | undefined> {
    const result = await Effect.runPromise(
      Stream.runCollect(this._materialize().pipe(Stream.filter(predicate), Stream.take(1))),
    );
    const arr = Chunk.toArray(result);
    return arr.length > 0 ? arr[0] : undefined;
  }

  async collectWhile(predicate: (value: T) => boolean): Promise<T[]> {
    const chunk = await Effect.runPromise(
      Stream.runCollect(this._materialize().pipe(Stream.takeWhile(predicate))),
    );
    return Chunk.toArray(chunk);
  }

  async collect(): Promise<T[]> {
    const chunk = await Effect.runPromise(Stream.runCollect(this._materialize()));
    return Chunk.toArray(chunk);
  }

  async reduce<U>(initial: U, fn: (acc: U, value: T) => U): Promise<U> {
    return Effect.runPromise(Stream.runFold(this._materialize(), initial, fn));
  }

  async drain(): Promise<void> {
    await Effect.runPromise(Stream.runDrain(this._materialize()));
  }

  toStream(): Stream.Stream<T, E> {
    return this._materialize();
  }
}
