import { Stream, Effect, Chunk } from "effect";
import { type FusibleOp, SKIP, compileFused, hasFilterOps } from "./fusion.ts";

// ---------------------------------------------------------------------------
// RawStream<T> — zero-overhead stream, no Effect runtime
// ---------------------------------------------------------------------------

/**
 * A zero-overhead stream that uses plain iterables/async-iterables
 * with fused operator chains. No Effect, no fibers, no scopes.
 *
 * Use for CPU-bound hot paths where per-element overhead matters.
 * For I/O-bound work with retry/timeout/concurrency, use StreamPipeline.
 *
 * @example
 * ```ts
 * const result = await RawStream.fromIterable(data)
 *   .map(transform)
 *   .filter(isValid)
 *   .map(enrich)
 *   .collect(); // runs as a tight fused loop
 * ```
 */
export class RawStream<T> {
  private readonly _source: Iterable<any> | AsyncIterable<any>;
  private readonly _ops: FusibleOp[];
  private readonly _async: boolean;

  constructor(source: Iterable<any> | AsyncIterable<any>, ops?: FusibleOp[], isAsync?: boolean) {
    this._source = source;
    this._ops = ops ?? [];
    this._async = isAsync ?? Symbol.asyncIterator in source;
  }

  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  static fromIterable<T>(items: Iterable<T>): RawStream<T> {
    return new RawStream<T>(items, [], false);
  }

  static fromAsyncIterable<T>(items: AsyncIterable<T>): RawStream<T> {
    return new RawStream<T>(items, [], true);
  }

  static fromArray<T>(items: T[]): RawStream<T> {
    return new RawStream<T>(items, [], false);
  }

  static empty<T>(): RawStream<T> {
    return new RawStream<T>([], [], false);
  }

  static range(start: number, end: number): RawStream<number> {
    return new RawStream<number>(
      {
        *[Symbol.iterator]() {
          for (let i = start; i < end; i++) yield i;
        },
      },
      [],
      false,
    );
  }

  // -------------------------------------------------------------------------
  // Fusible operators — append to plan, zero overhead
  // -------------------------------------------------------------------------

  map<U>(fn: (value: T) => U): RawStream<U> {
    return new RawStream<U>(this._source, [...this._ops, { tag: "map", fn }], this._async);
  }

  filter(fn: (value: T) => boolean): RawStream<T> {
    return new RawStream<T>(this._source, [...this._ops, { tag: "filter", fn }], this._async);
  }

  filterMap<U>(fn: (value: T) => U | undefined): RawStream<U> {
    return new RawStream<U>(this._source, [...this._ops, { tag: "filterMap", fn }], this._async);
  }

  tap(fn: (value: T) => void): RawStream<T> {
    return new RawStream<T>(this._source, [...this._ops, { tag: "tap", fn }], this._async);
  }

  // -------------------------------------------------------------------------
  // Non-fusible operators — materialize into new source
  // -------------------------------------------------------------------------

  take(n: number): RawStream<T> {
    const collected = this._collectIterable();
    return new RawStream<T>(
      {
        *[Symbol.iterator]() {
          let count = 0;
          for (const item of collected) {
            if (count >= n) break;
            yield item;
            count++;
          }
        },
      },
      [],
      false,
    );
  }

  drop(n: number): RawStream<T> {
    const collected = this._collectIterable();
    return new RawStream<T>(
      {
        *[Symbol.iterator]() {
          let count = 0;
          for (const item of collected) {
            if (count >= n) yield item;
            count++;
          }
        },
      },
      [],
      false,
    );
  }

  takeWhile(fn: (value: T) => boolean): RawStream<T> {
    const collected = this._collectIterable();
    return new RawStream<T>(
      {
        *[Symbol.iterator]() {
          for (const item of collected) {
            if (!fn(item)) break;
            yield item;
          }
        },
      },
      [],
      false,
    );
  }

  dropWhile(fn: (value: T) => boolean): RawStream<T> {
    const collected = this._collectIterable();
    return new RawStream<T>(
      {
        *[Symbol.iterator]() {
          let dropping = true;
          for (const item of collected) {
            if (dropping && fn(item)) continue;
            dropping = false;
            yield item;
          }
        },
      },
      [],
      false,
    );
  }

  grouped(size: number): RawStream<T[]> {
    const collected = this._collectIterable();
    return new RawStream<T[]>(
      {
        *[Symbol.iterator]() {
          let batch: T[] = [];
          for (const item of collected) {
            batch.push(item);
            if (batch.length >= size) {
              yield batch;
              batch = [];
            }
          }
          if (batch.length > 0) yield batch;
        },
      },
      [],
      false,
    );
  }

  scan<U>(initial: U, fn: (acc: U, value: T) => U): RawStream<U> {
    const collected = this._collectIterable();
    return new RawStream<U>(
      {
        *[Symbol.iterator]() {
          let acc = initial;
          yield acc;
          for (const item of collected) {
            acc = fn(acc, item);
            yield acc;
          }
        },
      },
      [],
      false,
    );
  }

  flatMap<U>(fn: (value: T) => RawStream<U>): RawStream<U> {
    const collected = this._collectIterable();
    return new RawStream<U>(
      {
        *[Symbol.iterator]() {
          for (const item of collected) {
            yield* fn(item).collectSync();
          }
        },
      },
      [],
      false,
    );
  }

  concat(other: RawStream<T>): RawStream<T> {
    const a = this._collectIterable();
    return new RawStream<T>(
      {
        *[Symbol.iterator]() {
          yield* a;
          yield* other.collectSync();
        },
      },
      [],
      false,
    );
  }

  zipWithIndex(): RawStream<[T, number]> {
    const collected = this._collectIterable();
    return new RawStream<[T, number]>(
      {
        *[Symbol.iterator]() {
          let i = 0;
          for (const item of collected) {
            yield [item, i++] as [T, number];
          }
        },
      },
      [],
      false,
    );
  }

  dedupe(): RawStream<T> {
    const collected = this._collectIterable();
    return new RawStream<T>(
      {
        *[Symbol.iterator]() {
          let prev: T | typeof SKIP = SKIP;
          for (const item of collected) {
            if (item !== prev) {
              yield item;
              prev = item;
            }
          }
        },
      },
      [],
      false,
    );
  }

  distinctBy<K>(fn: (value: T) => K): RawStream<T> {
    const collected = this._collectIterable();
    return new RawStream<T>(
      {
        *[Symbol.iterator]() {
          const seen = new Set<K>();
          for (const item of collected) {
            const key = fn(item);
            if (!seen.has(key)) {
              seen.add(key);
              yield item;
            }
          }
        },
      },
      [],
      false,
    );
  }

  // -------------------------------------------------------------------------
  // Bridge: RawStream ↔ StreamPipeline / Effect Stream
  // -------------------------------------------------------------------------

  /** Convert to an Effect Stream. */
  toEffectStream(): Stream.Stream<T, never> {
    if (!this._async) {
      return Stream.fromIterable(this._collectIterable());
    }
    return Stream.fromAsyncIterable(this._collectAsyncIterable(), (e) => e as never);
  }

  /** Convert to StreamPipeline. */
  toStreamPipeline(): { stream: Stream.Stream<T, never> } {
    return { stream: this.toEffectStream() };
  }

  /** Create from an Effect Stream (consumes the stream). */
  static async fromEffectStream<T>(stream: Stream.Stream<T, any>): Promise<RawStream<T>> {
    const chunk = await Effect.runPromise(Stream.runCollect(stream));
    return RawStream.fromArray(Chunk.toArray(chunk) as T[]);
  }

  // -------------------------------------------------------------------------
  // Internal: materialization
  // -------------------------------------------------------------------------

  /** Collect fused ops over a sync iterable source → sync iterable result. */
  private _collectIterable(): Iterable<T> {
    if (this._ops.length === 0) return this._source as Iterable<T>;

    const source = this._source as Iterable<any>;
    const fused = compileFused(this._ops);
    const hasFilter = hasFilterOps(this._ops);

    if (hasFilter) {
      return {
        *[Symbol.iterator]() {
          for (const item of source) {
            const result = fused(item);
            if (result !== SKIP) yield result;
          }
        },
      };
    }
    return {
      *[Symbol.iterator]() {
        for (const item of source) {
          yield fused(item);
        }
      },
    };
  }

  /** Collect fused ops over an async iterable source. */
  private _collectAsyncIterable(): AsyncIterable<T> {
    if (this._ops.length === 0) return this._source as AsyncIterable<T>;

    const source = this._source as AsyncIterable<any>;
    const fused = compileFused(this._ops);
    const hasFilter = hasFilterOps(this._ops);

    if (hasFilter) {
      return {
        async *[Symbol.asyncIterator]() {
          for await (const item of source) {
            const result = fused(item);
            if (result !== SKIP) yield result;
          }
        },
      };
    }
    return {
      async *[Symbol.asyncIterator]() {
        for await (const item of source) {
          yield fused(item);
        }
      },
    };
  }

  // -------------------------------------------------------------------------
  // Terminals
  // -------------------------------------------------------------------------

  /** Collect all items into an array (sync path for sync sources). */
  collectSync(): T[] {
    if (this._async) {
      throw new Error("Cannot collectSync on an async source. Use collect() instead.");
    }

    const source = this._source;

    if (this._ops.length === 0) {
      return Array.isArray(source) ? (source as T[]) : Array.from(source as Iterable<T>);
    }

    const fused = compileFused(this._ops);
    const hasFilter = hasFilterOps(this._ops);

    // Fast path: array source → indexed for-loop (no iterator protocol overhead)
    if (Array.isArray(source)) {
      const len = source.length;
      if (hasFilter) {
        const result: T[] = [];
        for (let i = 0; i < len; i++) {
          const v = fused(source[i]);
          if (v !== SKIP) result.push(v);
        }
        return result;
      }
      const result = new Array<T>(len);
      for (let i = 0; i < len; i++) {
        result[i] = fused(source[i]);
      }
      return result;
    }

    // Generic iterable path
    const result: T[] = [];
    if (hasFilter) {
      for (const item of source as Iterable<any>) {
        const v = fused(item);
        if (v !== SKIP) result.push(v);
      }
    } else {
      for (const item of source as Iterable<any>) {
        result.push(fused(item));
      }
    }
    return result;
  }

  /** Collect all items into an array. */
  async collect(): Promise<T[]> {
    if (!this._async) return this.collectSync();

    const result: T[] = [];
    for await (const item of this._collectAsyncIterable()) {
      result.push(item);
    }
    return result;
  }

  /** Process each item. */
  forEach(fn: (value: T) => void): void {
    if (this._async) {
      throw new Error("Cannot forEach sync on an async source. Use forEachAsync() instead.");
    }
    if (Array.isArray(this._source) && this._ops.length > 0) {
      const source = this._source;
      const fused = compileFused(this._ops);
      const hasFilter = hasFilterOps(this._ops);
      for (let i = 0; i < source.length; i++) {
        const v = fused(source[i]);
        if (hasFilter && v === SKIP) continue;
        fn(v);
      }
      return;
    }
    for (const item of this._collectIterable()) {
      fn(item);
    }
  }

  /** Process each item (async sources). */
  async forEachAsync(fn: (value: T) => void | Promise<void>): Promise<void> {
    if (this._async) {
      for await (const item of this._collectAsyncIterable()) {
        await fn(item);
      }
    } else {
      for (const item of this._collectIterable()) {
        await fn(item);
      }
    }
  }

  /** Fold all items to a single value. */
  reduce<U>(initial: U, fn: (acc: U, value: T) => U): U {
    if (this._async) {
      throw new Error("Cannot reduce sync on an async source. Use reduceAsync() instead.");
    }
    let acc = initial;
    if (Array.isArray(this._source) && this._ops.length > 0) {
      const source = this._source;
      const fused = compileFused(this._ops);
      const hasFilter = hasFilterOps(this._ops);
      for (let i = 0; i < source.length; i++) {
        const v = fused(source[i]);
        if (hasFilter && v === SKIP) continue;
        acc = fn(acc, v);
      }
      return acc;
    }
    for (const item of this._collectIterable()) {
      acc = fn(acc, item);
    }
    return acc;
  }

  /** Fold all items to a single value (async). */
  async reduceAsync<U>(initial: U, fn: (acc: U, value: T) => U): Promise<U> {
    let acc = initial;
    if (this._async) {
      for await (const item of this._collectAsyncIterable()) {
        acc = fn(acc, item);
      }
    } else {
      for (const item of this._collectIterable()) {
        acc = fn(acc, item);
      }
    }
    return acc;
  }

  /** Drain (consume all items, discard values). */
  drain(): void {
    if (this._async) {
      throw new Error("Cannot drain sync on an async source. Use drainAsync() instead.");
    }
    if (Array.isArray(this._source) && this._ops.length > 0) {
      const source = this._source;
      const fused = compileFused(this._ops);
      for (let i = 0; i < source.length; i++) {
        fused(source[i]);
      }
      return;
    }
    for (const _ of this._collectIterable()) {
      // consume
    }
  }

  /** Drain async. */
  async drainAsync(): Promise<void> {
    if (this._async) {
      for await (const _ of this._collectAsyncIterable()) {
        // consume
      }
    } else {
      this.drain();
    }
  }

  /** Get the first item. */
  first(): T | undefined {
    if (this._async) {
      throw new Error("Cannot call first() on an async source. Use firstAsync() instead.");
    }
    for (const item of this._collectIterable()) {
      return item;
    }
    return undefined;
  }

  /** Get the first item (async). */
  async firstAsync(): Promise<T | undefined> {
    if (this._async) {
      for await (const item of this._collectAsyncIterable()) {
        return item;
      }
      return undefined;
    }
    return this.first();
  }
}
