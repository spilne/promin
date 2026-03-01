import { Effect, Stream, Chunk, Option, Duration } from "effect";
import {
  HttpNetworkError,
  HttpParseError,
  type HttpClientError,
  type ResponseParser,
} from "./http-client-error.ts";
import { httpFetchOk, type HttpRequestOptions, type HttpTransport } from "./http-client.ts";

// ---------------------------------------------------------------------------
// SSE types
// ---------------------------------------------------------------------------

export interface SSEvent {
  /** Event type (from `event:` field). Defaults to "message". */
  readonly event: string;
  /** Event data (from `data:` field). May span multiple lines. */
  readonly data: string;
  /** Optional event ID (from `id:` field). */
  readonly id?: string;
  /** Optional retry hint in ms (from `retry:` field). */
  readonly retry?: number;
}

// ---------------------------------------------------------------------------
// Core: fetch → Stream<string> (raw text chunks)
// ---------------------------------------------------------------------------

/**
 * Execute a fetch and stream the response body as raw text chunks.
 *
 * Each chunk is whatever the transport delivers — could be a partial line,
 * multiple lines, etc. Use `httpStreamLines`, `httpStreamSSE`, or
 * `httpStreamNDJSON` for structured parsing.
 */
export function httpStreamText(
  options: HttpRequestOptions & {
    readonly acceptStatus?: (status: number) => boolean;
    readonly transport?: HttpTransport;
  },
): Stream.Stream<string, HttpClientError> {
  return Stream.unwrapScoped(
    httpFetchOk(options).pipe(Effect.map((response) => readBodyAsTextStream(response, options))),
  );
}

/**
 * Execute a fetch and stream the response body line-by-line.
 * Handles partial chunks and line splitting across chunk boundaries.
 */
export function httpStreamLines(
  options: HttpRequestOptions & {
    readonly acceptStatus?: (status: number) => boolean;
    readonly transport?: HttpTransport;
  },
): Stream.Stream<string, HttpClientError> {
  return httpStreamText(options).pipe(splitLines);
}

// ---------------------------------------------------------------------------
// SSE: fetch → Stream<SSEvent>
// ---------------------------------------------------------------------------

/**
 * Execute a fetch and parse the response as Server-Sent Events.
 *
 * Automatically sets `Accept: text/event-stream` and `Cache-Control: no-cache`.
 * Each emitted value is a parsed SSEvent with `event`, `data`, `id`, and `retry` fields.
 *
 * @example
 * ```ts
 * const events = httpStreamSSE({
 *   url: "https://api.example.com/events",
 * });
 *
 * await Stream.runForEach(events, (event) =>
 *   Effect.sync(() => console.log(event.event, event.data)),
 * ).pipe(Effect.runPromise);
 * ```
 */
export function httpStreamSSE(
  options: HttpRequestOptions & {
    readonly acceptStatus?: (status: number) => boolean;
    readonly transport?: HttpTransport;
  },
): Stream.Stream<SSEvent, HttpClientError> {
  const sseHeaders = {
    ...options.headers,
    Accept: "text/event-stream",
    "Cache-Control": "no-cache",
  };

  return httpStreamLines({ ...options, headers: sseHeaders }).pipe(parseSSELines);
}

// ---------------------------------------------------------------------------
// NDJSON: fetch → Stream<T> with Zod validation
// ---------------------------------------------------------------------------

/**
 * Execute a fetch and parse each line as JSON, validated against a Zod schema.
 *
 * Each line of the response body is expected to be a valid JSON object (NDJSON format).
 * Empty lines are skipped.
 *
 * @example
 * ```ts
 * const items = httpStreamNDJSON({
 *   url: "https://api.example.com/export",
 *   schema: ItemSchema,
 * });
 *
 * await Stream.runForEach(items, (item) =>
 *   Effect.sync(() => process(item)),
 * ).pipe(Effect.runPromise);
 * ```
 */
export function httpStreamNDJSON<T>(
  options: HttpRequestOptions & {
    readonly schema: ResponseParser<T>;
    readonly acceptStatus?: (status: number) => boolean;
    readonly transport?: HttpTransport;
  },
): Stream.Stream<T, HttpClientError> {
  const { schema, ...rest } = options;
  const urlStr = typeof rest.url === "string" ? rest.url : rest.url.toString();

  return httpStreamLines(rest).pipe(
    Stream.filter((line) => line.trim().length > 0),
    Stream.mapEffect((line) =>
      Effect.try({
        try: () => {
          const parsed = JSON.parse(line);
          const result = schema.safeParse(parsed);
          if (!result.success) {
            throw result.error;
          }
          return result.data;
        },
        catch: (cause) =>
          new HttpParseError({
            url: urlStr,
            cause,
            message: `Failed to parse NDJSON line from ${urlStr}: ${line.slice(0, 200)}`,
          }),
      }),
    ),
  );
}

// ---------------------------------------------------------------------------
// HttpStreamPipeline — chainable wrapper for streams
// ---------------------------------------------------------------------------

/**
 * A chainable, lazily-evaluated streaming HTTP pipeline.
 *
 * Nothing executes until you call a terminal (`.forEach()`, `.collect()`, `.reduce()`).
 * Handles backpressure naturally — the producer only advances when the consumer is ready.
 *
 * @example
 * ```ts
 * // Process SSE events
 * await api.postSSE("/chat/completions", { json: { prompt: "Hello" } })
 *   .filter((event) => event.event === "delta")
 *   .map((event) => JSON.parse(event.data).text)
 *   .forEach((text) => process.stdout.write(text));
 *
 * // Collect NDJSON into array
 * const items = await api.postNDJSON("/export", ItemSchema, { json: { format: "ndjson" } })
 *   .filter((item) => item.score > 0.5)
 *   .collect();
 * ```
 */
export class HttpStreamPipeline<T> {
  constructor(readonly stream: Stream.Stream<T, HttpClientError>) {}

  // -------------------------------------------------------------------------
  // Transform
  // -------------------------------------------------------------------------

  /** Transform each chunk. */
  map<U>(fn: (value: T) => U): HttpStreamPipeline<U> {
    return new HttpStreamPipeline(Stream.map(this.stream, fn));
  }

  /** Async transform each chunk — takes a function returning a Promise. */
  mapAsync<U>(fn: (value: T) => Promise<U>): HttpStreamPipeline<U> {
    return new HttpStreamPipeline(
      Stream.mapEffect(this.stream, (value) => Effect.promise(() => fn(value))),
    );
  }

  /** Async transform each chunk (returns an Effect). Escape hatch for Effect-native code. */
  mapEffect<U>(fn: (value: T) => Effect.Effect<U, HttpClientError>): HttpStreamPipeline<U> {
    return new HttpStreamPipeline(Stream.mapEffect(this.stream, fn));
  }

  /** Filter chunks by predicate. */
  filter(fn: (value: T) => boolean): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(Stream.filter(this.stream, fn));
  }

  /** Run a sync side-effect for each chunk without changing it. */
  tap(fn: (value: T) => void): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(Stream.tap(this.stream, (value) => Effect.sync(() => fn(value))));
  }

  /** Run an async side-effect for each chunk without changing it. Awaits the Promise before continuing. */
  tapAsync(fn: (value: T) => Promise<void>): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(
      Stream.tap(this.stream, (value) => Effect.promise(() => fn(value))),
    );
  }

  /** Take the first N chunks then stop. */
  take(n: number): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(Stream.take(this.stream, n));
  }

  /** Take chunks while predicate is true, then stop. */
  takeWhile(fn: (value: T) => boolean): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(Stream.takeWhile(this.stream, fn));
  }

  /** Skip the first N chunks. */
  drop(n: number): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(Stream.drop(this.stream, n));
  }

  /** Emit only when the value changes. */
  dedupe(): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(Stream.changes(this.stream));
  }

  /** Flat-map each chunk into multiple values. */
  flatMap<U>(fn: (value: T) => HttpStreamPipeline<U>): HttpStreamPipeline<U> {
    return new HttpStreamPipeline(Stream.flatMap(this.stream, (value) => fn(value).stream));
  }

  // -------------------------------------------------------------------------
  // Parallel & batching (fs2-style)
  // -------------------------------------------------------------------------

  /**
   * Async transform with bounded concurrency. Processes up to `concurrency` items
   * in parallel, preserving input order.
   *
   * @example
   * ```ts
   * await api.getNDJSON("/export", ItemSchema)
   *   .parAsyncMap(10, (item) => enrichFromDb(item.id))
   *   .forEach((enriched) => process(enriched));
   * ```
   */
  parAsyncMap<U>(concurrency: number, fn: (value: T) => Promise<U>): HttpStreamPipeline<U> {
    return new HttpStreamPipeline(
      Stream.mapEffect(this.stream, (value) => Effect.promise(() => fn(value)), { concurrency }),
    );
  }

  /**
   * Like `parAsyncMap` but results arrive in completion order, not input order.
   * Higher throughput when order doesn't matter.
   *
   * @example
   * ```ts
   * await api.getNDJSON("/export", ItemSchema)
   *   .parAsyncMapUnordered(20, (item) => fetchDetails(item.id))
   *   .forEach((details) => index(details));
   * ```
   */
  parAsyncMapUnordered<U>(
    concurrency: number,
    fn: (value: T) => Promise<U>,
  ): HttpStreamPipeline<U> {
    return new HttpStreamPipeline(
      Stream.mapEffect(this.stream, (value) => Effect.promise(() => fn(value)), {
        concurrency,
        unordered: true,
      }),
    );
  }

  /**
   * Buffer items into batches of up to `maxSize` items or `maxWaitMs` milliseconds,
   * whichever comes first. Emits `T[]` chunks.
   *
   * @example
   * ```ts
   * // Bulk insert: batch up to 500 rows or flush every 1s
   * await api.getNDJSON("/firehose", RowSchema)
   *   .groupWithin(500, 1_000)
   *   .tapAsync((batch) => db.bulkInsert(batch))
   *   .drain();
   * ```
   */
  groupWithin(maxSize: number, maxWaitMs: number): HttpStreamPipeline<T[]> {
    return new HttpStreamPipeline(
      Stream.groupedWithin(this.stream, maxSize, Duration.millis(maxWaitMs)).pipe(
        Stream.map(Chunk.toArray),
      ),
    );
  }

  /**
   * Buffer items into fixed-size batches. Last batch may be smaller.
   *
   * @example
   * ```ts
   * await stream.grouped(100).tapAsync((batch) => processBatch(batch)).drain();
   * ```
   */
  grouped(size: number): HttpStreamPipeline<T[]> {
    return new HttpStreamPipeline(
      Stream.grouped(this.stream, size).pipe(Stream.map(Chunk.toArray)),
    );
  }

  /**
   * Running accumulator — like `reduce` but emits every intermediate result.
   *
   * @example
   * ```ts
   * // Running average of view counts
   * await api.getNDJSON("/videos", VideoSchema)
   *   .scan({ sum: 0, count: 0 }, (acc, v) => ({ sum: acc.sum + v.views, count: acc.count + 1 }))
   *   .map((acc) => acc.sum / acc.count)
   *   .forEach((avg) => gauge.set(avg));
   * ```
   */
  scan<U>(initial: U, fn: (acc: U, value: T) => U): HttpStreamPipeline<U> {
    return new HttpStreamPipeline(Stream.scan(this.stream, initial, fn));
  }

  /**
   * Merge another stream — interleave items from both as they arrive.
   *
   * @example
   * ```ts
   * const combined = api.getSSE("/events/channel-1")
   *   .merge(api.getSSE("/events/channel-2"))
   *   .forEach((event) => handle(event));
   * ```
   */
  merge(other: HttpStreamPipeline<T>): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(Stream.merge(this.stream, other.stream));
  }

  /**
   * Merge multiple streams — interleave items from all as they arrive.
   *
   * @example
   * ```ts
   * const combined = HttpStreamPipeline.mergeAll(
   *   api.getSSE("/events/channel-1"),
   *   api.getSSE("/events/channel-2"),
   *   api.getSSE("/events/channel-3"),
   * ).forEach((event) => handle(event));
   * ```
   */
  static mergeAll<U>(...streams: HttpStreamPipeline<U>[]): HttpStreamPipeline<U> {
    if (streams.length === 0) return new HttpStreamPipeline(Stream.empty);
    return streams.reduce((acc, s) => acc.merge(s));
  }

  /**
   * Reusable stream transformation. Pass a function that takes a stream pipeline
   * and returns a transformed one.
   *
   * @example
   * ```ts
   * const withMetrics = (s: HttpStreamPipeline<Event>) =>
   *   s.tap((e) => counter.inc({ type: e.event }));
   *
   * await api.getSSE("/events")
   *   .through(withMetrics)
   *   .forEach(handle);
   * ```
   */
  through<U>(
    pipe: (stream: HttpStreamPipeline<T>) => HttpStreamPipeline<U>,
  ): HttpStreamPipeline<U> {
    return pipe(this);
  }

  // -------------------------------------------------------------------------
  // Error handling
  // -------------------------------------------------------------------------

  /** Recover from stream errors with a fallback value and stop. */
  orElse(fallback: T): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(Stream.orElse(this.stream, () => Stream.make(fallback)));
  }

  /** Run a side-effect on error. */
  tapError(fn: (error: HttpClientError) => void): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(
      Stream.tapError(this.stream, (error) => Effect.sync(() => fn(error))),
    );
  }

  // -------------------------------------------------------------------------
  // Interruption
  // -------------------------------------------------------------------------

  /**
   * Stop this stream when an AbortSignal fires.
   *
   * For regular (non-streaming) requests, fiber interruption automatically aborts
   * the fetch via the scoped AbortController. But for streams consumed via
   * `.forEach()` / `.collect()` (which return Promises, not Effects), there's no
   * fiber handle to interrupt from outside. This method bridges that gap.
   *
   * Common use case: stop reading from upstream when the downstream client disconnects.
   *
   * @example
   * ```ts
   * app.get("/events", async (c) => {
   *   await api.getSSE("/upstream/events")
   *     .interruptOn(c.req.raw.signal)  // stop when client disconnects
   *     .forEach((event) => sendToClient(event));
   * });
   * ```
   */
  interruptOn(signal: AbortSignal): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(
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

  // -------------------------------------------------------------------------
  // Terminals
  // -------------------------------------------------------------------------

  /**
   * Run a cleanup function when the stream ends (success, error, or interruption).
   *
   * @example
   * ```ts
   * await api.postSSE("/events", { json: body })
   *   .forEach((event) => process(event))
   *   .finally(() => cleanup());
   * ```
   */
  finally(fn: () => void): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(Stream.ensuring(this.stream, Effect.sync(fn)));
  }

  /** Process each chunk. Returns a Promise that resolves when the stream ends. */
  async forEach(fn: (value: T) => void): Promise<void> {
    await Effect.runPromise(
      Stream.runForEach(this.stream, (value) => Effect.sync(() => fn(value))),
    );
  }

  /** Collect all chunks into an array. */
  async collect(): Promise<T[]> {
    const chunk = await Effect.runPromise(Stream.runCollect(this.stream));
    return Chunk.toArray(chunk);
  }

  /** Fold over all chunks to produce a single value. */
  async reduce<U>(initial: U, fn: (acc: U, value: T) => U): Promise<U> {
    return Effect.runPromise(Stream.runFold(this.stream, initial, fn));
  }

  /** Drain the stream (consume all chunks, discard values). */
  async drain(): Promise<void> {
    await Effect.runPromise(Stream.runDrain(this.stream));
  }

  /** Escape hatch: get the raw Effect Stream for advanced composition. */
  toStream(): Stream.Stream<T, HttpClientError> {
    return this.stream;
  }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function readBodyAsTextStream(
  response: Response,
  options: HttpRequestOptions,
): Stream.Stream<string, HttpClientError> {
  const urlStr = typeof options.url === "string" ? options.url : options.url.toString();

  if (!response.body) {
    return Stream.fail(
      new HttpNetworkError({
        url: urlStr,
        cause: new Error("Response has no body"),
        message: `Response from ${urlStr} has no body to stream`,
      }),
    );
  }

  // Acquire the reader as a scoped resource. On early termination (.take(),
  // .interruptOn(), fiber interrupt), the reader is cancelled — releasing
  // the lock on the ReadableStream and closing the underlying TCP connection.
  type Reader = { read(): Promise<{ done: boolean; value?: Uint8Array }>; cancel(): Promise<void> };

  const acquireReader = Effect.sync((): Reader => response.body!.getReader());
  const releaseReader = (reader: Reader) => Effect.promise(() => reader.cancel().catch(() => {}));

  return Stream.unwrapScoped(
    Effect.acquireRelease(acquireReader, releaseReader).pipe(
      Effect.map((reader) => {
        const decoder = new TextDecoder();

        return Stream.repeatEffectOption(
          Effect.tryPromise({
            try: async () => {
              const { done, value } = await reader.read();
              if (done) return null;
              return decoder.decode(value, { stream: true });
            },
            catch: (cause) =>
              new HttpNetworkError({
                url: urlStr,
                cause,
                message: `Error reading stream from ${urlStr}: ${cause instanceof Error ? cause.message : String(cause)}`,
              }),
          }).pipe(
            Effect.flatMap(
              (text): Effect.Effect<string, Option.Option<HttpClientError>> =>
                text === null ? Effect.fail(Option.none<HttpClientError>()) : Effect.succeed(text),
            ),
            Effect.mapError(
              (e): Option.Option<HttpClientError> =>
                Option.isOption(e) ? e : Option.some(e as HttpClientError),
            ),
          ),
        );
      }),
    ),
  );
}

/** Pipe operator: split a text stream into lines, handling partial chunks. */
function splitLines<E>(stream: Stream.Stream<string, E>): Stream.Stream<string, E> {
  // Use a stateful scan to buffer partial lines across chunks
  return Stream.suspend(() => {
    let buffer = "";

    return stream.pipe(
      Stream.mapConcatEffect((chunk) =>
        Effect.sync(() => {
          buffer += chunk;
          const parts = buffer.split("\n");
          // Last element is the partial line (or empty if chunk ended with \n)
          buffer = parts.pop()!;
          return parts;
        }),
      ),
      // When the stream ends, flush any remaining buffer
      Stream.concat(Stream.suspend(() => (buffer.length > 0 ? Stream.make(buffer) : Stream.empty))),
    );
  });
}

/** Pipe operator: parse a line stream into SSE events. */
function parseSSELines<E>(stream: Stream.Stream<string, E>): Stream.Stream<SSEvent, E> {
  return Stream.suspend(() => {
    let currentEvent = "";
    let currentData: string[] = [];
    let currentId: string | undefined;
    let currentRetry: number | undefined;

    return stream.pipe(
      Stream.mapConcatEffect((line) =>
        Effect.sync(() => {
          // Empty line = dispatch event
          if (line === "") {
            if (currentData.length === 0) return [];

            const event: SSEvent = {
              event: currentEvent || "message",
              data: currentData.join("\n"),
              ...(currentId !== undefined ? { id: currentId } : {}),
              ...(currentRetry !== undefined ? { retry: currentRetry } : {}),
            };

            // Reset state
            currentEvent = "";
            currentData = [];
            currentId = undefined;
            currentRetry = undefined;

            return [event];
          }

          // Skip comments
          if (line.startsWith(":")) return [];

          const colonIndex = line.indexOf(":");
          if (colonIndex === -1) return [];

          const field = line.slice(0, colonIndex);
          // Value starts after colon + optional space
          const value =
            line[colonIndex + 1] === " " ? line.slice(colonIndex + 2) : line.slice(colonIndex + 1);

          switch (field) {
            case "event":
              currentEvent = value;
              break;
            case "data":
              currentData.push(value);
              break;
            case "id":
              currentId = value;
              break;
            case "retry": {
              const n = parseInt(value, 10);
              if (!isNaN(n)) currentRetry = n;
              break;
            }
          }

          return [];
        }),
      ),
    );
  });
}
