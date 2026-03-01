import { Effect, Stream } from "effect";
import { Pipeline } from "@ts-backend/core";
import type { HttpClientError, ResponseParser } from "./http-client-error.ts";
import { HttpParseError, HttpStatusError } from "./http-client-error.ts";
import type {
  HttpClient,
  HttpRequestParams,
  RequestOptions,
  RequestBodyOptions,
} from "./http-pipeline.ts";
import { AbstractHttpClient, HttpPipeline, createHttpPipeline } from "./http-pipeline.ts";
import { HttpStreamPipeline, type SSEvent } from "./http-stream.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A recorded call to the mock client. Fields match {@link HttpRequestParams} (minus schema/timeoutMs/acceptStatus). */
export interface RecordedCall {
  readonly method: string;
  readonly path: string;
  readonly json?: unknown;
  readonly body?: string | ArrayBuffer | ReadableStream | Blob | FormData;
  readonly headers?: Record<string, string>;
  readonly tag?: string;
}

/** A dynamic response handler — receives the recorded call, returns a value or error. */
export type ResponseHandler = (call: RecordedCall) => unknown | HttpClientError;

/** A route entry: static value, error, dynamic handler, or ordered queue. */
type RouteEntry =
  | { type: "static"; value: unknown | HttpClientError }
  | { type: "handler"; fn: ResponseHandler }
  | {
      type: "queue";
      responses: (unknown | HttpClientError)[];
      fallback?: unknown | HttpClientError;
    };

// ---------------------------------------------------------------------------
// MockHttpClient
// ---------------------------------------------------------------------------

/**
 * Mock HTTP client for unit tests. Tracks all calls and lets you set up
 * responses with a simple API.
 *
 * Note: setup and assertion methods use positional args for brevity — this is
 * a test utility where readability of `.on("GET", "/users/1", data)` trumps
 * the CLAUDE.md params-object convention.
 *
 * @example
 * ```ts
 * const client = new MockHttpClient()
 *   .on("GET", "/users/1", { id: 1, name: "Alice" })
 *   .on("POST", "/users", { id: 2, name: "Created" })
 *   .on("GET", "/404", MockHttpClient.fail(404));
 *
 * await client.get("/users/1", UserSchema).runPromise();
 * expect(client.calledWith("GET", "/users/1")).toBe(true);
 * expect(client.calledTimes("GET", "/users/1")).toBe(1);
 * ```
 */
export class MockHttpClient extends AbstractHttpClient {
  /** All recorded calls, in order. */
  readonly calls: RecordedCall[] = [];

  private defaultResponse: unknown = {};
  private readonly routes = new Map<string, RouteEntry>();
  private readonly sseRoutes = new Map<string, SSEvent[]>();
  private readonly ndjsonRoutes = new Map<string, unknown[]>();
  private readonly streamRoutes = new Map<string, string>();

  // -------------------------------------------------------------------------
  // Setup: responses
  // -------------------------------------------------------------------------

  /** Set a default response value for all requests that don't match a specific route. */
  respondWith<T>(value: T): this {
    this.defaultResponse = value;
    return this;
  }

  /** Register a static response for a specific method + path. */
  on(method: string, path: string, response: unknown | HttpClientError): this {
    this.routes.set(`${method} ${path}`, { type: "static", value: response });
    return this;
  }

  /** Register a dynamic response handler — response depends on the request. */
  onFn(method: string, path: string, handler: ResponseHandler): this {
    this.routes.set(`${method} ${path}`, { type: "handler", fn: handler });
    return this;
  }

  /**
   * Register an ordered sequence of responses. Each call consumes the next.
   * After exhausted, repeats the last response.
   */
  onSequence(method: string, path: string, responses: (unknown | HttpClientError)[]): this {
    this.routes.set(`${method} ${path}`, {
      type: "queue",
      responses: [...responses],
      fallback: responses[responses.length - 1],
    });
    return this;
  }

  /** Register SSE events for a path. */
  onSSE(path: string, events: SSEvent[]): this {
    this.sseRoutes.set(path, events);
    return this;
  }

  /** Register NDJSON items for a path. */
  onNDJSON(path: string, items: unknown[]): this {
    this.ndjsonRoutes.set(path, items);
    return this;
  }

  /** Register raw text stream for a path. */
  onStream(path: string, text: string): this {
    this.streamRoutes.set(path, text);
    return this;
  }

  // -------------------------------------------------------------------------
  // Static helpers
  // -------------------------------------------------------------------------

  /** Create an `HttpStatusError` for use with `.on()`, `.onSequence()`, etc. */
  static fail(status: number, body = ""): HttpStatusError {
    return new HttpStatusError({ url: "mock", status, body, message: `Mock ${status}` });
  }

  // -------------------------------------------------------------------------
  // Assertion helpers
  // -------------------------------------------------------------------------

  /** Check if a specific method + path was called at least once. */
  calledWith(method: string, path: string): boolean {
    return this.calls.some((c) => c.method === method && c.path === path);
  }

  /** Count how many times a specific method + path was called. */
  calledTimes(method: string, path: string): number {
    return this.calls.filter((c) => c.method === method && c.path === path).length;
  }

  /** Check if a method + path was called with a matching JSON body (deep equality). */
  calledWithJson(method: string, path: string, expectedJson: unknown): boolean {
    return this.calls.some(
      (c) => c.method === method && c.path === path && Bun.deepEquals(c.json, expectedJson),
    );
  }

  /** Get all recorded calls for a specific method + path. */
  callsFor(method: string, path: string): RecordedCall[] {
    return this.calls.filter((c) => c.method === method && c.path === path);
  }

  /** Get the last recorded call, or undefined if none. */
  get lastCall(): RecordedCall | undefined {
    return this.calls[this.calls.length - 1];
  }

  // -------------------------------------------------------------------------
  // Reset
  // -------------------------------------------------------------------------

  /** Reset all recorded calls. Keeps route registrations. */
  resetCalls(): this {
    this.calls.length = 0;
    return this;
  }

  /** Reset everything — calls, routes, default response. */
  reset(): this {
    this.calls.length = 0;
    this.routes.clear();
    this.sseRoutes.clear();
    this.ndjsonRoutes.clear();
    this.streamRoutes.clear();
    this.defaultResponse = {};
    return this;
  }

  // -------------------------------------------------------------------------
  // Abstract implementations
  // -------------------------------------------------------------------------

  /** No-op in mock — returns the same mock instance. */
  withOverrides(): MockHttpClient {
    return this;
  }

  request<T>(params: HttpRequestParams<T>): Pipeline<T, HttpClientError> {
    return createHttpPipeline(
      Effect.suspend(() => {
        const path = typeof params.path === "string" ? params.path : params.path.toString();
        const call: RecordedCall = {
          method: params.method,
          path,
          ...(params.json !== undefined ? { json: params.json } : {}),
          ...(params.body !== undefined ? { body: params.body } : {}),
          ...(params.headers !== undefined ? { headers: params.headers } : {}),
          ...(params.tag !== undefined ? { tag: params.tag } : {}),
        };
        this.calls.push(call);

        const entry = this.routes.get(`${params.method} ${path}`);
        const raw = entry ? this.resolveEntry(entry, call) : this.defaultResponse;
        if (this.isHttpClientError(raw)) return Effect.fail(raw);
        return this.parseResponse<T>(raw, params.schema, path);
      }),
    );
  }

  getText(path: string | URL, _options?: RequestOptions): Pipeline<string, HttpClientError> {
    return createHttpPipeline(
      Effect.suspend(() => {
        const p = typeof path === "string" ? path : path.toString();
        this.calls.push({ method: "GET", path: p });
        const entry = this.routes.get(`GET ${p}`);
        if (entry)
          return this.toResult<string>(this.resolveEntry(entry, { method: "GET", path: p })).effect;
        return Effect.succeed(this.defaultResponse as string);
      }),
    );
  }

  getStream(path: string | URL, _options?: RequestOptions): HttpStreamPipeline<string> {
    const p = typeof path === "string" ? path : path.toString();
    this.calls.push({ method: "GET", path: p });
    const text = this.streamRoutes.get(p);
    if (text) return new HttpStreamPipeline(Stream.make(text));
    return new HttpStreamPipeline(Stream.empty as Stream.Stream<string>);
  }

  postStream(path: string | URL, _options?: RequestBodyOptions): HttpStreamPipeline<string> {
    const p = typeof path === "string" ? path : path.toString();
    this.calls.push({ method: "POST", path: p });
    const text = this.streamRoutes.get(p);
    if (text) return new HttpStreamPipeline(Stream.make(text));
    return new HttpStreamPipeline(Stream.empty as Stream.Stream<string>);
  }

  getSSE(path: string | URL, _options?: RequestOptions): HttpStreamPipeline<SSEvent> {
    const p = typeof path === "string" ? path : path.toString();
    this.calls.push({ method: "GET", path: p });
    const events = this.sseRoutes.get(p);
    if (events) return new HttpStreamPipeline(Stream.fromIterable(events));
    return new HttpStreamPipeline(Stream.empty as Stream.Stream<SSEvent>);
  }

  postSSE(path: string | URL, _options?: RequestBodyOptions): HttpStreamPipeline<SSEvent> {
    const p = typeof path === "string" ? path : path.toString();
    this.calls.push({ method: "POST", path: p });
    const events = this.sseRoutes.get(p);
    if (events) return new HttpStreamPipeline(Stream.fromIterable(events));
    return new HttpStreamPipeline(Stream.empty as Stream.Stream<SSEvent>);
  }

  getNDJSON<T>(
    path: string | URL,
    _schema: ResponseParser<T>,
    _options?: RequestOptions,
  ): HttpStreamPipeline<T> {
    const p = typeof path === "string" ? path : path.toString();
    this.calls.push({ method: "GET", path: p });
    const items = this.ndjsonRoutes.get(p);
    if (items) return new HttpStreamPipeline(Stream.fromIterable(items) as Stream.Stream<T>);
    return new HttpStreamPipeline(Stream.empty as Stream.Stream<T>);
  }

  postNDJSON<T>(
    path: string | URL,
    _schema: ResponseParser<T>,
    _options?: RequestBodyOptions,
  ): HttpStreamPipeline<T> {
    const p = typeof path === "string" ? path : path.toString();
    this.calls.push({ method: "POST", path: p });
    const items = this.ndjsonRoutes.get(p);
    if (items) return new HttpStreamPipeline(Stream.fromIterable(items) as Stream.Stream<T>);
    return new HttpStreamPipeline(Stream.empty as Stream.Stream<T>);
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private resolveEntry(entry: RouteEntry, call: RecordedCall): unknown | HttpClientError {
    switch (entry.type) {
      case "static":
        return entry.value;
      case "handler":
        return entry.fn(call);
      case "queue":
        return entry.responses.length > 0 ? entry.responses.shift()! : (entry.fallback ?? {});
    }
  }

  private toResult<T>(value: unknown): Pipeline<T, HttpClientError> {
    if (this.isHttpClientError(value)) {
      return createHttpPipeline(Effect.fail(value));
    }
    return HttpPipeline.succeed(value as T);
  }

  private parseResponse<T>(
    raw: unknown,
    schema: ResponseParser<T> | undefined,
    path: string,
  ): Effect.Effect<T, HttpClientError> {
    if (!schema) return Effect.succeed(raw as T);
    const result = schema.safeParse(raw);
    if (result.success) return Effect.succeed(result.data);
    return Effect.fail(
      new HttpParseError({ url: path, cause: result.error, message: "Mock response parse error" }),
    );
  }

  private isHttpClientError(value: unknown): value is HttpClientError {
    if (value == null || typeof value !== "object" || !("_tag" in value)) return false;
    const tag = (value as { _tag: string })._tag;
    const tags: Record<HttpClientError["_tag"], true> = {
      HttpNetworkError: true,
      HttpTimeoutError: true,
      HttpStatusError: true,
      HttpParseError: true,
      PollTimeoutError: true,
    } satisfies Record<HttpClientError["_tag"], true>;
    return tag in tags;
  }
}

/**
 * Create a mock `HttpClient` for unit tests.
 */
export function createMockHttpClient(overrides?: Partial<HttpClient>): MockHttpClient & HttpClient {
  const mock = new MockHttpClient();
  if (!overrides) return mock as MockHttpClient & HttpClient;

  return new Proxy(mock, {
    get(target, prop, receiver) {
      if (prop in overrides) {
        return (overrides as any)[prop];
      }
      return Reflect.get(target, prop, receiver);
    },
  }) as MockHttpClient & HttpClient;
}
