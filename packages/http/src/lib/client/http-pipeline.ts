import { Effect } from "effect";
import { Pipeline, type PipelineDefaults } from "@promin/core";
import type { HttpClientError, ResponseParser } from "./http-client-error.ts";
import { httpRequest, httpRequestText, type HttpTransport } from "./http-client.ts";
import {
  httpStreamSSE,
  httpStreamNDJSON,
  httpStreamText,
  HttpStreamPipeline,
  type SSEvent,
} from "./http-stream.ts";

// ---------------------------------------------------------------------------
// HTTP pipeline defaults — retry only transient HTTP errors
// ---------------------------------------------------------------------------

/** Default retry predicate for HTTP pipelines: 5xx, 429, timeouts, network errors. */
export const HTTP_RETRYABLE: (error: HttpClientError) => boolean = (error) =>
  error._tag === "HttpTimeoutError" ||
  error._tag === "HttpNetworkError" ||
  (error._tag === "HttpStatusError" && error.isRetryable);

const HTTP_PIPELINE_DEFAULTS: PipelineDefaults<HttpClientError> = {
  retryWhen: HTTP_RETRYABLE,
};

// ---------------------------------------------------------------------------
// HttpPipeline — type alias + static helpers for backwards compatibility
// ---------------------------------------------------------------------------

/** An HTTP pipeline is a Pipeline with HttpClientError as the error type. */
export type HttpPipeline<T> = Pipeline<T, HttpClientError>;

/** Create an HttpPipeline from a raw Effect, with HTTP retry defaults injected. */
export function createHttpPipeline<T>(
  effect: Effect.Effect<T, HttpClientError>,
): Pipeline<T, HttpClientError> {
  return Pipeline.from(effect, { defaults: HTTP_PIPELINE_DEFAULTS });
}

/**
 * Static helpers that mirror the old `HttpPipeline.succeed(...)`, `HttpPipeline.fail(...)` API.
 * Preserves backward compatibility for consumer code.
 */
export const HttpPipeline = {
  succeed: <T>(value: T): Pipeline<T, HttpClientError> =>
    Pipeline.succeed(value) as Pipeline<T, HttpClientError>,
  fail: (error: HttpClientError): Pipeline<never, HttpClientError> => Pipeline.fail(error),
  all: Pipeline.all,
  allSettled: Pipeline.allSettled,
  race: Pipeline.race,
  fallback: Pipeline.fallback,
};

// ---------------------------------------------------------------------------
// Shared options type for HttpClient methods (no signal — use Effect interruption)
// ---------------------------------------------------------------------------

export interface RequestOptions {
  headers?: Record<string, string>;
  timeoutMs?: number;
  /** Optional label for middleware (metrics, logging). Avoids high-cardinality URLs in Prometheus. */
  tag?: string;
}

export interface RequestBodyOptions extends RequestOptions {
  json?: unknown;
  body?: string | ArrayBuffer | ReadableStream | Blob | FormData;
}

export interface MultipartOptions extends RequestOptions {
  /** The file to upload. */
  file: Blob | File;
  /** Name of the form field for the file. Defaults to `"file"`. */
  fileField?: string;
  /** Additional string fields to include in the multipart form. */
  fields?: Record<string, string>;
}

// ---------------------------------------------------------------------------
// HttpClient interface + DefaultHttpClient implementation
// ---------------------------------------------------------------------------

/**
 * Context passed to middleware — describes the request being made.
 */
export interface HttpRequestContext {
  readonly method: string;
  readonly url: string;
  /** Optional label for metrics/logging. Avoids high-cardinality raw URLs in Prometheus. */
  readonly tag?: string;
}

/**
 * Hooks called during request lifecycle. All callbacks are plain sync functions — no Effect needed.
 *
 * Exceptions thrown inside a hook become untyped defects (fiber crash).
 * Keep hooks defensive (try-catch internally) if they might throw.
 */
export interface HttpMiddleware {
  /** Called before the request is sent. */
  onRequest?: (context: HttpRequestContext) => void;
  /** Called after a successful response (including parse + validation). */
  onResponse?: (context: HttpRequestContext & { durationMs: number }) => void;
  /** Called when the request fails at any stage. */
  onError?: (context: HttpRequestContext & { durationMs: number }, error: HttpClientError) => void;
}

export interface HttpClientConfig {
  /** Base URL prepended to all relative paths. */
  readonly baseUrl?: string;
  /** Default headers sent with every request. */
  readonly headers?: Record<string, string>;
  /** Default timeout in ms for every request. Defaults to 30 000. */
  readonly timeoutMs?: number;
  /**
   * Middleware applied to every non-streaming request, in order.
   * Does not apply to streaming methods.
   */
  readonly middleware?: readonly HttpMiddleware[];
  /**
   * Pluggable transport layer. Defaults to `FetchTransport` (global `fetch`).
   *
   * Override this to swap in a different HTTP backend (e.g. `@effect/platform`
   * HttpClient) without changing any consumer code. See {@link HttpTransport}
   * for design rationale.
   */
  readonly transport?: HttpTransport;
}

/** Params accepted by {@link HttpClient.request}. */
export interface HttpRequestParams<T> {
  path: string | URL;
  method: string;
  schema: ResponseParser<T>;
  json?: unknown;
  body?: string | ArrayBuffer | ReadableStream | Blob | FormData;
  headers?: Record<string, string>;
  timeoutMs?: number;
  acceptStatus?: (status: number) => boolean;
  tag?: string;
}

/**
 * Interface for the HTTP client — accept this in services, inject via constructor.
 */
export interface HttpClient {
  get<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestOptions,
  ): Pipeline<T, HttpClientError>;
  post<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Pipeline<T, HttpClientError>;
  put<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Pipeline<T, HttpClientError>;
  patch<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Pipeline<T, HttpClientError>;
  delete<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Pipeline<T, HttpClientError>;
  getJson(path: string | URL, options?: RequestOptions): Pipeline<unknown, HttpClientError>;
  postJson(path: string | URL, options?: RequestBodyOptions): Pipeline<unknown, HttpClientError>;
  getText(path: string | URL, options?: RequestOptions): Pipeline<string, HttpClientError>;
  getStream(path: string | URL, options?: RequestOptions): HttpStreamPipeline<string>;
  postStream(path: string | URL, options?: RequestBodyOptions): HttpStreamPipeline<string>;
  getSSE(path: string | URL, options?: RequestOptions): HttpStreamPipeline<SSEvent>;
  postSSE(path: string | URL, options?: RequestBodyOptions): HttpStreamPipeline<SSEvent>;
  getNDJSON<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestOptions,
  ): HttpStreamPipeline<T>;
  postNDJSON<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): HttpStreamPipeline<T>;
  postMultipart<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options: MultipartOptions,
  ): Pipeline<T, HttpClientError>;
  request<T>(params: HttpRequestParams<T>): Pipeline<T, HttpClientError>;
  /** Create a new client with overridden config. Unset fields inherit from the parent. */
  withOverrides(overrides: Partial<HttpClientConfig>): HttpClient;
}

// ---------------------------------------------------------------------------
// Identity parser — passes data through without validation
// ---------------------------------------------------------------------------

export const identityParser: ResponseParser<unknown> = {
  safeParse: (data: unknown) => ({ success: true as const, data }),
};

// ---------------------------------------------------------------------------
// AbstractHttpClient — convenience methods delegate to abstract request()
// ---------------------------------------------------------------------------

export abstract class AbstractHttpClient implements HttpClient {
  get<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestOptions,
  ): Pipeline<T, HttpClientError> {
    return this.request({
      path,
      method: "GET",
      schema,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      tag: options?.tag,
    });
  }

  post<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Pipeline<T, HttpClientError> {
    return this.request({
      path,
      method: "POST",
      schema,
      json: options?.json,
      body: options?.body,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      tag: options?.tag,
    });
  }

  put<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Pipeline<T, HttpClientError> {
    return this.request({
      path,
      method: "PUT",
      schema,
      json: options?.json,
      body: options?.body,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      tag: options?.tag,
    });
  }

  patch<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Pipeline<T, HttpClientError> {
    return this.request({
      path,
      method: "PATCH",
      schema,
      json: options?.json,
      body: options?.body,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      tag: options?.tag,
    });
  }

  delete<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): Pipeline<T, HttpClientError> {
    return this.request({
      path,
      method: "DELETE",
      schema,
      json: options?.json,
      body: options?.body,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      tag: options?.tag,
    });
  }

  getJson(path: string | URL, options?: RequestOptions): Pipeline<unknown, HttpClientError> {
    return this.request({
      path,
      method: "GET",
      schema: identityParser,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      tag: options?.tag,
    });
  }

  postJson(path: string | URL, options?: RequestBodyOptions): Pipeline<unknown, HttpClientError> {
    return this.request({
      path,
      method: "POST",
      schema: identityParser,
      json: options?.json,
      body: options?.body,
      headers: options?.headers,
      timeoutMs: options?.timeoutMs,
      tag: options?.tag,
    });
  }

  postMultipart<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options: MultipartOptions,
  ): Pipeline<T, HttpClientError> {
    const formData = new FormData();
    formData.append(options.fileField ?? "file", options.file);
    if (options.fields) {
      for (const [key, value] of Object.entries(options.fields)) {
        formData.append(key, value);
      }
    }
    return this.request({
      path,
      method: "POST",
      schema,
      body: formData,
      headers: options.headers,
      timeoutMs: options.timeoutMs,
      tag: options.tag,
    });
  }

  abstract getText(path: string | URL, options?: RequestOptions): Pipeline<string, HttpClientError>;
  abstract getStream(path: string | URL, options?: RequestOptions): HttpStreamPipeline<string>;
  abstract postStream(path: string | URL, options?: RequestBodyOptions): HttpStreamPipeline<string>;
  abstract getSSE(path: string | URL, options?: RequestOptions): HttpStreamPipeline<SSEvent>;
  abstract postSSE(path: string | URL, options?: RequestBodyOptions): HttpStreamPipeline<SSEvent>;
  abstract getNDJSON<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestOptions,
  ): HttpStreamPipeline<T>;
  abstract postNDJSON<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): HttpStreamPipeline<T>;
  abstract request<T>(params: HttpRequestParams<T>): Pipeline<T, HttpClientError>;
  abstract withOverrides(overrides: Partial<HttpClientConfig>): HttpClient;
}

// ---------------------------------------------------------------------------
// DefaultHttpClient — real HTTP via fetch
// ---------------------------------------------------------------------------

export class DefaultHttpClient extends AbstractHttpClient {
  constructor(private readonly config: HttpClientConfig = {}) {
    super();
  }

  withOverrides(overrides: Partial<HttpClientConfig>): DefaultHttpClient {
    return new DefaultHttpClient({
      baseUrl: overrides.baseUrl ?? this.config.baseUrl,
      headers: { ...this.config.headers, ...overrides.headers },
      timeoutMs: overrides.timeoutMs ?? this.config.timeoutMs,
      middleware: overrides.middleware
        ? [...(this.config.middleware ?? []), ...overrides.middleware]
        : this.config.middleware,
      transport: overrides.transport ?? this.config.transport,
    });
  }

  private resolveUrl(path: string | URL): string {
    const p = typeof path === "string" ? path : path.toString();
    if (this.config.baseUrl && !p.startsWith("http://") && !p.startsWith("https://")) {
      const base = this.config.baseUrl.endsWith("/")
        ? this.config.baseUrl.slice(0, -1)
        : this.config.baseUrl;
      const rel = p.startsWith("/") ? p : `/${p}`;
      return `${base}${rel}`;
    }
    return p;
  }

  private mergeHeaders(extra?: Record<string, string>): Record<string, string> {
    return { ...this.config.headers, ...extra };
  }

  private pipeline<T>(
    effect: Effect.Effect<T, HttpClientError>,
    context: HttpRequestContext,
  ): Pipeline<T, HttpClientError> {
    if (!this.config.middleware?.length) return createHttpPipeline(effect);

    const middlewares = this.config.middleware;
    const wrapped = Effect.sync(() => {
      const start = performance.now();
      for (const mw of middlewares) mw.onRequest?.(context);
      return start;
    }).pipe(
      Effect.flatMap((start) =>
        effect.pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              const durationMs = performance.now() - start;
              for (const mw of middlewares) mw.onResponse?.({ ...context, durationMs });
            }),
          ),
          Effect.tapError((error) =>
            Effect.sync(() => {
              const durationMs = performance.now() - start;
              for (const mw of middlewares) mw.onError?.({ ...context, durationMs }, error);
            }),
          ),
        ),
      ),
    );

    return createHttpPipeline(wrapped);
  }

  // -------------------------------------------------------------------------
  // Abstract implementations
  // -------------------------------------------------------------------------

  request<T>(params: HttpRequestParams<T>): Pipeline<T, HttpClientError> {
    const url = this.resolveUrl(params.path);
    return this.pipeline(
      httpRequest({
        url,
        method: params.method,
        headers: this.mergeHeaders(params.headers),
        json: params.json,
        body: params.body,
        timeoutMs: params.timeoutMs ?? this.config.timeoutMs,
        schema: params.schema,
        acceptStatus: params.acceptStatus,
        transport: this.config.transport,
      }),
      { method: params.method, url, tag: params.tag },
    );
  }

  getText(path: string | URL, options?: RequestOptions): Pipeline<string, HttpClientError> {
    const url = this.resolveUrl(path);
    return this.pipeline(
      httpRequestText({
        url,
        method: "GET",
        headers: this.mergeHeaders(options?.headers),
        timeoutMs: options?.timeoutMs ?? this.config.timeoutMs,
        transport: this.config.transport,
      }),
      { method: "GET", url, tag: options?.tag },
    );
  }

  // -------------------------------------------------------------------------
  // Streaming methods
  // -------------------------------------------------------------------------

  getStream(path: string | URL, options?: RequestOptions): HttpStreamPipeline<string> {
    return new HttpStreamPipeline(
      httpStreamText({
        url: this.resolveUrl(path),
        method: "GET",
        headers: this.mergeHeaders(options?.headers),
        timeoutMs: options?.timeoutMs ?? this.config.timeoutMs,
        transport: this.config.transport,
      }),
    );
  }

  postStream(path: string | URL, options?: RequestBodyOptions): HttpStreamPipeline<string> {
    return new HttpStreamPipeline(
      httpStreamText({
        url: this.resolveUrl(path),
        method: "POST",
        headers: this.mergeHeaders(options?.headers),
        json: options?.json,
        body: options?.body,
        timeoutMs: options?.timeoutMs ?? this.config.timeoutMs,
        transport: this.config.transport,
      }),
    );
  }

  getSSE(path: string | URL, options?: RequestOptions): HttpStreamPipeline<SSEvent> {
    return new HttpStreamPipeline(
      httpStreamSSE({
        url: this.resolveUrl(path),
        method: "GET",
        headers: this.mergeHeaders(options?.headers),
        timeoutMs: options?.timeoutMs ?? this.config.timeoutMs,
        transport: this.config.transport,
      }),
    );
  }

  postSSE(path: string | URL, options?: RequestBodyOptions): HttpStreamPipeline<SSEvent> {
    return new HttpStreamPipeline(
      httpStreamSSE({
        url: this.resolveUrl(path),
        method: "POST",
        headers: this.mergeHeaders(options?.headers),
        json: options?.json,
        body: options?.body,
        timeoutMs: options?.timeoutMs ?? this.config.timeoutMs,
        transport: this.config.transport,
      }),
    );
  }

  getNDJSON<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestOptions,
  ): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(
      httpStreamNDJSON({
        url: this.resolveUrl(path),
        method: "GET",
        headers: this.mergeHeaders(options?.headers),
        timeoutMs: options?.timeoutMs ?? this.config.timeoutMs,
        schema,
        transport: this.config.transport,
      }),
    );
  }

  postNDJSON<T>(
    path: string | URL,
    schema: ResponseParser<T>,
    options?: RequestBodyOptions,
  ): HttpStreamPipeline<T> {
    return new HttpStreamPipeline(
      httpStreamNDJSON({
        url: this.resolveUrl(path),
        method: "POST",
        headers: this.mergeHeaders(options?.headers),
        json: options?.json,
        body: options?.body,
        timeoutMs: options?.timeoutMs ?? this.config.timeoutMs,
        schema,
        transport: this.config.transport,
      }),
    );
  }
}
