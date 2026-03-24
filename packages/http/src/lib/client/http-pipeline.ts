import { Effect } from "effect";
import { Pipeline, type PipelineDefaults } from "@promin/core";
import type { HttpClientError, ResponseParser } from "./http-client-error.ts";
import { HttpNetworkError, HttpTimeoutError, HttpStatusError } from "./http-client-error.ts";
import {
  httpRequest,
  httpRequestText,
  type HttpTransport,
  type HttpProxyConfig,
} from "./http-client.ts";
import {
  httpStreamSSE,
  httpStreamNDJSON,
  httpStreamText,
  HttpStreamPipeline,
  type SSEvent,
} from "./http-stream.ts";

// ---------------------------------------------------------------------------
// HttpResponse — typed response with metadata
// ---------------------------------------------------------------------------

/**
 * HTTP response with typed body.
 *
 * The body type `T` depends on the decoder: `ReadableStream<Uint8Array>` for
 * binary, `string` for text, parsed objects for JSON, etc.
 */
export interface HttpResponse<T> {
  readonly status: number;
  readonly contentType: string | null;
  readonly contentLength: number | null;
  readonly body: T;
}

// ---------------------------------------------------------------------------
// ResponseDecoder — pluggable body decoding
// ---------------------------------------------------------------------------

/**
 * Decodes a raw fetch Response body into type `T`.
 *
 * Built-in decoders: `binaryDecoder`, `textDecoder`, `jsonDecoder`.
 * Create custom decoders for protobuf, msgpack, etc.
 */
export type ResponseDecoder<T> = (response: Response) => Promise<T>;

/** Returns the raw body stream — no buffering, no copying. */
export const binaryDecoder: ResponseDecoder<ReadableStream<Uint8Array>> = async (response) =>
  response.body!;

/** Reads the full body as a UTF-8 string. */
export const textDecoder: ResponseDecoder<string> = (response) => response.text();

/** Reads the full body as parsed JSON (unknown). */
export const jsonDecoder: ResponseDecoder<unknown> = (response) => response.json();

/** Returns the full body as an ArrayBuffer — for binary files. */
export const arrayBufferDecoder: ResponseDecoder<ArrayBuffer> = (response) =>
  response.arrayBuffer();

/** Returns the full body as a Blob. */
export const blobDecoder: ResponseDecoder<Blob> = (response) => response.blob();

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
  /**
   * Default proxy configuration for all requests.
   * Per-request proxy can override this via request options.
   *
   * @example
   * ```ts
   * const client = new DefaultHttpClient({
   *   baseUrl: "https://api.example.com",
   *   proxy: { url: "http://proxy.internal:8080" },
   * });
   *
   * // TLS-intercepting proxy with custom CA:
   * const client = new DefaultHttpClient({
   *   proxy: {
   *     url: "http://proxy.corp:3128",
   *     ca: fs.readFileSync("corp-ca.pem", "utf-8"),
   *   },
   * });
   * ```
   */
  readonly proxy?: HttpProxyConfig;
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
  /**
   * Fetch a response with a typed body via decoder. Defaults to binary stream.
   *
   * @example
   * ```ts
   * // Download a file as binary stream (default)
   * const { body, contentLength } = await client.getResponse("/file.zip").runPromise();
   *
   * // Download as ArrayBuffer
   * const { body } = await client
   *   .getResponse("/file.zip", { decoder: arrayBufferDecoder })
   *   .runPromise();
   *
   * // Download as text
   * const { body } = await client
   *   .getResponse("/page.html", { decoder: textDecoder })
   *   .runPromise();
   * ```
   */
  getResponse<T = ReadableStream<Uint8Array>>(
    path: string | URL,
    options?: RequestOptions & { decoder?: ResponseDecoder<T> },
  ): Pipeline<HttpResponse<T>, HttpClientError>;
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
  abstract getResponse<T = ReadableStream<Uint8Array>>(
    path: string | URL,
    options?: RequestOptions & { decoder?: ResponseDecoder<T> },
  ): Pipeline<HttpResponse<T>, HttpClientError>;
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
      proxy: overrides.proxy ?? this.config.proxy,
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
        proxy: this.config.proxy,
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
        proxy: this.config.proxy,
      }),
      { method: "GET", url, tag: options?.tag },
    );
  }

  getResponse<T = ReadableStream<Uint8Array>>(
    path: string | URL,
    options?: RequestOptions & { decoder?: ResponseDecoder<T> },
  ): Pipeline<HttpResponse<T>, HttpClientError> {
    const url = this.resolveUrl(path);
    const timeoutMs = options?.timeoutMs ?? this.config.timeoutMs ?? 30_000;
    const decoder = (options?.decoder ?? binaryDecoder) as ResponseDecoder<T>;

    const fetchOptions: RequestInit & { proxy?: string; tls?: { ca?: string } } = {
      method: "GET",
      headers: this.mergeHeaders(options?.headers),
      signal: AbortSignal.timeout(timeoutMs),
    };
    if (this.config.proxy) {
      fetchOptions.proxy = this.config.proxy.url;
      if (this.config.proxy.ca) fetchOptions.tls = { ca: this.config.proxy.ca };
    }

    const effect: Effect.Effect<HttpResponse<T>, HttpClientError> = Effect.tryPromise({
      try: () => fetch(url, fetchOptions),
      catch: (error): HttpClientError => {
        if (error instanceof DOMException && error.name === "TimeoutError") {
          return new HttpTimeoutError({
            url,
            timeoutMs,
            message: `Request to ${url} timed out after ${timeoutMs}ms`,
          });
        }
        return new HttpNetworkError({
          url,
          cause: error,
          message: `Fetch to ${url} failed: ${error instanceof Error ? error.message : String(error)}`,
        });
      },
    }).pipe(
      Effect.flatMap((response) => {
        if (response.status >= 200 && response.status < 300) {
          return Effect.tryPromise({
            try: async (): Promise<HttpResponse<T>> => ({
              status: response.status,
              body: await decoder(response),
              contentType: response.headers.get("content-type"),
              contentLength: response.headers.has("content-length")
                ? Number(response.headers.get("content-length"))
                : null,
            }),
            catch: (cause): HttpClientError =>
              new HttpNetworkError({
                url,
                cause,
                message: `Failed to decode response from ${url}: ${cause instanceof Error ? cause.message : String(cause)}`,
              }),
          });
        }
        return Effect.tryPromise({
          try: () => response.text().catch(() => ""),
          catch: (): HttpClientError =>
            new HttpStatusError({
              url,
              status: response.status,
              body: "",
              message: `GET ${url} returned ${response.status}`,
            }),
        }).pipe(
          Effect.flatMap((body) =>
            Effect.fail<HttpClientError>(
              new HttpStatusError({
                url,
                status: response.status,
                body,
                message: `GET ${url} returned ${response.status}`,
              }),
            ),
          ),
        );
      }),
    ) as Effect.Effect<HttpResponse<T>, HttpClientError>;

    return this.pipeline(effect, { method: "GET", url, tag: options?.tag });
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
        proxy: this.config.proxy,
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
        proxy: this.config.proxy,
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
        proxy: this.config.proxy,
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
        proxy: this.config.proxy,
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
        proxy: this.config.proxy,
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
        proxy: this.config.proxy,
      }),
    );
  }
}
