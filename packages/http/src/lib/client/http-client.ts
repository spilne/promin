import { Effect, Duration, Schedule, Scope } from "effect";
import {
  HttpNetworkError,
  HttpTimeoutError,
  HttpStatusError,
  HttpParseError,
  type HttpClientError,
  type ResponseParser,
} from "./http-client-error.ts";

const DEFAULT_TIMEOUT_MS = 30_000;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface HttpRequestOptions {
  /** Full URL (string or URL object). */
  readonly url: string | URL;
  /** HTTP method. Defaults to GET. */
  readonly method?: string;
  /** Request headers. */
  readonly headers?: Record<string, string>;
  /** JSON-serializable request body. Automatically sets Content-Type. */
  readonly json?: unknown;
  /** Raw body (when you need FormData, streams, etc.). Mutually exclusive with `json`. */
  readonly body?: string | ArrayBuffer | ReadableStream | Blob | FormData;
  /** Per-request timeout in ms. Defaults to 30 000. */
  readonly timeoutMs?: number;
  /** Extra abort signal to combine with the timeout signal. */
  readonly signal?: AbortSignal;
}

export interface RetryPolicy {
  /** Max number of retries (excluding the initial attempt). Defaults to 3. */
  readonly maxRetries?: number;
  /** Base delay for exponential backoff. Defaults to 250ms. */
  readonly baseDelayMs?: number;
  /** Only retry when this predicate returns true. Defaults to retryable status codes (5xx, 429). */
  readonly when?: (error: HttpClientError) => boolean;
}

// ---------------------------------------------------------------------------
// HttpTransport — pluggable transport layer
// ---------------------------------------------------------------------------

/**
 * Pluggable transport layer for HTTP requests.
 *
 * **Why this exists:**
 *
 * Everything above the transport — URL resolution, header merging, Zod validation,
 * retry, streaming, pipelines — is generic orchestration that doesn't care *how*
 * bytes get sent. By isolating the "send bytes, get Response" concern behind an
 * interface we gain:
 *
 * 1. **Swappable runtimes** — The default `FetchTransport` uses the global `fetch`
 *    (Bun's built-in). A future `@effect/platform` transport can slot in to get
 *    OpenTelemetry tracing, layer-based test mocking, and (on Node) undici's
 *    tunable connection pool — without touching any consumer code.
 *
 * 2. **Transport-level testing** — You can inject a custom transport that returns
 *    canned `Response` objects to test the full pipeline (status checks, JSON
 *    parsing, Zod validation) without a running server.
 *
 * 3. **Separation of concerns** — Abort-controller management, signal combining,
 *    and timeout handling live inside the transport. Higher layers only deal with
 *    "I have a Response, now what?"
 *
 * **Connection pooling note:** Bun's `fetch` maintains an implicit per-host
 * keep-alive pool with no user-facing knobs. A `@effect/platform` transport
 * backed by `NodeHttpClient.layerUndici` would expose explicit pool settings
 * (max connections, idle timeout, pipelining), but that only applies to a Node
 * runtime — on Bun the pool behaviour stays the same regardless of transport.
 */
export interface HttpTransport {
  /**
   * Execute an HTTP request and return the raw `Response`.
   *
   * The returned Effect requires a `Scope` so that transports can manage
   * resources (e.g. an `AbortController`) that are released when the scope
   * closes — whether normally, on error, or on fiber interruption.
   */
  execute(options: HttpRequestOptions): Effect.Effect<Response, HttpClientError, Scope.Scope>;
}

// ---------------------------------------------------------------------------
// FetchTransport — default transport using global fetch
// ---------------------------------------------------------------------------

/**
 * Default transport: delegates to the global `fetch`.
 *
 * An `AbortController` is acquired as a scoped resource. When the Effect fiber
 * is interrupted (via timeout, race, or manual `Fiber.interrupt`), the controller
 * aborts the fetch — killing the TCP connection immediately, whether the server
 * has responded or not. No leaked sockets, no waiting for `timeoutMs`.
 */
export class FetchTransport implements HttpTransport {
  execute(options: HttpRequestOptions): Effect.Effect<Response, HttpClientError, Scope.Scope> {
    const {
      url,
      method = "GET",
      headers = {},
      json,
      body,
      timeoutMs = DEFAULT_TIMEOUT_MS,
      signal,
    } = options;

    const urlStr = typeof url === "string" ? url : url.toString();

    const finalHeaders: Record<string, string> = { ...headers };
    let finalBody: string | ArrayBuffer | ReadableStream | Blob | FormData | undefined = body;
    if (json !== undefined) {
      finalHeaders["Content-Type"] ??= "application/json";
      finalBody = JSON.stringify(json);
    }

    // Acquire an AbortController as a scoped resource.
    // On release (fiber interrupt, scope close), it aborts the fetch immediately.
    const acquireController = Effect.sync(() => new AbortController());
    const releaseController = (controller: AbortController) =>
      Effect.sync(() => controller.abort());

    return Effect.flatMap(
      Effect.acquireRelease(acquireController, releaseController),
      (controller) => {
        // Combine: our controller signal + timeout signal + optional external signal
        const signals: AbortSignal[] = [controller.signal, AbortSignal.timeout(timeoutMs)];
        if (signal) signals.push(signal);

        return Effect.tryPromise({
          try: () =>
            fetch(urlStr, {
              method,
              headers: finalHeaders,
              body: finalBody,
              signal: AbortSignal.any(signals),
            }),
          catch: (error) => {
            // If our controller aborted (fiber interrupted), don't surface as timeout
            if (controller.signal.aborted) {
              return new HttpNetworkError({
                url: urlStr,
                cause: error,
                message: `Request to ${urlStr} was aborted`,
              });
            }
            if (error instanceof DOMException && error.name === "TimeoutError") {
              return new HttpTimeoutError({
                url: urlStr,
                timeoutMs,
                message: `Request to ${urlStr} timed out after ${timeoutMs}ms`,
              });
            }
            return new HttpNetworkError({
              url: urlStr,
              cause: error,
              message: `Fetch to ${urlStr} failed: ${error instanceof Error ? error.message : String(error)}`,
            });
          },
        });
      },
    );
  }
}

/** Shared default transport instance — used when no custom transport is provided. */
const DEFAULT_TRANSPORT = new FetchTransport();

// ---------------------------------------------------------------------------
// Core: raw fetch → Effect<Response, HttpClientError>
// ---------------------------------------------------------------------------

/**
 * Low-level: execute a request and get back the raw Response in an Effect.
 * Delegates to the provided transport (defaults to `FetchTransport`).
 */
export function httpFetch(
  options: HttpRequestOptions & { readonly transport?: HttpTransport },
): Effect.Effect<Response, HttpClientError, Scope.Scope> {
  const { transport = DEFAULT_TRANSPORT, ...rest } = options;
  return transport.execute(rest);
}

// ---------------------------------------------------------------------------
// Mid-level: fetch + status check → Effect<Response, HttpClientError>
// ---------------------------------------------------------------------------

/**
 * Like `httpFetch`, but automatically rejects non-OK responses as `HttpStatusError`.
 * Pass `acceptStatus` to override what counts as "OK" (default: 200-299).
 */
export function httpFetchOk(
  options: HttpRequestOptions & {
    readonly acceptStatus?: (status: number) => boolean;
    readonly transport?: HttpTransport;
  },
): Effect.Effect<Response, HttpClientError, Scope.Scope> {
  const { acceptStatus = (s) => s >= 200 && s < 300, transport, ...rest } = options;

  return httpFetch({ ...rest, transport }).pipe(
    Effect.flatMap((response) => {
      if (acceptStatus(response.status)) {
        return Effect.succeed(response);
      }
      return Effect.flatMap(
        Effect.promise(() => response.text().catch(() => "")),
        (body) =>
          Effect.fail(
            new HttpStatusError({
              url: typeof rest.url === "string" ? rest.url : rest.url.toString(),
              status: response.status,
              body,
              message: `${rest.method ?? "GET"} ${rest.url} returned ${response.status}`,
            }),
          ),
      );
    }),
  );
}

// ---------------------------------------------------------------------------
// High-level: fetch + status check + JSON parse + Zod validation
// ---------------------------------------------------------------------------

/**
 * Full request pipeline: fetch → status check → JSON parse → Zod validation.
 *
 * Returns an `Effect<T, HttpClientError>` that you can freely compose with
 * `Effect.all`, `Effect.race`, `Effect.retry`, `pipe`, etc.
 *
 * @example
 * ```ts
 * const getUser = httpRequest({
 *   url: "https://api.example.com/users/1",
 *   schema: UserSchema,
 * });
 *
 * // Run directly
 * const user = await Effect.runPromise(getUser);
 *
 * // Or compose
 * const [user, posts] = await Effect.runPromise(
 *   Effect.all([getUser, getPosts], { concurrency: "unbounded" }),
 * );
 * ```
 */
export function httpRequest<T>(
  options: HttpRequestOptions & {
    /** Parser to validate the JSON response body. Zod schemas work out of the box. */
    readonly schema: ResponseParser<T>;
    /** Custom status acceptance predicate. */
    readonly acceptStatus?: (status: number) => boolean;
    /** Custom transport layer. Defaults to FetchTransport. */
    readonly transport?: HttpTransport;
  },
): Effect.Effect<T, HttpClientError> {
  const { schema, acceptStatus, transport, ...fetchOpts } = options;
  const urlStr = typeof fetchOpts.url === "string" ? fetchOpts.url : fetchOpts.url.toString();

  return Effect.scoped(
    httpFetchOk({ ...fetchOpts, acceptStatus, transport }).pipe(
      Effect.flatMap((response) =>
        Effect.tryPromise({
          try: () => response.json(),
          catch: (cause) =>
            new HttpParseError({
              url: urlStr,
              cause,
              message: `Failed to parse JSON from ${urlStr}`,
            }),
        }),
      ),
      Effect.flatMap((data) => {
        const result = schema.safeParse(data);
        if (result.success) return Effect.succeed(result.data);
        return Effect.fail(
          new HttpParseError({
            url: urlStr,
            cause: result.error,
            message: `Response from ${urlStr} doesn't match schema: ${String(result.error)}`,
          }),
        );
      }),
    ),
  );
}

/**
 * Like `httpRequest`, but returns the raw JSON (unknown) without schema validation.
 * Useful when the response shape is dynamic or you want to validate manually.
 */
export function httpRequestJson(
  options: HttpRequestOptions & {
    readonly acceptStatus?: (status: number) => boolean;
    readonly transport?: HttpTransport;
  },
): Effect.Effect<unknown, HttpClientError> {
  const { acceptStatus, transport, ...fetchOpts } = options;
  const urlStr = typeof fetchOpts.url === "string" ? fetchOpts.url : fetchOpts.url.toString();

  return Effect.scoped(
    httpFetchOk({ ...fetchOpts, acceptStatus, transport }).pipe(
      Effect.flatMap((response) =>
        Effect.tryPromise({
          try: () => response.json(),
          catch: (cause) =>
            new HttpParseError({
              url: urlStr,
              cause,
              message: `Failed to parse JSON from ${urlStr}`,
            }),
        }),
      ),
    ),
  );
}

/**
 * Like `httpRequest`, but returns the raw text body without parsing.
 */
export function httpRequestText(
  options: HttpRequestOptions & {
    readonly acceptStatus?: (status: number) => boolean;
    readonly transport?: HttpTransport;
  },
): Effect.Effect<string, HttpClientError> {
  const { acceptStatus, transport, ...fetchOpts } = options;
  const urlStr = typeof fetchOpts.url === "string" ? fetchOpts.url : fetchOpts.url.toString();

  return Effect.scoped(
    httpFetchOk({ ...fetchOpts, acceptStatus, transport }).pipe(
      Effect.flatMap((response) =>
        Effect.tryPromise({
          try: () => response.text(),
          catch: (cause) =>
            new HttpParseError({
              url: urlStr,
              cause,
              message: `Failed to read text from ${urlStr}`,
            }),
        }),
      ),
    ),
  );
}

// ---------------------------------------------------------------------------
// PipelineResult — ADT for retry outcome discrimination
// ---------------------------------------------------------------------------

/** Result of a pipeline execution — success, HTTP error, or thrown error. */
export type PipelineResult<T> =
  | PipelineResult.Success<T>
  | PipelineResult.HttpError
  | PipelineResult.Error;

export namespace PipelineResult {
  export interface Success<T> {
    readonly _tag: "success";
    readonly value: T;
  }

  export interface HttpError {
    readonly _tag: "httpError";
    readonly error: HttpClientError;
  }

  export interface Error {
    readonly _tag: "error";
    readonly error: globalThis.Error;
  }

  export const success = <T>(value: T): Success<T> => ({ _tag: "success", value });
  export const httpError = (error: HttpClientError): HttpError => ({ _tag: "httpError", error });
  export const error = (error: globalThis.Error): Error => ({ _tag: "error", error });

  export const isSuccess = <T>(outcome: PipelineResult<T>): outcome is Success<T> =>
    outcome._tag === "success";
  export const isHttpError = <T>(outcome: PipelineResult<T>): outcome is HttpError =>
    outcome._tag === "httpError";
  export const isError = <T>(outcome: PipelineResult<T>): outcome is Error =>
    outcome._tag === "error";
}

// ---------------------------------------------------------------------------
// retryAll — full-spectrum retry
// ---------------------------------------------------------------------------

export interface RetryAllPolicy<T = unknown> {
  /** Max number of retries. Defaults to 3. */
  readonly maxRetries?: number;
  /** Base delay for exponential backoff. Defaults to 250ms. */
  readonly baseDelayMs?: number;
  /**
   * Should we retry this result? Receives the full result — success, HTTP error, or thrown error.
   * Defaults to: retry errors, don't retry success.
   *
   * @example
   * ```ts
   * // Retry everything including "not ready" success
   * .retryAll({
   *   shouldRetry: (r) =>
   *     !PipelineResult.isSuccess(r) || r.value.status !== "completed",
   * })
   *
   * // Only retry thrown errors from mapAsync, not HTTP errors
   * .retryAll({
   *   shouldRetry: PipelineResult.isError,
   * })
   * ```
   */
  readonly shouldRetry?: (result: PipelineResult<T>) => boolean;
}

const DEFAULT_SHOULD_RETRY_ALL = <T>(result: PipelineResult<T>): boolean =>
  !PipelineResult.isSuccess(result);

/**
 * Retry that sees ALL results — HTTP errors, thrown errors from transforms, and success values.
 * The `shouldRetry` predicate decides what triggers a retry.
 *
 * The entire effect (HTTP fetch + transforms) is re-executed on each retry —
 * there's no way to split the pipeline mid-execution.
 */
export function withRetryAll<T>(
  effect: Effect.Effect<T, HttpClientError>,
  policy: RetryAllPolicy<T> = {},
): Effect.Effect<T, HttpClientError> {
  const { maxRetries = 3, baseDelayMs = 250, shouldRetry = DEFAULT_SHOULD_RETRY_ALL } = policy;

  const schedule = Schedule.intersect(
    Schedule.exponential(Duration.millis(baseDelayMs), 2),
    Schedule.recurs(maxRetries),
  );

  // Normalize everything into the error channel as PipelineResult so retry can see it all
  const normalized: Effect.Effect<T, PipelineResult<T>> = effect.pipe(
    // Map HttpClientError to result
    Effect.mapError((e) => PipelineResult.httpError(e)),
    // Absorb thrown errors into error channel.
    // If the defect is already an Error, pass it through unchanged.
    // Otherwise wrap it — String() gives a readable message for primitives (e.g. throw "oops"),
    // and { cause } preserves the original value for objects (e.g. throw { code: "FAIL" }).
    Effect.catchAllDefect((defect) => {
      const err = defect instanceof Error ? defect : new Error(String(defect), { cause: defect });
      return Effect.fail<PipelineResult<T>>(PipelineResult.error(err));
    }),
    // Check success value — if shouldRetry says retry, push it to error channel to trigger retry
    Effect.flatMap((value) => {
      const result = PipelineResult.success(value);
      if (shouldRetry(result)) return Effect.fail<PipelineResult<T>>(result);
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
        case "httpError":
          return Effect.fail(result.error);
        case "error":
          return Effect.die(result.error);
      }
    }),
  );
}

// ---------------------------------------------------------------------------
// retry — HTTP-error-only retry (delegates to retryAll)
// ---------------------------------------------------------------------------

const DEFAULT_RETRYABLE: (error: HttpClientError) => boolean = (error) =>
  error._tag === "HttpTimeoutError" ||
  error._tag === "HttpNetworkError" ||
  (error._tag === "HttpStatusError" && error.isRetryable);

/**
 * Retry on transient HTTP errors only. Delegates to {@link withRetryAll} with
 * `shouldRetry` scoped to `HttpClientError`.
 */
export function withRetry<T>(
  effect: Effect.Effect<T, HttpClientError>,
  policy: RetryPolicy = {},
): Effect.Effect<T, HttpClientError> {
  const { when = DEFAULT_RETRYABLE } = policy;

  return withRetryAll(effect, {
    maxRetries: policy.maxRetries,
    baseDelayMs: policy.baseDelayMs,
    shouldRetry: (result) => PipelineResult.isHttpError(result) && when(result.error),
  });
}
