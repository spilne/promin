import { Effect, Duration, Scope, Stream, type Layer } from "effect";
import * as PlatformHttpClient from "@effect/platform/HttpClient";
import * as HttpClientRequest from "@effect/platform/HttpClientRequest";
import type * as HttpClientResponse from "@effect/platform/HttpClientResponse";
import * as HttpClientError from "@effect/platform/HttpClientError";
import * as HttpBody from "@effect/platform/HttpBody";
import * as FetchHttpClient from "@effect/platform/FetchHttpClient";
import type { HttpTransport, HttpRequestOptions } from "./http-client.ts";
import {
  HttpNetworkError,
  HttpStatusError,
  HttpTimeoutError,
  type HttpClientError as OurHttpClientError,
} from "./http-client-error.ts";

const DEFAULT_TIMEOUT_MS = 30_000;

/**
 * HTTP transport backed by `@effect/platform`'s `HttpClient`.
 *
 * **Why use this over `FetchTransport`:**
 *
 * - **OpenTelemetry tracing** — every outbound request automatically gets an OTel
 *   span (`http.client GET`, etc.) with method, URL, status, and duration attributes.
 *   No manual instrumentation needed.
 *
 * - **Layer-based testability** — swap the layer at construction time to inject a
 *   mock `HttpClient` without spinning up a real server.
 *
 * - **Composable transport middleware** — `@effect/platform`'s `HttpClient` supports
 *   `.pipe(HttpClient.mapRequest(...))` and friends at the transport level.
 *
 * **Connection pooling:** When using `FetchHttpClient.layer` (the default), pooling
 * is identical to `FetchTransport` — Bun's implicit per-host keep-alive pool with no
 * user-facing knobs. On Node with `NodeHttpClient.layerUndici`, you'd get explicit
 * pool control (max connections, idle timeout, pipelining).
 *
 * @example
 * ```ts
 * import { EffectPlatformTransport } from "@ts-backend/http/client";
 *
 * const api = new DefaultHttpClient({
 *   baseUrl: "https://api.example.com",
 *   transport: new EffectPlatformTransport(),
 * });
 * ```
 *
 * @example Custom layer for testing:
 * ```ts
 * const testLayer = Layer.succeed(PlatformHttpClient.HttpClient, myMockClient);
 * const api = new DefaultHttpClient({
 *   transport: new EffectPlatformTransport({ layer: testLayer }),
 * });
 * ```
 */
export class EffectPlatformTransport implements HttpTransport {
  private readonly layer: Layer.Layer<PlatformHttpClient.HttpClient>;

  constructor(options?: { layer?: Layer.Layer<PlatformHttpClient.HttpClient> }) {
    this.layer = options?.layer ?? FetchHttpClient.layer;
  }

  execute(options: HttpRequestOptions): Effect.Effect<Response, OurHttpClientError, Scope.Scope> {
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

    // Build platform request
    const request = this.buildRequest({ urlStr, method, headers, json, body });

    // Execute through platform HttpClient, reconstruct a web-standard Response
    // from the platform response's public API (status, headers, stream).
    // This avoids depending on internal implementation details of @effect/platform.
    const execute = PlatformHttpClient.execute(request).pipe(
      Effect.flatMap((response) => this.toWebResponse(response)),
      // Map platform errors → our error types
      Effect.catchAll((error: HttpClientError.HttpClientError) =>
        Effect.fail(this.mapError(error, urlStr, timeoutMs)),
      ),
      // Apply timeout
      Effect.timeoutFail({
        duration: Duration.millis(timeoutMs),
        onTimeout: () =>
          new HttpTimeoutError({
            url: urlStr,
            timeoutMs,
            message: `Request to ${urlStr} timed out after ${timeoutMs}ms`,
          }),
      }),
      // Provide the platform layer so HttpClient requirement is eliminated
      Effect.provide(this.layer),
    );

    // Handle external abort signal if provided
    if (signal) {
      return this.withAbortSignal(execute, signal, urlStr);
    }

    return execute;
  }

  private buildRequest(params: {
    urlStr: string;
    method: string;
    headers: Record<string, string>;
    json?: unknown;
    body?: string | ArrayBuffer | ReadableStream | Blob | FormData;
  }): HttpClientRequest.HttpClientRequest {
    const httpMethod = params.method.toUpperCase() as HttpClientRequest.HttpClientRequest["method"];
    let request = HttpClientRequest.make(httpMethod)(params.urlStr);

    // Set headers
    if (Object.keys(params.headers).length > 0) {
      request = HttpClientRequest.setHeaders(request, params.headers);
    }

    // Set body
    if (params.json !== undefined) {
      request = HttpClientRequest.bodyUnsafeJson(request, params.json);
    } else if (params.body instanceof FormData) {
      request = HttpClientRequest.bodyFormData(request, params.body);
    } else if (params.body !== undefined) {
      request = HttpClientRequest.setBody(request, HttpBody.raw(params.body));
    }

    return request;
  }

  /**
   * Reconstruct a web-standard `Response` from the platform response's public API.
   *
   * Uses `.status`, `.headers`, and `.stream` — all part of @effect/platform's
   * public `HttpIncomingMessage` contract. This avoids reaching into internal
   * implementation details (like `.source` or `.original`) that may change
   * between versions.
   */
  private toWebResponse(
    response: HttpClientResponse.HttpClientResponse,
  ): Effect.Effect<Response, never> {
    return Effect.sync(() => {
      const headers = new Headers();
      for (const [key, value] of Object.entries(response.headers)) {
        if (value !== undefined) {
          if (Array.isArray(value)) {
            for (const v of value) headers.append(key, v);
          } else {
            headers.set(key, value);
          }
        }
      }

      const body = Stream.toReadableStream(response.stream);

      return new globalThis.Response(body, {
        status: response.status,
        headers,
      });
    });
  }

  private mapError(
    error: HttpClientError.HttpClientError,
    url: string,
    timeoutMs: number,
  ): OurHttpClientError {
    if (error instanceof HttpClientError.RequestError) {
      // Check if the underlying cause is a timeout
      if (
        error.cause instanceof DOMException &&
        (error.cause as DOMException).name === "TimeoutError"
      ) {
        return new HttpTimeoutError({
          url,
          timeoutMs,
          message: `Request to ${url} timed out after ${timeoutMs}ms`,
        });
      }
      return new HttpNetworkError({
        url,
        cause: error.cause,
        message: `Request to ${url} failed: ${error.message}`,
      });
    }

    // ResponseError means a response was received but something went wrong
    // reading the body. Map to HttpStatusError to avoid confusing retry logic
    // (HttpNetworkError is retried unconditionally by HTTP_RETRYABLE).
    if (error instanceof HttpClientError.ResponseError) {
      return new HttpStatusError({
        url,
        status: error.response.status,
        body: error.message,
        message: `Response from ${url} failed: ${error.message}`,
      });
    }

    return new HttpNetworkError({
      url,
      cause: error,
      message: `Request to ${url} failed: ${String(error)}`,
    });
  }

  /**
   * Race the main effect against an abort signal. When the signal fires,
   * the main effect's fiber is interrupted (which propagates to the platform's
   * internal AbortController).
   */
  private withAbortSignal(
    effect: Effect.Effect<Response, OurHttpClientError>,
    signal: AbortSignal,
    url: string,
  ): Effect.Effect<Response, OurHttpClientError, Scope.Scope> {
    const waitForAbort = Effect.async<never, OurHttpClientError>((resume) => {
      if (signal.aborted) {
        resume(
          Effect.fail(
            new HttpNetworkError({
              url,
              cause: signal.reason,
              message: `Request to ${url} was aborted`,
            }),
          ),
        );
        return;
      }
      const onAbort = () =>
        resume(
          Effect.fail(
            new HttpNetworkError({
              url,
              cause: signal.reason,
              message: `Request to ${url} was aborted`,
            }),
          ),
        );
      signal.addEventListener("abort", onAbort, { once: true });
      return Effect.sync(() => signal.removeEventListener("abort", onAbort));
    });

    return Effect.race(effect, waitForAbort);
  }
}
