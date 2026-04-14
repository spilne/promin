import { Data } from "effect";
import type { SchemaParser } from "@promin/core";

/** Network-level failure (DNS, connection refused, socket hang up). */
export class HttpNetworkError extends Data.TaggedError("HttpNetworkError")<{
  readonly url: string;
  readonly cause: unknown;
  readonly message: string;
}> {}

/** Request exceeded its timeout budget. */
export class HttpTimeoutError extends Data.TaggedError("HttpTimeoutError")<{
  readonly url: string;
  readonly timeoutMs: number;
  readonly message: string;
}> {}

/** Server returned a non-OK status code. */
export class HttpStatusError extends Data.TaggedError("HttpStatusError")<{
  readonly url: string;
  readonly status: number;
  readonly body: string;
  readonly message: string;
}> {
  get isRetryable(): boolean {
    return this.status >= 500 || this.status === 429;
  }

  get isClientError(): boolean {
    return this.status >= 400 && this.status < 500;
  }

  get isServerError(): boolean {
    return this.status >= 500;
  }
}

/** Response body could not be parsed or didn't match the expected schema. */
export class HttpParseError extends Data.TaggedError("HttpParseError")<{
  readonly url: string;
  readonly cause: unknown;
  readonly message: string;
}> {}

/** Polling exhausted all attempts or the time budget. */
export class PollTimeoutError extends Data.TaggedError("PollTimeoutError")<{
  readonly attempts: number;
  readonly lastResult: unknown;
  readonly message: string;
}> {}

/**
 * Generic response parser interface. Any object with a `safeParse` method works.
 *
 * Zod schemas satisfy this out of the box — no wrapper needed:
 * ```ts
 * const UserSchema = z.object({ id: z.number(), name: z.string() });
 * api.get("/users/1", UserSchema) // UserSchema IS a ResponseParser<User>
 * ```
 *
 * For other validation libraries (Valibot, ArkType, @effect/schema, plain functions):
 * ```ts
 * const parser: ResponseParser<User> = {
 *   safeParse: (data) => {
 *     const result = myValidate(data);
 *     return result.ok
 *       ? { success: true, data: result.value }
 *       : { success: false, error: result.issues };
 *   },
 * };
 * ```
 */
export type ResponseParser<T> = SchemaParser<T>;

/** Union of all HTTP client errors — use `_tag` to discriminate. */
export type HttpClientError =
  | HttpNetworkError
  | HttpTimeoutError
  | HttpStatusError
  | HttpParseError
  | PollTimeoutError;
