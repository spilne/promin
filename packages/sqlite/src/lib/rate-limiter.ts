import { TaggedError } from "@spilne/perfect-core";

/**
 * Raised by `acquireAsync` / `withLimitAsync` when the limit is exhausted.
 * `message` is left empty on purpose; callers branch on `_tag` and read
 * `retryAfterMs`.
 */
export class RateLimitExceeded extends TaggedError("RateLimitExceeded")<{
  readonly retryAfterMs: number;
  readonly message: string;
}>() {
  constructor(params: { readonly retryAfterMs: number }) {
    super({ retryAfterMs: params.retryAfterMs, message: "" });
  }
}

/** Pluggable rate limiter interface — test against this, implement with any backend. */
export interface RateLimiter {
  acquireAsync(resource?: string): Promise<void>;
  tryAcquireAsync(resource?: string): Promise<boolean>;
  withLimitAsync<T>(fn: () => Promise<T>, resource?: string): Promise<T>;
  remainingAsync(resource?: string): Promise<number>;
}
