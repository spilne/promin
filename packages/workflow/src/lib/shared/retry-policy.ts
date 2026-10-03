// ---------------------------------------------------------------------------
// RetryPolicy — typed-error-only retry, as persisted on step definitions
// ---------------------------------------------------------------------------

/**
 * Plain-data retry configuration for a step or activity. It is stored with
 * workflow definitions, so the shape (and its defaults) must stay stable.
 */
export interface RetryPolicy<E> {
  /** Max number of retries. Defaults to 3. */
  readonly maxRetries?: number;
  /** Base delay for exponential backoff. Defaults to 250ms. */
  readonly baseDelayMs?: number;
  /** Filter which errors are retryable. Defaults to all typed errors. */
  readonly when?: (error: E) => boolean;
  /** Add ±25% randomness to delays (prevents thundering herd). */
  readonly jitter?: boolean;
  /** Cap the maximum delay between retries. */
  readonly maxDelayMs?: number;
  /** Total time budget for all retries combined. */
  readonly timeBudgetMs?: number;
}
