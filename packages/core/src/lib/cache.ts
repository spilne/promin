import { Effect } from "effect";

// ---------------------------------------------------------------------------
// PipelineCache — single-value cache with TTL
// ---------------------------------------------------------------------------

interface CacheEntry<T> {
  value: T;
  expiresAt: number;
}

/**
 * Single-value cache with TTL. First call executes, subsequent calls return
 * the cached value until it expires.
 *
 * @example
 * ```ts
 * const tokenCache = new PipelineCache<Token>(55 * 60 * 1000);
 *
 * const getToken = pipeline.cached(tokenCache);
 * await getToken.runPromise(); // hits API
 * await getToken.runPromise(); // returns cached
 *
 * tokenCache.invalidate(); // force refresh
 * ```
 */
export class PipelineCache<T> {
  private entry?: CacheEntry<T>;

  constructor(private readonly ttlMs: number) {}

  /** Wrap an Effect — returns cached value if fresh, otherwise re-executes. */
  wrap<E>(effect: Effect.Effect<T, E>): Effect.Effect<T, E> {
    return Effect.suspend(() => {
      if (this.entry && Date.now() < this.entry.expiresAt) {
        return Effect.succeed(this.entry.value);
      }
      return effect.pipe(
        Effect.tap((value) =>
          Effect.sync(() => {
            this.entry = { value, expiresAt: Date.now() + this.ttlMs };
          }),
        ),
      );
    });
  }

  /** Get the current cached value, or `undefined` if not fresh. Useful for test assertions. */
  get current(): T | undefined {
    return this.entry && Date.now() < this.entry.expiresAt ? this.entry.value : undefined;
  }

  /** Invalidate the cache — next call will re-execute. */
  invalidate(): void {
    this.entry = undefined;
  }

  /** Check if the cache has a fresh value. */
  get isFresh(): boolean {
    return this.entry != null && Date.now() < this.entry.expiresAt;
  }
}
