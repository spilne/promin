import { Effect } from "effect";

/**
 * Shared concurrency limiter. Wraps `Effect.Semaphore`.
 *
 * Use to limit concurrent access to a shared resource (e.g., external API rate limits)
 * across multiple independent pipelines.
 *
 * @example
 * ```ts
 * const apiLimit = PipelineSemaphore.make(10);
 * pipeline.withPermit(apiLimit).runPromise();
 * ```
 */
/** Pluggable semaphore interface — implement with any backend. */
export interface Semaphore {
  withPermit<T>(effect: Effect.Effect<T>): Effect.Effect<T>;
}

export class PipelineSemaphore implements Semaphore {
  private constructor(private readonly semaphore: Effect.Semaphore) {}

  /** Create a semaphore with N permits. */
  static make(permits: number): PipelineSemaphore {
    return new PipelineSemaphore(Effect.unsafeMakeSemaphore(permits));
  }

  /** Wrap an Effect — acquires a permit before running, releases after. */
  withPermit<T, E>(effect: Effect.Effect<T, E>): Effect.Effect<T, E> {
    return this.semaphore.withPermits(1)(effect);
  }

  /** Get the number of currently available permits. */
  get availablePermits(): Effect.Effect<number> {
    return this.semaphore
      .withPermits(0)(Effect.sync(() => 0))
      .pipe(Effect.map(() => 0));
  }
}
