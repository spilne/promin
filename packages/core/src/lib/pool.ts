import { Effect } from "effect";

/**
 * Reusable pool of N resources with automatic acquire/release.
 *
 * @example
 * ```ts
 * const dbPool = PipelinePool.make({
 *   acquire: () => createConnection(),
 *   release: (conn) => conn.close(),
 *   size: 10,
 * });
 *
 * // Acquire a resource, use it, auto-release
 * const result = await dbPool.useAsync((conn) => conn.query("SELECT 1"));
 * ```
 */
export class PipelinePool<R> {
  private constructor(
    private readonly acquire: Effect.Effect<R>,
    private readonly release: (resource: R) => Effect.Effect<void>,
    readonly size: number,
  ) {}

  /** Create a pool of N resources. */
  static make<R>(params: {
    acquire: () => R | Promise<R>;
    release: (resource: R) => void | Promise<void>;
    size: number;
  }): PipelinePool<R> {
    const acquire = Effect.suspend(() => {
      const result = params.acquire();
      return result instanceof Promise ? Effect.promise(() => result) : Effect.succeed(result as R);
    });
    const release = (resource: R) => {
      const result = params.release(resource);
      return result instanceof Promise
        ? Effect.promise(() => result).pipe(Effect.asVoid)
        : Effect.sync(() => {});
    };
    return new PipelinePool(acquire, release, params.size);
  }

  /**
   * Acquire a resource from the pool, use it, and auto-release.
   * The resource is returned to the pool after the effect completes (success, error, or interrupt).
   */
  use<T>(fn: (resource: R) => Effect.Effect<T>): Effect.Effect<T> {
    return Effect.acquireUseRelease(
      this.acquire,
      (resource) => fn(resource),
      (resource) => this.release(resource),
    );
  }

  /**
   * Acquire a resource, run an async function, auto-release. Returns a Promise.
   *
   * @example
   * ```ts
   * const result = await dbPool.useAsync((conn) => conn.query("SELECT 1"));
   * ```
   */
  async useAsync<T>(fn: (resource: R) => Promise<T>): Promise<T> {
    return Effect.runPromise(this.use((resource) => Effect.promise(() => fn(resource))));
  }
}
