import { Effect, Deferred, Ref } from "effect";
import { Pipeline, type TaggedError } from "./pipeline.ts";

/** Pluggable singleflight interface — test against this, implement with any backend. */
export interface Singleflight {
  doAsync<T>(key: string, fn: () => Promise<T>): Promise<T>;
}

/**
 * Request deduplication primitive. When multiple callers request the same key
 * concurrently, only one executes — the rest join and receive the same result.
 * No caching: the key is cleared once the call settles.
 *
 * @example
 * ```ts
 * const sf = PipelineSingleflight.make();
 * const [a, b] = await Promise.all([
 *   sf.doAsync("user:1", () => fetchUser(1)),
 *   sf.doAsync("user:1", () => fetchUser(1)),
 * ]);
 * // fetchUser called only once, both get same result
 * ```
 */
export class PipelineSingleflight implements Singleflight {
  private constructor(
    private readonly flights: Ref.Ref<Map<string, Deferred.Deferred<unknown, unknown>>>,
    private readonly asyncFlights: Map<string, Promise<unknown>>,
  ) {}

  /** Create a new singleflight instance. */
  static make(): PipelineSingleflight {
    return new PipelineSingleflight(
      Effect.runSync(Ref.make(new Map<string, Deferred.Deferred<unknown, unknown>>())),
      new Map(),
    );
  }

  // -------------------------------------------------------------------------
  // Effect-returning (for Effect composition)
  // -------------------------------------------------------------------------

  /** Deduplicate an Effect by key. Concurrent calls with the same key share one execution. */
  doEffect<T, E>(key: string, effect: Effect.Effect<T, E>): Effect.Effect<T, E> {
    const flights = this.flights;

    return Effect.flatten(
      Ref.modify(flights, (map) => {
        const existing = map.get(key);
        if (existing) {
          return [Deferred.await(existing) as Effect.Effect<T, E>, map];
        }

        const deferred = Effect.runSync(Deferred.make<unknown, unknown>());
        const updated = new Map(map);
        updated.set(key, deferred);

        const cleanup = Ref.update(flights, (m) => {
          const n = new Map(m);
          n.delete(key);
          return n;
        });

        const run = Effect.matchCauseEffect(effect, {
          onFailure: (cause) =>
            Deferred.failCause(deferred, cause as any).pipe(
              Effect.andThen(cleanup),
              Effect.andThen(Effect.failCause(cause)),
            ),
          onSuccess: (value) =>
            Deferred.succeed(deferred, value).pipe(
              Effect.andThen(cleanup),
              Effect.andThen(Effect.succeed(value)),
            ),
        });

        return [run, updated];
      }),
    );
  }

  // -------------------------------------------------------------------------
  // Pipeline-returning (for Pipeline composition)
  // -------------------------------------------------------------------------

  /** Deduplicate a Pipeline by key. Concurrent calls with the same key share one execution. */
  do<T, E extends TaggedError>(key: string, pipeline: Pipeline<T, E>): Pipeline<T, E> {
    return Pipeline.from(this.doEffect(key, pipeline.effect));
  }

  // -------------------------------------------------------------------------
  // Promise-returning (no Effect import needed)
  // -------------------------------------------------------------------------

  /** Deduplicate a Promise by key. Concurrent calls with the same key share one execution. */
  async doAsync<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const existing = this.asyncFlights.get(key);
    if (existing) return existing as Promise<T>;

    const p = fn().finally(() => this.asyncFlights.delete(key));
    this.asyncFlights.set(key, p);
    return p;
  }
}
