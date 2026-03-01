import { Effect, SubscriptionRef } from "effect";
import { StreamPipeline } from "./stream-pipeline.ts";

/**
 * Shared mutable value with change notifications.
 * Wraps Effect's `SubscriptionRef`.
 *
 * @example
 * ```ts
 * const config = PipelineSignal.make(defaultConfig);
 * await config.setAsync(newConfig);
 * const current = await config.getAsync();
 * ```
 */
export class PipelineSignal<T> {
  private constructor(readonly ref: SubscriptionRef.SubscriptionRef<T>) {}

  /** Create a signal with an initial value. */
  static make<T>(initial: T): PipelineSignal<T> {
    return new PipelineSignal(Effect.runSync(SubscriptionRef.make(initial)));
  }

  // -------------------------------------------------------------------------
  // Effect-returning (for Pipeline/Effect composition)
  // -------------------------------------------------------------------------

  /** Get the current value (Effect). */
  get get(): Effect.Effect<T> {
    return SubscriptionRef.get(this.ref);
  }

  /** Set a new value — notifies all subscribers (Effect). */
  set(value: T): Effect.Effect<void> {
    return SubscriptionRef.set(this.ref, value);
  }

  /** Update the value with a function — notifies all subscribers (Effect). */
  update(fn: (current: T) => T): Effect.Effect<void> {
    return SubscriptionRef.update(this.ref, fn);
  }

  // -------------------------------------------------------------------------
  // Promise-returning (no Effect import needed)
  // -------------------------------------------------------------------------

  /** Get the current value. */
  getAsync(): Promise<T> {
    return Effect.runPromise(this.get);
  }

  /** Set a new value — notifies all subscribers. */
  setAsync(value: T): Promise<void> {
    return Effect.runPromise(this.set(value));
  }

  /** Update the value with a function — notifies all subscribers. */
  updateAsync(fn: (current: T) => T): Promise<void> {
    return Effect.runPromise(this.update(fn));
  }

  // -------------------------------------------------------------------------
  // Stream
  // -------------------------------------------------------------------------

  /** Get a stream of value changes (including the current value as the first emission). */
  changes(): StreamPipeline<T, never> {
    return StreamPipeline.from(this.ref.changes);
  }
}
