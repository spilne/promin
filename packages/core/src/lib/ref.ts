import { Effect, Ref } from "effect";

/**
 * Atomic mutable reference for coordination between concurrent pipelines.
 * Wraps `Effect.Ref`.
 *
 * @example
 * ```ts
 * const counter = PipelineRef.make(0);
 * await counter.updateAsync((n) => n + 1);
 * const value = await counter.getAsync(); // 1
 * ```
 */
/** Pluggable ref interface — implement with any backend. */
export interface AtomicRef<T> {
  getAsync(): Promise<T>;
  setAsync(value: T): Promise<void>;
  updateAsync(fn: (current: T) => T): Promise<void>;
}

export class PipelineRef<T> implements AtomicRef<T> {
  private constructor(readonly ref: Ref.Ref<T>) {}

  /** Create a ref with an initial value. */
  static make<T>(initial: T): PipelineRef<T> {
    return new PipelineRef(Effect.runSync(Ref.make(initial)));
  }

  // -------------------------------------------------------------------------
  // Effect-returning (for Pipeline/Effect composition)
  // -------------------------------------------------------------------------

  /** Get the current value (Effect). */
  get get(): Effect.Effect<T> {
    return Ref.get(this.ref);
  }

  /** Set a new value (Effect). */
  set(value: T): Effect.Effect<void> {
    return Ref.set(this.ref, value);
  }

  /** Update the value with a function (Effect). */
  update(fn: (current: T) => T): Effect.Effect<void> {
    return Ref.update(this.ref, fn);
  }

  /** Update and return the new value (Effect). */
  updateAndGet(fn: (current: T) => T): Effect.Effect<T> {
    return Ref.updateAndGet(this.ref, fn);
  }

  /** Atomically modify the value and return a derived result (Effect). */
  modify<U>(fn: (current: T) => readonly [U, T]): Effect.Effect<U> {
    return Ref.modify(this.ref, fn);
  }

  // -------------------------------------------------------------------------
  // Sync (for test assertions and non-async contexts)
  // -------------------------------------------------------------------------

  /** Get the current value synchronously. Useful for test assertions. */
  get value(): T {
    return Effect.runSync(this.get);
  }

  // -------------------------------------------------------------------------
  // Promise-returning (no Effect import needed)
  // -------------------------------------------------------------------------

  /** Get the current value. */
  getAsync(): Promise<T> {
    return Effect.runPromise(this.get);
  }

  /** Set a new value. */
  setAsync(value: T): Promise<void> {
    return Effect.runPromise(this.set(value));
  }

  /** Update the value with a function. */
  updateAsync(fn: (current: T) => T): Promise<void> {
    return Effect.runPromise(this.update(fn));
  }

  /** Update and return the new value. */
  updateAndGetAsync(fn: (current: T) => T): Promise<T> {
    return Effect.runPromise(this.updateAndGet(fn));
  }
}
