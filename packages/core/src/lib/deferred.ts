import { Effect, Deferred } from "effect";

/**
 * One-shot synchronization primitive. One fiber waits, another completes it.
 * Wraps `Effect.Deferred`.
 *
 * @example
 * ```ts
 * const gate = PipelineDeferred.make<Config>();
 * // Fiber A:
 * const config = await gate.awaitAsync();
 * // Fiber B:
 * await gate.succeedAsync(loadedConfig);
 * ```
 */
export class PipelineDeferred<T> {
  private constructor(readonly deferred: Deferred.Deferred<T>) {}

  /** Create a new deferred value. */
  static make<T>(): PipelineDeferred<T> {
    return new PipelineDeferred(Effect.runSync(Deferred.make<T>()));
  }

  // -------------------------------------------------------------------------
  // Effect-returning (for Pipeline/Effect composition)
  // -------------------------------------------------------------------------

  /** Wait for the deferred to be completed (Effect). */
  await(): Effect.Effect<T> {
    return Deferred.await(this.deferred);
  }

  /** Complete the deferred with a success value (Effect). */
  succeed(value: T): Effect.Effect<boolean> {
    return Deferred.succeed(this.deferred, value);
  }

  /** Complete the deferred with an error (Effect). */
  fail(error: Error): Effect.Effect<boolean> {
    return Deferred.die(this.deferred, error);
  }

  /** Check if the deferred has been completed (Effect). */
  get isDone(): Effect.Effect<boolean> {
    return Deferred.isDone(this.deferred);
  }

  // -------------------------------------------------------------------------
  // Promise-returning (no Effect import needed)
  // -------------------------------------------------------------------------

  /** Wait for the deferred to be completed. */
  awaitAsync(): Promise<T> {
    return Effect.runPromise(this.await());
  }

  /** Complete the deferred with a success value. */
  succeedAsync(value: T): Promise<boolean> {
    return Effect.runPromise(this.succeed(value));
  }

  /** Complete the deferred with an error. */
  failAsync(error: Error): Promise<boolean> {
    return Effect.runPromise(this.fail(error));
  }

  /** Check if the deferred has been completed. */
  isDoneAsync(): Promise<boolean> {
    return Effect.runPromise(this.isDone);
  }
}
