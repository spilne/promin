import { Effect, Ref, Deferred } from "effect";

/** Pluggable latch interface — implement with any backend. */
export interface Latch {
  countDownAsync(): Promise<void>;
  awaitAsync(): Promise<void>;
  remainingAsync(): Promise<number>;
}

export class PipelineLatch implements Latch {
  private constructor(
    private readonly count: Ref.Ref<number>,
    private readonly deferred: Deferred.Deferred<void>,
  ) {}

  static make(params: { count: number }): PipelineLatch {
    return new PipelineLatch(
      Effect.runSync(Ref.make(params.count)),
      Effect.runSync(Deferred.make<void>()),
    );
  }

  // ---------------------------------------------------------------------------
  // Effect-returning (for Pipeline/Effect composition)
  // ---------------------------------------------------------------------------

  get countDown(): Effect.Effect<void> {
    return Effect.flatMap(
      Ref.updateAndGet(this.count, (n) => Math.max(0, n - 1)),
      (n) => (n === 0 ? Deferred.succeed(this.deferred, void 0).pipe(Effect.asVoid) : Effect.void),
    );
  }

  countDownBy(n: number): Effect.Effect<void> {
    return Effect.flatMap(
      Ref.updateAndGet(this.count, (c) => Math.max(0, c - n)),
      (c) => (c === 0 ? Deferred.succeed(this.deferred, void 0).pipe(Effect.asVoid) : Effect.void),
    );
  }

  get await(): Effect.Effect<void> {
    return Deferred.await(this.deferred);
  }

  get remaining(): Effect.Effect<number> {
    return Ref.get(this.count);
  }

  // ---------------------------------------------------------------------------
  // Promise-returning (no Effect import needed)
  // ---------------------------------------------------------------------------

  async countDownAsync(): Promise<void> {
    return Effect.runPromise(this.countDown);
  }

  async awaitAsync(): Promise<void> {
    return Effect.runPromise(this.await);
  }

  async remainingAsync(): Promise<number> {
    return Effect.runPromise(this.remaining);
  }
}
