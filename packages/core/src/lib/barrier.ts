import { Effect, Ref, Deferred } from "effect";

/** Pluggable barrier interface — implement with any backend. */
export interface Barrier {
  awaitAsync(): Promise<void>;
  arrivedAsync(): Promise<number>;
}

export class PipelineBarrier implements Barrier {
  private constructor(
    private readonly parties: number,
    private readonly count: Ref.Ref<number>,
    private readonly deferred: Deferred.Deferred<void>,
  ) {}

  static make(params: { parties: number }): PipelineBarrier {
    return new PipelineBarrier(
      params.parties,
      Effect.runSync(Ref.make(0)),
      Effect.runSync(Deferred.make<void>()),
    );
  }

  get await(): Effect.Effect<void> {
    return Effect.flatMap(
      Ref.updateAndGet(this.count, (n) => n + 1),
      (n) =>
        n >= this.parties
          ? Deferred.succeed(this.deferred, void 0).pipe(Effect.asVoid)
          : Deferred.await(this.deferred),
    );
  }

  get arrived(): Effect.Effect<number> {
    return Ref.get(this.count);
  }

  async awaitAsync(): Promise<void> {
    return Effect.runPromise(this.await);
  }

  async arrivedAsync(): Promise<number> {
    return Effect.runPromise(this.arrived);
  }
}
