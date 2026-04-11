import { Effect, Ref, Duration } from "effect";
import { Pipeline, type TaggedError } from "./pipeline.ts";

/** Pluggable throttle interface — test against this, implement with any backend. */
export interface Throttle {
  acquireAsync(): Promise<void>;
  tryAcquireAsync(): Promise<boolean>;
  withPermitAsync<T>(fn: () => Promise<T>): Promise<T>;
}

/**
 * Time-based concurrency limiter: N permits per sliding time window.
 * Callers block until a permit becomes available.
 *
 * @example
 * ```ts
 * const throttle = PipelineThrottle.make({ permits: 5, windowMs: 1000 });
 * await throttle.withPermitAsync(() => fetch("/api"));
 * ```
 */
export class PipelineThrottle implements Throttle {
  private constructor(
    private readonly permits: number,
    private readonly windowMs: number,
    private readonly timestamps: Ref.Ref<number[]>,
    private readonly asyncTimestamps: number[],
  ) {}

  static make(params: { permits: number; windowMs: number }): PipelineThrottle {
    return new PipelineThrottle(
      params.permits,
      params.windowMs,
      Effect.runSync(Ref.make<number[]>([])),
      [],
    );
  }

  /** Acquire a permit, blocking until one is available (Effect). */
  get acquire(): Effect.Effect<void> {
    const { permits, windowMs, timestamps } = this;
    const tryOnce = (): Effect.Effect<void> =>
      Effect.flatten(
        Ref.modify(timestamps, (stamps) => {
          const now = Date.now();
          const valid = stamps.filter((t) => t > now - windowMs);
          if (valid.length < permits) {
            return [Effect.void, [...valid, now]];
          }
          const oldest = valid[0]!;
          const waitMs = oldest + windowMs - now;
          return [Effect.sleep(Duration.millis(waitMs)).pipe(Effect.andThen(tryOnce)), valid];
        }),
      );
    return tryOnce();
  }

  /** Try to acquire a permit without blocking (Effect). */
  get tryAcquire(): Effect.Effect<boolean> {
    return Ref.modify(this.timestamps, (stamps) => {
      const now = Date.now();
      const valid = stamps.filter((t) => t > now - this.windowMs);
      if (valid.length < this.permits) {
        return [true, [...valid, now]];
      }
      return [false, valid];
    });
  }

  /** Wrap an Effect — acquire a permit then run. */
  withPermit<T, E>(effect: Effect.Effect<T, E>): Effect.Effect<T, E> {
    return Effect.andThen(this.acquire, effect);
  }

  /** Wrap a Pipeline — acquire a permit then run. */
  withPermitPipeline<T, E extends TaggedError>(pipeline: Pipeline<T, E>): Pipeline<T, E> {
    return Pipeline.from(this.withPermit(pipeline.effect));
  }

  /** Number of permits remaining in the current window (Effect). */
  get remaining(): Effect.Effect<number> {
    return Ref.get(this.timestamps).pipe(
      Effect.map((stamps) => {
        const now = Date.now();
        const valid = stamps.filter((t) => t > now - this.windowMs);
        return Math.max(0, this.permits - valid.length);
      }),
    );
  }

  /** Milliseconds until the next permit opens (Effect). */
  get nextSlotIn(): Effect.Effect<number> {
    return Ref.get(this.timestamps).pipe(
      Effect.map((stamps) => {
        const now = Date.now();
        const valid = stamps.filter((t) => t > now - this.windowMs);
        if (valid.length < this.permits) return 0;
        return Math.max(0, valid[0]! + this.windowMs - now);
      }),
    );
  }

  /** Acquire a permit, blocking until one is available (Promise). */
  async acquireAsync(): Promise<void> {
    const now = Date.now();
    const cutoff = now - this.windowMs;
    while (this.asyncTimestamps.length > 0 && this.asyncTimestamps[0]! <= cutoff) {
      this.asyncTimestamps.shift();
    }
    if (this.asyncTimestamps.length < this.permits) {
      this.asyncTimestamps.push(now);
      return;
    }
    const oldest = this.asyncTimestamps[0]!;
    const waitMs = oldest + this.windowMs - now;
    await new Promise((r) => setTimeout(r, waitMs));
    return this.acquireAsync();
  }

  /** Try to acquire a permit without blocking (Promise). */
  async tryAcquireAsync(): Promise<boolean> {
    const now = Date.now();
    const cutoff = now - this.windowMs;
    while (this.asyncTimestamps.length > 0 && this.asyncTimestamps[0]! <= cutoff) {
      this.asyncTimestamps.shift();
    }
    if (this.asyncTimestamps.length < this.permits) {
      this.asyncTimestamps.push(now);
      return true;
    }
    return false;
  }

  /** Wrap a function — acquire a permit then run (Promise). */
  async withPermitAsync<T>(fn: () => Promise<T>): Promise<T> {
    await this.acquireAsync();
    return fn();
  }

  /** Number of permits remaining in the current window (Promise). */
  remainingAsync(): Promise<number> {
    return Effect.runPromise(this.remaining);
  }

  /** Milliseconds until the next permit opens (Promise). */
  nextSlotInAsync(): Promise<number> {
    return Effect.runPromise(this.nextSlotIn);
  }
}
