import { Effect, PubSub, Stream } from "effect";
import { StreamPipeline } from "./stream-pipeline.ts";

/**
 * Broadcast to multiple subscribers — each subscriber sees every message.
 * Wraps Effect's `PubSub`.
 *
 * @example
 * ```ts
 * const events = PipelinePubSub.make<Event>(100);
 * await events.publishAsync(event);
 * await events.subscribe().forEach(handle);
 * ```
 */
/** Pluggable pub/sub interface — implement with any backend. */
export interface PubSubBroadcast<T> {
  publishAsync(value: T): Promise<boolean>;
  shutdownAsync(): Promise<void>;
}

export class PipelinePubSub<T> implements PubSubBroadcast<T> {
  private constructor(readonly pubsub: PubSub.PubSub<T>) {}

  /** Create a bounded PubSub. Publisher blocks when all subscribers are full. */
  static make<T>(capacity: number): PipelinePubSub<T> {
    return new PipelinePubSub(Effect.runSync(PubSub.bounded<T>(capacity)));
  }

  /** Create an unbounded PubSub. */
  static makeUnbounded<T>(): PipelinePubSub<T> {
    return new PipelinePubSub(Effect.runSync(PubSub.unbounded<T>()));
  }

  // -------------------------------------------------------------------------
  // Effect-returning (for Pipeline/Effect composition)
  // -------------------------------------------------------------------------

  /** Publish a message to all subscribers (Effect). */
  publish(value: T): Effect.Effect<boolean> {
    return PubSub.publish(this.pubsub, value);
  }

  /** Shutdown the PubSub — all subscriber streams will end (Effect). */
  shutdown(): Effect.Effect<void> {
    return PubSub.shutdown(this.pubsub);
  }

  // -------------------------------------------------------------------------
  // Promise-returning (no Effect import needed)
  // -------------------------------------------------------------------------

  /** Publish a message to all subscribers. */
  publishAsync(value: T): Promise<boolean> {
    return Effect.runPromise(this.publish(value));
  }

  /** Shutdown the PubSub — all subscriber streams will end. */
  shutdownAsync(): Promise<void> {
    return Effect.runPromise(this.shutdown());
  }

  // -------------------------------------------------------------------------
  // Stream
  // -------------------------------------------------------------------------

  /** Subscribe and receive a StreamPipeline of messages. Each subscriber is independent. */
  subscribe(): StreamPipeline<T, never> {
    return StreamPipeline.from(Stream.fromPubSub(this.pubsub));
  }
}
