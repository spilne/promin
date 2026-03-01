import { Effect, Queue, Stream } from "effect";
import { StreamPipeline } from "./stream-pipeline.ts";

/**
 * Bounded queue with backpressure. Bridges Effect's `Queue` into StreamPipeline.
 *
 * @example
 * ```ts
 * const queue = PipelineQueue.make<Job>(100);
 * await queue.offerAsync(job);
 * const next = await queue.takeAsync();
 * ```
 */
export class PipelineQueue<T> {
  private constructor(readonly queue: Queue.Queue<T>) {}

  /** Create a bounded queue with the given capacity. Producer blocks when full. */
  static make<T>(capacity: number): PipelineQueue<T> {
    return new PipelineQueue(Effect.runSync(Queue.bounded<T>(capacity)));
  }

  /** Create an unbounded queue. */
  static makeUnbounded<T>(): PipelineQueue<T> {
    return new PipelineQueue(Effect.runSync(Queue.unbounded<T>()));
  }

  // -------------------------------------------------------------------------
  // Effect-returning (for Pipeline/Effect composition)
  // -------------------------------------------------------------------------

  /** Offer an item to the queue. Blocks if full (Effect). */
  offer(item: T): Effect.Effect<void> {
    return Queue.offer(this.queue, item).pipe(Effect.asVoid);
  }

  /** Offer multiple items (Effect). */
  offerAll(items: Iterable<T>): Effect.Effect<void> {
    return Queue.offerAll(this.queue, items).pipe(Effect.asVoid);
  }

  /** Take an item. Blocks if empty (Effect). */
  take(): Effect.Effect<T> {
    return Queue.take(this.queue);
  }

  /** Shutdown the queue — signals consumers to stop (Effect). */
  shutdown(): Effect.Effect<void> {
    return Queue.shutdown(this.queue);
  }

  /** Get the current number of items (Effect). */
  get size(): Effect.Effect<number> {
    return Queue.size(this.queue);
  }

  // -------------------------------------------------------------------------
  // Promise-returning (no Effect import needed)
  // -------------------------------------------------------------------------

  /** Offer an item. Blocks if full. */
  offerAsync(item: T): Promise<void> {
    return Effect.runPromise(this.offer(item));
  }

  /** Offer multiple items. */
  offerAllAsync(items: Iterable<T>): Promise<void> {
    return Effect.runPromise(this.offerAll(items));
  }

  /** Take an item. Blocks if empty. */
  takeAsync(): Promise<T> {
    return Effect.runPromise(this.take());
  }

  /** Shutdown the queue — signals consumers to stop. */
  shutdownAsync(): Promise<void> {
    return Effect.runPromise(this.shutdown());
  }

  /** Get the current number of items. */
  sizeAsync(): Promise<number> {
    return Effect.runPromise(this.size);
  }

  // -------------------------------------------------------------------------
  // Stream
  // -------------------------------------------------------------------------

  /** Consume the queue as a StreamPipeline. */
  toStream(): StreamPipeline<T, never> {
    return StreamPipeline.from(Stream.fromQueue(this.queue));
  }
}
