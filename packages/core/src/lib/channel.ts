import { Effect, Queue, Stream } from "effect";
import { StreamPipeline } from "./stream-pipeline.ts";

/**
 * Multi-producer, single-consumer channel with close semantics.
 * When closed, the consumer stream terminates cleanly.
 *
 * @example
 * ```ts
 * const ch = PipelineChannel.make<Job>();
 * await ch.sendAsync(job1);
 * await ch.sendAsync(job2);
 * await ch.closeAsync();
 * await ch.toStream().parAsyncMap(5, process).drain();
 * ```
 */
/** Pluggable channel interface — implement with any backend. */
export interface Channel<T> {
  sendAsync(item: T): Promise<void>;
  closeAsync(): Promise<void>;
  readonly isClosed: boolean;
}

export class PipelineChannel<T> implements Channel<T> {
  private closed = false;

  private constructor(private readonly queue: Queue.Queue<T>) {}

  /** Create a bounded channel. */
  static make<T>(capacity = 16): PipelineChannel<T> {
    return new PipelineChannel(Effect.runSync(Queue.bounded<T>(capacity)));
  }

  /** Whether the channel has been closed. */
  get isClosed(): boolean {
    return this.closed;
  }

  // -------------------------------------------------------------------------
  // Effect-returning (for Pipeline/Effect composition)
  // -------------------------------------------------------------------------

  /** Send an item. Blocks if full. Fails if closed (Effect). */
  send(item: T): Effect.Effect<void> {
    return Effect.suspend(() => {
      if (this.closed) return Effect.die(new Error("Channel is closed"));
      return Queue.offer(this.queue, item).pipe(Effect.asVoid);
    });
  }

  /** Close the channel — signals consumers to stop (Effect). */
  close(): Effect.Effect<void> {
    return Effect.sync(() => {
      this.closed = true;
    }).pipe(Effect.flatMap(() => Queue.shutdown(this.queue)));
  }

  // -------------------------------------------------------------------------
  // Promise-returning (no Effect import needed)
  // -------------------------------------------------------------------------

  /** Send an item. Blocks if full. Fails if closed. */
  sendAsync(item: T): Promise<void> {
    return Effect.runPromise(this.send(item));
  }

  /** Close the channel — signals consumers to stop. */
  closeAsync(): Promise<void> {
    return Effect.runPromise(this.close());
  }

  // -------------------------------------------------------------------------
  // Stream
  // -------------------------------------------------------------------------

  /** Consume the channel as a StreamPipeline. Ends when the channel is closed and drained. */
  toStream(): StreamPipeline<T, never> {
    return StreamPipeline.from(Stream.fromQueue(this.queue));
  }
}
