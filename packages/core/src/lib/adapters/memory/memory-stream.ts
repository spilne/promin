// ---------------------------------------------------------------------------
// MemoryStream<T> — bidirectional in-memory stream
// Implements: Partitionable + KeyedSinkable
// ---------------------------------------------------------------------------

import { Stream, Queue, Effect } from "effect";
import { StreamPipeline } from "../../stream-pipeline.ts";
import type { Codec } from "../../typeclasses/codec.ts";
import { JsonCodec } from "../../typeclasses/codec.ts";
import type { Partitionable } from "../../typeclasses/streamable.ts";
import type { KeyedSinkable } from "../../typeclasses/streamable.ts";

export class MemoryStream<T> implements Partitionable<T>, KeyedSinkable<T> {
  private queues: Queue.Queue<T>[] = [];
  private _closed = false;

  constructor(
    readonly partitions: number = 1,
    readonly codec: Codec<T> = JsonCodec as Codec<T>,
  ) {}

  /** Initialize the internal queues. Must be called before use. */
  static async create<T>(params?: {
    partitions?: number;
    codec?: Codec<T>;
  }): Promise<MemoryStream<T>> {
    const ms = new MemoryStream<T>(
      params?.partitions ?? 1,
      params?.codec ?? (JsonCodec as Codec<T>),
    );
    for (let i = 0; i < ms.partitions; i++) {
      const queue = await Effect.runPromise(Queue.unbounded<T>());
      ms.queues.push(queue);
    }
    return ms;
  }

  subscribe(params?: { group?: string; partitions?: number[] }): StreamPipeline<T, never> {
    const selectedPartitions =
      params?.partitions ?? Array.from({ length: this.partitions }, (_, i) => i);
    const streams = selectedPartitions.map((p) => {
      const queue = this.queues[p];
      if (!queue) {
        return Stream.empty;
      }
      return Stream.fromQueue(queue);
    });

    if (streams.length === 0) return StreamPipeline.from(Stream.empty);
    if (streams.length === 1) return StreamPipeline.from(streams[0]!);

    const merged = streams.reduce((acc, s) => Stream.merge(acc, s));
    return StreamPipeline.from(merged);
  }

  async publish(value: T, params?: { key: string }): Promise<void> {
    if (this._closed) return;
    const partition = params?.key ? Math.abs(hashString(params.key)) % this.partitions : 0;
    const queue = this.queues[partition];
    if (queue) {
      await Effect.runPromise(Queue.offer(queue, value));
    }
  }

  async close(): Promise<void> {
    this._closed = true;
    for (const queue of this.queues) {
      await Effect.runPromise(Queue.shutdown(queue));
    }
  }

  get closed(): boolean {
    return this._closed;
  }
}

function hashString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash |= 0;
  }
  return hash;
}
