// ---------------------------------------------------------------------------
// IterableSource<T> — wraps any Iterable as a Streamable
// Implements: Streamable (+ Partitionable via chunking)
// ---------------------------------------------------------------------------

import { Stream } from "effect";
import { StreamPipeline } from "../../stream-pipeline.ts";
import type { Codec } from "../../typeclasses/codec.ts";
import { JsonCodec } from "../../typeclasses/codec.ts";
import type { Streamable, Partitionable } from "../../typeclasses/streamable.ts";

export class IterableSource<T> implements Partitionable<T> {
  readonly partitions: number;

  constructor(
    private readonly items: Iterable<T>,
    readonly codec: Codec<T> = JsonCodec as Codec<T>,
    partitions?: number,
  ) {
    this.partitions = partitions ?? 1;
  }

  subscribe(params?: { group?: string; partitions?: number[] }): StreamPipeline<T, never> {
    const allItems = [...this.items];

    if (this.partitions <= 1 || !params?.partitions) {
      return StreamPipeline.from(Stream.fromIterable(allItems));
    }

    // Distribute items across partitions, return only requested ones
    const selected = new Set(params.partitions);
    const filtered = allItems.filter((_, i) => selected.has(i % this.partitions));
    return StreamPipeline.from(Stream.fromIterable(filtered));
  }
}

/** Create a Streamable from any iterable. */
export function fromIterable<T>(items: Iterable<T>, codec?: Codec<T>): Streamable<T> {
  return new IterableSource(items, codec);
}
