// ---------------------------------------------------------------------------
// StreamTopology — stateful stream processing with windows, joins, and checkpointing
//
// StreamPipeline is for general-purpose data transformation (stateless, finite or infinite).
// StreamTopology is for long-running event processing: keyed state, time windows,
// stream joins, deduplication, and automatic checkpointing for crash recovery.
//
// Runs in a single process. Kafka consumer groups handle partition assignment
// across instances — each instance processes its assigned partitions independently.
//
// Think: Kafka Streams Topology / Flink JobGraph, with our API style.
// ---------------------------------------------------------------------------

import type { Streamable, Acknowledgeable, Sinkable } from "@promin/core";
import type {
  TopologyNode,
  AggregateSpec,
  ProcessSpec,
  JoinConfig,
  CompiledTopology,
} from "./types.ts";

// ---------------------------------------------------------------------------
// StreamTopology — entry point (unkeyed stream)
// ---------------------------------------------------------------------------

/**
 * Stateful stream processing with windows, joins, and automatic checkpointing.
 *
 * Build a declarative processing DAG from a message source (Kafka, etc.),
 * apply transformations, partition by key, window, aggregate, and sink results.
 * State is checkpointed periodically and restored on crash/rebalance.
 *
 * Runs in a single process — Kafka consumer groups handle partition assignment
 * across instances. Use {@link TopologyRunner.run} to execute.
 *
 * @example
 * ```ts
 * const topology = StreamTopology.source(kafkaClickEvents)
 *   .filter(e => e.type !== "bot")
 *   .keyBy(e => e.userId)
 *   .tumbling(60_000)
 *   .count()
 *   .to(outputTopic);
 *
 * const handle = await TopologyRunner.run(topology, {
 *   group: "click-counter",
 *   checkpointIntervalMs: 10_000,
 * });
 * ```
 *
 * @typeParam T - The item type in the stream
 */
export class StreamTopology<T> {
  constructor(readonly node: TopologyNode<T>) {}

  /** Start a topology from any Streamable + Acknowledgeable source. */
  static source<T>(source: Streamable<T> & Acknowledgeable<T>): StreamTopology<T> {
    return new StreamTopology({ type: "source", source });
  }

  /** Transform each item. */
  map<U>(fn: (value: T) => U): StreamTopology<U> {
    return new StreamTopology({ type: "map", parent: this.node, fn: fn as any });
  }

  /** Filter items by predicate. */
  filter(fn: (value: T) => boolean): StreamTopology<T> {
    return new StreamTopology({ type: "filter", parent: this.node, fn: fn as any });
  }

  /** Async transform with bounded concurrency. */
  mapAsync<U>(concurrency: number, fn: (value: T) => Promise<U>): StreamTopology<U> {
    return new StreamTopology({
      type: "mapAsync",
      parent: this.node,
      concurrency,
      fn: fn as any,
    });
  }

  /** Partition by key — enables stateful per-key processing, windows, and joins. */
  keyBy<K extends string>(fn: (value: T) => K): KeyedTopology<K, T> {
    return new KeyedTopology({ type: "keyBy", parent: this.node, keyFn: fn as any });
  }

  /** Sink to any Sinkable. Returns a BuiltTopology ready to run. */
  to(sink: Sinkable<T>): BuiltTopology {
    const sinkNode = { type: "sink" as const, parent: this.node, sink };
    return new BuiltTopology({ nodes: [this.node, sinkNode], sinks: [sinkNode] });
  }

  /** Build without a sink (for testing or collect-style terminals). */
  build(): BuiltTopology {
    return new BuiltTopology({ nodes: [this.node], sinks: [] });
  }
}

// ---------------------------------------------------------------------------
// KeyedTopology — stream partitioned by key
// ---------------------------------------------------------------------------

/**
 * A stream partitioned by key, enabling stateful per-key processing,
 * time windows (tumbling, sliding, session), joins, and deduplication.
 *
 * Created via {@link StreamTopology.keyBy}. Each key is processed independently.
 *
 * @typeParam K - The key type (must extend string)
 * @typeParam T - The item type in the stream
 */
export class KeyedTopology<K extends string, T> {
  constructor(readonly node: TopologyNode<T>) {}

  /**
   * Repartition data by key via an intermediate transport (e.g. Kafka topic).
   * Ensures same-key records land on the same partition, enabling correct
   * stateful processing across multiple instances.
   *
   * In single-process mode (TopologyRunner), shuffle is a no-op passthrough.
   * In distributed mode (DistributedRunner), it creates a stage boundary.
   */
  shuffle(params?: { topicName?: string }): KeyedTopology<K, T> {
    return new KeyedTopology({
      type: "shuffle",
      parent: this.node,
      topicName: params?.topicName,
    });
  }

  /** Deduplicate by a key derived from each item. */
  dedupe(fn: (value: T) => string): KeyedTopology<K, T> {
    return new KeyedTopology({ type: "dedupe", parent: this.node, keyFn: fn as any });
  }

  /** Stateful per-key processing. Each key has its own state instance. */
  process<S, U>(spec: ProcessSpec<S, T, U>): StreamTopology<U> {
    return new StreamTopology({
      type: "process",
      parent: this.node,
      spec: spec as ProcessSpec<unknown, unknown, U>,
    });
  }

  /** Tumbling window — fixed-size, non-overlapping. */
  tumbling(windowMs: number): WindowedTopology<K, T> {
    return new WindowedTopology({
      type: "window",
      parent: this.node,
      windowType: { type: "tumbling", windowMs },
    });
  }

  /** Sliding window — fixed-size, overlapping. */
  sliding(params: { windowMs: number; slideMs: number }): WindowedTopology<K, T> {
    return new WindowedTopology({
      type: "window",
      parent: this.node,
      windowType: { type: "sliding", ...params },
    });
  }

  /** Session window — closes after inactivity gap. */
  session(gapMs: number): WindowedTopology<K, T> {
    return new WindowedTopology({
      type: "window",
      parent: this.node,
      windowType: { type: "session", gapMs },
    });
  }

  /** Join with another keyed stream by key within a time window. */
  join<U>(other: KeyedTopology<K, U>, config: JoinConfig): KeyedTopology<K, { left: T; right: U }> {
    return new KeyedTopology({
      type: "join",
      left: this.node,
      right: other.node,
      config,
    });
  }

  /** Transform each item (preserves keying). */
  map<U>(fn: (value: T) => U): KeyedTopology<K, U> {
    return new KeyedTopology({ type: "map", parent: this.node, fn: fn as any });
  }

  /** Filter items (preserves keying). */
  filter(fn: (value: T) => boolean): KeyedTopology<K, T> {
    return new KeyedTopology({ type: "filter", parent: this.node, fn: fn as any });
  }

  /** Sink to any Sinkable. */
  to(sink: Sinkable<T>): BuiltTopology {
    const sinkNode = { type: "sink" as const, parent: this.node, sink };
    return new BuiltTopology({ nodes: [this.node, sinkNode], sinks: [sinkNode] });
  }

  /** Build without a sink. */
  build(): BuiltTopology {
    return new BuiltTopology({ nodes: [this.node], sinks: [] });
  }
}

// ---------------------------------------------------------------------------
// WindowedTopology — keyed stream with windowing applied
// ---------------------------------------------------------------------------

/**
 * A keyed stream with a time window applied. Use {@link aggregate}, {@link count},
 * or {@link sum} to reduce items within each window per key.
 *
 * Created via {@link KeyedTopology.tumbling}, {@link KeyedTopology.sliding},
 * or {@link KeyedTopology.session}.
 *
 * @typeParam K - The key type
 * @typeParam T - The item type in the stream
 */
export class WindowedTopology<K extends string, T> {
  constructor(readonly node: TopologyNode<T>) {}

  /** Aggregate items within each window. */
  aggregate<S, U>(spec: AggregateSpec<S, T, U>): StreamTopology<U> {
    return new StreamTopology({
      type: "aggregate",
      parent: this.node,
      spec: spec as AggregateSpec<unknown, unknown, U>,
    });
  }

  /** Count items per key per window. */
  count(): StreamTopology<{ key: K; window: { start: number; end: number }; count: number }> {
    return this.aggregate({
      init: () => ({ count: 0 }),
      add: (state) => ({ count: state.count + 1 }),
      emit: (key, window, state) => ({ key: key as K, window, count: state.count }),
    });
  }

  /** Sum a numeric field per key per window. */
  sum(
    fn: (value: T) => number,
  ): StreamTopology<{ key: K; window: { start: number; end: number }; sum: number }> {
    return this.aggregate({
      init: () => ({ sum: 0 }),
      add: (state, value) => ({ sum: state.sum + fn(value) }),
      emit: (key, window, state) => ({ key: key as K, window, sum: state.sum }),
    });
  }
}

// ---------------------------------------------------------------------------
// BuiltTopology — compiled, ready to run
// ---------------------------------------------------------------------------

export class BuiltTopology {
  constructor(readonly compiled: CompiledTopology) {}
}
