// ---------------------------------------------------------------------------
// StreamTopology — declarative distributed stream processing DAG
//
// StreamPipeline is single-process pull-based.
// StreamTopology is its distributed counterpart: a declarative processing
// DAG that runs across partitions with co-located state and checkpointing.
//
// Think: Kafka Streams Topology / Flink JobGraph, with our API style.
// ---------------------------------------------------------------------------

import type { Streamable, Acknowledgeable, Sinkable } from "../typeclasses/streamable.ts";
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

export class KeyedTopology<K extends string, T> {
  constructor(readonly node: TopologyNode<T>) {}

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
