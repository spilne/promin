// ---------------------------------------------------------------------------
// ShuffleTransport — transport-agnostic interface for repartition channels
//
// Core package has no Kafka dependency. Kafka implementation lives in @promin/kafka.
// ---------------------------------------------------------------------------

import type { Streamable, Acknowledgeable, KeyedSinkable } from "../typeclasses/streamable.ts";
import type { Codec } from "../typeclasses/codec.ts";

/**
 * Transport-agnostic interface for creating repartition channels.
 * Each channel is a keyed topic that data is published to (sink) and
 * consumed from (source) during shuffle.
 */
export interface ShuffleTransport<T = unknown> {
  getOrCreateRepartitionChannel(params: { name: string; group: string; codec: Codec<T> }): Promise<{
    source: Streamable<T> & Acknowledgeable<T>;
    sink: KeyedSinkable<T>;
  }>;
}

/**
 * Config for DistributedRunner — extends TopologyConfig with shuffle transport.
 */
export interface DistributedTopologyConfig {
  group: string;
  shuffleTransport: ShuffleTransport;
  stateBackend?: unknown;
  checkpointIntervalMs?: number;
  maxBufferSize?: number;
  maxItemsPerSecond?: number;
  maxDedupeSize?: number;
  ackBatchSize?: number;
}
