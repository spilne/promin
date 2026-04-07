// ---------------------------------------------------------------------------
// KafkaShuffleTransport — ShuffleTransport implementation backed by Kafka
//
// Creates repartition channels using KafkaTopic. Each channel is a Kafka topic
// that data is published to (keyed by the keyBy function) and consumed from.
// ---------------------------------------------------------------------------

import type { ShuffleTransport } from "@promin/core";
import type { Codec, Streamable, Acknowledgeable, KeyedSinkable } from "@promin/core";
import { KafkaTopic } from "./kafka-topic.ts";
import type { KafkaClient } from "./kafka-types.ts";

export interface KafkaShuffleTransportConfig {
  /** Kafka client instance. */
  kafka: KafkaClient;
  /** Number of partitions for repartition topics. Default: 6. */
  partitions?: number;
}

export class KafkaShuffleTransport implements ShuffleTransport {
  private readonly kafka: KafkaClient;
  private readonly partitions: number;
  private readonly topics = new Map<string, KafkaTopic<unknown>>();

  constructor(config: KafkaShuffleTransportConfig) {
    this.kafka = config.kafka;
    this.partitions = config.partitions ?? 6;
  }

  async getOrCreateRepartitionChannel<T>(params: {
    name: string;
    group: string;
    codec: Codec<T>;
  }): Promise<{
    source: Streamable<T> & Acknowledgeable<T>;
    sink: KeyedSinkable<T>;
  }> {
    // Ensure topic exists
    const admin = this.kafka.admin();
    await admin.connect();
    if (admin.createTopics) {
      await admin.createTopics({
        topics: [
          {
            topic: params.name,
            numPartitions: this.partitions,
            replicationFactor: 1,
          },
        ],
      });
    }
    await admin.disconnect();

    // Create KafkaTopic for this repartition channel
    const kt = new KafkaTopic<T>({
      kafka: this.kafka,
      topic: params.name,
      groupId: params.group,
      codec: params.codec,
    });
    this.topics.set(params.name, kt as KafkaTopic<unknown>);

    return { source: kt, sink: kt };
  }

  /** Disconnect all managed topics. */
  async disconnect(): Promise<void> {
    await Promise.all([...this.topics.values()].map((t) => t.disconnect()));
    this.topics.clear();
  }
}
