// ---------------------------------------------------------------------------
// KafkaTopic<T> — Kafka topic implementing streaming typeclasses
//
// Implements: Partitionable, Replayable, Acknowledgeable, KeyedSinkable, Checkpointable
// Uses kafkajs for consumer/producer.
// ---------------------------------------------------------------------------

import { Effect, Stream } from "effect";
import { Kafka, type Consumer, type Producer, type EachMessagePayload } from "kafkajs";
import { StreamPipeline, JsonCodec } from "@promin/core";
import type {
  KeyedSinkable,
  Partitionable,
  Replayable,
  Acknowledgeable,
  Checkpointable,
  Envelope,
  Codec,
  Offset,
} from "@promin/core";

export interface KafkaTopicConfig<T> {
  /** Kafka client instance (kafkajs). */
  kafka: Kafka;
  /** Topic name. */
  topic: string;
  /** Consumer group ID. */
  groupId: string;
  /** Codec for message serialization. Default: JsonCodec. */
  codec?: Codec<T>;
  /** Number of partitions (for Partitionable). */
  partitions?: number;
  /** Poll interval for subscribe. Default: 100ms. */
  pollIntervalMs?: number;
}

export class KafkaTopic<T>
  implements
    Partitionable<T>,
    Replayable<T>,
    Acknowledgeable<T>,
    KeyedSinkable<T>,
    Checkpointable<T>
{
  readonly codec: Codec<T>;
  readonly partitions: number;
  private readonly kafka: Kafka;
  private readonly topic: string;
  private readonly groupId: string;
  
  private consumer?: Consumer;
  private producer?: Producer;

  constructor(config: KafkaTopicConfig<T>) {
    this.kafka = config.kafka;
    this.topic = config.topic;
    this.groupId = config.groupId;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.partitions = config.partitions ?? 1;
  }

  // =========================================================================
  // Sinkable — publish messages
  // =========================================================================

  async publish(value: T, params?: { key: string }): Promise<void> {
    if (!this.producer) {
      this.producer = this.kafka.producer();
      await this.producer.connect();
    }

    const encoded = this.codec.encode(value);
    await this.producer.send({
      topic: this.topic,
      messages: [
        {
          key: params?.key ?? null,
          value: JSON.stringify(encoded),
        },
      ],
    });
  }

  async publishBatch(
    messages: { value: T; key?: string }[],
  ): Promise<void> {
    if (!this.producer) {
      this.producer = this.kafka.producer();
      await this.producer.connect();
    }

    await this.producer.send({
      topic: this.topic,
      messages: messages.map((m) => ({
        key: m.key ?? null,
        value: JSON.stringify(this.codec.encode(m.value)),
      })),
    });
  }

  // =========================================================================
  // Streamable — subscribe to messages
  // =========================================================================

  subscribe(params?: { group?: string; partitions?: number[] }): StreamPipeline<T, never> {
    return this.createConsumerStream(params?.group);
  }

  // =========================================================================
  // Replayable — subscribe from offset
  // =========================================================================

  subscribeFrom(params: { offset: Offset; group?: string }): StreamPipeline<T, never> {
    return this.createConsumerStream(params.group, params.offset);
  }

  // =========================================================================
  // Acknowledgeable — manual ack/nack
  // =========================================================================

  subscribeAck(params?: { group?: string }): StreamPipeline<Envelope<T>, never> {
    const codec = this.codec;
    const kafka = this.kafka;
    const topic = this.topic;
    const groupId = params?.group ?? this.groupId;

    const stream = Stream.async<Envelope<T>, never>((emit) => {
      const consumer = kafka.consumer({ groupId });

      const run = async () => {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: false });

        await consumer.run({
          eachMessage: async (payload: EachMessagePayload) => {
            const value = codec.decode(JSON.parse(payload.message.value!.toString()));
            const envelope: Envelope<T> = {
              value,
              ack: async () => {
                await consumer.commitOffsets([
                  {
                    topic,
                    partition: payload.partition,
                    offset: (Number(payload.message.offset) + 1).toString(),
                  },
                ]);
              },
              nack: async () => {
                // Kafka doesn't have nack — message will be redelivered on next poll
                // if offset isn't committed
              },
              metadata: {
                topic,
                partition: payload.partition,
                offset: payload.message.offset,
                key: payload.message.key?.toString(),
                timestamp: payload.message.timestamp,
              },
            };
            emit.single(envelope);
          },
        });
      };

      run().catch(() => {});

      return Effect.promise(async () => {
        await consumer.disconnect();
      });
    });

    return StreamPipeline.from(stream);
  }

  // =========================================================================
  // Checkpointable — offset management
  // =========================================================================

  async commitOffset(params: { group: string; offset: string }): Promise<void> {
    const consumer = this.kafka.consumer({ groupId: params.group });
    await consumer.connect();
    // Note: in production, use the existing consumer instance
    await consumer.commitOffsets([
      { topic: this.topic, partition: 0, offset: params.offset },
    ]);
    await consumer.disconnect();
  }

  async getCommittedOffset(params: { group: string }): Promise<string | null> {
    const admin = this.kafka.admin();
    await admin.connect();
    const offsets = await admin.fetchOffsets({ groupId: params.group, topics: [this.topic] });
    await admin.disconnect();
    const topicOffset = offsets.find((o) => o.topic === this.topic);
    if (!topicOffset || topicOffset.partitions.length === 0) return null;
    return topicOffset.partitions[0]!.offset;
  }

  // =========================================================================
  // Internal — consumer stream creation
  // =========================================================================

  private createConsumerStream(group?: string, offset?: Offset): StreamPipeline<T, never> {
    const codec = this.codec;
    const kafka = this.kafka;
    const topic = this.topic;
    const groupId = group ?? this.groupId;

    const stream = Stream.async<T, never>((emit) => {
      const consumer = kafka.consumer({ groupId });

      const run = async () => {
        await consumer.connect();
        await consumer.subscribe({
          topic,
          fromBeginning: offset?.type === "earliest",
        });

        if (offset?.type === "timestamp") {
          const admin = kafka.admin();
          await admin.connect();
          const result = await admin.fetchTopicOffsetsByTimestamp(topic, offset.value);
          await admin.disconnect();
          for (const p of result) {
            consumer.seek({ topic, partition: p.partition, offset: p.offset });
          }
        }

        await consumer.run({
          eachMessage: async (payload: EachMessagePayload) => {
            const value = codec.decode(JSON.parse(payload.message.value!.toString()));
            emit.single(value);
          },
        });
      };

      run().catch(() => {});

      return Effect.promise(async () => {
        await consumer.disconnect();
      });
    });

    return StreamPipeline.from(stream);
  }

  // =========================================================================
  // Cleanup
  // =========================================================================

  async disconnect(): Promise<void> {
    await this.producer?.disconnect();
    await this.consumer?.disconnect();
  }
}
