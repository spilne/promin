// ---------------------------------------------------------------------------
// KafkaTopic<T> — Kafka topic implementing streaming typeclasses
//
// Implements: Partitionable, Replayable, Acknowledgeable, KeyedSinkable, Checkpointable
// Uses kafkajs for consumer/producer.
// ---------------------------------------------------------------------------

import { Effect, Stream } from "effect";
import { OffsetTracker } from "./offset-tracker.ts";
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

  /**
   * Subscribe with manual ack — parallel-safe offset tracking.
   *
   * When processing messages in parallel, offsets are only committed
   * up to the highest *contiguous* completed offset (high-water mark).
   * This prevents message loss on crash:
   *
   *   Processing: [1:done, 2:pending, 3:done, 4:done]
   *   Committable: offset 2 (only offset 1 is contiguous)
   *
   *   Later: [1:done, 2:done, 3:done, 4:done]
   *   Committable: offset 5 (all contiguous)
   *
   * Set `commitIntervalMs` to control how often accumulated acks are flushed.
   * Inspired by fs2-kafka / kafka4s commit batching pattern.
   */
  subscribeAck(params?: {
    group?: string;
    /** How often to flush committed offsets (ms). Default: 1000. */
    commitIntervalMs?: number;
  }): StreamPipeline<Envelope<T>, never> {
    const codec = this.codec;
    const kafka = this.kafka;
    const topic = this.topic;
    const groupId = params?.group ?? this.groupId;
    const commitIntervalMs = params?.commitIntervalMs ?? 1000;

    const stream = Stream.async<Envelope<T>, never>((emit) => {
      const consumer = kafka.consumer({ groupId, maxWaitTimeInMs: 100 });
      const tracker = new OffsetTracker();
      let commitTimer: ReturnType<typeof setInterval> | undefined;

      const flushCommits = async () => {
        const committable = tracker.committable();
        if (committable.size === 0) return;

        const offsets = [...committable.entries()].map(([partition, offset]) => ({
          topic,
          partition,
          offset: offset.toString(),
        }));

        try {
          await consumer.commitOffsets(offsets);
        } catch {
          // Commit failed — will retry on next interval
        }
      };

      const run = async () => {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: false });

        // Periodic commit flush — batches acks for efficiency
        commitTimer = setInterval(flushCommits, commitIntervalMs);

        await consumer.run({
          autoCommit: false,
          eachMessage: async (payload: EachMessagePayload) => {
            const value = codec.decode(JSON.parse(payload.message.value!.toString()));
            const offset = Number(payload.message.offset);
            const partition = payload.partition;

            const envelope: Envelope<T> = {
              value,
              ack: async () => {
                // Mark as completed — tracker handles ordering
                tracker.complete(partition, offset);
              },
              nack: async () => {
                // Don't mark as completed — offset won't advance past this message.
                // On next commit, this offset stays uncommitted.
                // The message will be redelivered after consumer rebalance or restart.
              },
              metadata: {
                topic,
                partition,
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
        if (commitTimer) clearInterval(commitTimer);
        // Final flush before disconnect
        await flushCommits();
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
