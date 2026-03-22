// ---------------------------------------------------------------------------
// KafkaTopic<T> — Kafka topic implementing streaming typeclasses
//
// Implements: Partitionable, Replayable, Acknowledgeable, KeyedSinkable, Checkpointable
//
// Works with any KafkaClient implementation:
//   - kafkajs / @confluentinc/kafka-javascript (callback-based consumer)
//   - @platformatic/kafka (stream-based consumer)
// ---------------------------------------------------------------------------

import { Effect, Stream } from "effect";
import { OffsetTracker } from "./offset-tracker.ts";
import type {
  KafkaClient,
  KafkaConsumer,
  KafkaProducer,
  KafkaMessage,
} from "./kafka-types.ts";
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
  /** Kafka client instance. */
  kafka: KafkaClient;
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
  private readonly kafka: KafkaClient;
  private readonly topic: string;
  private readonly groupId: string;

  private consumer?: KafkaConsumer;
  private producer?: KafkaProducer;

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

  async publishBatch(messages: { value: T; key?: string }[]): Promise<void> {
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
   * This prevents message loss on crash.
   *
   * Supports both stream-based (platformatic) and callback-based (kafkajs) consumers.
   */
  subscribeAck(params?: {
    group?: string;
    commitIntervalMs?: number;
  }): StreamPipeline<Envelope<T>, never> {
    const codec = this.codec;
    const kafka = this.kafka;
    const topic = this.topic;
    const groupId = params?.group ?? this.groupId;
    const commitIntervalMs = params?.commitIntervalMs ?? 1000;

    const stream = Stream.async<Envelope<T>, never>((emit) => {
      const consumer = kafka.consumer({ groupId });
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

      const makeEnvelope = (msg: KafkaMessage): Envelope<T> => {
        const raw = msg.message.value;
        const str = raw instanceof Buffer ? raw.toString() : (raw as string);
        const value = codec.decode(JSON.parse(str));
        const offset = Number(msg.message.offset);
        const partition = msg.partition;

        return {
          value,
          ack: async () => {
            tracker.complete(partition, offset);
          },
          nack: async () => {
            // Don't mark — offset won't advance, redelivered on restart
          },
          metadata: {
            topic: msg.topic,
            partition,
            offset: msg.message.offset,
            key: msg.message.key?.toString(),
            timestamp: msg.message.timestamp,
          },
        };
      };

      const run = async () => {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: false });

        commitTimer = setInterval(flushCommits, commitIntervalMs);

        // Prefer stream mode (platformatic), fall back to callback mode (kafkajs)
        if (consumer.stream) {
          for await (const msg of consumer.stream()) {
            emit.single(makeEnvelope(msg));
          }
        } else if (consumer.run) {
          await consumer.run({
            autoCommit: false,
            eachMessage: async (msg: KafkaMessage) => {
              emit.single(makeEnvelope(msg));
            },
          });
        } else {
          throw new Error("Consumer must implement either stream() or run()");
        }
      };

      run().catch(() => {});

      return Effect.promise(async () => {
        if (commitTimer) clearInterval(commitTimer);
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

      const decodeMessage = (msg: KafkaMessage): T => {
        const raw = msg.message.value;
        const str = raw instanceof Buffer ? raw.toString() : (raw as string);
        return codec.decode(JSON.parse(str));
      };

      const run = async () => {
        await consumer.connect();
        await consumer.subscribe({
          topic,
          fromBeginning: offset?.type === "earliest",
        });

        // Seek to timestamp if requested (requires admin + seek support)
        if (offset?.type === "timestamp") {
          const admin = kafka.admin();
          await admin.connect();
          const result = await admin.fetchTopicOffsetsByTimestamp(topic, offset.value);
          await admin.disconnect();
          if (consumer.seek) {
            for (const p of result) {
              consumer.seek({ topic, partition: p.partition, offset: p.offset });
            }
          }
        }

        // Prefer stream mode, fall back to callback mode
        if (consumer.stream) {
          for await (const msg of consumer.stream()) {
            emit.single(decodeMessage(msg));
          }
        } else if (consumer.run) {
          await consumer.run({
            eachMessage: async (msg: KafkaMessage) => {
              emit.single(decodeMessage(msg));
            },
          });
        } else {
          throw new Error("Consumer must implement either stream() or run()");
        }
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
