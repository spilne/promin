// ---------------------------------------------------------------------------
// KafkaTopic<T> — Kafka topic implementing streaming typeclasses
//
// Implements: Partitionable, Replayable, Acknowledgeable, KeyedSinkable, Checkpointable
//
// Works with any KafkaClient implementation:
//   - kafkajs / @confluentinc/kafka-javascript (callback-based consumer)
//   - @platformatic/kafka (stream-based consumer)
// ---------------------------------------------------------------------------

import { Chunk, Effect, Stream } from "effect";
import { OffsetTracker } from "./offset-tracker.ts";
import type {
  KafkaBatchPayload,
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
  /** Poll interval for subscribe. Default: 100ms. */
  pollIntervalMs?: number;
  /**
   * Use `consumer.run({ eachBatch })` for both subscribe paths when the
   * driver is callback-style (kafkajs, @confluentinc/kafka-javascript).
   * Emits one Stream chunk per Kafka FetchResponse so downstream
   * operators see fewer fiber-scheduling events — noticeable at
   * ≥10K msg/sec.
   *
   * Opt-in because `eachBatch` is mutually exclusive with `eachMessage`
   * in kafkajs; turning it on for a driver that doesn't implement
   * `eachBatch` would silently stall consumption. Stream-based drivers
   * (platformatic) ignore this flag — they always use `consumer.stream()`.
   *
   * Default: `false` (per-message eachMessage, unchanged behaviour).
   */
  batchEmit?: boolean;
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
  private readonly kafka: KafkaClient;
  private readonly topic: string;
  private readonly groupId: string;
  private readonly batchEmit: boolean;

  private consumer?: KafkaConsumer;
  private producer?: KafkaProducer;
  private _partitions?: number;

  constructor(config: KafkaTopicConfig<T>) {
    this.kafka = config.kafka;
    this.topic = config.topic;
    this.groupId = config.groupId;
    this.codec = config.codec ?? (JsonCodec as Codec<T>);
    this.batchEmit = config.batchEmit ?? false;
  }

  /** Partition count — fetched from broker on first access. */
  get partitions(): number {
    return this._partitions ?? 1;
  }

  /** Fetch and cache the partition count from the broker. */
  async fetchPartitions(): Promise<number> {
    if (this._partitions) return this._partitions;
    const admin = this.kafka.admin();
    await admin.connect();
    if (admin.fetchTopicPartitionCount) {
      this._partitions = await admin.fetchTopicPartitionCount(this.topic);
    }
    await admin.disconnect();
    return this._partitions ?? 1;
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

  subscribeAck(params?: {
    group?: string;
    commitIntervalMs?: number;
    fromBeginning?: boolean;
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
      let stopped = false;

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
          nack: async () => {},
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
        await consumer.subscribe({ topic, fromBeginning: params?.fromBeginning ?? false });

        commitTimer = setInterval(flushCommits, commitIntervalMs);

        if (consumer.stream) {
          // Platformatic-style per-message iteration — no native batch API.
          for await (const msg of consumer.stream()) {
            if (stopped) break;
            emit.single(makeEnvelope(msg));
          }
        } else if (consumer.run) {
          // Callback drivers: eachBatch (opt-in) or eachMessage. Passing
          // both to kafkajs is a ConfigurationError — they're mutually
          // exclusive — so commit to one based on the config flag.
          if (this.batchEmit) {
            await consumer.run({
              autoCommit: false,
              eachBatch: async (payload: KafkaBatchPayload) => {
                if (stopped) return;
                const b = payload.batch;
                if (b.messages.length === 0) return;
                const envelopes = b.messages.map((m) =>
                  makeEnvelope({ topic: b.topic, partition: b.partition, message: m }),
                );
                emit.chunk(Chunk.fromIterable(envelopes));
              },
            });
          } else {
            await consumer.run({
              autoCommit: false,
              eachMessage: async (msg: KafkaMessage) => {
                if (stopped) return;
                emit.single(makeEnvelope(msg));
              },
            });
          }
        }
      };

      run().catch(() => {});

      return Effect.promise(async () => {
        stopped = true;
        if (commitTimer) clearInterval(commitTimer);
        await flushCommits();
        await consumer.disconnect().catch(() => {});
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
    await consumer.commitOffsets([{ topic: this.topic, partition: 0, offset: params.offset }]);
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
      let stopped = false;

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

        if (consumer.stream) {
          // Platformatic stream mode — per-message, no batch API.
          for await (const msg of consumer.stream()) {
            if (stopped) break;
            emit.single(decodeMessage(msg));
          }
        } else if (consumer.run) {
          if (this.batchEmit) {
            await consumer.run({
              eachBatch: async (payload: KafkaBatchPayload) => {
                if (stopped) return;
                const b = payload.batch;
                if (b.messages.length === 0) return;
                const values = b.messages.map((m) =>
                  decodeMessage({ topic: b.topic, partition: b.partition, message: m }),
                );
                emit.chunk(Chunk.fromIterable(values));
              },
            });
          } else {
            await consumer.run({
              eachMessage: async (msg: KafkaMessage) => {
                if (stopped) return;
                emit.single(decodeMessage(msg));
              },
            });
          }
        }
      };

      run().catch(() => {});

      return Effect.promise(async () => {
        stopped = true;
        await consumer.disconnect().catch(() => {});
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
