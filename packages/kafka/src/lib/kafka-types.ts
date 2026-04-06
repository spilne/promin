// ---------------------------------------------------------------------------
// Kafka client types — driver-agnostic interface
//
// Works with any Kafka client:
//   - kafkajs (factory pattern: kafka.producer())
//   - @confluentinc/kafka-javascript (kafkajs-compatible)
//   - @platformatic/kafka (independent classes with stream-based consumer)
//
// The interface is stream-native: consumers return async iterables rather
// than requiring subscribe() + run({ eachMessage }) callbacks.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Client — entry point for creating producers, consumers, admins
// ---------------------------------------------------------------------------

export interface KafkaClient {
  producer(): KafkaProducer;
  consumer(config: { groupId: string }): KafkaConsumer;
  admin(): KafkaAdmin;
}

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

export interface KafkaProducer {
  /** Connect to the broker. No-op if already connected or auto-connecting. */
  connect(): Promise<void>;
  /** Disconnect from the broker. */
  disconnect(): Promise<void>;
  /** Send messages to a topic. */
  send(params: { topic: string; messages: KafkaOutgoingMessage[] }): Promise<void>;
}

export interface KafkaOutgoingMessage {
  key: string | null;
  value: string;
  headers?: Record<string, string>;
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

export interface KafkaConsumer {
  /** Connect to the broker. No-op if already connected or auto-connecting. */
  connect(): Promise<void>;
  /** Disconnect from the broker. */
  disconnect(): Promise<void>;

  /** Subscribe to a topic. */
  subscribe(params: { topic: string; fromBeginning?: boolean }): Promise<void>;

  /**
   * Start consuming messages. Two patterns supported:
   *
   * **Callback mode** (kafkajs-compatible):
   * ```ts
   * await consumer.run({ eachMessage: async (msg) => { ... } });
   * ```
   *
   * **Stream mode** (platformatic-compatible):
   * ```ts
   * const stream = consumer.stream();
   * for await (const msg of stream) { ... }
   * ```
   *
   * Implementations must support at least one. KafkaTopic uses
   * whichever is available, preferring stream mode.
   */
  run?(params: {
    autoCommit?: boolean;
    eachMessage?: (payload: KafkaMessage) => Promise<void>;
  }): Promise<void>;

  /** Async iterable of messages. Alternative to callback-based run(). */
  stream?(): AsyncIterable<KafkaMessage>;

  /** Commit offsets manually. */
  commitOffsets(offsets: KafkaOffsetCommit[]): Promise<void>;

  /** Seek to a specific offset (runtime). Optional — not all clients support this. */
  seek?(params: { topic: string; partition: number; offset: string }): void;
}

// ---------------------------------------------------------------------------
// Admin
// ---------------------------------------------------------------------------

export interface KafkaAdmin {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  fetchOffsets(params: { groupId: string; topics: string[] }): Promise<KafkaTopicOffsets[]>;
  fetchTopicOffsetsByTimestamp(topic: string, timestamp: number): Promise<KafkaPartitionOffset[]>;
  /** Fetch partition count for a topic. Optional — not all clients expose this. */
  fetchTopicPartitionCount?(topic: string): Promise<number>;
}

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

export interface KafkaMessage {
  topic: string;
  partition: number;
  message: {
    key: Buffer | string | null;
    value: Buffer | string | null;
    offset: string;
    timestamp: string;
    headers?: Record<string, Buffer | string>;
  };
}

export interface KafkaOffsetCommit {
  topic: string;
  partition: number;
  offset: string;
}

export interface KafkaTopicOffsets {
  topic: string;
  partitions: KafkaPartitionOffset[];
}

export interface KafkaPartitionOffset {
  partition: number;
  offset: string;
}
