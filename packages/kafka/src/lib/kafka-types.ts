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

/**
 * Consumer config. Beyond `groupId`, the timeout knobs guard the
 * slow-handler → rebalance-loop failure mode: if a message handler runs longer
 * than `maxPollInterval`, the broker considers the consumer dead and
 * rebalances → the message redelivers → you can get a reprocess loop that
 * never commits. Raise these when handlers do slow I/O (or bound the handler).
 * Fields are optional; drivers ignore knobs they don't support.
 */
export interface KafkaConsumerOptions {
  groupId: string;
  /** Max time (ms) a handler may run before the broker rebalances the group. */
  maxPollInterval?: number;
  /** Group session timeout (ms). */
  sessionTimeout?: number;
  /** Heartbeat interval (ms). */
  heartbeatInterval?: number;
}

export interface KafkaClient {
  producer(): KafkaProducer;
  consumer(config: KafkaConsumerOptions): KafkaConsumer;
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
   * Start consuming messages. Three patterns supported, in preference order:
   *
   * **Batch callback** (kafkajs-compatible, preferred for callback drivers):
   * ```ts
   * await consumer.run({ eachBatch: async ({ batch }) => { ... } });
   * ```
   * Lets KafkaTopic emit a whole Kafka batch as a single Stream chunk —
   * fewer fiber-scheduling events per message at high throughput.
   *
   * **Per-message callback** (kafkajs-compatible):
   * ```ts
   * await consumer.run({ eachMessage: async (msg) => { ... } });
   * ```
   * Fallback when the driver doesn't expose eachBatch.
   *
   * **Stream mode** (platformatic-compatible):
   * ```ts
   * const stream = consumer.stream();
   * for await (const msg of stream) { ... }
   * ```
   *
   * Implementations must support at least one. KafkaTopic picks based on
   * what the consumer actually exposes.
   */
  run?(params: {
    autoCommit?: boolean;
    eachMessage?: (payload: KafkaMessage) => Promise<void>;
    /**
     * Batch callback. Payload shape matches kafkajs's own:
     * `{ batch: { topic, partition, messages: [{key,value,offset,...}] } }`.
     * Drivers that don't support this simply ignore the param — their
     * run() still returns, having processed nothing, and KafkaTopic's
     * feature-detection (see kafka-topic.ts) never dispatches to
     * eachBatch on such drivers.
     */
    eachBatch?: (payload: KafkaBatchPayload) => Promise<void>;
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
  /** Create topics. Optional — used by KafkaShuffleTransport for repartition topics. */
  createTopics?(params: {
    topics: { topic: string; numPartitions: number; replicationFactor: number }[];
  }): Promise<void>;
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

/**
 * Shape of the kafkajs `eachBatch` callback payload, minus the helper
 * methods (`resolveOffset`, `heartbeat`, etc.) that KafkaTopic doesn't
 * use. Declared separately so drivers and users can type against the
 * same shape.
 */
export interface KafkaBatchPayload {
  readonly batch: {
    readonly topic: string;
    readonly partition: number;
    readonly messages: ReadonlyArray<KafkaMessage["message"]>;
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
