// ---------------------------------------------------------------------------
// Kafka client types — compatible with kafkajs and @confluentinc/kafka-javascript
//
// These types match the kafkajs API surface we use. Works with any
// kafkajs-compatible client (kafkajs, @confluentinc/kafka-javascript, etc).
// ---------------------------------------------------------------------------

export interface KafkaClient {
  producer(): KafkaProducer;
  consumer(config: { groupId: string; maxWaitTimeInMs?: number }): KafkaConsumer;
  admin(): KafkaAdmin;
}

export interface KafkaProducer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(params: {
    topic: string;
    messages: { key: string | null; value: string }[];
  }): Promise<unknown>;
}

export interface KafkaConsumer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(params: { topic: string; fromBeginning?: boolean }): Promise<void>;
  run(params: {
    autoCommit?: boolean;
    eachMessage?: (payload: EachMessagePayload) => Promise<void>;
  }): Promise<void>;
  commitOffsets(offsets: { topic: string; partition: number; offset: string }[]): Promise<void>;
  seek(params: { topic: string; partition: number; offset: string }): void;
}

export interface KafkaAdmin {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  fetchOffsets(params: { groupId: string; topics: string[] }): Promise<
    { topic: string; partitions: { partition: number; offset: string }[] }[]
  >;
  fetchTopicOffsetsByTimestamp(topic: string, timestamp: number): Promise<
    { partition: number; offset: string }[]
  >;
}

export interface EachMessagePayload {
  topic: string;
  partition: number;
  message: {
    key: Buffer | null;
    value: Buffer | null;
    offset: string;
    timestamp: string;
  };
}
