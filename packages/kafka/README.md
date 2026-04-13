# @promin/kafka

Kafka transport adapter with a driver-agnostic `KafkaClient` interface. Works with any Kafka client library — `@confluentinc/kafka-javascript`, `kafkajs`, `@platformatic/kafka`, or your own implementation. Implements all streaming typeclasses — use Kafka topics as sources, sinks, and shuffle targets with the same API as any other transport.

## Usage

```typescript
import { Kafka } from "@confluentinc/kafka-javascript/kafkajs";
import { KafkaTopic } from "@promin/kafka";
import { StreamPipeline, trigger } from "@promin/core";

const kafka = new Kafka({ brokers: ["localhost:9092"] });

const orderEvents = new KafkaTopic<OrderEvent>({
  kafka,
  topic: "order-events",
  groupId: "order-processor",
});

// Subscribe — same API as PgQueue, MemoryStream, or any Streamable
await StreamPipeline.fromSource(orderEvents)
  .filter((e) => e.type === "order.placed")
  .through(trigger({ workflow: processOrder, ... }))
  .drain();

// Publish
await orderEvents.publish({ type: "order.placed", orderId: "123" });

// Keyed publish (routes to partition by key)
await orderEvents.publish(event, { key: event.userId });

// Manual ack
await orderEvents.subscribeAck()
  .forEach(async (envelope) => {
    await processEvent(envelope.value);
    await envelope.ack();
  });

// Replay from timestamp
await orderEvents.subscribeFrom({ offset: { type: "timestamp", value: Date.now() - 3600_000 } })
  .forEach(handleEvent);

// Offset management
await orderEvents.commitOffset({ group: "my-group", offset: "42" });
const offset = await orderEvents.getCommittedOffset({ group: "my-group" });
```

## Typeclasses Implemented

| Typeclass       | Methods                                                 |
| --------------- | ------------------------------------------------------- |
| Partitionable   | `subscribe({ partitions })`, `.partitions`              |
| Replayable      | `subscribeFrom({ offset })`                             |
| Acknowledgeable | `subscribeAck()` → `envelope.ack()` / `envelope.nack()` |
| KeyedSinkable   | `publish(value, { key })`                               |
| Checkpointable  | `commitOffset()`, `getCommittedOffset()`                |

## Configuration

`KafkaTopicConfig<T>` accepts the following options:

| Option           | Type          | Default     | Description                                                                                                           |
| ---------------- | ------------- | ----------- | --------------------------------------------------------------------------------------------------------------------- |
| `kafka`          | `KafkaClient` | (required)  | Any Kafka client implementing `KafkaClient` interface (kafkajs, @confluentinc/kafka-javascript, @platformatic/kafka). |
| `topic`          | `string`      | (required)  | Kafka topic name.                                                                                                     |
| `groupId`        | `string`      | (required)  | Consumer group ID for subscribing.                                                                                    |
| `codec`          | `Codec<T>`    | `JsonCodec` | Serialization codec. Override for Avro, Protobuf, etc.                                                                |
| `pollIntervalMs` | `number`      | `100`       | Poll interval for the consumer loop.                                                                                  |

```typescript
import { Kafka } from "@confluentinc/kafka-javascript/kafkajs";
import { KafkaTopic } from "@promin/kafka";

const kafka = new Kafka({
  brokers: ["broker1:9092", "broker2:9092"],
  ssl: true,
  sasl: { mechanism: "plain", username: "user", password: "pass" },
});

const topic = new KafkaTopic<OrderEvent>({
  kafka,
  topic: "order-events",
  groupId: "order-processor",
  pollIntervalMs: 50,
});
```

## Consumer Groups

Each `KafkaTopic` is bound to a `groupId`. When multiple instances subscribe with the same group ID, Kafka distributes partitions across them:

- **Partition assignment**: Kafka's coordinator assigns partitions to consumers in the group. Each partition is consumed by exactly one consumer in the group.
- **Rebalancing**: When a consumer joins or leaves the group, Kafka triggers a rebalance. During rebalance, consumption pauses briefly while partitions are reassigned.
- **Scaling**: Add more consumers (up to the number of partitions) to increase throughput. Consumers beyond the partition count sit idle.
- **Override group per call**: Both `subscribe()` and `subscribeAck()` accept an optional `group` parameter to use a different group ID than the one in the config.

```typescript
// Two processes with the same groupId share partitions
const consumer1 = new KafkaTopic<Event>({ kafka, topic: "events", groupId: "my-group" });
const consumer2 = new KafkaTopic<Event>({ kafka, topic: "events", groupId: "my-group" });

// Independent group — receives all messages independently
const auditor = new KafkaTopic<Event>({ kafka, topic: "events", groupId: "audit-group" });
```

## Keyed Publishing

Publishing with a key routes the message to a specific partition based on the key hash. All messages with the same key land on the same partition, guaranteeing ordering per key.

```typescript
// All events for user "u_42" go to the same partition — ordered
await topic.publish(event, { key: event.userId });

// Batch publish for throughput
await topic.publishBatch([
  { value: event1, key: event1.userId },
  { value: event2, key: event2.userId },
  { value: event3 }, // no key — round-robin partition
]);
```

Ordering guarantees:

- Messages with the same key are always on the same partition and consumed in order.
- Messages without a key are distributed round-robin across partitions.
- Ordering is per-partition only. Cross-partition ordering is not guaranteed.

## Error Handling

- **Broker unavailable**: `publish()` and `subscribe()` reject with the driver's connection error. The Confluent client retries internally based on its configuration (`retries`, `retry.backoff.ms`).
- **Message processing failures**: With `subscribeAck()`, if your handler throws before calling `envelope.ack()`, the offset is not committed. On the next rebalance or restart, the message will be redelivered (at-least-once semantics).
- **Auto-commit vs manual**: `subscribe()` uses auto-commit. `subscribeAck()` disables auto-commit and flushes offsets at a configurable interval (`commitIntervalMs`, default 1000ms). Failed commits are silently retried on the next interval.
- **Consumer crash**: Uncommitted offsets are lost. The consumer group rebalances and another consumer picks up the partition from the last committed offset.

## Shuffle Transport

`KafkaShuffleTransport` implements the `ShuffleTransport` interface for distributed stream topology (repartitioning). It creates Kafka topics as repartition channels, allowing data to be shuffled by key across workers.

```typescript
import { KafkaShuffleTransport } from "@promin/kafka";

const shuffle = new KafkaShuffleTransport({ kafka, partitions: 12 });

// Used internally by topology engine — creates a topic, returns source + sink
const channel = await shuffle.getOrCreateRepartitionChannel({
  name: "repartition-by-user",
  group: "topology-worker",
  codec: JsonCodec,
});

// Publish keyed data to the repartition topic
await channel.sink.publish(event, { key: event.userId });

// Consume repartitioned data
channel.source.subscribe().forEach(processEvent);

// Cleanup all managed topics
await shuffle.disconnect();
```

Configuration:

- `kafka` — Kafka client instance.
- `partitions` — Number of partitions for repartition topics (default: 6). Higher values allow more parallelism.

The transport auto-creates topics via the admin client if they do not exist.

## Cleanup / Shutdown

Call `disconnect()` to gracefully shut down the producer and consumer:

```typescript
await topic.disconnect();
```

For `subscribeAck()`, the stream finalizer automatically:

1. Stops consuming new messages.
2. Flushes any pending offset commits.
3. Disconnects the consumer.

When using `KafkaShuffleTransport`, call `disconnect()` to clean up all managed topic connections:

```typescript
await shuffle.disconnect();
```

For graceful shutdown in a process handler:

```typescript
process.on("SIGTERM", async () => {
  await topic.disconnect();
  await shuffle.disconnect();
  process.exit(0);
});
```
