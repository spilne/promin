# @promin/kafka

Kafka transport adapter. Uses `@confluentinc/kafka-javascript` (official Confluent client, built on librdkafka). Implements all streaming typeclasses — use Kafka topics as sources, sinks, and shuffle targets with the same API as any other transport.

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
