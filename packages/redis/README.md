# @promin/redis

Redis Streams transport adapter. Consumer groups, manual ack, dead consumer reclaim — same API as any other transport.

## Usage

```typescript
import Redis from "ioredis";
import { RedisStream } from "@promin/redis";
import { StreamPipeline } from "@promin/core";

const redis = new Redis("redis://localhost:6379");

const events = new RedisStream<UserEvent>({
  redis,
  stream: "user-events",
  group: "event-processor",
});

// Create consumer group (idempotent)
await events.ensureGroup();

// Subscribe — auto-ack after processing
await StreamPipeline.fromSource(events)
  .filter((e) => e.type === "signup")
  .forEach(handleSignup);

// Manual ack — for at-least-once processing
await events.subscribeAck().forEach(async (envelope) => {
  await processEvent(envelope.value);
  await envelope.ack();
  // If you don't ack, the message stays in the PEL (pending entries list)
  // and can be reclaimed by another consumer
});

// Publish
await events.publish({ type: "signup", userId: "u_42" });

// Keyed publish
await events.publish(event, { key: event.userId });

// Reclaim messages from dead consumers
const stale = await events.claimPending({ minIdleMs: 30_000, count: 100 });
for (const msg of stale) {
  await processEvent(msg.value);
  await redis.xack("user-events", "event-processor", msg.id);
}

// Stream info
const info = await events.info();
console.log(`Stream length: ${info.length}, groups: ${info.groups}`);
```

## Typeclasses Implemented

| Typeclass       | Methods                                                 |
| --------------- | ------------------------------------------------------- |
| Streamable      | `subscribe()` — auto-ack via XREADGROUP                 |
| Sinkable        | `publish(value)` — XADD                                 |
| KeyedSinkable   | `publish(value, { key })` — XADD with key field         |
| Acknowledgeable | `subscribeAck()` → `envelope.ack()` — XREADGROUP + XACK |
