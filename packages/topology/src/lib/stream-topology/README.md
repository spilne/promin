# StreamTopology

Stateful stream processing with windows, joins, deduplication, checkpointing, and distributed shuffle — like Kafka Streams / Flink, with a builder API.

## Main Idea

Build a declarative processing DAG from a message source. Partition by key, apply time windows, aggregate, join streams, and sink results. State is checkpointed periodically and restored on crash/rebalance. Add `.shuffle()` to repartition data for correct multi-instance stateful processing.

## Examples

### Tumbling window count

```typescript
import { StreamTopology, TopologyRunner } from "@promin/core";

const topology = StreamTopology.source(clickEvents)
  .filter((e) => e.type !== "bot")
  .keyBy((e) => e.userId)
  .tumbling(60_000)
  .count()
  .to(outputTopic);

await TopologyRunner.run(topology, {
  group: "click-counter",
  checkpointIntervalMs: 10_000,
});
```

### Stateful per-key processing

```typescript
const topology = StreamTopology.source(orders)
  .keyBy((e) => e.customerId)
  .process<{ total: number }, { customerId: string; runningTotal: number }>({
    init: () => ({ total: 0 }),
    process: (state, order) => ({
      state: { total: state.total + order.amount },
      emit: { customerId: order.customerId, runningTotal: state.total + order.amount },
    }),
  })
  .to(totals);
```

### Stream-stream join

```typescript
const orders = StreamTopology.source(orderEvents).keyBy((e) => e.orderId);
const payments = StreamTopology.source(paymentEvents).keyBy((e) => e.orderId);

const topology = orders
  .join(payments, { windowMs: 30_000 })
  .map(({ left, right }) => ({
    orderId: left.orderId,
    amount: left.amount,
    paidAt: right.timestamp,
  }))
  .to(matchedOrders);
```

### Distributed execution with shuffle

```typescript
import { DistributedRunner } from "@promin/core";
import { KafkaShuffleTransport } from "@promin/kafka";

const topology = StreamTopology.source(rawEvents)
  .map((e) => normalize(e))
  .keyBy((e) => e.userId)
  .shuffle() // repartition via Kafka topic
  .tumbling(60_000)
  .count()
  .to(output);

// Run distributed — shuffle creates repartition topics
await DistributedRunner.run(topology, {
  group: "counter",
  shuffleTransport: new KafkaShuffleTransport({ kafka }),
});

// Same topology works single-process (shuffle is no-op)
await TopologyRunner.run(topology, { group: "counter" });
```

### Topology analyzer

```typescript
import { analyzeTopology } from "@promin/core";

const warnings = analyzeTopology(topology.compiled);
// Warns: "aggregate" follows keyBy without .shuffle().
// In distributed mode, events for the same key may arrive at different instances.
```

## Window Types

Windows group events by time for aggregation. Apply a window after `.keyBy()` to get a `WindowedTopology`, then use `.count()`, `.sum()`, or `.aggregate()`.

### Tumbling window

Fixed-size, non-overlapping. Each event belongs to exactly one window.

```typescript
// Count clicks per user every 60 seconds
const topology = StreamTopology.source(clickEvents)
  .keyBy((e) => e.userId)
  .tumbling(60_000)
  .count()
  .to(outputTopic);
```

### Sliding window

Fixed-size, overlapping. Windows advance by `slideMs`, so events can appear in multiple windows.

```typescript
// Average request latency over 5-minute windows, sliding every 1 minute
const topology = StreamTopology.source(requestEvents)
  .keyBy((e) => e.endpoint)
  .sliding({ windowMs: 300_000, slideMs: 60_000 })
  .aggregate({
    init: () => ({ sum: 0, count: 0 }),
    add: (state, req) => ({ sum: state.sum + req.latencyMs, count: state.count + 1 }),
    emit: (key, window, state) => ({
      endpoint: key,
      window,
      avgLatency: state.sum / state.count,
    }),
  })
  .to(metricsOutput);
```

### Session window

Dynamic windows that close after an inactivity gap. Events within the gap extend the session.

```typescript
// Group user activity into sessions with a 30-minute inactivity gap
const topology = StreamTopology.source(userActivity)
  .keyBy((e) => e.userId)
  .session(1_800_000)
  .aggregate({
    init: () => ({ events: 0 }),
    add: (state) => ({ events: state.events + 1 }),
    emit: (key, window, state) => ({
      userId: key,
      sessionStart: window.start,
      sessionEnd: window.end,
      eventCount: state.events,
    }),
  })
  .to(sessionsOutput);
```

### Window aggregation methods

All window types support three aggregation methods:

```typescript
// .count() — count events per key per window
.tumbling(60_000).count()
// Emits: { key, window: { start, end }, count }

// .sum(fn) — sum a numeric field per key per window
.tumbling(60_000).sum((e) => e.amount)
// Emits: { key, window: { start, end }, sum }

// .aggregate(spec) — custom aggregation with init/add/emit
.tumbling(60_000).aggregate({
  init: () => initialState,
  add: (state, value) => newState,
  emit: (key, window, state) => outputRecord,
})
```

## Features

- **Windows**: tumbling, sliding, session
- **Aggregation**: count, sum, custom aggregate
- **Stateful processing**: per-key state with `process()`
- **Joins**: stream-stream joins within time windows
- **Deduplication**: LRU-bounded dedupe by key
- **Checkpointing**: periodic state snapshots for crash recovery
- **Backpressure**: configurable buffer limits and rate limiting
- **Shuffle**: repartition by key for distributed stateful processing
- **Stage planner**: splits topology at shuffle boundaries into independent stages
- **Topology analyzer**: static analysis for missing shuffles
- **Operator fusion**: adjacent map/filter ops fused via `Stream.mapChunks`

## Use Cases

- **Real-time analytics** — count clicks per user per minute, aggregate revenue per region
- **Event enrichment** — join events with reference data streams
- **Deduplication** — remove duplicate events across distributed sources
- **Session analysis** — group user activity into sessions by inactivity gap
- **Alerting** — detect anomalies in sliding windows (e.g., error rate > threshold)
- **Multi-instance processing** — shuffle ensures correctness when scaling horizontally
