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
