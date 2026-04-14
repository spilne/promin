# @promin/topology

Stateful stream processing — keyed state, time windows, joins, distributed shuffle.

## Install

```bash
bun add @promin/topology
```

## Quick Example

```typescript
import { StreamTopology, TopologyRunner } from "@promin/topology";

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

Build a declarative processing DAG from a message source. Partition by key, apply time windows, aggregate, join streams, and sink results. State is checkpointed periodically and restored on crash or rebalance.

## Features

- **Keyed state** — per-key stateful processing with typed state
- **Time windows** — tumbling, sliding, and session windows
- **Stream joins** — join multiple streams by key and time
- **Distributed shuffle** — repartition data across instances with `.shuffle()`
- **Checkpointing** — periodic state snapshots, automatic recovery
- **Deduplication** — exactly-once processing semantics

## Documentation

Full docs and examples: [packages/topology](https://github.com/spilne/promin/tree/main/packages/topology)
