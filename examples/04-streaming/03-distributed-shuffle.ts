/**
 * Distributed stream processing with shuffle.
 * Multiple instances share partitions via consumer groups.
 * Shuffle ensures same-key events land on the same instance.
 */

import { StreamTopology, DistributedRunner } from "@promin/topology";
import { KafkaTopic, KafkaShuffleTransport, type KafkaClient } from "@promin/kafka";

declare const kafka: KafkaClient;

interface Order {
  userId: string;
  amount: number;
  ts: number;
}

const orders = new KafkaTopic<Order>({ kafka, topic: "orders", groupId: "revenue" });
const output = new KafkaTopic<{ key: string; window: { start: number; end: number }; sum: number }>(
  {
    kafka,
    topic: "revenue-per-user",
    groupId: "revenue-consumer",
  },
);

const topology = StreamTopology.source(orders)
  .keyBy((e) => e.userId)
  .shuffle() // repartition by userId — correct multi-instance aggregation
  .tumbling(300_000) // 5-minute windows
  .sum((e) => e.amount)
  .to(output);

// Run distributed — each instance processes its assigned partitions
await DistributedRunner.run(topology, {
  group: "revenue-counter",
  shuffleTransport: new KafkaShuffleTransport({ kafka }),
});
