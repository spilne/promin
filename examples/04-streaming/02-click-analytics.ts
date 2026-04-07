/**
 * Real-time click analytics — count clicks per user per minute.
 * StreamTopology handles windowing, keyed state, and checkpointing.
 */

import { StreamTopology, TopologyRunner } from "@promin/core";
import { KafkaTopic, type KafkaClient } from "@promin/kafka";

declare const kafka: KafkaClient;

interface ClickEvent {
  userId: string;
  page: string;
  ts: number;
}

const clicks = new KafkaTopic<ClickEvent>({ kafka, topic: "clicks", groupId: "analytics" });
const output = new KafkaTopic<{
  key: string;
  window: { start: number; end: number };
  count: number;
}>({
  kafka,
  topic: "click-counts",
  groupId: "counts-consumer",
});

const topology = StreamTopology.source(clicks)
  .filter((e) => e.page !== "/healthcheck")
  .keyBy((e) => e.userId)
  .tumbling(60_000)
  .count()
  .to(output);

const handle = await TopologyRunner.run(topology, {
  group: "click-analytics",
  checkpointIntervalMs: 30_000,
});

// Shutdown on SIGTERM
process.on("SIGTERM", () => handle.shutdown());
