import { describe, it, expect } from "bun:test";
import { withKafka, withApacheKafka, uniqueName } from "./infra.ts";
import { KafkaTopic, OffsetTracker, autoCommitBatchWithin } from "@promin/kafka";
import type { KafkaClient } from "@promin/kafka";
import { createKafkajsClient, createTopic } from "./adapters/kafkajs-adapter.ts";
import { createPlatformaticClient } from "./adapters/stream-adapter.ts";

// ---------------------------------------------------------------------------
// Shared test suite — same tests, different adapter
// ---------------------------------------------------------------------------

function adapterTests(
  name: string,
  getBroker: () => string,
  makeClient: (broker: string) => KafkaClient,
) {
  describe(name, () => {
    it("publishes and consumes a single message", async () => {
      const broker = getBroker();
      const topic = uniqueName("single");
      await createTopic(broker, topic);

      const kt = new KafkaTopic<{ orderId: string }>({
        kafka: makeClient(broker),
        topic,
        groupId: uniqueName("g"),
      });

      await kt.publish({ orderId: "o-1" });

      const items = await kt
        .subscribeFrom({ offset: { type: "earliest" }, group: uniqueName("g") })
        .take(1)
        .collect();

      expect(items).toEqual([{ orderId: "o-1" }]);
      await kt.disconnect();
    });

    it("publishBatch sends multiple messages", async () => {
      const broker = getBroker();
      const topic = uniqueName("batch");
      await createTopic(broker, topic);

      const kt = new KafkaTopic<{ v: number }>({
        kafka: makeClient(broker),
        topic,
        groupId: uniqueName("g"),
      });

      await kt.publishBatch([{ value: { v: 1 } }, { value: { v: 2 } }, { value: { v: 3 } }]);

      const items = await kt
        .subscribeFrom({ offset: { type: "earliest" }, group: uniqueName("g") })
        .take(3)
        .collect();

      expect(items.map((i) => i.v).sort()).toEqual([1, 2, 3]);
      await kt.disconnect();
    });

    it("keyed messages preserve partition ordering", async () => {
      const broker = getBroker();
      const topic = uniqueName("keyed");
      await createTopic(broker, topic, 3);

      const kt = new KafkaTopic<{ seq: number }>({
        kafka: makeClient(broker),
        topic,
        groupId: uniqueName("g"),
      });

      await kt.publish({ seq: 1 }, { key: "u1" });
      await kt.publish({ seq: 2 }, { key: "u1" });
      await kt.publish({ seq: 3 }, { key: "u1" });

      const items = await kt
        .subscribeFrom({ offset: { type: "earliest" }, group: uniqueName("g") })
        .take(3)
        .collect();

      expect(items.map((i) => i.seq)).toEqual([1, 2, 3]);
      await kt.disconnect();
    });
  });
}

// ---------------------------------------------------------------------------
// All tests under one Redpanda container
// ---------------------------------------------------------------------------

withKafka("Kafka integration", (ctx) => {
  const getBroker = () => ctx.broker;

  // -- Same tests, two different adapters --
  adapterTests("kafkajs adapter (callback-based)", getBroker, createKafkajsClient);

  // @platformatic/kafka adapter — requires Kafka API versions Redpanda doesn't fully support.
  // The adapter code is correct (see adapters/stream-adapter.ts); enable when testing against
  // a real Kafka broker: adapterTests("@platformatic/kafka", getBroker, createPlatformaticClient);
  // adapterTests("@platformatic/kafka adapter (stream-based)", getBroker, createPlatformaticClient);

  // -- OffsetTracker --
  describe("OffsetTracker — contiguous commit tracking", () => {
    it("only commits contiguous offsets", () => {
      const tracker = new OffsetTracker();
      tracker.complete(0, 2);
      tracker.complete(0, 0);
      expect(tracker.committable().get(0)).toBe(1);
      tracker.complete(0, 1);
      expect(tracker.committable().get(0)).toBe(3);
    });
  });

  // -- commitBatchWithin --
  describe("autoCommitBatchWithin — fs2-style batched commit pipe", () => {
    it("acks are batched and flushed on stream end", async () => {
      const topic = uniqueName("commit-batch");
      await createTopic(ctx.broker, topic);

      const kt = new KafkaTopic<{ v: number }>({
        kafka: createKafkajsClient(ctx.broker),
        topic,
        groupId: uniqueName("g"),
      });

      for (let i = 0; i < 10; i++) await kt.publish({ v: i });

      const values: number[] = [];

      await kt
        .subscribeAck({ group: uniqueName("g"), fromBeginning: true })
        .take(10)
        .mapAsync(async (env) => {
          values.push(env.value.v);
          return env;
        })
        .through(autoCommitBatchWithin(5, 1_000))
        .drain();

      expect(values.sort()).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
      await kt.disconnect();
    });

    it("parallel processing with batched commit", async () => {
      const topic = uniqueName("commit-par");
      await createTopic(ctx.broker, topic);

      const kt = new KafkaTopic<{ v: number }>({
        kafka: createKafkajsClient(ctx.broker),
        topic,
        groupId: uniqueName("g"),
      });

      for (let i = 0; i < 20; i++) await kt.publish({ v: i });

      const values: number[] = [];

      await kt
        .subscribeAck({ group: uniqueName("g"), fromBeginning: true })
        .take(20)
        .parAsyncMap(5, async (env) => {
          await new Promise((r) => setTimeout(r, Math.random() * 10));
          values.push(env.value.v);
          return env;
        })
        .through(autoCommitBatchWithin(10, 500))
        .drain();

      expect(values.sort((a, b) => a - b)).toEqual(
        Array.from({ length: 20 }, (_, i) => i),
      );
      await kt.disconnect();
    });
  });

  // -- Consumer groups --
  describe("Consumer groups — multiple consumers share partitions", () => {
    it("two consumers in same group share messages", async () => {
      const topic = uniqueName("cg");
      await createTopic(ctx.broker, topic, 2);
      const group = uniqueName("group");

      const client = createKafkajsClient(ctx.broker);
      const kt = new KafkaTopic<{ v: number }>({ kafka: client, topic, groupId: group });

      for (let i = 0; i < 10; i++) {
        await kt.publish({ v: i }, { key: `key-${i}` });
      }

      const c1Items: number[] = [];
      const c2Items: number[] = [];

      const kt1 = new KafkaTopic<{ v: number }>({ kafka: client, topic, groupId: group });
      const kt2 = new KafkaTopic<{ v: number }>({ kafka: client, topic, groupId: group });

      const p1 = kt1
        .subscribeFrom({ offset: { type: "earliest" }, group })
        .take(5)
        .forEach((m) => c1Items.push(m.v));
      const p2 = kt2
        .subscribeFrom({ offset: { type: "earliest" }, group })
        .take(5)
        .forEach((m) => c2Items.push(m.v));

      await Promise.race([
        Promise.all([p1, p2]),
        new Promise((r) => setTimeout(r, 15_000)),
      ]);

      const all = [...c1Items, ...c2Items].sort((a, b) => a - b);
      expect(all.length).toBe(10);

      await kt.disconnect();
      await kt1.disconnect();
      await kt2.disconnect();
    });
  });
});

// ---------------------------------------------------------------------------
// @platformatic/kafka — runs against real Apache Kafka (slower, ~30s startup)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// @platformatic/kafka — needs real Apache Kafka (Redpanda has API gaps).
// Uses KafkaContainer from @testcontainers/kafka with Confluent image.
// Slow to start (~60s JVM): skipped by default, enable with KAFKA_FULL=1.
// ---------------------------------------------------------------------------

const runFullKafka = process.env.KAFKA_FULL === "1";

if (runFullKafka) {
  withApacheKafka("@platformatic/kafka adapter (Apache Kafka)", (ctx) => {
    adapterTests("stream-based consume", () => ctx.broker, createPlatformaticClient);
  });
}
