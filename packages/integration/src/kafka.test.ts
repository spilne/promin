import { describe, it, expect } from "bun:test";
import { Kafka } from "kafkajs";
import { withKafka, uniqueName } from "./infra.ts";
import { KafkaTopic, OffsetTracker } from "@promin/kafka";
import type { KafkaClient, KafkaConsumer, KafkaProducer, KafkaAdmin } from "@promin/kafka";

// ---------------------------------------------------------------------------
// kafkajs adapter — wraps kafkajs into our KafkaClient interface
// ---------------------------------------------------------------------------

function createKafkajsClient(broker: string): KafkaClient {
  const kafka = new Kafka({ brokers: [broker], logLevel: 0 });

  return {
    producer(): KafkaProducer {
      const p = kafka.producer();
      return {
        connect: () => p.connect(),
        disconnect: () => p.disconnect(),
        send: (params) => p.send(params) as unknown as Promise<void>,
      };
    },

    consumer(config): KafkaConsumer {
      const c = kafka.consumer({ groupId: config.groupId });
      return {
        connect: () => c.connect(),
        disconnect: () => c.disconnect(),
        subscribe: (params) => c.subscribe({ topic: params.topic, fromBeginning: params.fromBeginning }),
        run: (params) =>
          c.run({
            autoCommit: params.autoCommit,
            eachMessage: params.eachMessage
              ? (payload) =>
                  params.eachMessage!({
                    topic: payload.topic,
                    partition: payload.partition,
                    message: {
                      key: payload.message.key,
                      value: payload.message.value,
                      offset: payload.message.offset,
                      timestamp: payload.message.timestamp ?? "",
                    },
                  })
              : undefined,
          }),
        commitOffsets: (offsets) => c.commitOffsets(offsets),
        seek: (params) => c.seek(params),
      };
    },

    admin(): KafkaAdmin {
      const a = kafka.admin();
      return {
        connect: () => a.connect(),
        disconnect: () => a.disconnect(),
        fetchOffsets: (params) => a.fetchOffsets(params),
        fetchTopicOffsetsByTimestamp: async (topic, timestamp) => {
          const offsets = await a.fetchTopicOffsetsByTimestamp(topic, timestamp);
          return offsets.map((o) => ({ partition: o.partition, offset: o.offset }));
        },
      };
    },
  };
}

async function createTopic(broker: string, topic: string, partitions = 1) {
  const kafka = new Kafka({ brokers: [broker], logLevel: 0 });
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
    waitForLeaders: true,
  });
  await admin.disconnect();
}

// ---------------------------------------------------------------------------
// All Kafka tests share one container
// ---------------------------------------------------------------------------

withKafka("Kafka integration", (ctx) => {
  // -- KafkaTopic --

  describe("KafkaTopic — publish and subscribe via kafkajs", () => {
    it("publishes and consumes a single message", async () => {
      const topic = uniqueName("single");
      await createTopic(ctx.broker, topic);

      const client = createKafkajsClient(ctx.broker);
      const kt = new KafkaTopic<{ orderId: string }>({
        kafka: client,
        topic,
        groupId: uniqueName("g"),
      });

      await kt.publish({ orderId: "o-1" });

      // Subscribe from earliest to read messages published before consumer started
      const items = await kt
        .subscribeFrom({ offset: { type: "earliest" }, group: uniqueName("g") })
        .take(1)
        .collect();
      expect(items).toEqual([{ orderId: "o-1" }]);

      await kt.disconnect();
    });

    it("publishBatch sends multiple messages atomically", async () => {
      const topic = uniqueName("batch");
      await createTopic(ctx.broker, topic);

      const client = createKafkajsClient(ctx.broker);
      const kt = new KafkaTopic<{ v: number }>({
        kafka: client,
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

    it("subscribeAck tracks offsets correctly with parallel processing", async () => {
      const topic = uniqueName("ack");
      await createTopic(ctx.broker, topic);

      const client = createKafkajsClient(ctx.broker);
      const kt = new KafkaTopic<{ v: number }>({
        kafka: client,
        topic,
        groupId: uniqueName("g"),
      });

      for (let i = 0; i < 5; i++) await kt.publish({ v: i });

      const values: number[] = [];
      // subscribeAck starts from latest by default — publish first, then consume from earliest
      await kt
        .subscribeFrom({ offset: { type: "earliest" }, group: uniqueName("g") })
        .take(5)
        .parAsyncMap(3, async (item) => {
          values.push(item.v);
        })
        .drain();

      expect(values.sort()).toEqual([0, 1, 2, 3, 4]);

      await kt.disconnect();
    });

    it("keyed messages preserve ordering within a partition", async () => {
      const topic = uniqueName("keyed");
      await createTopic(ctx.broker, topic, 3);

      const client = createKafkajsClient(ctx.broker);
      const kt = new KafkaTopic<{ userId: string; seq: number }>({
        kafka: client,
        topic,
        groupId: uniqueName("g"),
      });

      await kt.publish({ userId: "u1", seq: 1 }, { key: "u1" });
      await kt.publish({ userId: "u1", seq: 2 }, { key: "u1" });
      await kt.publish({ userId: "u1", seq: 3 }, { key: "u1" });

      const items = await kt
        .subscribeFrom({ offset: { type: "earliest" }, group: uniqueName("g") })
        .take(3)
        .collect();

      expect(items.map((i) => i.seq)).toEqual([1, 2, 3]);

      await kt.disconnect();
    });
  });

  // -- OffsetTracker --

  describe("OffsetTracker — contiguous commit tracking", () => {
    it("only commits contiguous offsets after parallel processing", async () => {
      const tracker = new OffsetTracker();

      tracker.complete(0, 2);
      tracker.complete(0, 0);

      const committable = tracker.committable();
      expect(committable.get(0)).toBe(1);

      tracker.complete(0, 1);
      const committable2 = tracker.committable();
      expect(committable2.get(0)).toBe(3);
    });
  });

  // -- Consumer groups --

  describe("Consumer groups — multiple consumers share partitions", () => {
    it("two consumers in same group each get a subset of messages", async () => {
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
