// ---------------------------------------------------------------------------
// Unit tests for KafkaTopic batch emission — uses a fake KafkaClient that
// invokes our consumer callback with canned batches, so we can assert
// ordering and that the right callback (eachBatch vs eachMessage) was
// dispatched without booting a real broker.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { KafkaTopic } from "./kafka-topic.ts";
import type {
  KafkaAdmin,
  KafkaBatchPayload,
  KafkaClient,
  KafkaConsumer,
  KafkaMessage,
  KafkaProducer,
} from "./kafka-types.ts";

/**
 * Fake client that simulates a kafkajs-style consumer. The consumer's
 * `run()` receives the user's callbacks; the fake picks whichever is set
 * and invokes it with canned data, mirroring how kafkajs dispatches based
 * on whether `eachMessage` or `eachBatch` was provided.
 *
 * `batches` is the data to replay. `dispatchedVia` records whether the
 * code under test asked for batch or message mode — so tests can assert
 * the batchEmit flag actually routes correctly.
 */
function makeFakeKafka(opts: {
  topic: string;
  batches: ReadonlyArray<ReadonlyArray<{ value: unknown; offset: string; key?: string }>>;
}): {
  client: KafkaClient;
  dispatchedVia: { current: "eachMessage" | "eachBatch" | null };
} {
  const dispatchedVia: { current: "eachMessage" | "eachBatch" | null } = { current: null };

  const consumer: KafkaConsumer = {
    async connect() {},
    async disconnect() {},
    async subscribe() {},
    async commitOffsets() {},
    async run(params) {
      // kafkajs-style dispatch: use whichever callback is set. If both,
      // kafkajs would throw — we assert the code under test passes ONE,
      // not both.
      const hasMessage = typeof params.eachMessage === "function";
      const hasBatch = typeof params.eachBatch === "function";
      if (hasMessage && hasBatch) {
        throw new Error(
          "KafkaTopic passed both eachMessage and eachBatch — kafkajs would reject this as a ConfigurationError",
        );
      }
      if (hasBatch) {
        dispatchedVia.current = "eachBatch";
        for (let partition = 0; partition < opts.batches.length; partition++) {
          const batch = opts.batches[partition]!;
          const payload: KafkaBatchPayload = {
            batch: {
              topic: opts.topic,
              partition,
              messages: batch.map((m) => ({
                key: m.key ?? null,
                value: JSON.stringify(m.value),
                offset: m.offset,
                timestamp: "0",
              })),
            },
          };
          await params.eachBatch!(payload);
        }
      } else if (hasMessage) {
        dispatchedVia.current = "eachMessage";
        for (let partition = 0; partition < opts.batches.length; partition++) {
          const batch = opts.batches[partition]!;
          for (const m of batch) {
            const msg: KafkaMessage = {
              topic: opts.topic,
              partition,
              message: {
                key: m.key ?? null,
                value: JSON.stringify(m.value),
                offset: m.offset,
                timestamp: "0",
              },
            };
            await params.eachMessage!(msg);
          }
        }
      }
      // Block forever so the Stream keeps running until take() resolves.
      await new Promise(() => {});
    },
  };

  const producer: KafkaProducer = {
    async connect() {},
    async disconnect() {},
    async send() {},
  };
  const admin: KafkaAdmin = {
    async connect() {},
    async disconnect() {},
    async fetchOffsets() {
      return [];
    },
    async fetchTopicOffsetsByTimestamp() {
      return [];
    },
  };

  const client: KafkaClient = {
    producer: () => producer,
    consumer: () => consumer,
    admin: () => admin,
  };

  return { client, dispatchedVia };
}

describe("KafkaTopic — batchEmit flag", () => {
  it("default (batchEmit: false) uses eachMessage — backwards compatible", async () => {
    const { client, dispatchedVia } = makeFakeKafka({
      topic: "orders",
      batches: [
        [
          { value: { n: 1 }, offset: "0" },
          { value: { n: 2 }, offset: "1" },
        ],
      ],
    });

    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: "orders",
      groupId: "g",
    });

    const values = await topic.subscribe().take(2).collect();
    expect(values.map((v) => v.n)).toEqual([1, 2]);
    expect(dispatchedVia.current).toBe("eachMessage");
  });

  it("batchEmit: true uses eachBatch and emits a whole Kafka batch as one chunk", async () => {
    const { client, dispatchedVia } = makeFakeKafka({
      topic: "orders",
      batches: [
        [
          { value: { n: 10 }, offset: "0" },
          { value: { n: 20 }, offset: "1" },
          { value: { n: 30 }, offset: "2" },
          { value: { n: 40 }, offset: "3" },
        ],
      ],
    });

    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: "orders",
      groupId: "g",
      batchEmit: true,
    });

    const values = await topic.subscribe().take(4).collect();
    expect(values.map((v) => v.n)).toEqual([10, 20, 30, 40]);
    expect(dispatchedVia.current).toBe("eachBatch");
  });

  it("subscribeAck with batchEmit: true delivers envelopes in order", async () => {
    const { client, dispatchedVia } = makeFakeKafka({
      topic: "orders",
      batches: [
        [
          { value: { n: 1 }, offset: "0" },
          { value: { n: 2 }, offset: "1" },
        ],
      ],
    });

    const topic = new KafkaTopic<{ n: number }>({
      kafka: client,
      topic: "orders",
      groupId: "g",
      batchEmit: true,
    });

    const envelopes = await topic.subscribeAck().take(2).collect();
    expect(envelopes.map((e) => e.value.n)).toEqual([1, 2]);
    expect(envelopes[0]!.metadata?.offset).toBe("0");
    expect(envelopes[1]!.metadata?.offset).toBe("1");
    expect(dispatchedVia.current).toBe("eachBatch");
  });

  it("never passes BOTH eachBatch and eachMessage to the driver", async () => {
    // Regression guard: kafkajs throws a ConfigurationError when both are
    // set, so our code must commit to exactly one per run() call. The fake
    // client's run() throws if both are passed — so this test also fails
    // loudly if we ever regress.
    const both = makeFakeKafka({
      topic: "t",
      batches: [[{ value: { n: 1 }, offset: "0" }]],
    });
    const bothTopic = new KafkaTopic<{ n: number }>({
      kafka: both.client,
      topic: "t",
      groupId: "g",
      batchEmit: true,
    });
    await bothTopic.subscribe().take(1).collect();
    expect(both.dispatchedVia.current).toBe("eachBatch");

    const single = makeFakeKafka({
      topic: "t",
      batches: [[{ value: { n: 1 }, offset: "0" }]],
    });
    const singleTopic = new KafkaTopic<{ n: number }>({
      kafka: single.client,
      topic: "t",
      groupId: "g",
      batchEmit: false,
    });
    await singleTopic.subscribe().take(1).collect();
    expect(single.dispatchedVia.current).toBe("eachMessage");
  });
});
