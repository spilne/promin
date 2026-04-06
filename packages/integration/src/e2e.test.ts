import { it, expect, setDefaultTimeout } from "bun:test";

setDefaultTimeout(300_000);
import { Redis as IoRedis } from "ioredis";
import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";
import { Kafka } from "kafkajs";
import { withAll, uniqueName } from "./infra.ts";
import { KafkaTopic } from "@promin/kafka";
import type { KafkaClient, KafkaConsumer, KafkaProducer, KafkaAdmin } from "@promin/kafka";
import {
  RedisStateBackend,
  RedisCacheStore,
  type RedisClient as RedisClientType,
} from "@promin/redis";
import { PgStateBackend, PgStepQueue } from "@promin/postgres";
import type { DrizzleDb } from "@promin/postgres";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function kafkaClient(broker: string): KafkaClient {
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
        subscribe: (params) =>
          c.subscribe({ topic: params.topic, fromBeginning: params.fromBeginning }),
        run: (params) =>
          c.run({
            autoCommit: params.autoCommit,
            eachMessage: params.eachMessage
              ? (p) =>
                  params.eachMessage!({
                    topic: p.topic,
                    partition: p.partition,
                    message: {
                      key: p.message.key,
                      value: p.message.value,
                      offset: p.message.offset,
                      timestamp: p.message.timestamp ?? "",
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
// E2E: Kafka → process → Redis state checkpoint
// ---------------------------------------------------------------------------

withAll("E2E: Kafka source → process with ack → Redis state", (ctx) => {
  it("consumes from Kafka, tracks state in Redis, checkpoints survive restart", async () => {
    const topic = uniqueName("e2e-orders");
    await createTopic(ctx.kafka.broker, topic);

    const client = kafkaClient(ctx.kafka.broker);
    const redis = new IoRedis(ctx.redis.port, ctx.redis.host) as unknown as RedisClientType;
    const prefix = uniqueName("e2e") + ":";

    // Publish 5 orders
    const kt = new KafkaTopic<{ userId: string; amount: number }>({
      kafka: client,
      topic,
      groupId: uniqueName("g"),
    });

    for (const order of [
      { userId: "u1", amount: 100 },
      { userId: "u2", amount: 200 },
      { userId: "u1", amount: 150 },
      { userId: "u2", amount: 50 },
      { userId: "u1", amount: 300 },
    ]) {
      await kt.publish(order);
    }

    // Consume with ack, accumulate per-user totals in Redis state
    const state = new RedisStateBackend({ redis, prefix });
    const group = uniqueName("processor");

    const results: { userId: string; total: number }[] = [];

    await kt
      .subscribeAck({ group, commitIntervalMs: 100, fromBeginning: true })
      .take(5)
      .forEach(async (env) => {
        const { userId, amount } = env.value;
        const current = ((await state.get(`total:${userId}`)) as number) ?? 0;
        const newTotal = current + amount;
        await state.put(`total:${userId}`, newTotal);
        results.push({ userId, total: newTotal });
        await env.ack();
      });

    // Checkpoint
    await state.checkpoint({ name: "after-batch" });

    // Verify all 5 messages were processed
    expect(results).toHaveLength(5);

    // Verify final state — sum of all amounts per user
    const u1Total = (await state.get("total:u1")) as number;
    const u2Total = (await state.get("total:u2")) as number;
    expect(u1Total).toBe(550); // 100 + 150 + 300
    expect(u2Total).toBe(250); // 200 + 50

    // Simulate restart: clear live state, restore from checkpoint
    await state.clear();
    expect(await state.get("total:u1")).toBeUndefined();

    await state.restore({ name: "after-batch" });
    expect(await state.get("total:u1")).toBe(550);
    expect(await state.get("total:u2")).toBe(250);

    await kt.disconnect();
    redis.disconnect();
  });
});

// ---------------------------------------------------------------------------
// E2E: Postgres step queue → worker claim/complete cycle
// ---------------------------------------------------------------------------

withAll("E2E: Postgres step queue — distributed task lifecycle", (ctx) => {
  it("coordinator enqueues, workers claim and complete, no duplicates", async () => {
    const sql = postgres(ctx.postgres.url);
    const db = drizzle(sql) as DrizzleDb;
    const queue = new PgStepQueue({ db, workerId: "coordinator" });
    await queue.ensureTable();

    // Enqueue 10 tasks
    for (let i = 0; i < 10; i++) {
      await queue.enqueue({
        workflowId: "wf-e2e",
        stepName: `step-${i}`,
        queue: "default",
        input: { index: i },
        prevResults: {},
      });
    }

    // 3 workers compete for tasks
    const w1 = new PgStepQueue({ db, workerId: "w1" });
    const w2 = new PgStepQueue({ db, workerId: "w2" });
    const w3 = new PgStepQueue({ db, workerId: "w3" });

    const completed: string[] = [];
    const workerAssignments: Record<string, string[]> = { w1: [], w2: [], w3: [] };

    async function work(q: PgStepQueue, name: string) {
      while (true) {
        const tasks = await q.claim({ queues: ["default"], limit: 1 });
        if (tasks.length === 0) break;
        const task = tasks[0]!;
        workerAssignments[name]!.push(task.stepName);
        await q.complete({ taskId: task.id, result: { done: true }, durationMs: 1 });
        completed.push(task.stepName);
      }
    }

    await Promise.all([work(w1, "w1"), work(w2, "w2"), work(w3, "w3")]);

    // All 10 tasks completed, no duplicates
    expect(completed.sort()).toEqual(Array.from({ length: 10 }, (_, i) => `step-${i}`).sort());

    // Work was distributed (at least 1 worker got tasks)
    const activeWorkers = Object.values(workerAssignments).filter((a) => a.length > 0);
    expect(activeWorkers.length).toBeGreaterThanOrEqual(1);

    await sql.end();
  });
});

// ---------------------------------------------------------------------------
// E2E: Cache layering — Redis L1 + Postgres L2
// ---------------------------------------------------------------------------

withAll("E2E: Redis cache with Postgres state fallback", (ctx) => {
  it("cache hit avoids state backend lookup", async () => {
    const redis = new IoRedis(ctx.redis.port, ctx.redis.host) as unknown as RedisClientType;
    const sql = postgres(ctx.postgres.url);
    const db = drizzle(sql) as DrizzleDb;

    const cache = new RedisCacheStore<{ score: number }>({
      redis,
      ttlMs: 5_000,
      prefix: uniqueName("cache") + ":",
    });

    const state = new PgStateBackend({ db, table: uniqueName("state").replace(/-/g, "_") });
    await state.ensureTable();

    // Store in both
    await state.put("user:1", { score: 42 });
    await cache.set("user:1", { score: 42 });

    // Cache hit
    const cached = await cache.get("user:1");
    expect(cached).toEqual({ score: 42 });

    // After cache eviction, fall back to state backend
    await cache.delete("user:1");
    expect(await cache.get("user:1")).toBeUndefined();
    expect(await state.get("user:1")).toEqual({ score: 42 });

    redis.disconnect();
    await sql.end();
  });
});
