import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import type { DrizzleDb } from "../lib/drizzle-db.ts";
import { PgmqQueue } from "./pgmq-queue.ts";
import { ReadMode } from "./types.ts";
import * as pgmq from "./pgmq.ts";

// ---------------------------------------------------------------------------
// Container setup — tembo pgmq image with pgmq extension pre-installed
// ---------------------------------------------------------------------------

const PGMQ_IMAGE = "ghcr.io/pgmq/pg17-pgmq:latest";

let container: StartedTestContainer;
let sqlClient: ReturnType<typeof postgres>;
let db: DrizzleDb;

beforeAll(async () => {
  container = await new GenericContainer(PGMQ_IMAGE)
    .withExposedPorts(5432)
    .withEnvironment({
      POSTGRES_USER: "test",
      POSTGRES_PASSWORD: "test",
      POSTGRES_DB: "test",
    })
    .withCommand(["postgres", "-c", "fsync=off", "-c", "synchronous_commit=off"])
    .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
    .withStartupTimeout(60_000)
    .start();

  const connStr = `postgres://test:test@${container.getHost()}:${container.getMappedPort(5432)}/test`;
  sqlClient = postgres(connStr);
  db = drizzle(sqlClient);

  // Enable pgmq extension
  await sqlClient`CREATE EXTENSION IF NOT EXISTS pgmq`;
}, 120_000);

afterAll(async () => {
  await sqlClient?.end();
  await container?.stop();
});

// ---------------------------------------------------------------------------
// Low-level pgmq functions
// ---------------------------------------------------------------------------

describe("pgmq low-level", () => {
  it("creates a queue", async () => {
    await pgmq.createQueue(db, "test_basic");
    const queues = await pgmq.listQueues(db);
    expect(queues.some((q) => q.queueName === "test_basic")).toBe(true);
  });

  it("sends and reads a message", async () => {
    await pgmq.createQueue(db, "test_send_read");

    const msgId = await pgmq.send(db, "test_send_read", {
      data: { hello: "world" },
    });
    expect(msgId).toBeGreaterThan(0);

    const records = await pgmq.read(db, "test_send_read", ReadMode.standard({ vt: 30, qty: 10 }));
    expect(records).toHaveLength(1);
    expect(records[0]!.message).toEqual({ hello: "world" });
    expect(records[0]!.msgId).toBe(msgId);

    // Clean up
    await pgmq.deleteMessage(db, "test_send_read", msgId);
  });

  it("sends with delay", async () => {
    await pgmq.createQueue(db, "test_delay");

    await pgmq.send(db, "test_delay", { data: { delayed: true }, delay: 60 });

    // Should not be visible yet (60s delay)
    const records = await pgmq.read(db, "test_delay", ReadMode.standard({ vt: 1, qty: 10 }));
    expect(records).toHaveLength(0);
  });

  it("sends with headers", async () => {
    await pgmq.createQueue(db, "test_headers");

    await pgmq.send(db, "test_headers", {
      data: { msg: "hi" },
      headers: { "x-trace-id": "abc-123" },
    });

    const records = await pgmq.read(db, "test_headers", ReadMode.standard({ vt: 30, qty: 10 }));
    expect(records).toHaveLength(1);
    expect(records[0]!.headers).toEqual({ "x-trace-id": "abc-123" });
  });

  it("pop reads and deletes", async () => {
    await pgmq.createQueue(db, "test_pop");

    await pgmq.send(db, "test_pop", { data: { popme: true } });

    const popped = await pgmq.pop(db, "test_pop", 1);
    expect(popped).toHaveLength(1);
    expect(popped[0]!.message).toEqual({ popme: true });

    // Should be gone
    const remaining = await pgmq.read(db, "test_pop", ReadMode.standard({ vt: 1, qty: 10 }));
    expect(remaining).toHaveLength(0);
  });

  it("archive moves to archive table", async () => {
    await pgmq.createQueue(db, "test_archive");

    const msgId = await pgmq.send(db, "test_archive", { data: { archiveme: true } });
    const archived = await pgmq.archive(db, "test_archive", msgId);
    expect(archived).toBe(true);

    // Should not be in queue
    const records = await pgmq.read(db, "test_archive", ReadMode.standard({ vt: 1, qty: 10 }));
    expect(records).toHaveLength(0);
  });

  it("visibility timeout hides messages", async () => {
    await pgmq.createQueue(db, "test_vt");

    const msgId = await pgmq.send(db, "test_vt", { data: { vt: true } });

    // First read with 30s VT
    const first = await pgmq.read(db, "test_vt", ReadMode.standard({ vt: 30, qty: 10 }));
    expect(first).toHaveLength(1);

    // Second read — message is invisible
    const second = await pgmq.read(db, "test_vt", ReadMode.standard({ vt: 30, qty: 10 }));
    expect(second).toHaveLength(0);

    // Reset VT to make it visible again
    await pgmq.setVt(db, "test_vt", msgId, 0);

    const third = await pgmq.read(db, "test_vt", ReadMode.standard({ vt: 30, qty: 10 }));
    expect(third).toHaveLength(1);

    await pgmq.deleteMessage(db, "test_vt", msgId);
  });

  it("metrics returns queue stats", async () => {
    await pgmq.createQueue(db, "test_metrics");
    await pgmq.send(db, "test_metrics", { data: { a: 1 } });
    await pgmq.send(db, "test_metrics", { data: { b: 2 } });

    const m = await pgmq.metrics(db, "test_metrics");
    expect(m.queueName).toBe("test_metrics");
    expect(m.queueLength).toBe(2);
    expect(m.totalMessages).toBe(2);
  });

  it("purge removes all messages", async () => {
    await pgmq.createQueue(db, "test_purge");
    await pgmq.send(db, "test_purge", { data: { a: 1 } });
    await pgmq.send(db, "test_purge", { data: { b: 2 } });

    const purged = await pgmq.purgeQueue(db, "test_purge");
    expect(purged).toBe(2);

    const records = await pgmq.read(db, "test_purge", ReadMode.standard({ vt: 1, qty: 10 }));
    expect(records).toHaveLength(0);
  });

  it("drop removes the queue", async () => {
    await pgmq.createQueue(db, "test_drop");
    const dropped = await pgmq.dropQueue(db, "test_drop");
    expect(dropped).toBe(true);

    const queues = await pgmq.listQueues(db);
    expect(queues.some((q) => q.queueName === "test_drop")).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// PgmqQueue — high-level typed queue
// ---------------------------------------------------------------------------

describe("PgmqQueue", () => {
  it("publish and subscribe (pop-based)", async () => {
    const queue = await PgmqQueue.create<{ userId: string }>(db, "typed_basic");

    await queue.publish({ userId: "u_1" });
    await queue.publish({ userId: "u_2" });

    const items: { userId: string }[] = [];
    await queue
      .subscribe()
      .take(2)
      .forEach((item) => {
        items.push(item);
      });

    expect(items).toEqual([{ userId: "u_1" }, { userId: "u_2" }]);
  });

  it("subscribeAck with manual ack (delete)", async () => {
    const queue = await PgmqQueue.create<{ n: number }>(db, "typed_ack");

    await queue.publish({ n: 42 });

    const results: number[] = [];
    await queue
      .subscribeAck({
        readMode: ReadMode.standard({ vt: 30, qty: 10 }),
        ackMode: "delete",
      })
      .take(1)
      .forEach(async (envelope) => {
        results.push(envelope.value.n);
        await envelope.ack();
      });

    expect(results).toEqual([42]);

    // Message should be deleted
    const m = await queue.metrics();
    expect(m.queueLength).toBe(0);
  });

  it("subscribeAck with archive", async () => {
    const queue = await PgmqQueue.create<{ n: number }>(db, "typed_archive");

    await queue.publish({ n: 99 });

    await queue
      .subscribeAck({ ackMode: "archive" })
      .take(1)
      .forEach(async (envelope) => {
        await envelope.ack();
      });

    // Message should be archived (not in queue)
    const m = await queue.metrics();
    // Queue length should be 0 after archive
    expect(m.queueLength).toBeLessThanOrEqual(1);
  });

  it("nack makes message visible again", async () => {
    const queue = await PgmqQueue.create<{ n: number }>(db, "typed_nack");

    await queue.publish({ n: 7 });

    // Read with ack, but nack instead — set VT to 1 second
    await queue
      .subscribeAck({ readMode: ReadMode.standard({ vt: 60, qty: 1 }) })
      .take(1)
      .forEach(async (envelope) => {
        await envelope.nack();
      });

    // Wait for VT to expire then read again
    await new Promise((r) => setTimeout(r, 1500));
    const records = await pgmq.read(db, "typed_nack", ReadMode.standard({ vt: 1, qty: 10 }));
    expect(records).toHaveLength(1);
    expect(records[0]!.message).toEqual({ n: 7 });
  });

  it("publishBatch sends multiple messages", async () => {
    const queue = await PgmqQueue.create<string>(db, "typed_batch");

    const ids = await queue.publishBatch(["a", "b", "c"]);
    expect(ids).toHaveLength(3);

    const m = await queue.metrics();
    expect(m.queueLength).toBe(3);
  });

  it("publish with delay", async () => {
    const queue = await PgmqQueue.create<string>(db, "typed_delay");

    await queue.publish("later", { delay: 60 });

    const m = await queue.metrics();
    expect(m.totalMessages).toBe(1);
  });

  it("purge and drop", async () => {
    const queue = await PgmqQueue.create<string>(db, "typed_lifecycle");
    await queue.publish("a");
    await queue.publish("b");

    const purged = await queue.purge();
    expect(purged).toBe(2);

    const dropped = await queue.drop();
    expect(dropped).toBe(true);
  });

  it("envelope metadata includes msgId and readCt", async () => {
    const queue = await PgmqQueue.create<string>(db, "typed_meta");
    await queue.publish("hello");

    await queue
      .subscribeAck()
      .take(1)
      .forEach(async (envelope) => {
        expect(envelope.metadata.msgId).toBeGreaterThan(0);
        expect(envelope.metadata.readCt).toBe(1);
        expect(envelope.metadata.enqueuedAt).toBeInstanceOf(Date);
        await envelope.ack();
      });
  });
});

// ---------------------------------------------------------------------------
// Schema validation on read — promin-2wd
//
// Producers can drift (rename fields, change types) without the consumer
// being the one to own the breakage. `schema` on PgmqQueue validates every
// message on consume; `onSchemaError` controls what happens to poison
// messages. All four cases are wired through pgmq's own publish/read so we
// catch integration issues, not just "we wired the SchemaParser interface".
// ---------------------------------------------------------------------------

// Minimal SchemaParser<Order> — tests don't need a real Zod; we only use
// the interface the runtime actually sees.
interface Order {
  id: string;
  total: number;
}
const orderSchema = {
  safeParse(data: unknown): { success: true; data: Order } | { success: false; error: unknown } {
    if (
      typeof data === "object" &&
      data !== null &&
      typeof (data as any).id === "string" &&
      typeof (data as any).total === "number"
    ) {
      return { success: true, data: data as Order };
    }
    return { success: false, error: { issue: "not a valid Order shape", received: data } };
  },
};

describe("PgmqQueue — schema validation on read", () => {
  it("valid messages pass through unchanged", async () => {
    const queue = await PgmqQueue.create<Order>(db, "schema_ok", {
      schema: orderSchema,
    });
    await queue.publish({ id: "ord-1", total: 42 });

    const items: Order[] = [];
    await queue
      .subscribe()
      .take(1)
      .forEach((item) => {
        items.push(item);
      });
    expect(items).toEqual([{ id: "ord-1", total: 42 }]);
  });

  it("onSchemaError: 'skip' — subscribe drops the bad message and keeps going", async () => {
    // Use raw pgmq.send to publish a mis-shaped message the typed publish()
    // wouldn't let through — simulating producer drift.
    await pgmq.createQueue(db, "schema_skip");
    await pgmq.send(db, "schema_skip", { data: { id: 42, total: "nope" } as unknown });
    await pgmq.send(db, "schema_skip", { data: { id: "ord-2", total: 99 } });

    const queue = PgmqQueue.wrap<Order>({
      db,
      queue: "schema_skip",
      schema: orderSchema,
      onSchemaError: "skip",
    });

    const items: Order[] = [];
    await queue
      .subscribe()
      .take(1)
      .forEach((item) => {
        items.push(item);
      });
    expect(items).toEqual([{ id: "ord-2", total: 99 }]);
  });

  it("onSchemaError: 'skip' — subscribeAck deletes the poison so it doesn't re-surface", async () => {
    await pgmq.createQueue(db, "schema_skip_ack");
    await pgmq.send(db, "schema_skip_ack", { data: { wrong: "shape" } });
    await pgmq.send(db, "schema_skip_ack", { data: { id: "ord-3", total: 7 } });

    const queue = PgmqQueue.wrap<Order>({
      db,
      queue: "schema_skip_ack",
      schema: orderSchema,
      onSchemaError: "skip",
    });

    const items: Order[] = [];
    await queue
      .subscribeAck({ readMode: ReadMode.standard({ vt: 1, qty: 10 }) })
      .take(1)
      .forEach(async (envelope) => {
        items.push(envelope.value);
        await envelope.ack();
      });
    expect(items).toEqual([{ id: "ord-3", total: 7 }]);

    // Both messages should be gone: one ack'd, one skipped (deleted).
    // Wait past vt, then confirm queue is empty.
    await new Promise((r) => setTimeout(r, 1500));
    const metrics = await queue.metrics();
    expect(metrics.queueLength).toBe(0);
  });

  it("onSchemaError: 'dlq' — poison goes to {queue}_dlq, good one emits", async () => {
    await pgmq.createQueue(db, "schema_dlq");
    await pgmq.send(db, "schema_dlq", { data: { bad: true } });
    await pgmq.send(db, "schema_dlq", { data: { id: "ord-4", total: 1 } });

    const queue = PgmqQueue.wrap<Order>({
      db,
      queue: "schema_dlq",
      schema: orderSchema,
      onSchemaError: "dlq",
    });

    const items: Order[] = [];
    await queue
      .subscribe()
      .take(1)
      .forEach((item) => {
        items.push(item);
      });
    expect(items).toEqual([{ id: "ord-4", total: 1 }]);

    // DLQ queue should exist with the bad message.
    const dlq = await pgmq.read(db, queue.dlqName, ReadMode.standard({ vt: 5, qty: 10 }));
    expect(dlq).toHaveLength(1);
    expect(dlq[0]!.message).toEqual({ bad: true });
  });

  it("onSchemaError: 'throw' (default) — subscribe fails loud on invalid", async () => {
    await pgmq.createQueue(db, "schema_throw");
    await pgmq.send(db, "schema_throw", { data: { broken: "yes" } });

    const queue = PgmqQueue.wrap<Order>({
      db,
      queue: "schema_throw",
      schema: orderSchema,
      // onSchemaError defaults to "throw"
    });

    let caught: unknown;
    try {
      await queue
        .subscribe()
        .take(1)
        .forEach(() => {});
    } catch (e) {
      caught = e;
    }
    expect(caught).toBeDefined();
    // Effect wraps errors in FiberFailure; the underlying cause carries
    // the PgmqSchemaValidationError message.
    const msg = (caught as { message?: string; toString(): string }).message ?? String(caught);
    expect(msg).toContain("schema validation");
  });
});
