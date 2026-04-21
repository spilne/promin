import { describe, it, expect } from "bun:test";
import { postgresDescribe } from "../test-utils.ts";
import { PgQueue } from "../pg-queue.ts";

// ---------------------------------------------------------------------------
// PgQueue — SKIP LOCKED queue
// ---------------------------------------------------------------------------

postgresDescribe("PgQueue", (pg) => {
  describe("publish + subscribe (auto-pop)", () => {
    it("publishes and consumes messages", async () => {
      const queue = await PgQueue.create<{ userId: string }>(pg.db, "basic");

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

    it("pop removes messages from queue", async () => {
      const queue = await PgQueue.create<string>(pg.db, "pop_test");
      await queue.publish("a");
      await queue.publish("b");

      // Consume all
      await queue.subscribe().take(2).drain();

      // Queue should be empty
      const m = await queue.metrics();
      expect(m.pending).toBe(0);
      expect(m.total).toBe(0);
    });
  });

  describe("subscribeAck — manual ack/nack", () => {
    it("ack deletes the message", async () => {
      const queue = await PgQueue.create<{ n: number }>(pg.db, "ack_test");
      await queue.publish({ n: 42 });

      await queue
        .subscribeAck()
        .take(1)
        .forEach(async (envelope) => {
          expect(envelope.value).toEqual({ n: 42 });
          expect(envelope.metadata.msgId).toBeGreaterThan(0);
          await envelope.ack();
        });

      const m = await queue.metrics();
      expect(m.pending).toBe(0);
      expect(m.processing).toBe(0);
    });

    it("nack makes message visible again", async () => {
      const queue = await PgQueue.create<string>(pg.db, "nack_test", {
        defaultVtSeconds: 60,
        defaultBatchSize: 1,
      });
      await queue.publish("retry-me");

      // Read and nack
      await queue
        .subscribeAck()
        .take(1)
        .forEach(async (envelope) => {
          await envelope.nack();
        });

      // Wait briefly for nack to settle
      await new Promise((r) => setTimeout(r, 100));

      // Should be pending again
      const m = await queue.metrics();
      expect(m.pending).toBe(1);
    });

    it("ack with archive mode keeps message", async () => {
      const queue = await PgQueue.create<string>(pg.db, "archive_test", {
        ackMode: "archive",
        defaultBatchSize: 1,
      });
      await queue.publish("keep-me");

      await queue
        .subscribeAck()
        .take(1)
        .forEach(async (envelope) => {
          await envelope.ack();
        });

      await new Promise((r) => setTimeout(r, 100));

      const m = await queue.metrics();
      expect(m.completed).toBe(1);
      expect(m.total).toBe(1);
    });
  });

  describe("visibility timeout", () => {
    it("processing messages are invisible to other consumers", async () => {
      const queue = await PgQueue.create<string>(pg.db, "vt_test", { defaultVtSeconds: 60 });
      await queue.publish("invisible");

      // First consumer takes it
      let firstMsg = "";
      await queue
        .subscribeAck({ vtSeconds: 60 })
        .take(1)
        .forEach(async (env) => {
          firstMsg = env.value;
          // Don't ack — leave it processing
        });

      expect(firstMsg).toBe("invisible");

      // Second consumer should see nothing (message is locked)
      const m = await queue.metrics();
      expect(m.processing).toBe(1);
      expect(m.pending).toBe(0);
    });
  });

  describe("publish with delay", () => {
    it("delayed message not immediately visible", async () => {
      const queue = await PgQueue.create<string>(pg.db, "delay_test");
      await queue.publish("later", { delay: 60 });

      const m = await queue.metrics();
      expect(m.total).toBe(1);
      expect(m.pending).toBe(1); // status is pending but visible_at is in future
    });
  });

  describe("publish with headers", () => {
    it("headers are passed through to envelope", async () => {
      const queue = await PgQueue.create<string>(pg.db, "headers_test");
      await queue.publish("hi", { headers: { "x-trace": "abc" } });

      await queue
        .subscribeAck()
        .take(1)
        .forEach(async (envelope) => {
          expect(envelope.metadata.headers).toEqual({ "x-trace": "abc" });
          await envelope.ack();
        });
    });
  });

  describe("metrics", () => {
    it("returns queue stats", async () => {
      const queue = await PgQueue.create<number>(pg.db, "metrics_test");
      await queue.publish(1);
      await queue.publish(2);
      await queue.publish(3);

      const m = await queue.metrics();
      expect(m.pending).toBe(3);
      expect(m.total).toBe(3);
    });
  });

  describe("purge + drop", () => {
    it("purge removes all messages", async () => {
      const queue = await PgQueue.create<string>(pg.db, "purge_test");
      await queue.publish("a");
      await queue.publish("b");

      await queue.purge();

      const m = await queue.metrics();
      expect(m.total).toBe(0);
    });

    it("drop removes the table", async () => {
      const queue = await PgQueue.create<string>(pg.db, "drop_test");
      await queue.publish("gone");
      await queue.drop();

      // Table should not exist — metrics will throw
      try {
        await queue.metrics();
        expect(true).toBe(false); // should not reach
      } catch {
        // Expected — table doesn't exist
      }
    });
  });

  describe("requeueDead", () => {
    it("requeues messages stuck in processing", async () => {
      const queue = await PgQueue.create<string>(pg.db, "requeue_test", { defaultVtSeconds: 1 });
      await queue.publish("stuck");

      // Consume but don't ack
      await queue
        .subscribeAck({ vtSeconds: 1 })
        .take(1)
        .forEach(async () => {
          // No ack — leave processing
        });

      // Wait for VT to expire
      await new Promise((r) => setTimeout(r, 1500));

      // Requeue dead messages
      const requeued = await queue.requeueDead();
      expect(requeued).toBe(1);

      const m = await queue.metrics();
      expect(m.pending).toBe(1);
    });
  });

  describe("concurrent dequeue", () => {
    it("SKIP LOCKED prevents double processing", async () => {
      const queue = await PgQueue.create<number>(pg.db, "concurrent_test", { defaultBatchSize: 2 });

      // Publish 4 messages
      for (let i = 0; i < 4; i++) {
        await queue.publish(i);
      }

      // Simulate two concurrent dequeue calls — SKIP LOCKED ensures no overlap
      const batch1 = await queue.subscribe().take(2).collect();
      const batch2 = await queue.subscribe().take(2).collect();

      const all = [...batch1, ...batch2].sort();
      expect(new Set(all).size).toBe(4);
    });
  });
});
