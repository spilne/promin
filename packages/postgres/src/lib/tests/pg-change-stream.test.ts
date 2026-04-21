import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { sql } from "drizzle-orm";
import { PostgresTestContainer } from "../test-utils.ts";
import { PgChangeStream } from "../pg-change-stream.ts";

// ---------------------------------------------------------------------------
// Container setup — plain Postgres (no extension needed)
// ---------------------------------------------------------------------------

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();

  // Create a test table for CDC
  await pg.db.execute(
    sql.raw(`
    CREATE TABLE IF NOT EXISTS events (
      id BIGSERIAL PRIMARY KEY,
      payload JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `),
  );
}, 60_000);

afterAll(async () => {
  await pg.stop();
});

// ---------------------------------------------------------------------------
// PgChangeStream — LISTEN/NOTIFY + poll fallback
// ---------------------------------------------------------------------------

describe("PgChangeStream", () => {
  describe("poll-based subscribe", () => {
    it("polls for new rows", async () => {
      const stream = new PgChangeStream<{ type: string }>({
        db: pg.db,
        sql: pg.sql,
        channel: "events_poll",
        table: "events",
        payloadColumn: "payload",
        pollIntervalMs: 100,
      });

      // Insert some rows
      await pg.db.execute(
        sql.raw(
          `INSERT INTO events (payload) VALUES ('{"type":"a"}'::jsonb), ('{"type":"b"}'::jsonb)`,
        ),
      );

      // Subscribe from earliest — should pick up existing rows
      const items = await stream
        .subscribeFrom({ offset: { type: "earliest" } })
        .take(2)
        .collect();

      expect(items).toHaveLength(2);
      expect(items[0]).toEqual({ type: "a" });
      expect(items[1]).toEqual({ type: "b" });
    });
  });

  describe("LISTEN/NOTIFY subscribe", () => {
    it("receives notifications in real-time", async () => {
      const stream = new PgChangeStream<{ n: number }>({
        db: pg.db,
        sql: pg.sql,
        channel: "events_listen",
        table: "events",
        payloadColumn: "payload",
        pollIntervalMs: 60_000, // High poll interval so we rely on LISTEN
      });

      // Start collecting before insert
      const collectPromise = stream.subscribe().take(2).collect();

      // Small delay to let LISTEN register
      await new Promise((r) => setTimeout(r, 200));

      // Send notifications
      await stream.notify({ n: 1 });
      await stream.notify({ n: 2 });

      const items = await collectPromise;
      expect(items).toHaveLength(2);
      expect(items[0]).toEqual({ n: 1 });
      expect(items[1]).toEqual({ n: 2 });
    });
  });

  describe("trigger", () => {
    it("installs and removes trigger", async () => {
      const stream = new PgChangeStream<unknown>({
        db: pg.db,
        sql: pg.sql,
        channel: "events_trigger",
        table: "events",
        payloadColumn: "payload",
      });

      await stream.installTrigger();

      // Check trigger exists
      const triggers = (await pg.db.execute(
        sql.raw(`SELECT tgname FROM pg_trigger WHERE tgname = 'trg_notify_events_trigger'`),
      )) as any[];
      expect(triggers).toHaveLength(1);

      await stream.removeTrigger();

      // Check trigger removed
      const after = (await pg.db.execute(
        sql.raw(`SELECT tgname FROM pg_trigger WHERE tgname = 'trg_notify_events_trigger'`),
      )) as any[];
      expect(after).toHaveLength(0);
    });

    it("auto-notifies on INSERT when trigger installed", async () => {
      const stream = new PgChangeStream<{ event: string }>({
        db: pg.db,
        sql: pg.sql,
        channel: "events_auto",
        table: "events",
        payloadColumn: "payload",
        pollIntervalMs: 60_000,
      });

      await stream.installTrigger();

      // Start collecting
      const collectPromise = stream.subscribe().take(1).collect();

      // Small delay for LISTEN
      await new Promise((r) => setTimeout(r, 200));

      // INSERT triggers auto-NOTIFY
      await pg.db.execute(
        sql.raw(`INSERT INTO events (payload) VALUES ('{"event":"triggered"}'::jsonb)`),
      );

      const items = await collectPromise;
      expect(items).toHaveLength(1);
      expect(items[0]).toEqual({ event: "triggered" });

      await stream.removeTrigger();
    });
  });

  describe("subscribeFrom with offset", () => {
    it("replays from timestamp", async () => {
      // Insert a row with known timestamp
      const now = new Date();
      await pg.db.execute(
        sql.raw(
          `INSERT INTO events (payload, created_at) VALUES ('{"offset":"test"}'::jsonb, '${now.toISOString()}')`,
        ),
      );

      const stream = new PgChangeStream<{ offset: string }>({
        db: pg.db,
        sql: pg.sql,
        channel: "events_offset",
        table: "events",
        payloadColumn: "payload",
        pollIntervalMs: 100,
      });

      const items = await stream
        .subscribeFrom({ offset: { type: "timestamp", value: now.getTime() } })
        .take(1)
        .collect();

      expect(items).toHaveLength(1);
      expect(items[0]).toEqual({ offset: "test" });
    });
  });
});
