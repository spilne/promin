import { it, expect } from "bun:test";
import {
  pgTable,
  text,
  integer,
  jsonb,
  timestamp,
  bigserial,
  index,
  primaryKey,
  uniqueIndex,
  boolean,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { ensureTable } from "./schema-utils.ts";
import { postgresDescribe } from "./test-utils.ts";
import { execRaw } from "./drizzle-db.ts";
import { stepQueue } from "./schema.ts";
import { topologyState } from "./pg-state-schema.ts";
import { createQueueTable } from "./pg-queue-schema.ts";

// ---------------------------------------------------------------------------
// ensureTable — creates tables from Drizzle schema definitions
// ---------------------------------------------------------------------------

postgresDescribe("ensureTable — derives DDL from Drizzle pgTable", (pg) => {
  it("creates a simple table with text and integer columns", async () => {
    const table = pgTable("simple_test", {
      id: text("id").primaryKey(),
      name: text("name").notNull(),
      count: integer("count").notNull().default(0),
    });

    await ensureTable(pg.db, table);

    // Verify table exists and has correct columns
    const rows = await execRaw(
      pg.db,
      sql.raw(`
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'simple_test'
        ORDER BY ordinal_position
      `),
    );

    expect(rows).toHaveLength(3);
    expect(rows[0].column_name).toBe("id");
    expect(rows[0].data_type).toBe("text");
    expect(rows[0].is_nullable).toBe("NO");
    expect(rows[1].column_name).toBe("name");
    expect(rows[2].column_name).toBe("count");
    expect(rows[2].column_default).toBe("0");
  });

  it("creates a table with jsonb, timestamp, and bigserial", async () => {
    const table = pgTable("complex_test", {
      id: bigserial("id", { mode: "number" }).primaryKey(),
      data: jsonb("data"),
      active: boolean("active").notNull().default(true),
      createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    });

    await ensureTable(pg.db, table);

    const rows = await execRaw(
      pg.db,
      sql.raw(`
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'complex_test'
        ORDER BY ordinal_position
      `),
    );

    expect(rows).toHaveLength(4);
    expect(rows[0].data_type).toBe("bigint"); // bigserial
    expect(rows[1].data_type).toBe("jsonb");
    expect(rows[2].data_type).toBe("boolean");
    expect(rows[3].data_type).toContain("timestamp");
  });

  it("creates indexes defined on the table", async () => {
    const table = pgTable(
      "indexed_test",
      {
        id: text("id").primaryKey(),
        status: text("status").notNull(),
        priority: integer("priority").notNull().default(5),
      },
      (t) => [index("indexed_test_status_idx").on(t.status, t.priority)],
    );

    await ensureTable(pg.db, table);

    const idxRows = await execRaw(
      pg.db,
      sql.raw(`
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'indexed_test' AND indexname = 'indexed_test_status_idx'
      `),
    );

    expect(idxRows).toHaveLength(1);
  });

  it("creates unique indexes", async () => {
    const table = pgTable(
      "unique_test",
      {
        id: text("id").primaryKey(),
        email: text("email").notNull(),
      },
      (t) => [uniqueIndex("unique_test_email_idx").on(t.email)],
    );

    await ensureTable(pg.db, table);

    const idxRows = await execRaw(
      pg.db,
      sql.raw(`
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'unique_test' AND indexname = 'unique_test_email_idx'
      `),
    );

    expect(idxRows).toHaveLength(1);
  });

  it("handles composite primary keys", async () => {
    const table = pgTable(
      "composite_pk_test",
      {
        workflowId: text("workflow_id").notNull(),
        stepName: text("step_name").notNull(),
        value: jsonb("value"),
      },
      (t) => [primaryKey({ columns: [t.workflowId, t.stepName] })],
    );

    await ensureTable(pg.db, table);

    // Insert a row to verify the PK works
    await pg.db.execute(
      sql.raw(
        `INSERT INTO composite_pk_test (workflow_id, step_name, value) VALUES ('w1', 's1', '{}')`,
      ),
    );

    // Duplicate PK should fail
    let threw = false;
    try {
      await pg.db.execute(
        sql.raw(
          `INSERT INTO composite_pk_test (workflow_id, step_name, value) VALUES ('w1', 's1', '{}')`,
        ),
      );
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);

    // Different combo should succeed
    await pg.db.execute(
      sql.raw(
        `INSERT INTO composite_pk_test (workflow_id, step_name, value) VALUES ('w1', 's2', '{}')`,
      ),
    );
  });

  it("is idempotent — calling twice does not error", async () => {
    const table = pgTable("idempotent_test", {
      id: text("id").primaryKey(),
    });

    await ensureTable(pg.db, table);
    await ensureTable(pg.db, table); // second call should not throw
  });

  it("works with the real stepQueue schema", async () => {
    await ensureTable(pg.db, stepQueue);

    // Verify table exists
    const rows = await execRaw(
      pg.db,
      sql.raw(`
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'wf_step_queue'
        ORDER BY ordinal_position
      `),
    );

    const colNames = rows.map((r: { column_name: string }) => r.column_name);
    expect(colNames).toContain("id");
    expect(colNames).toContain("workflow_id");
    expect(colNames).toContain("step_name");
    expect(colNames).toContain("priority");
    expect(colNames).toContain("status");
  });

  it("works with the real topologyState schema", async () => {
    await ensureTable(pg.db, topologyState);

    const rows = await execRaw(
      pg.db,
      sql.raw(`
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'topology_state'
        ORDER BY ordinal_position
      `),
    );

    const colNames = rows.map((r: { column_name: string }) => r.column_name);
    expect(colNames).toContain("key");
    expect(colNames).toContain("value");
    expect(colNames).toContain("checkpoint");
    expect(colNames).toContain("updated_at");
  });

  it("works with the real PgQueue schema factory", async () => {
    const ordersQueue = createQueueTable("test_orders");
    await ensureTable(pg.db, ordersQueue);

    const rows = await execRaw(
      pg.db,
      sql.raw(`
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'pgq_test_orders'
        ORDER BY ordinal_position
      `),
    );

    const colNames = rows.map((r: { column_name: string }) => r.column_name);
    expect(colNames).toContain("id");
    expect(colNames).toContain("payload");
    expect(colNames).toContain("status");
    expect(colNames).toContain("visible_at");
  });

  it("inserts and reads data from created table", async () => {
    const table = pgTable("readwrite_test", {
      id: text("id").primaryKey(),
      data: jsonb("data").notNull(),
      count: integer("count").notNull().default(0),
    });

    await ensureTable(pg.db, table);

    await pg.db.execute(
      sql.raw(`INSERT INTO readwrite_test (id, data) VALUES ('k1', '{"x": 42}')`),
    );

    const rows = await execRaw(
      pg.db,
      sql.raw(`SELECT id, data, count FROM readwrite_test WHERE id = 'k1'`),
    );

    expect(rows).toHaveLength(1);
    expect(rows[0].id).toBe("k1");
    expect(rows[0].data).toEqual({ x: 42 });
    expect(rows[0].count).toBe(0); // default
  });
});
