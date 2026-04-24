import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test";
import { PostgresTestContainer } from "../test-utils.ts";
import { refTestSuite } from "@promin/core/testing";
import { PgRef } from "../pg-ref.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
}, 60_000);

afterAll(async () => {
  await pg.stop();
});

// ---------------------------------------------------------------------------
// Conformance suite
// Factory creates a PgRef<number> with initial value 0
// ---------------------------------------------------------------------------

let setupDone = false;

refTestSuite(async () => {
  if (!setupDone) {
    const ref = new PgRef({ db: pg.db, name: "conformance", initial: 0 });
    // @ts-ignore
    await ref._ensureReady();
    setupDone = true;
  }
  // Reset to initial value 0 for each test
  await pg.sql`DELETE FROM promin_ref WHERE name = 'conformance'`;
  return new PgRef({ db: pg.db, name: "conformance", initial: 0 });
});

// ---------------------------------------------------------------------------
// Postgres-specific tests
// ---------------------------------------------------------------------------

describe("PgRef — Postgres-specific", () => {
  beforeEach(async () => {
    await pg.sql`DELETE FROM promin_ref WHERE name LIKE 'pg-ref%'`;
  });

  it("persists value across instances", async () => {
    const ref1 = new PgRef({ db: pg.db, name: "pg-ref-persist", initial: 10 });
    await ref1.setAsync(42);

    const ref2 = new PgRef({ db: pg.db, name: "pg-ref-persist", initial: 0 });
    const value = await ref2.getAsync();
    expect(value).toBe(42);
  });

  it("updateAsync is atomic via transaction", async () => {
    const ref = new PgRef({ db: pg.db, name: "pg-ref-atomic", initial: 0 });

    // Run 5 concurrent increments
    await Promise.all([
      ref.updateAsync((v) => v + 1),
      ref.updateAsync((v) => v + 1),
      ref.updateAsync((v) => v + 1),
      ref.updateAsync((v) => v + 1),
      ref.updateAsync((v) => v + 1),
    ]);

    const value = await ref.getAsync();
    expect(value).toBe(5);
  });

  it("stores and retrieves complex JSON values", async () => {
    const ref = new PgRef<{ name: string; count: number }>({
      db: pg.db,
      name: "pg-ref-json",
      initial: { name: "init", count: 0 },
    });

    await ref.setAsync({ name: "updated", count: 99 });
    const value = await ref.getAsync();
    expect(value).toEqual({ name: "updated", count: 99 });
  });
});
