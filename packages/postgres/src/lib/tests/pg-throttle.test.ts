import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test";
import { PostgresTestContainer } from "../test-utils.ts";
import { throttleTestSuite } from "@promin/core/testing";
import { PgThrottle } from "../pg-throttle.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
}, 60_000);

afterAll(async () => {
  await pg.stop();
});

// ---------------------------------------------------------------------------
// Conformance suite
// Factory creates a throttle with permits=1, windowMs=100 (matches suite expectations)
// ---------------------------------------------------------------------------

let setupDone = false;

throttleTestSuite(async () => {
  if (!setupDone) {
    const t = new PgThrottle({
      db: pg.db,
      key: "conformance",
      permits: 1,
      windowMs: 100,
      pollIntervalMs: 10,
    });
    // @ts-ignore
    await t._ensureReady();
    setupDone = true;
  }
  await pg.sql`DELETE FROM promin_throttle WHERE key LIKE 'conformance%'`;
  return new PgThrottle({
    db: pg.db,
    key: "conformance",
    permits: 1,
    windowMs: 100,
    pollIntervalMs: 10,
  });
});

// ---------------------------------------------------------------------------
// Postgres-specific tests
// ---------------------------------------------------------------------------

describe("PgThrottle — Postgres-specific", () => {
  beforeEach(async () => {
    await pg.sql`DELETE FROM promin_throttle WHERE key LIKE 'pg-throttle%'`;
  });

  it("persists permit usage across instances", async () => {
    const t1 = new PgThrottle({
      db: pg.db,
      key: "pg-throttle-persist",
      permits: 1,
      windowMs: 1000,
      pollIntervalMs: 10,
    });
    await t1.acquireAsync();

    const t2 = new PgThrottle({
      db: pg.db,
      key: "pg-throttle-persist",
      permits: 1,
      windowMs: 1000,
      pollIntervalMs: 10,
    });
    expect(await t2.tryAcquireAsync()).toBe(false);
  });

  it("two concurrent instances share the throttle", async () => {
    const t1 = new PgThrottle({
      db: pg.db,
      key: "pg-throttle-concurrent",
      permits: 2,
      windowMs: 1000,
      pollIntervalMs: 10,
    });
    const t2 = new PgThrottle({
      db: pg.db,
      key: "pg-throttle-concurrent",
      permits: 2,
      windowMs: 1000,
      pollIntervalMs: 10,
    });

    await t1.acquireAsync();
    await t2.acquireAsync();

    // Both permits consumed across instances
    expect(await t1.tryAcquireAsync()).toBe(false);
    expect(await t2.tryAcquireAsync()).toBe(false);
  });

  it("withPermitAsync runs callback after acquiring", async () => {
    const t = new PgThrottle({
      db: pg.db,
      key: "pg-throttle-fn",
      permits: 1,
      windowMs: 100,
      pollIntervalMs: 10,
    });
    const result = await t.withPermitAsync(async () => "hello");
    expect(result).toBe("hello");
  });
});
