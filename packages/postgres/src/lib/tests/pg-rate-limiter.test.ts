import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test";
import { PostgresTestContainer } from "../test-utils.ts";
import { rateLimiterTestSuite } from "@promin/core/testing";
import { PgRateLimiter } from "../pg-rate-limiter.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
}, 60_000);

afterAll(async () => {
  await pg.stop();
});

// ---------------------------------------------------------------------------
// Conformance suite
// Factory creates a rate limiter with limit=2, windowMs=100 (matches suite expectations)
// ---------------------------------------------------------------------------

let setupDone = false;

rateLimiterTestSuite(async () => {
  if (!setupDone) {
    // Trigger table creation on first call
    const rl = new PgRateLimiter({ db: pg.db, key: "conformance", limit: 2, windowMs: 100 });
    // @ts-ignore
    await rl._ensureReady();
    setupDone = true;
  }
  // Clean up table rows between tests
  await pg.sql`DELETE FROM promin_rate_limit WHERE key LIKE 'conformance%'`;
  return new PgRateLimiter({ db: pg.db, key: "conformance", limit: 2, windowMs: 100 });
});

// ---------------------------------------------------------------------------
// Postgres-specific tests
// ---------------------------------------------------------------------------

describe("PgRateLimiter — Postgres-specific", () => {
  let rl: PgRateLimiter;

  beforeEach(async () => {
    await pg.sql`DELETE FROM promin_rate_limit WHERE key LIKE 'pg-specific%'`;
    rl = new PgRateLimiter({ db: pg.db, key: "pg-specific", limit: 3, windowMs: 1000 });
  });

  it("persists state across instances with the same key", async () => {
    // First instance consumes 2 permits
    await rl.acquireAsync();
    await rl.acquireAsync();

    // Second instance with same key sees the consumed permits
    const rl2 = new PgRateLimiter({ db: pg.db, key: "pg-specific", limit: 3, windowMs: 1000 });
    const remaining = await rl2.remainingAsync();
    expect(remaining).toBe(1);
  });

  it("two instances share the same sliding window", async () => {
    const rl1 = new PgRateLimiter({
      db: pg.db,
      key: "pg-specific-shared",
      limit: 2,
      windowMs: 1000,
    });
    const rl2 = new PgRateLimiter({
      db: pg.db,
      key: "pg-specific-shared",
      limit: 2,
      windowMs: 1000,
    });

    await rl1.acquireAsync();
    await rl2.acquireAsync();

    // Both instances have consumed all permits — neither can acquire more
    await expect(rl1.tryAcquireAsync()).resolves.toBe(false);
    await expect(rl2.tryAcquireAsync()).resolves.toBe(false);
  });

  it("different keys are fully independent", async () => {
    const rlA = new PgRateLimiter({ db: pg.db, key: "pg-specific-a", limit: 1, windowMs: 1000 });
    const rlB = new PgRateLimiter({ db: pg.db, key: "pg-specific-b", limit: 1, windowMs: 1000 });

    await pg.sql`DELETE FROM promin_rate_limit WHERE key IN ('pg-specific-a:', 'pg-specific-b:')`;

    await rlA.acquireAsync();
    // rlB is unaffected
    await expect(rlB.tryAcquireAsync()).resolves.toBe(true);
  });
});
