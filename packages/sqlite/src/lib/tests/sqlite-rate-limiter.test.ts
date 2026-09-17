import { describe, it, expect, beforeEach } from "bun:test";
import { Database } from "bun:sqlite";
import { rateLimiterTestSuite } from "@promin/core/testing";
import { SqliteRateLimiter } from "../sqlite-rate-limiter.ts";

function makeDb() {
  return new Database(":memory:");
}

// ---- conformance suite ----

rateLimiterTestSuite(
  () =>
    SqliteRateLimiter.make({
      db: makeDb(),
      key: "test",
      limit: 2,
      windowMs: 100,
    }),
  { strategy: "sliding-window" },
);

// ---- SQLite-specific tests ----

describe("SqliteRateLimiter", () => {
  let limiter: SqliteRateLimiter;

  beforeEach(() => {
    limiter = SqliteRateLimiter.make({ db: makeDb(), key: "test", limit: 3, windowMs: 200 });
  });

  it("remainingAsync reflects available slots", async () => {
    expect(await limiter.remainingAsync()).toBe(3);
    await limiter.acquireAsync();
    expect(await limiter.remainingAsync()).toBe(2);
    await limiter.acquireAsync();
    expect(await limiter.remainingAsync()).toBe(1);
  });

  it("per-resource keys are isolated", async () => {
    await limiter.acquireAsync("a");
    await limiter.acquireAsync("a");
    await limiter.acquireAsync("a");
    // "a" is exhausted, "b" should be unaffected
    expect(await limiter.tryAcquireAsync("b")).toBe(true);
  });

  it("state persists across instances sharing the same db file", async () => {
    const db = makeDb();
    const l1 = SqliteRateLimiter.make({ db, key: "shared", limit: 2, windowMs: 1_000 });
    await l1.acquireAsync();
    await l1.acquireAsync();
    // Second instance against same DB should see the consumed slots.
    const l2 = SqliteRateLimiter.make({ db, key: "shared", limit: 2, windowMs: 1_000 });
    expect(await l2.tryAcquireAsync()).toBe(false);
  });

  it("multiple queues can share one table via distinct keys", async () => {
    const db = makeDb();
    const a = SqliteRateLimiter.make({ db, key: "svc-a", limit: 1, windowMs: 1_000 });
    const b = SqliteRateLimiter.make({ db, key: "svc-b", limit: 1, windowMs: 1_000 });
    await a.acquireAsync();
    expect(await a.tryAcquireAsync()).toBe(false);
    expect(await b.tryAcquireAsync()).toBe(true);
  });

  it("custom table name works", async () => {
    const l = SqliteRateLimiter.make({
      db: makeDb(),
      key: "k",
      limit: 1,
      windowMs: 1_000,
      table: "my_limits",
    });
    await l.acquireAsync();
    expect(await l.tryAcquireAsync()).toBe(false);
  });
});
