import { describe, it, expect, beforeEach } from "bun:test";
import { Database } from "bun:sqlite";
import { throttleTestSuite } from "@promin/core/testing";
import { SqliteThrottle } from "../sqlite-throttle.ts";

function makeDb() {
  return new Database(":memory:");
}

// ---- conformance suite ----

throttleTestSuite(() =>
  SqliteThrottle.make({ db: makeDb(), key: "test", permits: 1, windowMs: 100 }),
);

// ---- SQLite-specific tests ----

describe("SqliteThrottle", () => {
  let throttle: SqliteThrottle;

  beforeEach(() => {
    throttle = SqliteThrottle.make({ db: makeDb(), key: "test", permits: 2, windowMs: 200 });
  });

  it("per-resource keys are isolated", async () => {
    await throttle.acquireAsync("a");
    await throttle.acquireAsync("a");
    // "a" is full but "b" is empty
    expect(await throttle.tryAcquireAsync("b")).toBe(true);
  });

  it("state persists across instances sharing the same db", async () => {
    const db = makeDb();
    const t1 = SqliteThrottle.make({ db, key: "shared", permits: 1, windowMs: 2_000 });
    await t1.acquireAsync();
    const t2 = SqliteThrottle.make({ db, key: "shared", permits: 1, windowMs: 2_000 });
    expect(await t2.tryAcquireAsync()).toBe(false);
  });

  it("custom table name works", async () => {
    const t = SqliteThrottle.make({
      db: makeDb(),
      key: "k",
      permits: 1,
      windowMs: 1_000,
      table: "my_throttle",
    });
    expect(await t.tryAcquireAsync()).toBe(true);
    expect(await t.tryAcquireAsync()).toBe(false);
  });
});
