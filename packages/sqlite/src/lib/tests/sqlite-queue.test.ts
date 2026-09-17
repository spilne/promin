import { describe, it, expect, beforeEach } from "bun:test";
import { Database } from "bun:sqlite";
import { queueTestSuite } from "@promin/core/testing";
import { SqliteQueue } from "../sqlite-queue.ts";

function makeDb() {
  return new Database(":memory:");
}

// ---- conformance suite ----

queueTestSuite(() => SqliteQueue.make<number>({ db: makeDb(), name: "test", pollIntervalMs: 10 }));

// ---- SQLite-specific tests ----

describe("SqliteQueue", () => {
  let queue: SqliteQueue<{ id: number }>;

  beforeEach(() => {
    queue = SqliteQueue.make({ db: makeDb(), name: "jobs", pollIntervalMs: 10 });
  });

  it("items persist across instances sharing the same db", async () => {
    const db = makeDb();
    const q1 = SqliteQueue.make<number>({ db, name: "shared", pollIntervalMs: 10 });
    await q1.offerAsync(99);
    const q2 = SqliteQueue.make<number>({ db, name: "shared", pollIntervalMs: 10 });
    expect(await q2.takeAsync()).toBe(99);
  });

  it("multiple named queues share one table without interference", async () => {
    const db = makeDb();
    const a = SqliteQueue.make<number>({ db, name: "q-a", pollIntervalMs: 10 });
    const b = SqliteQueue.make<number>({ db, name: "q-b", pollIntervalMs: 10 });
    await a.offerAsync(1);
    await b.offerAsync(2);
    expect(await a.takeAsync()).toBe(1);
    expect(await b.takeAsync()).toBe(2);
    expect(await a.sizeAsync()).toBe(0);
    expect(await b.sizeAsync()).toBe(0);
  });

  it("sizeAsync reflects pending count", async () => {
    expect(await queue.sizeAsync()).toBe(0);
    await queue.offerAsync({ id: 1 });
    await queue.offerAsync({ id: 2 });
    expect(await queue.sizeAsync()).toBe(2);
    await queue.takeAsync();
    expect(await queue.sizeAsync()).toBe(1);
  });

  it("throws on offer/take after shutdown", async () => {
    await queue.shutdownAsync();
    await expect(queue.offerAsync({ id: 1 })).rejects.toThrow("shut down");
    await expect(queue.takeAsync()).rejects.toThrow("shut down");
  });

  it("custom table name works", async () => {
    const q = SqliteQueue.make<string>({
      db: makeDb(),
      name: "work",
      table: "my_jobs",
      pollIntervalMs: 10,
    });
    await q.offerAsync("task-1");
    expect(await q.takeAsync()).toBe("task-1");
  });

  it("FIFO ordering is maintained under load", async () => {
    for (let i = 0; i < 10; i++) await queue.offerAsync({ id: i });
    const results: number[] = [];
    for (let i = 0; i < 10; i++) results.push((await queue.takeAsync()).id);
    expect(results).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });
});
