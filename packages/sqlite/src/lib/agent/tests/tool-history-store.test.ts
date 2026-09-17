// ---------------------------------------------------------------------------
// SqliteToolHistoryStore — runs the @promin/agent toolHistoryStoreTestSuite
// against an in-memory SQLite database, plus SQLite-specific checks
// (FakeClock exact timing, custom table name).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { FakeClock } from "@promin/core";
import { toolHistoryStoreTestSuite } from "@promin/agent/testing";
import type { ToolObservation } from "@promin/agent";
import { SqliteToolHistoryStore } from "../tool-history-store.ts";

toolHistoryStoreTestSuite(() => SqliteToolHistoryStore.make({ db: new Database(":memory:") }));

const observation: ToolObservation = {
  name: "search",
  sourceKind: "in-process",
  sourceDetail: "",
  schemaHash: "hash-a",
  description: "Search",
};

describe("SqliteToolHistoryStore — SQLite-specific", () => {
  it("stamps first/last seen with the clock, holding firstSeenAt on re-record", async () => {
    const clock = FakeClock.create(1_000);
    const store = SqliteToolHistoryStore.make({ db: new Database(":memory:"), clock });
    await store.recordSnapshot([observation]);

    clock.advance(5_000);
    await store.recordSnapshot([observation]);

    const [r] = await store.list();
    expect(r?.firstSeenAt).toBe(1_000);
    expect(r?.lastSeenAt).toBe(6_000);
  });

  it("respects a custom table name", async () => {
    const db = new Database(":memory:");
    const store = SqliteToolHistoryStore.make({ db, table: "my_tool_history" });
    await store.recordSnapshot([observation]);
    const rows = db.query("SELECT name FROM my_tool_history").all() as Array<{ name: string }>;
    expect(rows).toEqual([{ name: "search" }]);
  });
});
