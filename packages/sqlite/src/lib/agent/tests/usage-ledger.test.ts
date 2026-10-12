// ---------------------------------------------------------------------------
// SqliteUsageLedger — runs the @promin/agent usageLedgerTestSuite against an
// in-memory SQLite database, plus SQLite-specific checks.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { FakeWallClock } from "@promin/workflow";
import { EMPTY_USAGE } from "@promin/agent";
import { usageLedgerTestSuite } from "@promin/agent/testing";
import { SqliteUsageLedger } from "../usage-ledger.ts";

usageLedgerTestSuite(() => {
  const clock = FakeWallClock.create(1_700_000_000_000);
  return {
    ledger: new SqliteUsageLedger({ db: new Database(":memory:"), clock }),
    age: async (ms) => {
      clock.advance(ms);
    },
  };
});

describe("SqliteUsageLedger — SQLite-specific", () => {
  it("keeps rows across ledger instances over one database", async () => {
    const db = new Database(":memory:");
    await new SqliteUsageLedger({ db }).record({
      turnId: "t1",
      namespaceId: "ns",
      usage: { ...EMPTY_USAGE, outputTokens: 9 },
    });
    const reopened = new SqliteUsageLedger({ db });
    expect((await reopened.total({ namespaceId: "ns" })).outputTokens).toBe(9);
    expect(await reopened.record({ turnId: "t1", namespaceId: "ns", usage: EMPTY_USAGE })).toBe(
      false,
    );
  });
});
