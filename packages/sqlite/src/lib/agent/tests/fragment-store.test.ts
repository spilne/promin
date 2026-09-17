// `SqliteFragmentStore` — runs the conformance suite + a sanity test
// that rows persist across instances pointing at the same db.

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { fragmentStoreTestSuite } from "@promin/agent/testing";
import { SqliteFragmentStore } from "../fragment-store.ts";

fragmentStoreTestSuite(() => SqliteFragmentStore.make({ db: new Database(":memory:") }));

describe("SqliteFragmentStore — persistence", () => {
  it("rows persist across instances sharing one db", async () => {
    const db = new Database(":memory:");
    const a = SqliteFragmentStore.make({ db });
    await a.set("verifier-tiered-checks", "## Tiered checks\nT1/T2/T3");

    const b = SqliteFragmentStore.make({ db });
    const all = await b.loadAll();
    expect(all).toHaveLength(1);
    expect(all[0]!.key).toBe("verifier-tiered-checks");
    expect(all[0]!.content).toContain("T1/T2/T3");
  });
});
