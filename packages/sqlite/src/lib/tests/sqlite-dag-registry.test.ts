// ---------------------------------------------------------------------------
// SqliteDagRegistry parity check + a couple of persistence-specific cases.
// The conformance suite covers semantics; the local cases pin SQLite-only
// concerns (cross-instance survival, custom table name).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { dagRegistryTestSuite } from "@promin/agent/testing";
import { SqliteDagRegistry } from "../sqlite-dag-registry.ts";

dagRegistryTestSuite(() => SqliteDagRegistry.make({ db: new Database(":memory:") }));

describe("SqliteDagRegistry — persistence", () => {
  it("survives a re-open against the same db", async () => {
    const db = new Database(":memory:");
    const r1 = SqliteDagRegistry.make({ db });
    await r1.register({
      id: "research",
      version: "v1",
      nodes: [
        { id: "a", agentId: "x", inputs: { task: { kind: "initial", path: "task" } } },
        { id: "b", agentId: "x", inputs: { task: { kind: "node", nodeId: "a", path: "" } } },
      ],
      edges: [{ from: "a", to: "b" }],
      entry: ["a"],
      terminals: ["b"],
      metadata: { tags: ["analysis"] },
    });
    const r2 = SqliteDagRegistry.make({ db });
    const fetched = await r2.get("research");
    expect(fetched).not.toBeNull();
    expect(fetched!.nodes).toHaveLength(2);
    expect(fetched!.metadata?.tags).toEqual(["analysis"]);
  });

  it("custom table name is honored", async () => {
    const db = new Database(":memory:");
    const r = SqliteDagRegistry.make({ db, table: "my_dag_table" });
    await r.register({
      id: "x",
      version: "v1",
      nodes: [{ id: "a", agentId: "x", inputs: {} }],
      edges: [],
      entry: ["a"],
      terminals: ["a"],
    });
    const exists = db
      .query<{ name: string }>(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='my_dag_table'",
      )
      .get();
    expect(exists).not.toBeNull();
  });
});
