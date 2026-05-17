// ---------------------------------------------------------------------------
// `SqliteAgentRegistry` — runs the full conformance suite plus
// SQLite-specific persistence + custom-table sanity tests.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { agentRegistryTestSuite } from "@promin/agent/testing";
import { SqliteAgentRegistry } from "../agent-registry.ts";

function makeRegistry() {
  return SqliteAgentRegistry.make({ db: new Database(":memory:") });
}

agentRegistryTestSuite(makeRegistry);

describe("SqliteAgentRegistry — persistence", () => {
  it("registers persist across instances sharing one db", async () => {
    const db = new Database(":memory:");
    const r1 = SqliteAgentRegistry.make({ db });
    await r1.register({
      id: "support",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        systemPrompt: "Helpful assistant",
        tools: ["search"],
      },
      metadata: { capabilities: ["chat"], tags: ["beta"] },
    });

    const r2 = SqliteAgentRegistry.make({ db });
    const got = await r2.get("support");
    expect(got?.id).toBe("support");
    expect(got?.metadata.tags).toEqual(["beta"]);
    if (got?.backend.type === "local") {
      expect(got.backend.model.id).toBe("claude-sonnet-4-6");
    }
  });

  it("respects a custom table name", async () => {
    const db = new Database(":memory:");
    const r = SqliteAgentRegistry.make({ db, table: "my_registry" });
    await r.register({
      id: "x",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "x" },
        systemPrompt: null,
        tools: [],
      },
    });
    const rows = db.query("SELECT id FROM my_registry").all() as Array<{ id: string }>;
    expect(rows).toEqual([{ id: "x" }]);
  });
});
