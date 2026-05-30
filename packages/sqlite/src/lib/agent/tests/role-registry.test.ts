// ---------------------------------------------------------------------------
// `SqliteRoleRegistry` — runs the full conformance suite plus SQLite-specific
// persistence + custom-table sanity tests.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { roleRegistryTestSuite } from "@promin/agent/testing";
import { SqliteRoleRegistry } from "../role-registry.ts";

function makeRegistry() {
  return SqliteRoleRegistry.make({ db: new Database(":memory:") });
}

roleRegistryTestSuite(makeRegistry);

describe("SqliteRoleRegistry — persistence", () => {
  it("registrations persist across instances sharing one db", async () => {
    const db = new Database(":memory:");
    const r1 = SqliteRoleRegistry.make({ db });
    await r1.register({
      id: "git-master",
      definition: {
        systemPrompt: { base: "you are a git master", layers: ["findings-table"] },
        tools: ["bash"],
        skills: [{ id: "structured-debugging" }],
        capabilities: ["chat"],
      },
      metadata: { description: "Git expert", tags: ["role"], suggestedSecrets: ["GH_TOKEN"] },
    });

    const r2 = SqliteRoleRegistry.make({ db });
    const got = await r2.get("git-master");
    expect(got?.id).toBe("git-master");
    expect(got?.metadata.tags).toEqual(["role"]);
    expect(got?.metadata.suggestedSecrets).toEqual(["GH_TOKEN"]);
    expect(got?.definition.tools).toEqual(["bash"]);
    expect(got?.definition.systemPrompt).toEqual({
      base: "you are a git master",
      layers: ["findings-table"],
    });
  });

  it("respects a custom table name", async () => {
    const db = new Database(":memory:");
    const r = SqliteRoleRegistry.make({ db, table: "my_roles" });
    await r.register({ id: "x", definition: { systemPrompt: null, tools: [] } });
    const rows = db.query("SELECT id FROM my_roles").all() as Array<{ id: string }>;
    expect(rows).toEqual([{ id: "x" }]);
  });
});
