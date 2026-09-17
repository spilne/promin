// ---------------------------------------------------------------------------
// SqliteAgentInstanceRegistry — the shared conformance suite plus the
// SQLite-specific "instances survive across registry instances on one db"
// persistence check.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { agentInstanceRegistryTestSuite } from "@promin/agent/testing";
import { SqliteAgentInstanceRegistry } from "../agent-instance-registry.ts";

agentInstanceRegistryTestSuite(() =>
  SqliteAgentInstanceRegistry.make({ db: new Database(":memory:") }),
);

describe("SqliteAgentInstanceRegistry — persistence", () => {
  it("instances survive across registry instances sharing one db", async () => {
    const db = new Database(":memory:");
    const r1 = SqliteAgentInstanceRegistry.make({ db });
    const created = await r1.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
      displayName: "alice's writer",
      metadata: { color: "blue" },
    });

    // A fresh registry over the same db sees the persisted row.
    const r2 = SqliteAgentInstanceRegistry.make({ db });
    const got = await r2.get(created.id);
    expect(got?.displayName).toBe("alice's writer");
    expect(got?.metadata).toEqual({ color: "blue" });

    const same = await r2.resolveOrCreate({
      registeredAgentId: "writer",
      namespaceId: "acme",
      ownerId: "alice",
    });
    expect(same.id).toBe(created.id);
    expect(same.createdAt).toBe(created.createdAt);
  });
});
