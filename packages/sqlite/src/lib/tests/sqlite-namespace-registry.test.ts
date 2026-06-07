import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { namespaceRegistryTestSuite } from "@promin/core/testing";
import { SqliteNamespaceRegistry } from "../sqlite-namespace-registry.ts";

let counter = 0;
function makeRegistry(): SqliteNamespaceRegistry {
  return SqliteNamespaceRegistry.make({
    db: new Database(":memory:"),
    tableName: `promin_namespace_${++counter}`,
  });
}

namespaceRegistryTestSuite(makeRegistry);

describe("SqliteNamespaceRegistry — persistence", () => {
  it("namespaces persist across instances sharing one db", async () => {
    const db = new Database(":memory:");
    const r1 = SqliteNamespaceRegistry.make({ db });
    await r1.create({
      id: "acme",
      displayName: "Acme",
      capabilities: { workflows: { maxConcurrentRuns: 3 } },
    });

    const r2 = SqliteNamespaceRegistry.make({ db });
    const got = await r2.get("acme");
    expect(got?.displayName).toBe("Acme");
    expect(got?.capabilities.workflows?.maxConcurrentRuns).toBe(3);
  });
});
