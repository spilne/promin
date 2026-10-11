import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { namespaceRegistryTestSuite } from "@promin/workflow/testing";
import { SqliteNamespaceRegistry } from "../sqlite-namespace-registry.ts";

// A fresh :memory: database per factory call gives every test a clean slate.
namespaceRegistryTestSuite({
  factory: ({ clock }) => SqliteNamespaceRegistry.make({ db: new Database(":memory:"), clock }),
});

describe("SqliteNamespaceRegistry", () => {
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

  it("a table prefix keeps two registries in one db apart", async () => {
    const db = new Database(":memory:");
    const a = SqliteNamespaceRegistry.make({ db, tablePrefix: "reg_a" });
    const b = SqliteNamespaceRegistry.make({ db, tablePrefix: "reg_b" });
    await a.create({ id: "acme" });
    expect(await b.get("acme")).toBeNull();
  });

  it("rejects a table prefix that is not a plain identifier", () => {
    expect(() =>
      SqliteNamespaceRegistry.make({ db: new Database(":memory:"), tablePrefix: "x; DROP" }),
    ).toThrow(/invalid table prefix/);
  });

  it("the CHECK constraint refuses a status the registry does not know", () => {
    const db = new Database(":memory:");
    SqliteNamespaceRegistry.make({ db });
    expect(() =>
      db.run(
        `INSERT INTO promin_wf_namespaces (id, display_name, status, created_at, updated_at)
         VALUES ('x', 'x', 'deleted', 0, 0)`,
      ),
    ).toThrow();
  });
});
