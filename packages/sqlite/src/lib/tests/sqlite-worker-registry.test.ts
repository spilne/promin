import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { workerRegistryConformance } from "@promin/workflow/testing";
import { SqliteWorkerRegistry } from "../sqlite-worker-registry.ts";

// Each factory call gets a fresh :memory: database — natural per-test
// isolation so the shared conformance suite (which expects a clean slate)
// works without TRUNCATE.
workerRegistryConformance({
  factory: async () => SqliteWorkerRegistry.make({ db: new Database(":memory:") }),
});

// ---- SQLite-specific tests ----

describe("SqliteWorkerRegistry", () => {
  it("persists across instances sharing the same db", async () => {
    const db = new Database(":memory:");
    const r1 = SqliteWorkerRegistry.make({ db });
    await r1.register({
      workerId: "w-1",
      capabilities: ["summarize"],
      concurrency: 2,
      metadata: { host: "node-1" },
    });

    const r2 = SqliteWorkerRegistry.make({ db });
    const [row] = await r2.list();
    expect(row!.workerId).toBe("w-1");
    expect(row!.capabilities).toEqual(["summarize"]);
    expect(row!.metadata).toEqual({ host: "node-1" });
  });

  it("custom table prefix avoids conflicts across registries in the same db", async () => {
    const db = new Database(":memory:");
    const a = SqliteWorkerRegistry.make({ db, tablePrefix: "reg_a" });
    const b = SqliteWorkerRegistry.make({ db, tablePrefix: "reg_b" });

    await a.register({ workerId: "same-id", capabilities: ["A"], concurrency: 1 });
    await b.register({ workerId: "same-id", capabilities: ["B"], concurrency: 1 });

    expect((await a.list())[0]!.capabilities).toEqual(["A"]);
    expect((await b.list())[0]!.capabilities).toEqual(["B"]);
  });

  it("capabilities round-trip through JSON serialization unchanged", async () => {
    const registry = SqliteWorkerRegistry.make({ db: new Database(":memory:") });
    const caps = ["gpu", "summarize", "code:review"];
    await registry.register({ workerId: "w-1", capabilities: caps, concurrency: 1 });
    const [row] = await registry.list();
    expect(row!.capabilities).toEqual(caps);
  });

  it("missing metadata round-trips as undefined, not empty object or null", async () => {
    const registry = SqliteWorkerRegistry.make({ db: new Database(":memory:") });
    await registry.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
    const [row] = await registry.list();
    expect(row!.metadata).toBeUndefined();
  });

  it("migrates a legacy pre-retired table — rebuilds it, preserving rows", async () => {
    const db = new Database(":memory:");
    // Hand-create the old schema: status CHECK-constrained to the three
    // pre-retirement values, no retired_at column.
    db.run(`
      CREATE TABLE promin_wf_workers (
        worker_id         TEXT    NOT NULL PRIMARY KEY,
        status            TEXT    NOT NULL DEFAULT 'active'
          CHECK (status IN ('active', 'draining', 'dead')),
        capabilities      TEXT    NOT NULL DEFAULT '[]',
        concurrency       INTEGER NOT NULL DEFAULT 1,
        metadata          TEXT,
        started_at        INTEGER NOT NULL,
        last_heartbeat_at INTEGER NOT NULL
      )
    `);
    db.run(
      `INSERT INTO promin_wf_workers
         (worker_id, status, capabilities, concurrency, started_at, last_heartbeat_at)
       VALUES ('legacy-1', 'active', '["gpu"]', 2, ?, ?)`,
      Date.now(),
      Date.now(),
    );

    // make() runs _setup → detects the legacy schema → rebuilds the table.
    const registry = SqliteWorkerRegistry.make({ db });

    // The legacy row survived the rebuild.
    const all = await registry.list();
    expect(all.map((w) => w.workerId)).toEqual(["legacy-1"]);
    expect(all[0]!.capabilities).toEqual(["gpu"]);
    expect(all[0]!.concurrency).toBe(2);

    // 'retired' — rejected by the old CHECK — now works post-migration.
    await registry.deregister("legacy-1");
    expect(await registry.list({ status: "retired" })).toHaveLength(1);
  });
});
