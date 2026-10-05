import { sql } from "drizzle-orm";
import { workerRegistryConformance } from "@promin/workflow/testing";
import { PostgresWorkerRegistry } from "../postgres-worker-registry.ts";
import { migrate } from "../migrate.ts";
import { postgresDescribe } from "../test-utils.ts";

// The conformance suite asserts a clean slate per test ('workers.length
// === 1' after registering w-1). postgresDescribe shares one database
// across the tests in this block, so we truncate the worker table in the
// factory to give each `it` the same fresh-registry semantics InMemory
// gets for free.
//
// The registry stamps and compares times with the server's NOW(), so the
// suite can't move its time with a fake clock (`serverClock`) and waits
// real time instead; heartbeatToleranceMs is bumped vs the 50ms default
// because timestamps round-trip through the network + driver.
postgresDescribe("PostgresWorkerRegistry conformance", { migrate }, (pg) => {
  workerRegistryConformance({
    factory: async () => {
      await pg.db.execute(sql`TRUNCATE TABLE wf_worker_registry`);
      return PostgresWorkerRegistry.create({ db: pg.db });
    },
    serverClock: true,
    heartbeatToleranceMs: 150,
  });
});
