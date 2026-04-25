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
// heartbeatToleranceMs bumped vs InMemory — Postgres timestamps round-trip
// through the network + driver so a 50ms window flakes on slow CI.
postgresDescribe("PostgresWorkerRegistry conformance", { migrate }, (pg) => {
  workerRegistryConformance({
    factory: async () => {
      await pg.db.execute(sql`TRUNCATE TABLE wf_worker_registry`);
      return PostgresWorkerRegistry.create({ db: pg.db });
    },
    heartbeatToleranceMs: 150,
  });
});
