import { stateMachineStorageTestSuite } from "@promin/workflow/testing";
import { PgStateMachineStorage } from "../pg-state-machine-storage.ts";
import { migrate } from "../migrate.ts";
import { postgresDescribe } from "../test-utils.ts";

// Peers share the pool but are separate instances — the shape of two
// processes on one database.
postgresDescribe("PgStateMachineStorage conformance", { migrate }, (pg) => {
  stateMachineStorageTestSuite(() => new PgStateMachineStorage(pg.db), {
    createPeer: () => new PgStateMachineStorage(pg.db),
  });
});
