import { stateMachineStorageTestSuite } from "@promin/workflow/testing";
import { PgStateMachineStorage } from "../pg-state-machine-storage.ts";
import { migrate } from "../migrate.ts";
import { postgresDescribe } from "../test-utils.ts";

// Peers share the pool but are separate instances (own lock owner id) —
// the shape of two processes on one database. `sm_machine_events` has no
// event-data column, so `eventData` is not persisted.
postgresDescribe("PgStateMachineStorage conformance", { migrate }, (pg) => {
  stateMachineStorageTestSuite(() => new PgStateMachineStorage(pg.db), {
    createPeer: () => new PgStateMachineStorage(pg.db),
    persistsEventData: false,
  });
});
