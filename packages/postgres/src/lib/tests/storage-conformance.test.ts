import { storageTestSuite } from "@promin/workflow/testing";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";
import { migrate } from "../migrate.ts";
import { postgresDescribe } from "../test-utils.ts";

postgresDescribe("PostgresWorkflowStorage conformance", { migrate }, (pg) => {
  storageTestSuite(
    async () => {
      return PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false });
    },
    { hasJournal: true, hasJournaledSuspend: true },
  );
});
