import { versionDrainTestSuite } from "@promin/workflow/testing";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";
import { migrate } from "../migrate.ts";
import { postgresDescribe } from "../test-utils.ts";

postgresDescribe("PostgresWorkflowStorage version drain", { migrate }, (pg) => {
  versionDrainTestSuite(() =>
    PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false }),
  );
});
