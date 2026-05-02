import { beforeEach } from "bun:test";
import { storageTestSuite } from "@promin/workflow/testing";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";
import { migrate } from "../migrate.ts";
import { postgresDescribe } from "../test-utils.ts";

postgresDescribe("PostgresWorkflowStorage conformance", { migrate }, (pg) => {
  // The conformance suite expects a fresh storage per test (in-memory just
  // re-news the object). With one shared Postgres container per file, we
  // emulate that by truncating workflow tables between tests so list/order/
  // distinct assertions don't see leftover rows from siblings.
  beforeEach(async () => {
    await pg.sql`TRUNCATE TABLE
      wf_workflow_step_tasks,
      wf_workflow_steps,
      wf_workflow_signals,
      wf_workflow_locks,
      wf_workflow_runs,
      wf_workflows,
      wf_step_queue
    RESTART IDENTITY CASCADE`;
  });

  storageTestSuite(
    async () => {
      return PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false });
    },
    { hasJournal: true, hasJournaledSuspend: true },
  );
});
