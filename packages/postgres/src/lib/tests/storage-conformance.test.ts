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
      wf_activity_journal,
      wf_signal_tokens,
      wf_streams,
      wf_workflows,
      wf_step_queue
    RESTART IDENTITY CASCADE`;
  });

  // Peers share the connection pool but are separate storage instances
  // (own instance id) — the shape of two workers on one database.
  storageTestSuite(
    async () => {
      return PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false });
    },
    {
      hasJournal: true,
      hasJournaledSuspend: true,
      hasResetSteps: true,
      hasScannerQueries: true,
      hasCompensationLedger: true,
      createPeer: () => PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false }),
    },
  );
});
