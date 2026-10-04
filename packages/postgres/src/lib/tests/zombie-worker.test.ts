import { beforeEach } from "bun:test";
import { zombieWorkerTestSuite } from "@promin/workflow/testing";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";
import { migrate } from "../migrate.ts";
import { postgresDescribe } from "../test-utils.ts";

postgresDescribe("PostgresWorkflowStorage zombie worker", { migrate }, (pg) => {
  beforeEach(async () => {
    await pg.sql`TRUNCATE TABLE
      wf_workflow_step_tasks,
      wf_workflow_steps,
      wf_workflow_signals,
      wf_workflow_locks,
      wf_workflow_runs,
      wf_activity_journal,
      wf_workflows
    RESTART IDENTITY CASCADE`;
  });

  // Two storage instances on one database: two workers. The stalled
  // holder's lease runs out when its lock row's expiry moves into the past
  // on the server clock that judges it.
  zombieWorkerTestSuite({
    createStorage: () => PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false }),
    createPeer: () => PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false }),
    expireLock: async ({ workflowId }) => {
      await pg.sql`UPDATE wf_workflow_locks SET expires_at = NOW() - INTERVAL '1 second'
        WHERE workflow_id = ${workflowId}`;
    },
  });
});
