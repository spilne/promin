import { journalReplayTestSuite } from "@promin/workflow/testing";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";
import { migrate } from "../migrate.ts";
import { postgresDescribe } from "../test-utils.ts";

// Journal replay conformance: branch paths written by ctx.parallel
// round-trip through `wf_activity_journal.branch_path`.
postgresDescribe("PostgresWorkflowStorage journal replay", { migrate }, (pg) => {
  journalReplayTestSuite(
    () => PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false }),
    { timingRuns: 10 },
  );
});
