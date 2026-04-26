// ---------------------------------------------------------------------------
// PgSchedulerStorage — portable conformance suite invocation.
//
// Mirrors storage-conformance.test.ts. The factory truncates the scheduler
// tables before returning so each test sees a clean storage — the suite
// reuses short ids ("min-1", "early", ...) across tests that would otherwise
// collide on a shared DB.
// ---------------------------------------------------------------------------

import { sql } from "drizzle-orm";
import { schedulerStorageTestSuite } from "@promin/workflow/testing";
import { migrate } from "../migrate.ts";
import { PgSchedulerStorage } from "../pg-scheduler-storage.ts";
import { postgresDescribe } from "../test-utils.ts";

postgresDescribe("PgSchedulerStorage conformance", { migrate }, (pg) => {
  schedulerStorageTestSuite(async () => {
    // Truncate both scheduler tables so every test in the suite sees an
    // empty backend. CASCADE handles the FK from durable_schedule_ticks
    // back to durable_schedules.
    await pg.db.execute(sql`TRUNCATE TABLE wf_schedules, wf_schedule_ticks CASCADE`);
    return new PgSchedulerStorage({ db: pg.db });
  });
});
