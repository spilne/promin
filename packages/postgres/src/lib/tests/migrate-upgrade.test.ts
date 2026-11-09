import { it, expect } from "bun:test";
import { cpSync, readFileSync, mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { postgresDescribe } from "../test-utils.ts";
import { migrate } from "../migrate.ts";
import { PgStepQueue } from "../pg-step-queue.ts";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";

// ---------------------------------------------------------------------------
// A database that applied an earlier migration chain (the baseline alone, no
// `wf_workflows.step_progress`) upgrades through `migrate()`: reads and
// `saveStepProgress` work afterwards, and migrating again changes nothing.
// ---------------------------------------------------------------------------

const DRIZZLE = new URL("../../../drizzle", import.meta.url).pathname;

/** A copy of the migrations folder holding the first `count` migrations (the chain an old database applied). */
function chainFolder(count: number): string {
  const dir = mkdtempSync(join(tmpdir(), "promin-pg-baseline-"));
  mkdirSync(join(dir, "meta"));
  const journal = JSON.parse(readFileSync(join(DRIZZLE, "meta", "_journal.json"), "utf8"));
  journal.entries = journal.entries.filter((e: { idx: number }) => e.idx < count);
  for (const e of journal.entries) cpSync(join(DRIZZLE, `${e.tag}.sql`), join(dir, `${e.tag}.sql`));
  writeFileSync(join(dir, "meta", "_journal.json"), JSON.stringify(journal));
  return dir;
}

/** Only the baseline. */
const baselineOnlyFolder = () => chainFolder(1);

postgresDescribe("migrate() upgrades an existing database", {}, (pg) => {
  it("adds step_progress to a baseline-only database, idempotently", async () => {
    await migrate(pg.db, { migrationsFolder: baselineOnlyFolder() });
    const has = async () =>
      (
        await pg.sql`SELECT 1 FROM information_schema.columns
          WHERE table_name = 'wf_workflows' AND column_name = 'step_progress'`
      ).length === 1;
    // Earlier changes edited the baseline in place; simulate a database from before them.
    await pg.sql`ALTER TABLE wf_workflows DROP COLUMN compensation_origin`;
    await pg.sql`ALTER TABLE wf_step_queue DROP COLUMN group_name, DROP COLUMN group_max_concurrent,
      DROP COLUMN group_max_starts, DROP COLUMN group_window_ms, DROP COLUMN fairness_key,
      DROP COLUMN heartbeat_details`;
    expect(await has()).toBe(false);
    const storage = await PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false });
    await pg.sql`INSERT INTO wf_workflows (workflow_id, workflow_name, input)
      VALUES ('old-1', 'w', '{}'::jsonb)`;
    // The old shape cannot serve the new reads or writes.
    await expect(storage.loadWorkflow("old-1")).rejects.toThrow();

    await migrate(pg.db);
    await migrate(pg.db);
    expect(await has()).toBe(true);
    const columns = await pg.sql`SELECT column_name FROM information_schema.columns
      WHERE (table_name = 'wf_workflows' AND column_name = 'compensation_origin')
         OR (table_name = 'wf_step_queue' AND column_name IN ('group_name', 'heartbeat_details'))`;
    expect(columns.length).toBe(3);
    const index =
      await pg.sql`SELECT 1 FROM pg_indexes WHERE indexname = 'wf_step_queue_group_started_idx'`;
    expect(index.length).toBe(1);

    const at = new Date("2026-01-01T00:00:00.000Z");
    const loaded = await storage.loadWorkflow("old-1");
    expect(loaded?.workflowName).toBe("w");
    await storage.saveStepProgress({
      workflowId: "old-1",
      stepName: "a",
      progress: { phase: "running", attempt: 1, at },
    });
    expect((await storage.loadWorkflow("old-1"))?.stepProgress?.["a"]).toEqual({
      phase: "running",
      attempt: 1,
      at,
      run: 1,
    });
  });

  it("adds claim lanes to a database migrated through 0001, keeping its queued tasks", async () => {
    await pg.sql`DROP SCHEMA public CASCADE`;
    await pg.sql`CREATE SCHEMA public`;
    await pg.sql`DROP SCHEMA IF EXISTS drizzle CASCADE`;
    await migrate(pg.db, { migrationsFolder: chainFolder(2) });
    const lane = async () =>
      pg.sql`SELECT attgenerated FROM pg_attribute
        WHERE attrelid = 'wf_step_queue'::regclass AND attname = 'lane' AND NOT attisdropped`;
    expect((await lane()).length).toBe(0);
    await pg.sql`INSERT INTO wf_step_queue (workflow_id, step_name, concurrency_key, concurrency_scope, concurrency_limit)
      VALUES ('old-a', 's', 'k', 'sc', 1), ('old-b', 's', 'k', 'sc', 1), ('old-c', 's', NULL, NULL, NULL)`;

    await migrate(pg.db);
    await migrate(pg.db);
    expect((await lane())[0]?.attgenerated).toBe("s");
    const lanes = await pg.sql`SELECT lane, count(*)::int AS n FROM wf_step_queue GROUP BY lane`;
    expect(lanes.length).toBe(2);
    expect(lanes.every((l) => l.lane != null)).toBe(true);
    const indexes = await pg.sql`SELECT indexname FROM pg_indexes
      WHERE indexname IN ('wf_step_queue_lane_idx', 'wf_step_queue_pending_group_idx')`;
    expect(indexes.length).toBe(2);

    // Rows queued before the upgrade are claimed by the lane path, key limit honoured.
    const queue = new PgStepQueue({ db: pg.db });
    const claimed = await queue.claim({ workerId: "w", limit: 10 });
    expect(claimed.map((t) => t.workflowId).sort()).toEqual(["old-a", "old-c"]);
  });
});
