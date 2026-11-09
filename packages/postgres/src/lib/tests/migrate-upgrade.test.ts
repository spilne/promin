import { it, expect } from "bun:test";
import { cpSync, readFileSync, mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { postgresDescribe } from "../test-utils.ts";
import { migrate } from "../migrate.ts";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";

// ---------------------------------------------------------------------------
// A database that applied an earlier migration chain (the baseline alone, no
// `wf_workflows.step_progress`) upgrades through `migrate()`: reads and
// `saveStepProgress` work afterwards, and migrating again changes nothing.
// ---------------------------------------------------------------------------

const DRIZZLE = new URL("../../../drizzle", import.meta.url).pathname;

/** A copy of the migrations folder holding the chain up to migration `lastIdx` (what an old database applied). */
function chainUpTo(lastIdx: number): string {
  const dir = mkdtempSync(join(tmpdir(), "promin-pg-chain-"));
  mkdirSync(join(dir, "meta"));
  const journal = JSON.parse(readFileSync(join(DRIZZLE, "meta", "_journal.json"), "utf8"));
  journal.entries = journal.entries.filter((e: { idx: number }) => e.idx <= lastIdx);
  for (const e of journal.entries as Array<{ tag: string }>) {
    cpSync(join(DRIZZLE, `${e.tag}.sql`), join(dir, `${e.tag}.sql`));
  }
  writeFileSync(join(dir, "meta", "_journal.json"), JSON.stringify(journal));
  return dir;
}

/** The baseline alone (the chain the oldest databases applied). */
const baselineOnlyFolder = (): string => chainUpTo(0);

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
});

const TIMER_SCAN_INDEXES = [
  "wf_workflow_steps_sleep_keyset_idx",
  "wf_workflow_steps_signal_timeout_keyset_idx",
  "wf_workflows_suspended_idx",
];

postgresDescribe(
  "migrate() adds the timer scan indexes to a database from before them",
  {},
  (pg) => {
    it("creates the keyset indexes on upgrade, idempotently, and keeps scanning the old rows", async () => {
      const present = async (): Promise<string[]> =>
        (
          (await pg.sql`SELECT indexname FROM pg_indexes WHERE indexname = ANY(${TIMER_SCAN_INDEXES})`) as unknown as Array<{
            indexname: string;
          }>
        )
          .map((r) => r.indexname)
          .sort();

      // A database that applied every migration up to the one before the indexes.
      await migrate(pg.db, { migrationsFolder: chainUpTo(1) });
      expect(await present()).toEqual([]);

      const storage = await PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false });
      for (const workflowId of ["old-a", "old-b", "old-c"]) {
        await storage.createWorkflow({ workflowId, workflowName: "w", input: {} });
        await storage.suspendWorkflow({
          workflowId,
          stepName: "nap",
          stepUpdate: {
            status: "sleeping",
            stepType: "sleep",
            wakeAt: new Date(Date.now() - 60_000),
          },
        });
      }

      await migrate(pg.db);
      await migrate(pg.db);
      expect(await present()).toEqual([...TIMER_SCAN_INDEXES].sort());

      // Re-running the migration's statements (a database that already has
      // some of the indexes) changes nothing and does not fail.
      const statements = readFileSync(join(DRIZZLE, "0002_timer_scan_indexes.sql"), "utf8");
      await pg.sql.unsafe(statements);
      expect(await present()).toEqual([...TIMER_SCAN_INDEXES].sort());

      // The rows written before the upgrade are still scanned, in pages.
      const first = await storage.listDueTimers({ now: new Date(), limit: 2 });
      expect(first.map((r) => r.workflowId)).toEqual(["old-a", "old-b"]);
      const rest = await storage.listDueTimers({
        now: new Date(),
        limit: 2,
        afterWorkflowId: "old-b",
      });
      expect(rest.map((r) => r.workflowId)).toEqual(["old-c"]);
    });
  },
);
