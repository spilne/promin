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

/** A copy of the migrations folder holding the chain up to `lastIdx` (what an old database applied). */
function chainFolder(lastIdx: number): string {
  const dir = mkdtempSync(join(tmpdir(), "promin-pg-chain-"));
  mkdirSync(join(dir, "meta"));
  const journal = JSON.parse(readFileSync(join(DRIZZLE, "meta", "_journal.json"), "utf8"));
  journal.entries = journal.entries.filter((e: { idx: number }) => e.idx <= lastIdx);
  for (const entry of journal.entries as { tag: string }[]) {
    cpSync(join(DRIZZLE, `${entry.tag}.sql`), join(dir, `${entry.tag}.sql`));
  }
  writeFileSync(join(dir, "meta", "_journal.json"), JSON.stringify(journal));
  return dir;
}

/** The chain an old database applied: the baseline alone. */
function baselineOnlyFolder(): string {
  return chainFolder(0);
}

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

  it("adds the retention index to a database at 0001, idempotently, and the sweep reads work", async () => {
    // Bring the database to the chain's state at 0001: everything through 0001 applied, 0002 not.
    await migrate(pg.db, { migrationsFolder: chainFolder(1) });
    const last = JSON.parse(
      readFileSync(join(DRIZZLE, "meta", "_journal.json"), "utf8"),
    ).entries.at(-1) as { when: number };
    await pg.sql`DELETE FROM drizzle.__drizzle_migrations WHERE created_at = ${last.when}`;
    await pg.sql`DROP INDEX IF EXISTS wf_workflows_retention_idx`;
    const hasIndex = async () =>
      (await pg.sql`SELECT 1 FROM pg_indexes WHERE indexname = 'wf_workflows_retention_idx'`)
        .length === 1;
    expect(await hasIndex()).toBe(false);

    await migrate(pg.db);
    await migrate(pg.db);
    expect(await hasIndex()).toBe(true);

    const storage = await PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false });
    await storage.createWorkflow({ workflowId: "ret-up-1", workflowName: "w", input: {} });
    await storage.completeWorkflow({ workflowId: "ret-up-1", result: 1 });
    const rows = await storage.listRetentionCandidates({
      completedBefore: new Date(Date.now() + 60_000),
      limit: 10,
      name: "w",
    });
    expect(rows.map((r) => r.workflowId)).toEqual(["ret-up-1"]);
  });
});
