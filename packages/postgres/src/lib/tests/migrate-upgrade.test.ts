import { beforeEach, it, expect } from "bun:test";
import { cpSync, readFileSync, mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { postgresDescribe } from "../test-utils.ts";
import { migrate } from "../migrate.ts";
import { PgStepQueue } from "../pg-step-queue.ts";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";
import { PostgresPendingApprovalStore } from "../agent/pending-approval-store.ts";

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
  beforeEach(async () => {
    // Each test must start with its own historical chain, including the ledger.
    await pg.sql`DROP SCHEMA public CASCADE`;
    await pg.sql`CREATE SCHEMA public`;
    await pg.sql`DROP SCHEMA IF EXISTS drizzle CASCADE`;
  });

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
    const retentionMigration = JSON.parse(
      readFileSync(join(DRIZZLE, "meta", "_journal.json"), "utf8"),
    ).entries.find((entry: { idx: number }) => entry.idx === 2) as { when: number };
    await pg.sql`DELETE FROM drizzle.__drizzle_migrations
      WHERE created_at >= ${retentionMigration.when}`;
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

  it("adds claim lanes to a database migrated through 0002, keeping its queued tasks", async () => {
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

  it("adds nullable run security context to a database at 0004", async () => {
    await migrate(pg.db, { migrationsFolder: chainFolder(4) });
    const contextMigration = JSON.parse(
      readFileSync(join(DRIZZLE, "meta", "_journal.json"), "utf8"),
    ).entries.find((entry: { idx: number }) => entry.idx === 5) as { when: number };
    await pg.sql`DELETE FROM drizzle.__drizzle_migrations
      WHERE created_at >= ${contextMigration.when}`;
    await pg.sql`ALTER TABLE wf_workflows DROP COLUMN IF EXISTS security_context`;
    await pg.sql`INSERT INTO wf_workflows (workflow_id, workflow_name, input)
      VALUES ('legacy-security-1', 'w', '{}'::jsonb)`;

    await migrate(pg.db);
    await migrate(pg.db);
    const storage = await PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false });
    expect((await storage.loadWorkflow("legacy-security-1"))?.securityContext).toBeUndefined();

    await storage.createWorkflow({
      workflowId: "secured-1",
      workflowName: "w",
      input: {},
      securityContext: {
        actor: { kind: "service-account", id: "sa-1" },
        effective: { kind: "user", id: "user-7" },
      },
    });
    expect((await storage.loadWorkflow("secured-1"))?.securityContext).toEqual({
      actor: { kind: "service-account", id: "sa-1" },
      effective: { kind: "user", id: "user-7" },
    });
  });

  it("preserves legacy approvals as unknown while new rows are explicit", async () => {
    await migrate(pg.db, { migrationsFolder: chainFolder(5) });
    const approvalMigration = JSON.parse(
      readFileSync(join(DRIZZLE, "meta", "_journal.json"), "utf8"),
    ).entries.find((entry: { idx: number }) => entry.idx === 6) as { when: number };
    await pg.sql`DELETE FROM drizzle.__drizzle_migrations
      WHERE created_at >= ${approvalMigration.when}`;
    await pg.sql`ALTER TABLE agent_pending_approval
      DROP COLUMN IF EXISTS elevated, DROP COLUMN IF EXISTS request_context`;
    await pg.sql`INSERT INTO agent_pending_approval
      (namespace_id, thread_id, tool_call_id, workflow_id, tool_name, input, requested_at, status)
      VALUES ('acme', 't', 'legacy', 'wf-old', 'deploy', '{}'::jsonb, 1, 'pending')`;

    await migrate(pg.db);
    const store = new PostgresPendingApprovalStore({ db: pg.db });
    expect(
      await store.get({
        namespaceId: "acme",
        agentId: null,
        resourceId: null,
        threadId: "t",
        toolCallId: "legacy",
      }),
    ).toMatchObject({
      elevated: null,
      requestContext: null,
    });
    expect(
      await store.record({
        namespaceId: "acme",
        agentId: "ops-bot",
        resourceId: "alice",
        threadId: "t",
        toolCallId: "new",
        workflowId: "wf-new",
        toolName: "deploy",
        input: {},
        elevated: false,
        requestContext: { actor: { kind: "user", id: "alice" } },
      }),
    ).toMatchObject({
      elevated: false,
      requestContext: { actor: { kind: "user", id: "alice" } },
    });
  });

  it("migrates approval identity into the key without losing legacy rows", async () => {
    await migrate(pg.db, { migrationsFolder: chainFolder(6) });
    const scopeMigration = JSON.parse(
      readFileSync(join(DRIZZLE, "meta", "_journal.json"), "utf8"),
    ).entries.find((entry: { idx: number }) => entry.idx === 7) as { when: number };
    await pg.sql`DELETE FROM drizzle.__drizzle_migrations
      WHERE created_at >= ${scopeMigration.when}`;
    await pg.sql`INSERT INTO agent_pending_approval
      (namespace_id, thread_id, tool_call_id, workflow_id, tool_name, input,
       elevated, request_context, requested_at, status)
      VALUES ('acme', 'legacy-thread', 'same-call', 'wf-legacy', 'deploy', '{}'::jsonb,
       NULL, NULL, 1, 'pending')`;

    await migrate(pg.db);
    await migrate(pg.db);
    const store = new PostgresPendingApprovalStore({ db: pg.db });
    expect(
      await store.get({
        namespaceId: "acme",
        agentId: null,
        resourceId: null,
        threadId: "legacy-thread",
        toolCallId: "same-call",
      }),
    ).toMatchObject({ workflowId: "wf-legacy", elevated: null, requestContext: null });

    const base = {
      namespaceId: "acme",
      threadId: "shared",
      toolCallId: "same-call",
      toolName: "deploy",
      input: {},
      elevated: false,
      requestContext: { actor: { kind: "user" as const, id: "alice" } },
    };
    await store.record({
      ...base,
      agentId: "ops-bot",
      resourceId: "alice",
      workflowId: "wf-alice",
    });
    await store.record({
      ...base,
      agentId: "other-bot",
      resourceId: "bob",
      workflowId: "wf-bob",
    });
    expect(
      await store.get({
        namespaceId: "acme",
        agentId: "ops-bot",
        resourceId: "alice",
        threadId: "shared",
        toolCallId: "same-call",
      }),
    ).toMatchObject({ workflowId: "wf-alice" });
    expect(
      await store.get({
        namespaceId: "acme",
        agentId: "other-bot",
        resourceId: "bob",
        threadId: "shared",
        toolCallId: "same-call",
      }),
    ).toMatchObject({ workflowId: "wf-bob" });
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
      await migrate(pg.db, { migrationsFolder: chainFolder(3) });
      expect(await present()).toEqual([]);

      // The current storage adapter also selects the later security-context
      // column; add it without recording its migration so this test can keep
      // exercising the pre-index database shape.
      await pg.sql`ALTER TABLE wf_workflows ADD COLUMN IF NOT EXISTS security_context jsonb`;
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
      const statements = readFileSync(join(DRIZZLE, "0004_timer_scan_indexes.sql"), "utf8");
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
