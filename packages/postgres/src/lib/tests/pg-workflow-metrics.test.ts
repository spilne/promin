import { it, expect } from "bun:test";
import { postgresDescribe } from "../test-utils.ts";
import { migrate } from "../migrate.ts";
import { PgWorkflowMetrics } from "../pg-workflow-metrics.ts";
import { StepTypeIds, StepStatusIds } from "../workflow-lookups.ts";
import { sql } from "drizzle-orm";
import type { DrizzleDb } from "../drizzle-db.ts";

// All timestamps are anchored to created_at to avoid wall-clock race conditions.

async function createWorkflow(
  db: DrizzleDb,
  id: string,
  name: string,
  type?: string,
): Promise<void> {
  await db.execute(sql`
    INSERT INTO wf_workflows (workflow_id, workflow_name, workflow_type, input, created_at, updated_at)
    VALUES (${id}, ${name}, ${type ?? null}, '{}'::jsonb, NOW(), NOW())
  `);
}

// Sets started_at = created_at + pendingMs, ensuring pendingMs metric equals pendingMs exactly.
async function markRunning(db: DrizzleDb, id: string, pendingMs: number): Promise<void> {
  await db.execute(sql`
    UPDATE wf_workflows
    SET status_id = 1,
        started_at = created_at + (${pendingMs} || ' milliseconds')::INTERVAL,
        updated_at = NOW()
    WHERE workflow_id = ${id}
  `);
}

async function addSleepStep(
  db: DrizzleDb,
  workflowId: string,
  stepName: string,
  durationMs: number,
): Promise<void> {
  await db.execute(sql`
    INSERT INTO wf_workflow_steps (workflow_id, step_name, run, status_id, step_type_id, started_at, wake_at)
    VALUES (
      ${workflowId}, ${stepName}, 1,
      ${StepStatusIds.id.sleeping},
      ${StepTypeIds.id.sleep},
      NOW() - (${durationMs} || ' milliseconds')::INTERVAL,
      NOW()
    )
  `);
}

async function addSignalStep(
  db: DrizzleDb,
  workflowId: string,
  stepName: string,
  waitMs: number,
): Promise<void> {
  await db.execute(sql`
    INSERT INTO wf_workflow_steps (workflow_id, step_name, run, status_id, step_type_id, started_at, completed_at)
    VALUES (
      ${workflowId}, ${stepName}, 1,
      ${StepStatusIds.id.completed},
      ${StepTypeIds.id.signal},
      NOW() - (${waitMs} || ' milliseconds')::INTERVAL,
      NOW()
    )
  `);
}

// Sets completed_at = created_at + totalMs so totalMs metric equals totalMs exactly.
async function completeWorkflow(db: DrizzleDb, id: string, totalMs?: number): Promise<void> {
  if (totalMs !== undefined) {
    await db.execute(sql`
      UPDATE wf_workflows
      SET status_id = 2,
          completed_at = created_at + (${totalMs} || ' milliseconds')::INTERVAL,
          updated_at = NOW()
      WHERE workflow_id = ${id}
    `);
  } else {
    await db.execute(sql`
      UPDATE wf_workflows SET status_id = 2, completed_at = NOW(), updated_at = NOW()
      WHERE workflow_id = ${id}
    `);
  }
}

// ---------------------------------------------------------------------------

postgresDescribe("PgWorkflowMetrics", { migrate }, (pg) => {
  it("getMetrics returns null for unknown workflow", async () => {
    const m = await PgWorkflowMetrics.getMetrics(pg.db, "no-such-id");
    expect(m).toBeNull();
  });

  it("pendingMs = startedAt - createdAt", async () => {
    await createWorkflow(pg.db, "m-pending", "pendingTest");
    await markRunning(pg.db, "m-pending", 200);

    const m = await PgWorkflowMetrics.getMetrics(pg.db, "m-pending");
    expect(m).not.toBeNull();
    // started_at was set to created_at + 200ms, so pendingMs must be exactly 200ms.
    expect(m!.pendingMs).toBeGreaterThanOrEqual(195);
    expect(m!.pendingMs).toBeLessThan(500);
  });

  it("suspendedMs sums sleep step durations", async () => {
    await createWorkflow(pg.db, "m-sleep", "sleepTest");
    await markRunning(pg.db, "m-sleep", 0);
    await addSleepStep(pg.db, "m-sleep", "nap", 300);
    await completeWorkflow(pg.db, "m-sleep");

    const m = await PgWorkflowMetrics.getMetrics(pg.db, "m-sleep");
    expect(m!.suspendedMs).toBeGreaterThanOrEqual(250);
    expect(m!.suspendedMs).toBeLessThan(600);
  });

  it("suspendedMs sums signal-wait step durations", async () => {
    await createWorkflow(pg.db, "m-signal", "signalTest");
    await markRunning(pg.db, "m-signal", 0);
    await addSignalStep(pg.db, "m-signal", "approval", 400);
    await completeWorkflow(pg.db, "m-signal");

    const m = await PgWorkflowMetrics.getMetrics(pg.db, "m-signal");
    expect(m!.suspendedMs).toBeGreaterThanOrEqual(350);
    expect(m!.suspendedMs).toBeLessThan(700);
  });

  it("runningMs = totalMs - pendingMs - suspendedMs", async () => {
    // totalMs=600, pendingMs=100, suspendedMs≈200 → runningMs≈300.
    await createWorkflow(pg.db, "m-running", "runningTest");
    await markRunning(pg.db, "m-running", 100);
    await addSleepStep(pg.db, "m-running", "pause", 200);
    await completeWorkflow(pg.db, "m-running", 600);

    const m = await PgWorkflowMetrics.getMetrics(pg.db, "m-running");
    expect(m!.totalMs).toBe(600);
    expect(m!.pendingMs).toBe(100);
    expect(m!.suspendedMs).toBeGreaterThanOrEqual(190);
    expect(m!.suspendedMs).toBeLessThan(400);
    expect(m!.runningMs).toBeGreaterThan(0);
    expect(m!.runningMs + m!.pendingMs + m!.suspendedMs).toBe(m!.totalMs);
  });

  it("activeRatio is runningMs / totalMs, clamped to [0,1]", async () => {
    await createWorkflow(pg.db, "m-ratio", "ratioTest");
    await markRunning(pg.db, "m-ratio", 0);
    await completeWorkflow(pg.db, "m-ratio");

    const m = await PgWorkflowMetrics.getMetrics(pg.db, "m-ratio");
    expect(m!.activeRatio).toBeGreaterThanOrEqual(0);
    expect(m!.activeRatio).toBeLessThanOrEqual(1);
  });

  it("queryMetrics filters by workflowName", async () => {
    await createWorkflow(pg.db, "m-q1", "queryable");
    await createWorkflow(pg.db, "m-q2", "queryable");
    await createWorkflow(pg.db, "m-q3", "other");

    const results = await PgWorkflowMetrics.queryMetrics(pg.db, { workflowName: "queryable" });
    const ids = results.map((r) => r.workflowId);
    expect(ids).toContain("m-q1");
    expect(ids).toContain("m-q2");
    expect(ids).not.toContain("m-q3");
  });

  it("queryMetrics respects limit and offset", async () => {
    await createWorkflow(pg.db, "m-page1", "paged");
    await createWorkflow(pg.db, "m-page2", "paged");
    await createWorkflow(pg.db, "m-page3", "paged");

    const page1 = await PgWorkflowMetrics.queryMetrics(pg.db, { workflowName: "paged", limit: 2 });
    expect(page1.length).toBe(2);

    const page2 = await PgWorkflowMetrics.queryMetrics(pg.db, {
      workflowName: "paged",
      limit: 2,
      offset: 2,
    });
    expect(page2.length).toBe(1);
  });

  it("queryMetricsSummary groups by type with correct aggregates", async () => {
    await createWorkflow(pg.db, "m-s1", "summaryWf", "typeA");
    await markRunning(pg.db, "m-s1", 100);
    await completeWorkflow(pg.db, "m-s1", 500);

    await createWorkflow(pg.db, "m-s2", "summaryWf", "typeA");
    await markRunning(pg.db, "m-s2", 200);
    await completeWorkflow(pg.db, "m-s2", 800);

    await createWorkflow(pg.db, "m-s3", "summaryWf", "typeB");
    await markRunning(pg.db, "m-s3", 50);
    await completeWorkflow(pg.db, "m-s3", 300);

    const summary = await PgWorkflowMetrics.queryMetricsSummary(pg.db, {
      workflowName: "summaryWf",
      groupBy: "type",
    });

    const a = summary.find((s) => s.group === "typeA");
    const b = summary.find((s) => s.group === "typeB");

    expect(a).toBeDefined();
    expect(a!.count).toBe(2);
    expect(b).toBeDefined();
    expect(b!.count).toBe(1);
    expect(a!.avgPendingMs).toBeGreaterThan(0);
  });
});
