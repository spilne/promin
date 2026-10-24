// ---------------------------------------------------------------------------
// Round trips of the hot-path writes and of loadWorkflow: each is one
// statement, and loadWorkflow returns what reading its tables one by one
// returns.
// ---------------------------------------------------------------------------

import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { and, eq } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { succeed } from "@spilne/perfect-core";
import { createWorkflowRunner, workflow, type WorkflowState } from "@promin/workflow";
import { migrate } from "../migrate.ts";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";
import { workflows, workflowSteps, workflowStepTasks } from "../schema.ts";
import { postgresDescribe } from "../test-utils.ts";
import type { DrizzleDb } from "@spilne/perfect-postgres";

postgresDescribe("PostgresWorkflowStorage round trips", { migrate }, (pg) => {
  /** Statements sent through `counted` since the last reset. */
  let statements = 0;
  let client: ReturnType<typeof postgres>;
  let counted: DrizzleDb;
  let storage: PostgresWorkflowStorage;
  const at = new Date("2026-01-01T00:00:00.000Z");

  beforeAll(async () => {
    const o = pg.sql.options;
    client = postgres({
      host: o.host[0],
      port: o.port[0],
      user: o.user,
      pass: o.pass ?? undefined,
      database: o.database,
      onnotice: () => {},
      debug: () => void statements++,
    });
    counted = drizzle(client);
  });

  afterAll(async () => {
    await client.end();
  });

  beforeEach(async () => {
    await pg.sql`TRUNCATE TABLE wf_workflow_step_tasks, wf_workflow_steps, wf_workflow_locks,
      wf_step_attempts, wf_activity_journal, wf_workflows RESTART IDENTITY CASCADE`;
    storage = await PostgresWorkflowStorage.create({
      db: counted,
      autoSeedLookups: false,
      recordAttempts: true,
    });
  });

  /** Statements `run` sends. */
  async function count(run: () => Promise<unknown>): Promise<number> {
    const before = statements;
    await run();
    return statements - before;
  }

  async function locked(workflowId: string): Promise<{ fenceToken?: string }> {
    await storage.createWorkflow({ workflowId, workflowName: "t", input: {} });
    const lock = await storage.tryLock(workflowId, 30_000);
    return { fenceToken: lock.token };
  }

  describe("one statement per fenced write", () => {
    it("checkpointStep with attempt rows", async () => {
      const guard = await locked("rt-ckpt");
      const n = await count(() =>
        storage.checkpointStep(
          {
            workflowId: "rt-ckpt",
            stepName: "s",
            outcome: { kind: "completed", result: 1, durationMs: 1, startedAt: at },
            attempts: [1, 2].map((attempt) => ({
              workflowId: "rt-ckpt",
              stepName: "s",
              attempt,
              type: "execution" as const,
              status: attempt === 2 ? ("completed" as const) : ("failed" as const),
              durationMs: 1,
              startedAt: at,
              completedAt: at,
            })),
          },
          guard,
        ),
      );
      expect(n).toBe(1);
      expect(await storage.loadStepAttempts("rt-ckpt", "s")).toHaveLength(2);
    });

    it("saveStepResult, saveStepFailure, saveTaskResult, suspendWorkflow", async () => {
      const guard = await locked("rt-writes");
      const step = { workflowId: "rt-writes", durationMs: 1, startedAt: at };
      expect(
        await count(() => storage.saveStepResult({ ...step, stepName: "a", result: 1 }, guard)),
      ).toBe(1);
      expect(
        await count(() => storage.saveStepFailure({ ...step, stepName: "b", error: "x" }, guard)),
      ).toBe(1);
      expect(
        await count(() =>
          storage.saveTaskResult(
            { workflowId: "rt-writes", stepName: "m", taskIndex: 0, result: 1 },
            guard,
          ),
        ),
      ).toBe(1);
      expect(
        await count(() =>
          storage.suspendWorkflow("rt-writes", "z", { status: "sleeping", wakeAt: at }, guard),
        ),
      ).toBe(1);
    });

    it("journal appends and completions", async () => {
      const guard = await locked("rt-journal");
      const slot = { workflowId: "rt-journal", stepName: "j", activityName: "a" };
      expect(
        await count(() =>
          storage.appendEntry(
            { ...slot, activityIndex: 0, exit: { tag: "Success", value: 1 } },
            guard,
          ),
        ),
      ).toBe(1);
      expect(
        await count(() =>
          storage.appendPendingEntry({ ...slot, activityIndex: 1, stepType: "activity" }, guard),
        ),
      ).toBe(1);
      expect(
        await count(() =>
          storage.completePendingEntry(
            { ...slot, activityIndex: 1, exit: { tag: "Success", value: 2 } },
            guard,
          ),
        ),
      ).toBe(1);
    });

    it("a rejected token still writes nothing (one statement, then the error read)", async () => {
      const guard = await locked("rt-stale");
      await storage.releaseLock("rt-stale", guard);
      const n = await count(() =>
        expect(
          storage.saveStepResult(
            { workflowId: "rt-stale", stepName: "a", result: 1, durationMs: 1, startedAt: at },
            guard,
          ),
        ).rejects.toMatchObject({ _tag: "FenceTokenMismatchError" }),
      );
      expect(n).toBe(2);
      expect((await storage.loadWorkflow("rt-stale"))!.steps).toEqual({});
    });
  });

  it("a 20-step chain makes one storage round trip per step, plus a constant", async () => {
    let b: any = workflow<number>({ name: "rt-chain" });
    for (let i = 0; i < 20; i++) {
      b = b.step(`s${i}`, ({ prev, input }: { prev?: number; input: number }) =>
        succeed((prev ?? input) + 1),
      );
    }
    const wf = b.build();
    const n = await count(() =>
      createWorkflowRunner({ storage }).run({ workflow: wf, workflowId: "rt-chain", input: 0 }),
    );
    // 20 checkpoints, plus: the lock and the load (2), the create (a
    // three-statement transaction), the read-back of the created run, the
    // completion, the cancel check after it, and the release.
    expect(n).toBeLessThanOrEqual(20 + 9);
  });

  describe("loadWorkflow", () => {
    /** The state read table by table, as `loadWorkflow` read it in three queries. */
    async function readTables(workflowId: string): Promise<unknown> {
      const [wf] = await pg.db.select().from(workflows).where(eq(workflows.workflowId, workflowId));
      const steps = await pg.db
        .select()
        .from(workflowSteps)
        .where(and(eq(workflowSteps.workflowId, workflowId), eq(workflowSteps.run, wf!.run)));
      const tasks = await pg.db
        .select()
        .from(workflowStepTasks)
        .where(
          and(eq(workflowStepTasks.workflowId, workflowId), eq(workflowStepTasks.run, wf!.run)),
        );
      return { wf, steps: steps.length, tasks: tasks.length };
    }

    it("is one statement and returns every field of every row kind", async () => {
      const id = "rt-load";
      await storage.createWorkflow({
        workflowId: id,
        workflowName: "load",
        workflowType: "kind",
        namespace: "ns",
        version: "3",
        input: { big: [1, 2, 3] },
        metadata: { owner: "a" },
        runSource: "schedule",
        runSourceId: "sched-1",
      });
      await storage.saveStepResult({
        workflowId: id,
        stepName: "done",
        result: { ok: true },
        durationMs: 12,
        startedAt: at,
        metadata: { matchCase: "x" },
      });
      await storage.saveStepFailure({
        workflowId: id,
        stepName: "broke",
        error: "boom",
        errorTag: "Boom",
        durationMs: 3,
        startedAt: at,
      });
      await storage.saveTaskResult({ workflowId: id, stepName: "map", taskIndex: 0, result: "a" });
      await storage.saveTaskFailure({ workflowId: id, stepName: "map", taskIndex: 1, error: "b" });
      await storage.suspendWorkflow(id, "wait", {
        status: "waiting_for_signal",
        stepType: "signal",
        signalName: "go",
        signalTimeoutAt: new Date("2026-02-01T00:00:00.000Z"),
        signalJsonSchema: { type: "object" },
      });
      await storage.suspendWorkflow(id, "nap", {
        status: "sleeping",
        stepType: "sleep",
        wakeAt: new Date("2026-03-01T00:00:00.000Z"),
      });
      await storage.saveStepCompensation({
        workflowId: id,
        stepName: "done",
        status: "compensation_failed",
        error: "undo failed",
      });

      let state: WorkflowState | null = null;
      expect(await count(async () => (state = await storage.loadWorkflow(id)))).toBe(1);
      const s = state! as WorkflowState;
      expect(s).toMatchObject({
        workflowId: id,
        workflowName: "load",
        workflowType: "kind",
        namespace: "ns",
        version: "3",
        run: 1,
        runSource: "schedule",
        runSourceId: "sched-1",
        status: "suspended",
        input: { big: [1, 2, 3] },
        metadata: { owner: "a" },
      });
      expect(s.createdAt).toBeInstanceOf(Date);
      expect(s.startedAt).toBeInstanceOf(Date);
      expect(s.updatedAt).toBeInstanceOf(Date);
      expect(s.completedAt).toBeUndefined();
      expect(s.steps["done"]).toMatchObject({
        status: "completed",
        stepType: "single",
        dependsOn: [],
        result: { ok: true },
        durationMs: 12,
        attempt: 1,
        metadata: { matchCase: "x" },
        startedAt: at,
        compensationStatus: "compensation_failed",
        compensationError: "undo failed",
      });
      expect(s.steps["done"]!.compensatedAt).toBeInstanceOf(Date);
      expect(s.steps["broke"]).toMatchObject({ status: "failed", error: "boom", errorTag: "Boom" });
      expect(s.steps["map"]!.stepType).toBe("map");
      const tasks = [...s.steps["map"]!.tasks!].sort((x, y) => x.taskIndex - y.taskIndex);
      expect(tasks.map((t) => [t.taskIndex, t.status, t.result ?? t.error])).toEqual([
        [0, "completed", "a"],
        [1, "failed", "b"],
      ]);
      expect(tasks[0]!.completedAt).toBeInstanceOf(Date);
      expect(s.steps["wait"]).toMatchObject({
        status: "waiting_for_signal",
        stepType: "signal",
        signalName: "go",
        signalTimeoutAt: new Date("2026-02-01T00:00:00.000Z"),
        signalJsonSchema: { type: "object" },
      });
      expect(s.steps["nap"]).toMatchObject({
        status: "sleeping",
        wakeAt: new Date("2026-03-01T00:00:00.000Z"),
      });

      const tables = (await readTables(id)) as { wf: any; steps: number; tasks: number };
      expect(Object.keys(s.steps)).toHaveLength(tables.steps);
      expect(s.steps["map"]!.tasks).toHaveLength(tables.tasks);
      expect(s.createdAt).toEqual(tables.wf.createdAt);
      expect(s.updatedAt).toEqual(tables.wf.updatedAt);
    });

    it("reads only the current run's rows", async () => {
      await storage.createWorkflow({ workflowId: "rt-runs", workflowName: "t", input: {} });
      await storage.saveTaskResult({
        workflowId: "rt-runs",
        stepName: "m",
        taskIndex: 0,
        result: 1,
      });
      await storage.startFreshRun("rt-runs");
      await storage.saveStepResult({
        workflowId: "rt-runs",
        stepName: "fresh",
        result: 2,
        durationMs: 1,
        startedAt: at,
      });

      const state = (await storage.loadWorkflow("rt-runs"))!;
      expect(state.run).toBe(2);
      expect(Object.keys(state.steps)).toEqual(["fresh"]);
    });

    it("returns null for a missing workflow", async () => {
      expect(await storage.loadWorkflow("rt-none")).toBeNull();
    });
  });

  it("listWorkflowSummaries returns the listWorkflows headers, without the blobs", async () => {
    for (const id of ["sum-a", "sum-b"]) {
      await storage.createWorkflow({
        workflowId: id,
        workflowName: "sum",
        input: { blob: id },
        metadata: { id },
        runSource: "manual",
      });
    }
    await storage.completeWorkflow("sum-a", { out: 1 });

    const full = await storage.listWorkflows({ name: "sum", orderBy: "createdAt" });
    const lean = await storage.listWorkflowSummaries({ name: "sum", orderBy: "createdAt" });
    expect(lean).toEqual(
      full.map(
        ({
          input: _i,
          result: _r,
          error: _e,
          errorTag: _t,
          tripwire: _w,
          steps: _s,
          parentWorkflowId: _p,
          ...header
        }) => header,
      ),
    );
    expect((lean[0] as Record<string, unknown>)["input"]).toBeUndefined();
  });
});
