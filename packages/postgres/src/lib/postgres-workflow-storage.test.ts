import { describe, it, expect, beforeAll } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "@promin/core";
import { workflow, createWorkflowRunner } from "@promin/workflow";
import { PostgresWorkflowStorage } from "./postgres-workflow-storage.ts";
import { migrate } from "./migrate.ts";
import { postgresDescribe } from "./test-utils.ts";

// ---------------------------------------------------------------------------
// Test error types
// ---------------------------------------------------------------------------

class TestError extends Data.TaggedError("TestError")<{
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------
// migrate()
// ---------------------------------------------------------------------------

postgresDescribe("migrate", { migrate }, (pg) => {
  it("creates all tables", async () => {
    const result = await pg.sql`
      SELECT table_name FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name LIKE 'wf_%'
      ORDER BY table_name
    `;
    const tables = result.map((r) => r.table_name);
    expect(tables).toContain("wf_workflows");
    expect(tables).toContain("wf_workflow_steps");
    expect(tables).toContain("wf_workflow_step_tasks");
    expect(tables).toContain("wf_workflow_signals");
    expect(tables).toContain("wf_workflow_locks");
    expect(tables).toContain("wf_workflow_status");
    expect(tables).toContain("wf_step_status");
    expect(tables).toContain("wf_step_type");
  });

  it("seeds lookup tables", async () => {
    const statuses = await pg.sql`SELECT * FROM wf_workflow_status ORDER BY id`;
    expect(statuses.length).toBe(6); // pending, running, completed, failed, suspended, compensating
    expect(statuses[0]!.name).toBe("pending");

    const stepStatuses = await pg.sql`SELECT * FROM wf_step_status ORDER BY id`;
    expect(stepStatuses.length).toBe(9); // +compensated, compensation_failed

    const stepTypes = await pg.sql`SELECT * FROM wf_step_type ORDER BY id`;
    expect(stepTypes.length).toBe(4);

    const attemptTypes = await pg.sql`SELECT * FROM wf_attempt_type ORDER BY id`;
    expect(attemptTypes.length).toBe(2); // execution, compensation
  });

  it("is idempotent", async () => {
    await migrate(pg.db);
  });
});

// ---------------------------------------------------------------------------
// CRUD operations
// ---------------------------------------------------------------------------

postgresDescribe("PostgresWorkflowStorage", { migrate }, (pg) => {
  let storage: PostgresWorkflowStorage;

  beforeAll(async () => {
    storage = await PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false });
  });

  describe("createWorkflow + loadWorkflow", () => {
    it("creates and loads a workflow", async () => {
      await storage.createWorkflow({
        workflowId: "crud-1",
        workflowName: "test-wf",
        input: { userId: "u_42" },
        workflowType: "onboarding",
        metadata: { region: "us-east" },
      });

      const state = await storage.loadWorkflow("crud-1");
      expect(state).not.toBeNull();
      expect(state!.workflowId).toBe("crud-1");
      expect(state!.workflowName).toBe("test-wf");
      expect(state!.workflowType).toBe("onboarding");
      expect(state!.status).toBe("pending");
      expect(state!.input).toEqual({ userId: "u_42" });
      expect(state!.metadata).toEqual({ region: "us-east" });
      expect(state!.createdAt).toBeInstanceOf(Date);
    });

    it("returns null for non-existent workflow", async () => {
      expect(await storage.loadWorkflow("nonexistent")).toBeNull();
    });
  });

  describe("saveStepResult", () => {
    it("saves and loads step result", async () => {
      await storage.createWorkflow({ workflowId: "step-1", workflowName: "test", input: {} });
      await storage.saveStepResult({
        workflowId: "step-1",
        stepName: "fetch",
        result: { data: "hello" },
        durationMs: 150,
        startedAt: new Date(),
      });

      const state = await storage.loadWorkflow("step-1");
      const step = state!.steps["fetch"];
      expect(step!.status).toBe("completed");
      expect(step!.result).toEqual({ data: "hello" });
      expect(step!.durationMs).toBe(150);
      expect(step!.completedAt).toBeInstanceOf(Date);
    });
  });

  describe("saveStepFailure", () => {
    it("saves step failure", async () => {
      await storage.createWorkflow({ workflowId: "step-fail", workflowName: "test", input: {} });
      await storage.saveStepFailure({
        workflowId: "step-fail",
        stepName: "bad",
        error: "something broke",
        durationMs: 50,
        startedAt: new Date(),
      });

      const state = await storage.loadWorkflow("step-fail");
      expect(state!.steps["bad"]!.status).toBe("failed");
      expect(state!.steps["bad"]!.error).toBe("something broke");
    });
  });

  describe("saveTaskResult / saveTaskFailure", () => {
    it("saves task results", async () => {
      await storage.createWorkflow({ workflowId: "task-1", workflowName: "test", input: {} });
      await storage.saveTaskResult({
        workflowId: "task-1",
        stepName: "map-step",
        taskIndex: 0,
        result: "a",
      });
      await storage.saveTaskResult({
        workflowId: "task-1",
        stepName: "map-step",
        taskIndex: 1,
        result: "b",
      });

      const state = await storage.loadWorkflow("task-1");
      expect(state!.steps["map-step"]?.tasks).toHaveLength(2);
      expect(state!.steps["map-step"]?.tasks![0]!.result).toBe("a");
    });

    it("saves task failures", async () => {
      await storage.createWorkflow({ workflowId: "task-fail", workflowName: "test", input: {} });
      await storage.saveTaskFailure({
        workflowId: "task-fail",
        stepName: "s",
        taskIndex: 0,
        error: "boom",
      });

      const state = await storage.loadWorkflow("task-fail");
      expect(state!.steps["s"]?.tasks![0]!.status).toBe("failed");
    });
  });

  describe("completeWorkflow / failWorkflow", () => {
    it("completes a workflow", async () => {
      await storage.createWorkflow({ workflowId: "complete-1", workflowName: "test", input: {} });
      await storage.completeWorkflow("complete-1", { final: "result" });

      const state = await storage.loadWorkflow("complete-1");
      expect(state!.status).toBe("completed");
      expect(state!.result).toEqual({ final: "result" });
      expect(state!.completedAt).toBeInstanceOf(Date);
    });

    it("fails a workflow", async () => {
      await storage.createWorkflow({ workflowId: "fail-1", workflowName: "test", input: {} });
      await storage.failWorkflow("fail-1", "total failure");

      const state = await storage.loadWorkflow("fail-1");
      expect(state!.status).toBe("failed");
      expect(state!.error).toBe("total failure");
    });
  });

  describe("listWorkflows", () => {
    it("filters by status", async () => {
      const completed = await storage.listWorkflows({ status: "completed" });
      expect(completed.every((w) => w.status === "completed")).toBe(true);
    });

    it("filters by name", async () => {
      await storage.createWorkflow({ workflowId: "named-1", workflowName: "special", input: {} });
      await storage.completeWorkflow("named-1", null);

      const filtered = await storage.listWorkflows({ name: "special" });
      expect(filtered.length).toBeGreaterThanOrEqual(1);
      expect(filtered[0]!.workflowName).toBe("special");
    });

    it("filters by type", async () => {
      const onboarding = await storage.listWorkflows({ type: "onboarding" });
      expect(onboarding.every((w) => w.workflowType === "onboarding")).toBe(true);
    });

    it("supports limit", async () => {
      const page = await storage.listWorkflows({ limit: 2 });
      expect(page.length).toBeLessThanOrEqual(2);
    });
  });

  describe("cancelWorkflow", () => {
    it("cancels a running workflow", async () => {
      await storage.createWorkflow({ workflowId: "cancel-1", workflowName: "test", input: {} });
      await storage.cancelWorkflow("cancel-1");

      const state = await storage.loadWorkflow("cancel-1");
      expect(state!.status).toBe("failed");
      expect(state!.error).toBe("Cancelled");
    });

    it("does not cancel a completed workflow", async () => {
      await storage.createWorkflow({ workflowId: "cancel-2", workflowName: "test", input: {} });
      await storage.completeWorkflow("cancel-2", "done");
      await storage.cancelWorkflow("cancel-2");

      expect((await storage.loadWorkflow("cancel-2"))!.status).toBe("completed");
    });
  });

  describe("suspendWorkflow", () => {
    it("suspends with sleep state", async () => {
      await storage.createWorkflow({ workflowId: "suspend-1", workflowName: "test", input: {} });
      await storage.suspendWorkflow("suspend-1", "wait", {
        status: "sleeping",
        stepType: "sleep",
        wakeAt: new Date(Date.now() + 60_000),
      });

      const state = await storage.loadWorkflow("suspend-1");
      expect(state!.status).toBe("suspended");
      expect(state!.steps["wait"]!.status).toBe("sleeping");
    });
  });

  describe("signals", () => {
    it("delivers and loads signals", async () => {
      await storage.createWorkflow({ workflowId: "sig-1", workflowName: "test", input: {} });
      await storage.deliverSignal("sig-1", "approval", { approved: true });

      const signals = await storage.loadSignals("sig-1");
      expect(signals).toHaveLength(1);
      expect(signals[0]!.signalName).toBe("approval");
      expect(signals[0]!.payload).toEqual({ approved: true });
    });

    it("upserts on duplicate signal", async () => {
      await storage.createWorkflow({ workflowId: "sig-2", workflowName: "test", input: {} });
      await storage.deliverSignal("sig-2", "approval", { v: 1 });
      await storage.deliverSignal("sig-2", "approval", { v: 2 });

      const signals = await storage.loadSignals("sig-2");
      expect(signals).toHaveLength(1);
      expect(signals[0]!.payload).toEqual({ v: 2 });
    });
  });

  describe("locking", () => {
    it("acquires and releases a lock", async () => {
      expect((await storage.tryLock("lock-1", 30_000)).acquired).toBe(true);
      await storage.releaseLock("lock-1");
      expect((await storage.tryLock("lock-1", 30_000)).acquired).toBe(true);
      await storage.releaseLock("lock-1");
    });
  });
});

// ---------------------------------------------------------------------------
// End-to-end: workflow execution with Postgres storage
// ---------------------------------------------------------------------------

postgresDescribe("End-to-end workflow with Postgres", { migrate }, (pg) => {
  let storage: PostgresWorkflowStorage;

  beforeAll(async () => {
    storage = await PostgresWorkflowStorage.create({ db: pg.db, autoSeedLookups: false });
  });

  it("runs a linear workflow", async () => {
    const wf = workflow<{ n: number }>({ name: "e2e-linear" })
      .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
      .step("add-one", ({ prev }) => Pipeline.succeed(prev + 1))
      .build();
    const runner = createWorkflowRunner({ storage });
    const result = await runner.run({ workflow: wf, workflowId: "e2e-1", input: { n: 5 } });

    expect(result).toBe(11);
    expect((await storage.loadWorkflow("e2e-1"))!.status).toBe("completed");
  });

  it("runs a DAG workflow", async () => {
    const wf = workflow<{ text: string }>({ name: "e2e-dag" })
      .step("parse", ({ input }) => Pipeline.succeed(input.text.split(" ")))
      .step("count", { dependsOn: ["parse"] }, ({ deps }) => Pipeline.succeed(deps.parse.length))
      .step("join", { dependsOn: ["parse"] }, ({ deps }) => Pipeline.succeed(deps.parse.join("-")))
      .step("combine", { dependsOn: ["count", "join"] }, ({ deps }) =>
        Pipeline.succeed(`${deps.join} (${deps.count})`),
      )
      .build();
    const runner = createWorkflowRunner({ storage });
    const result = await runner.run({
      workflow: wf,
      workflowId: "e2e-dag-1",
      input: { text: "hello world" },
    });

    expect(result).toBe("hello-world (2)");
  });

  it("resumes after crash", async () => {
    await storage.createWorkflow({
      workflowId: "e2e-resume",
      workflowName: "e2e-resume",
      input: { n: 10 },
    });
    await storage.saveStepResult({
      workflowId: "e2e-resume",
      stepName: "step-1",
      result: 20,
      durationMs: 5,
      startedAt: new Date(),
    });

    let step1Called = false;
    const wf = workflow<{ n: number }>({ name: "e2e-resume" })
      .step("step-1", ({ input }) => {
        step1Called = true;
        return Pipeline.succeed(input.n * 2);
      })
      .step("step-2", ({ prev }) => Pipeline.succeed(prev + 100))
      .build();
    const runner = createWorkflowRunner({ storage });
    const result = await runner.run({
      workflow: wf,
      workflowId: "e2e-resume",
      input: { n: 10 },
    });

    expect(result).toBe(120);
    expect(step1Called).toBe(false);
  });

  it("handles step failure", async () => {
    const wf = workflow<{}>({ name: "e2e-fail" })
      .step("boom", () => Pipeline.fail(new TestError({ message: "test error" })))
      .build();
    const runner = createWorkflowRunner({ storage });
    const { error } = await runner.runSafe({
      workflow: wf,
      workflowId: "e2e-fail-1",
      input: {},
    });

    expect(error).not.toBeNull();
    expect((await storage.loadWorkflow("e2e-fail-1"))!.status).toBe("failed");
  });

  it("runs with stepAsync", async () => {
    const wf = workflow<{ name: string }>({ name: "e2e-async" })
      .stepAsync("greet", async ({ input }) => `Hello, ${input.name}!`)
      .build();
    const runner = createWorkflowRunner({ storage });
    const result = await runner.run({
      workflow: wf,
      workflowId: "e2e-async-1",
      input: { name: "Postgres" },
    });

    expect(result).toBe("Hello, Postgres!");
  });

  it("stores and queries workflow type and metadata", async () => {
    const runner = createWorkflowRunner({ storage });

    const onboardWf = workflow<{ userId: string }>({
      name: "onboard-user",
      type: "onboarding",
      metadata: { team: "growth", region: "us-east", priority: "high" },
    })
      .step("fetch", ({ input }) => Pipeline.succeed({ name: `User ${input.userId}` }))
      .stepAsync("provision", async ({ prev }) => ({ accountId: `acc-${prev.name}` }))
      .build();
    await runner.run({
      workflow: onboardWf,
      workflowId: "e2e-meta-onboard",
      input: { userId: "u_42" },
    });

    const reportWf = workflow<{ date: string }>({
      name: "daily-report",
      type: "report",
      metadata: { team: "data", schedule: "daily" },
    })
      .step("generate", ({ input }) => Pipeline.succeed(`Report for ${input.date}`))
      .build();
    await runner.run({
      workflow: reportWf,
      workflowId: "e2e-meta-report",
      input: { date: "2026-03-31" },
    });

    const etlWf = workflow<{ pipeline: string }>({
      name: "etl-pipeline",
      type: "etl",
      metadata: { team: "data", source: "clickhouse", destination: "postgres" },
    })
      .step("extract", () => Pipeline.succeed([1, 2, 3]))
      .step("load", ({ prev }) => Pipeline.succeed({ loaded: prev.length }))
      .build();
    await runner.run({
      workflow: etlWf,
      workflowId: "e2e-meta-etl",
      input: { pipeline: "events" },
    });

    const onboardingWfs = await storage.listWorkflows({ type: "onboarding" });
    expect(onboardingWfs.length).toBeGreaterThanOrEqual(1);
    expect(onboardingWfs.every((w) => w.workflowType === "onboarding")).toBe(true);
    expect(onboardingWfs[0]!.metadata).toEqual({
      team: "growth",
      region: "us-east",
      priority: "high",
    });

    const etlWfs = await storage.listWorkflows({ type: "etl" });
    expect(etlWfs.length).toBe(1);
    expect(etlWfs[0]!.metadata).toEqual({
      team: "data",
      source: "clickhouse",
      destination: "postgres",
    });

    const report = await storage.loadWorkflow("e2e-meta-report");
    expect(report!.workflowType).toBe("report");
    expect(report!.metadata).toEqual({ team: "data", schedule: "daily" });
    expect(report!.status).toBe("completed");
    expect(report!.result).toBe("Report for 2026-03-31");
  });
});

// ---------------------------------------------------------------------------
// .journaled() step end-to-end via PostgresWorkflowStorage
// ---------------------------------------------------------------------------

postgresDescribe("journaled step with Postgres storage", { migrate }, (pg) => {
  it("runs activities once on first execute, journals each, replays without re-running", async () => {
    const storage = await PostgresWorkflowStorage.create({ db: pg.db });
    let createCalls = 0;
    let notifyCalls = 0;

    const wf = workflow<{ user: string }>({ name: "signup-pg" })
      .step("load", ({ input }) => Pipeline.succeed(input))
      .journaled("setup", function* (ctx, prev) {
        const created = yield* ctx.activity("create", async () => {
          createCalls++;
          return { id: `u-${prev.user}`, name: prev.user };
        });
        const notified = yield* ctx.activity("notify", async () => {
          notifyCalls++;
          return `welcome-${created.id}`;
        });
        return { user: created, greeting: notified };
      })
      .build();
    const runner = createWorkflowRunner({ storage });

    const result = await runner.run({
      workflow: wf,
      workflowId: "pg-journal-1",
      input: { user: "alice" },
    });

    expect(createCalls).toBe(1);
    expect(notifyCalls).toBe(1);
    expect(result).toEqual({
      user: { id: "u-alice", name: "alice" },
      greeting: "welcome-u-alice",
    });

    // Journal persisted in Postgres with the right shape.
    const journal = await storage.loadJournal("pg-journal-1", "setup");
    expect(journal).toHaveLength(2);
    expect(journal[0]!.activityName).toBe("create");
    expect(journal[0]!.exit).toEqual({
      tag: "Success",
      value: { id: "u-alice", name: "alice" },
    });
    expect(journal[1]!.activityName).toBe("notify");
    expect(journal[1]!.exit).toEqual({ tag: "Success", value: "welcome-u-alice" });
  });

  it("appendEntry is idempotent — double-insert same index is a no-op", async () => {
    const storage = await PostgresWorkflowStorage.create({ db: pg.db });

    // Need a workflow row to satisfy the FK.
    await storage.createWorkflow({
      workflowId: "pg-journal-idem",
      workflowName: "idem",
      input: {},
    });

    const exit = { tag: "Success" as const, value: 42 };
    await storage.appendEntry({
      workflowId: "pg-journal-idem",
      stepName: "s",
      activityIndex: 0,
      activityName: "a",
      exit,
    });
    // Second call with same PK — should silently no-op.
    await storage.appendEntry({
      workflowId: "pg-journal-idem",
      stepName: "s",
      activityIndex: 0,
      activityName: "a",
      exit,
    });

    const journal = await storage.loadJournal("pg-journal-idem", "s");
    expect(journal).toHaveLength(1);
    expect(journal[0]!.exit).toEqual(exit);
  });

  it("ctx.sleep durable suspend/resume end-to-end with Postgres", async () => {
    const storage = await PostgresWorkflowStorage.create({ db: pg.db });
    let postSleepCalls = 0;

    const buildWorkflow = () =>
      workflow<{ id: string }>({ name: "pg-sleep" })
        .journaled("wait-then-do", function* (ctx) {
          yield* ctx.sleep(50);
          yield* ctx.activity("post-sleep", async () => {
            postSleepCalls++;
            return "done";
          });
          return { ok: true };
        })
        .build();
    const runner = createWorkflowRunner({ storage });

    // Kick off — suspends at sleep.
    await expect(
      runner.run({ workflow: buildWorkflow(), workflowId: "pg-sleep-1", input: { id: "a" } }),
    ).rejects.toThrow(/sleeping until/);
    expect(postSleepCalls).toBe(0);

    // Journal has a pending sleep entry.
    const pending = await storage.loadJournal("pg-sleep-1", "wait-then-do");
    expect(pending).toHaveLength(1);
    expect(pending[0]!.stepType).toBe("sleep");
    expect(pending[0]!.phase).toBe("pending");
    expect(pending[0]!.wakeAt).toBeInstanceOf(Date);

    // Simulate the scanner firing after wake: re-run the workflow. ctx.sleep
    // replay sees the pending entry and now >= wakeAt, auto-completes, and
    // the step continues.
    await new Promise((r) => setTimeout(r, 80));
    const result = await runner.run({
      workflow: buildWorkflow(),
      workflowId: "pg-sleep-1",
      input: { id: "a" },
    });
    expect(result).toEqual({ ok: true });
    expect(postSleepCalls).toBe(1);

    const completed = await storage.loadJournal("pg-sleep-1", "wait-then-do");
    expect(completed[0]!.phase).toBe("completed");
    expect(completed[0]!.exit).toMatchObject({ tag: "Success" });
  });

  it("findDueSleeps returns only pending sleeps past their wakeAt", async () => {
    const storage = await PostgresWorkflowStorage.create({ db: pg.db });

    // Seed three workflows — one overdue sleep, one future sleep, one completed.
    for (const id of ["due-a", "due-b", "future"]) {
      await storage.createWorkflow({
        workflowId: id,
        workflowName: "due-test",
        input: {},
      });
    }

    const past = new Date(Date.now() - 10_000);
    const future = new Date(Date.now() + 60_000);

    await storage.appendPendingEntry({
      workflowId: "due-a",
      stepName: "s",
      activityIndex: 0,
      activityName: "sleep",
      stepType: "sleep",
      wakeAt: past,
    });
    await storage.appendPendingEntry({
      workflowId: "due-b",
      stepName: "s",
      activityIndex: 0,
      activityName: "sleep",
      stepType: "sleep",
      wakeAt: past,
    });
    await storage.appendPendingEntry({
      workflowId: "future",
      stepName: "s",
      activityIndex: 0,
      activityName: "sleep",
      stepType: "sleep",
      wakeAt: future,
    });

    const due = await storage.findDueSleeps({ now: new Date(), limit: 10 });
    const dueIds = new Set(due.map((d) => d.workflowId));
    expect(dueIds.has("due-a")).toBe(true);
    expect(dueIds.has("due-b")).toBe(true);
    expect(dueIds.has("future")).toBe(false);
  });

  it("completePendingEntry is idempotent on repeated delivery", async () => {
    const storage = await PostgresWorkflowStorage.create({ db: pg.db });
    await storage.createWorkflow({
      workflowId: "idem-sig",
      workflowName: "idem",
      input: {},
    });
    await storage.appendPendingEntry({
      workflowId: "idem-sig",
      stepName: "gate",
      activityIndex: 0,
      activityName: "approval",
      stepType: "signal",
    });

    // First delivery.
    await storage.completePendingEntry({
      workflowId: "idem-sig",
      stepName: "gate",
      activityIndex: 0,
      exit: { tag: "Success", value: { approved: true } },
    });

    // Second delivery — should no-op; first value preserved.
    await storage.completePendingEntry({
      workflowId: "idem-sig",
      stepName: "gate",
      activityIndex: 0,
      exit: { tag: "Success", value: { approved: false } },
    });

    const [entry] = await storage.loadJournal("idem-sig", "gate");
    expect(entry!.phase).toBe("completed");
    expect(entry!.exit).toEqual({ tag: "Success", value: { approved: true } });
  });

  it("findPendingSignal returns the entry or null", async () => {
    const storage = await PostgresWorkflowStorage.create({ db: pg.db });
    await storage.createWorkflow({
      workflowId: "sig-find",
      workflowName: "sf",
      input: {},
    });
    await storage.appendPendingEntry({
      workflowId: "sig-find",
      stepName: "gate",
      activityIndex: 0,
      activityName: "approval",
      stepType: "signal",
    });

    const hit = await storage.findPendingSignal({
      workflowId: "sig-find",
      stepName: "gate",
      signalName: "approval",
    });
    expect(hit).not.toBeNull();
    expect(hit!.stepType).toBe("signal");
    expect(hit!.activityName).toBe("approval");

    const miss = await storage.findPendingSignal({
      workflowId: "sig-find",
      stepName: "gate",
      signalName: "different-name",
    });
    expect(miss).toBeNull();
  });

  it("FK cascade — deleting the workflow removes its journal entries", async () => {
    const storage = await PostgresWorkflowStorage.create({ db: pg.db });

    const wf = workflow<{ x: number }>({ name: "cascade-test" })
      .journaled("body", function* (ctx, _prev) {
        yield* ctx.activity("a", async () => 1);
        yield* ctx.activity("b", async () => 2);
        return "done";
      })
      .build();
    const runner = createWorkflowRunner({ storage });

    await runner.run({ workflow: wf, workflowId: "pg-journal-cascade", input: { x: 1 } });

    const before = await storage.loadJournal("pg-journal-cascade", "body");
    expect(before).toHaveLength(2);

    // Cascade via the workflow's FK. purgeCompleted is the public path for
    // deleting a completed workflow row and everything attached to it.
    await storage.purgeCompleted({ olderThanMs: -1, limit: 100 });

    const after = await storage.loadJournal("pg-journal-cascade", "body");
    expect(after).toHaveLength(0);
  });
});
