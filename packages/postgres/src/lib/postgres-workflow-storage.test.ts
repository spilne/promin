import { describe, it, expect, beforeAll } from "bun:test";
import { Data } from "effect";
import { Pipeline, workflow } from "@ts-backend/core";
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
    expect(statuses.length).toBe(4);
    expect(statuses[0]!.name).toBe("running");

    const stepStatuses = await pg.sql`SELECT * FROM wf_step_status ORDER BY id`;
    expect(stepStatuses.length).toBe(7);

    const stepTypes = await pg.sql`SELECT * FROM wf_step_type ORDER BY id`;
    expect(stepTypes.length).toBe(4);
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
      expect(state!.status).toBe("running");
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
      expect(await storage.tryLock("lock-1", 30_000)).toBe(true);
      await storage.releaseLock("lock-1");
      expect(await storage.tryLock("lock-1", 30_000)).toBe(true);
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
    const result = await workflow<{ n: number }>({ name: "e2e-linear", storage })
      .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
      .step("add-one", ({ prev }) => Pipeline.succeed(prev + 1))
      .run({ workflowId: "e2e-1", input: { n: 5 } });

    expect(result).toBe(11);
    expect((await storage.loadWorkflow("e2e-1"))!.status).toBe("completed");
  });

  it("runs a DAG workflow", async () => {
    const result = await workflow<{ text: string }>({ name: "e2e-dag", storage })
      .step("parse", ({ input }) => Pipeline.succeed(input.text.split(" ")))
      .step("count", { dependsOn: ["parse"] }, ({ deps }) => Pipeline.succeed(deps.parse.length))
      .step("join", { dependsOn: ["parse"] }, ({ deps }) => Pipeline.succeed(deps.parse.join("-")))
      .step("combine", { dependsOn: ["count", "join"] }, ({ deps }) =>
        Pipeline.succeed(`${deps.join} (${deps.count})`),
      )
      .run({ workflowId: "e2e-dag-1", input: { text: "hello world" } });

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
    const result = await workflow<{ n: number }>({ name: "e2e-resume", storage })
      .step("step-1", ({ input }) => {
        step1Called = true;
        return Pipeline.succeed(input.n * 2);
      })
      .step("step-2", ({ prev }) => Pipeline.succeed(prev + 100))
      .run({ workflowId: "e2e-resume", input: { n: 10 } });

    expect(result).toBe(120);
    expect(step1Called).toBe(false);
  });

  it("handles step failure", async () => {
    const { error } = await workflow<{}>({ name: "e2e-fail", storage })
      .step("boom", () => Pipeline.fail(new TestError({ message: "test error" })))
      .runSafe({ workflowId: "e2e-fail-1", input: {} });

    expect(error).not.toBeNull();
    expect((await storage.loadWorkflow("e2e-fail-1"))!.status).toBe("failed");
  });

  it("runs with stepAsync", async () => {
    const result = await workflow<{ name: string }>({ name: "e2e-async", storage })
      .stepAsync("greet", async ({ input }) => `Hello, ${input.name}!`)
      .run({ workflowId: "e2e-async-1", input: { name: "Postgres" } });

    expect(result).toBe("Hello, Postgres!");
  });

  it("stores and queries workflow type and metadata", async () => {
    await workflow<{ userId: string }>({
      name: "onboard-user",
      storage,
      type: "onboarding",
      metadata: { team: "growth", region: "us-east", priority: "high" },
    })
      .step("fetch", ({ input }) => Pipeline.succeed({ name: `User ${input.userId}` }))
      .stepAsync("provision", async ({ prev }) => ({ accountId: `acc-${prev.name}` }))
      .run({ workflowId: "e2e-meta-onboard", input: { userId: "u_42" } });

    await workflow<{ date: string }>({
      name: "daily-report",
      storage,
      type: "report",
      metadata: { team: "data", schedule: "daily" },
    })
      .step("generate", ({ input }) => Pipeline.succeed(`Report for ${input.date}`))
      .run({ workflowId: "e2e-meta-report", input: { date: "2026-03-31" } });

    await workflow<{ pipeline: string }>({
      name: "etl-pipeline",
      storage,
      type: "etl",
      metadata: { team: "data", source: "clickhouse", destination: "postgres" },
    })
      .step("extract", () => Pipeline.succeed([1, 2, 3]))
      .step("load", ({ prev }) => Pipeline.succeed({ loaded: prev.length }))
      .run({ workflowId: "e2e-meta-etl", input: { pipeline: "events" } });

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
