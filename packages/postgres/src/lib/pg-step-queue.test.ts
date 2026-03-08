import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { PostgresTestContainer } from "./test-utils.ts";
import { PgStepQueue } from "./pg-step-queue.ts";

// ---------------------------------------------------------------------------
// Container setup
// ---------------------------------------------------------------------------

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
}, 60_000);

afterAll(async () => {
  await pg.stop();
});

// ---------------------------------------------------------------------------
// PgStepQueue — SKIP LOCKED distributed step dispatch
// ---------------------------------------------------------------------------

describe("PgStepQueue", () => {
  it("enqueues and claims a task", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    const id = await queue.enqueue({
      workflowId: "wf-1",
      stepName: "double",
      queue: "default",
      input: { n: 5 },
      prevResults: {},
    });

    expect(id).toBeDefined();

    const tasks = await queue.claim({ queues: ["default"], limit: 10 });
    expect(tasks).toHaveLength(1);
    expect(tasks[0]!.workflowId).toBe("wf-1");
    expect(tasks[0]!.stepName).toBe("double");
    expect(tasks[0]!.status).toBe("running");
    expect(tasks[0]!.input).toEqual({ n: 5 });
  });

  it("claimed tasks are not re-claimed (SKIP LOCKED)", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-2",
      stepName: "step-a",
      queue: "default",
      input: {},
      prevResults: {},
    });

    const first = await queue.claim({ queues: ["default"], limit: 10 });
    expect(first).toHaveLength(1);

    // Second claim should get nothing — task is already running
    const second = await queue.claim({ queues: ["default"], limit: 10 });
    expect(second).toHaveLength(0);
  });

  it("claim respects queue filter", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-3",
      stepName: "cpu-step",
      queue: "cpu",
      input: {},
      prevResults: {},
    });
    await queue.enqueue({
      workflowId: "wf-3",
      stepName: "gpu-step",
      queue: "gpu",
      input: {},
      prevResults: {},
    });

    const cpuTasks = await queue.claim({ queues: ["cpu"], limit: 10 });
    expect(cpuTasks).toHaveLength(1);
    expect(cpuTasks[0]!.stepName).toBe("cpu-step");

    const gpuTasks = await queue.claim({ queues: ["gpu"], limit: 10 });
    expect(gpuTasks).toHaveLength(1);
    expect(gpuTasks[0]!.stepName).toBe("gpu-step");
  });

  it("claim respects limit", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    for (let i = 0; i < 5; i++) {
      await queue.enqueue({
        workflowId: "wf-4",
        stepName: `step-${i}`,
        queue: "batch",
        input: {},
        prevResults: {},
      });
    }

    const tasks = await queue.claim({ queues: ["batch"], limit: 2 });
    expect(tasks).toHaveLength(2);
  });

  it("complete marks task as completed with result", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    const id = await queue.enqueue({
      workflowId: "wf-5",
      stepName: "complete-me",
      queue: "test-complete",
      input: {},
      prevResults: {},
    });

    await queue.claim({ queues: ["test-complete"], limit: 1 });
    await queue.complete({ taskId: id, result: { answer: 42 }, durationMs: 150 });

    const metrics = await queue.metrics();
    expect(metrics["test-complete"]?.completed).toBe(1);
  });

  it("fail marks task as failed with error", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    const id = await queue.enqueue({
      workflowId: "wf-6",
      stepName: "fail-me",
      queue: "test-fail",
      input: {},
      prevResults: {},
    });

    await queue.claim({ queues: ["test-fail"], limit: 1 });
    await queue.fail({ taskId: id, error: "something broke", durationMs: 50 });

    const metrics = await queue.metrics();
    expect(metrics["test-fail"]?.failed).toBe(1);
  });

  it("metrics returns per-queue counts", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    // Enqueue to two queues
    await queue.enqueue({
      workflowId: "wf-7",
      stepName: "a",
      queue: "metrics-q1",
      input: {},
      prevResults: {},
    });
    await queue.enqueue({
      workflowId: "wf-7",
      stepName: "b",
      queue: "metrics-q1",
      input: {},
      prevResults: {},
    });
    await queue.enqueue({
      workflowId: "wf-7",
      stepName: "c",
      queue: "metrics-q2",
      input: {},
      prevResults: {},
    });

    const metrics = await queue.metrics();
    expect(metrics["metrics-q1"]?.pending).toBe(2);
    expect(metrics["metrics-q2"]?.pending).toBe(1);
  });

  it("preserves prevResults through claim", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-8",
      stepName: "with-deps",
      queue: "deps-test",
      input: { x: 1 },
      prevResults: { "step-a": "result-a", "step-b": 42 },
    });

    const tasks = await queue.claim({ queues: ["deps-test"], limit: 1 });
    expect(tasks[0]!.prevResults).toEqual({ "step-a": "result-a", "step-b": 42 });
  });

  it("claim orders by created_at (FIFO)", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-9",
      stepName: "first",
      queue: "fifo",
      input: {},
      prevResults: {},
    });
    await new Promise((r) => setTimeout(r, 10));
    await queue.enqueue({
      workflowId: "wf-9",
      stepName: "second",
      queue: "fifo",
      input: {},
      prevResults: {},
    });

    const tasks = await queue.claim({ queues: ["fifo"], limit: 1 });
    expect(tasks[0]!.stepName).toBe("first");
  });
});
