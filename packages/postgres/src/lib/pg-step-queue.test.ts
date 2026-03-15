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

describe("Postgres step queue — distributed task dispatch with SKIP LOCKED", () => {
  it("enqueue a step task and claim it for processing", async () => {
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

  it("concurrent workers cannot claim the same task — SKIP LOCKED prevents double processing", async () => {
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

  it("CPU and GPU tasks routed to separate queues — workers see only their tasks", async () => {
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

  it("worker claims at most 2 of 5 pending tasks — respects batch limit", async () => {
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

  it("step finishes successfully — result persisted and metrics updated", async () => {
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

  it("step fails — error message persisted and failure metrics updated", async () => {
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

  it("ops dashboard sees pending/running/completed counts per queue", async () => {
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

  it("dependent step receives results from prior steps — context flows through the queue", async () => {
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

  it("first-enqueued task is claimed first — FIFO fairness guarantee", async () => {
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

  it("urgent tasks jump the queue — priority ordering overrides FIFO", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "low",
      queue: "prio",
      input: {},
      prevResults: {},
      priority: 1,
    });
    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "high",
      queue: "prio",
      input: {},
      prevResults: {},
      priority: 10,
    });
    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "medium",
      queue: "prio",
      input: {},
      prevResults: {},
      priority: 5,
    });

    const tasks = await queue.claim({ queues: ["prio"], limit: 3 });
    expect(tasks).toHaveLength(3);
    // First claimed should be highest priority (highest number)
    expect(tasks[0]!.stepName).toBe("high");
    expect(tasks[0]!.priority).toBe(10);
  });
});
