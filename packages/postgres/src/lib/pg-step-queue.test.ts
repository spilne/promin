import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test";
import { PostgresTestContainer } from "./test-utils.ts";
import { PgStepQueue } from "./pg-step-queue.ts";
import { stepQueueTestSuite } from "@promin/workflow/testing";

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
  // Dedupe is on (workflow_id, step_name) so tests sharing IDs would
  // accumulate. Create the table once up front, then truncate between
  // tests so each starts with an empty queue.
  beforeAll(async () => {
    await new PgStepQueue({ db: pg.db }).ensureTable();
  });
  beforeEach(async () => {
    await pg.sql`TRUNCATE wf_step_queue RESTART IDENTITY`;
  });

  it("enqueue a step task and claim it for processing", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    const id = await queue.enqueue({
      workflowId: "wf-1",
      stepName: "double",
      needs: ["default"],
      input: { n: 5 },
      prevResults: {},
    });

    expect(id).toBeDefined();

    const tasks = await queue.claim({ capabilities: ["default"], limit: 10 });
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
      needs: ["default"],
      input: {},
      prevResults: {},
    });

    const first = await queue.claim({ capabilities: ["default"], limit: 10 });
    expect(first).toHaveLength(1);

    // Second claim should get nothing — task is already running
    const second = await queue.claim({ capabilities: ["default"], limit: 10 });
    expect(second).toHaveLength(0);
  });

  it("CPU and GPU tasks routed to separate queues — workers see only their tasks", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-3",
      stepName: "cpu-step",
      needs: ["cpu"],
      input: {},
      prevResults: {},
    });
    await queue.enqueue({
      workflowId: "wf-3",
      stepName: "gpu-step",
      needs: ["gpu"],
      input: {},
      prevResults: {},
    });

    const cpuTasks = await queue.claim({ capabilities: ["cpu"], limit: 10 });
    expect(cpuTasks).toHaveLength(1);
    expect(cpuTasks[0]!.stepName).toBe("cpu-step");

    const gpuTasks = await queue.claim({ capabilities: ["gpu"], limit: 10 });
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
        needs: ["batch"],
        input: {},
        prevResults: {},
      });
    }

    const tasks = await queue.claim({ capabilities: ["batch"], limit: 2 });
    expect(tasks).toHaveLength(2);
  });

  it("step finishes successfully — result persisted and metrics updated", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    const id = await queue.enqueue({
      workflowId: "wf-5",
      stepName: "complete-me",
      needs: ["test-complete"],
      input: {},
      prevResults: {},
    });

    await queue.claim({ capabilities: ["test-complete"], limit: 1 });
    await queue.complete({ taskId: id, result: { answer: 42 }, durationMs: 150 });

    const metrics = await queue.metrics();
    expect(metrics.completed).toBe(1);
  });

  it("step fails — error message persisted and failure metrics updated", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    const id = await queue.enqueue({
      workflowId: "wf-6",
      stepName: "fail-me",
      needs: ["test-fail"],
      input: {},
      prevResults: {},
    });

    await queue.claim({ capabilities: ["test-fail"], limit: 1 });
    await queue.fail({ taskId: id, error: "something broke", durationMs: 50 });

    const metrics = await queue.metrics();
    expect(metrics.failed).toBe(1);
  });

  it("ops dashboard sees pending/running/completed counts per queue", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    // Enqueue to two queues
    await queue.enqueue({
      workflowId: "wf-7",
      stepName: "a",
      needs: ["metrics-q1"],
      input: {},
      prevResults: {},
    });
    await queue.enqueue({
      workflowId: "wf-7",
      stepName: "b",
      needs: ["metrics-q1"],
      input: {},
      prevResults: {},
    });
    await queue.enqueue({
      workflowId: "wf-7",
      stepName: "c",
      needs: ["metrics-q2"],
      input: {},
      prevResults: {},
    });

    const metrics = await queue.metrics();
    // Flat metrics now (per-status totals, not per-queue breakdown). Three
    // enqueues all land as pending until claimed.
    expect(metrics.pending).toBe(3);
  });

  it("dependent step receives results from prior steps — context flows through the queue", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-8",
      stepName: "with-deps",
      needs: ["deps-test"],
      input: { x: 1 },
      prevResults: { "step-a": "result-a", "step-b": 42 },
    });

    const tasks = await queue.claim({ capabilities: ["deps-test"], limit: 1 });
    expect(tasks[0]!.prevResults).toEqual({ "step-a": "result-a", "step-b": 42 });
  });

  it("first-enqueued task is claimed first — FIFO fairness guarantee", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-9",
      stepName: "first",
      needs: ["fifo"],
      input: {},
      prevResults: {},
    });
    await new Promise((r) => setTimeout(r, 10));
    await queue.enqueue({
      workflowId: "wf-9",
      stepName: "second",
      needs: ["fifo"],
      input: {},
      prevResults: {},
    });

    const tasks = await queue.claim({ capabilities: ["fifo"], limit: 1 });
    expect(tasks[0]!.stepName).toBe("first");
  });

  it("urgent tasks jump the queue — priority ordering overrides FIFO", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "low",
      needs: ["prio"],
      input: {},
      prevResults: {},
      priority: 1,
    });
    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "high",
      needs: ["prio"],
      input: {},
      prevResults: {},
      priority: 10,
    });
    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "medium",
      needs: ["prio"],
      input: {},
      prevResults: {},
      priority: 5,
    });

    const tasks = await queue.claim({ capabilities: ["prio"], limit: 3 });
    expect(tasks).toHaveLength(3);
    // First claimed should be highest priority (highest number)
    expect(tasks[0]!.stepName).toBe("high");
    expect(tasks[0]!.priority).toBe(10);
  });
});

// ---------------------------------------------------------------------------
// Portable conformance suite
// ---------------------------------------------------------------------------

// Conformance against the shared container — dedupe is on
// `(workflow_id, step_name)` (no namespace in the key), so each test needs
// a clean table. `TRUNCATE ... RESTART IDENTITY` also resets the bigserial
// id counter so ordering-dependent assertions stay deterministic across
// runs.
stepQueueTestSuite(async () => {
  const queue = new PgStepQueue({ db: pg.db });
  await queue.ensureTable();
  await pg.sql`TRUNCATE wf_step_queue RESTART IDENTITY`;
  return queue;
});
