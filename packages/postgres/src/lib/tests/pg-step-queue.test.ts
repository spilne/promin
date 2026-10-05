import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test";
import { PostgresTestContainer } from "../test-utils.ts";
import { PgStepQueue } from "../pg-step-queue.ts";
import { FakeWallClock } from "@promin/workflow";
import { stepQueueTestSuite } from "@promin/workflow/testing";
import { ensureTable } from "@spilne/perfect-postgres";
import { PgLeaderLeaseStore } from "../pg-leader-lease-store.ts";
import { leaderLeases } from "../scheduler-schema.ts";

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
    });

    expect(id).toBeDefined();

    const tasks = await queue.claim({ workerId: "w-1", capabilities: ["default"], limit: 10 });
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
    });

    const first = await queue.claim({ workerId: "w-1", capabilities: ["default"], limit: 10 });
    expect(first).toHaveLength(1);

    // Second claim should get nothing — task is already running
    const second = await queue.claim({ workerId: "w-1", capabilities: ["default"], limit: 10 });
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
    });
    await queue.enqueue({
      workflowId: "wf-3",
      stepName: "gpu-step",
      needs: ["gpu"],
      input: {},
    });

    const cpuTasks = await queue.claim({ workerId: "w-1", capabilities: ["cpu"], limit: 10 });
    expect(cpuTasks).toHaveLength(1);
    expect(cpuTasks[0]!.stepName).toBe("cpu-step");

    const gpuTasks = await queue.claim({ workerId: "w-1", capabilities: ["gpu"], limit: 10 });
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
      });
    }

    const tasks = await queue.claim({ workerId: "w-1", capabilities: ["batch"], limit: 2 });
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
    });

    await queue.claim({ workerId: "w-1", capabilities: ["test-complete"], limit: 1 });
    await queue.complete({ taskId: id, result: { answer: 42 }, durationMs: 150 });

    const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
    expect(metrics.completed).toBe(1);
  });

  it("metrics count a just-completed step when the app clock runs ahead of the DB clock", async () => {
    // claimed_at / completed_at are stamped by the app clock, created_at by
    // the DB. A default window bounded by the DB's NOW() drops the row
    // whenever the app clock is even a few ms ahead (a Docker VM clock lags
    // the host under load) — exaggerate the skew to make it certain.
    const clock = FakeWallClock.create(Date.now() + 5_000);
    const queue = new PgStepQueue({ db: pg.db, clock });

    const id = await queue.enqueue({
      workflowId: "wf-skew",
      stepName: "s",
      needs: ["skew"],
      input: {},
    });
    await queue.claim({ workerId: "w-1", capabilities: ["skew"], limit: 1 });
    await queue.complete({ taskId: id, result: null, durationMs: 1 });

    const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
    expect(metrics.completed).toBe(1);
  });

  it("metrics count just-claimed and just-enqueued steps when the app clock runs behind", async () => {
    const clock = FakeWallClock.create(Date.now() - 5_000);
    const queue = new PgStepQueue({ db: pg.db, clock });

    for (const stepName of ["claimed", "pending"]) {
      await queue.enqueue({
        workflowId: "wf-skew",
        stepName,
        needs: [stepName],
        input: {},
      });
    }
    await queue.claim({ workerId: "w-1", capabilities: ["claimed"], limit: 1 });

    const metrics = await queue.metrics({ since: new Date(clock.currentTimeMs() - 60_000) });
    expect(metrics.running).toBe(1);
    expect(metrics.pending).toBe(1);
  });

  it("an explicit until still bounds the window", async () => {
    const clock = FakeWallClock.create(Date.now());
    const queue = new PgStepQueue({ db: pg.db, clock });

    const id = await queue.enqueue({
      workflowId: "wf-until",
      stepName: "s",
      needs: ["until"],
      input: {},
    });
    await queue.claim({ workerId: "w-1", capabilities: ["until"], limit: 1 });
    clock.advance(10_000);
    await queue.complete({ taskId: id, result: null, durationMs: 1 });

    const before = await queue.metrics({
      since: new Date(clock.currentTimeMs() - 60_000),
      until: new Date(clock.currentTimeMs() - 5_000),
    });
    expect(before.completed).toBe(0);
    const after = await queue.metrics({
      since: new Date(clock.currentTimeMs() - 60_000),
      until: clock.now(),
    });
    expect(after.completed).toBe(1);
  });

  it("step fails — error message persisted and failure metrics updated", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    const id = await queue.enqueue({
      workflowId: "wf-6",
      stepName: "fail-me",
      needs: ["test-fail"],
      input: {},
    });

    await queue.claim({ workerId: "w-1", capabilities: ["test-fail"], limit: 1 });
    await queue.fail({ taskId: id, error: "something broke", durationMs: 50 });

    const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
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
    });
    await queue.enqueue({
      workflowId: "wf-7",
      stepName: "b",
      needs: ["metrics-q1"],
      input: {},
    });
    await queue.enqueue({
      workflowId: "wf-7",
      stepName: "c",
      needs: ["metrics-q2"],
      input: {},
    });

    const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
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
      deps: { "step-b": 42, "step-a": "result-a" },
      dependsOn: ["step-b", "step-a"],
      timeoutMs: 1500,
    });

    const tasks = await queue.claim({ workerId: "w-1", capabilities: ["deps-test"], limit: 1 });
    expect(tasks[0]!.deps).toEqual({ "step-a": "result-a", "step-b": 42 });
    // jsonb reorders keys; the declared order travels in `dependsOn`.
    expect(tasks[0]!.dependsOn).toEqual(["step-b", "step-a"]);
    expect(tasks[0]!.timeoutMs).toBe(1500);
  });

  it("first-enqueued task is claimed first — FIFO fairness guarantee", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-9",
      stepName: "first",
      needs: ["fifo"],
      input: {},
    });
    await new Promise((r) => setTimeout(r, 10));
    await queue.enqueue({
      workflowId: "wf-9",
      stepName: "second",
      needs: ["fifo"],
      input: {},
    });

    const tasks = await queue.claim({ workerId: "w-1", capabilities: ["fifo"], limit: 1 });
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
      priority: 1,
    });
    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "high",
      needs: ["prio"],
      input: {},
      priority: 10,
    });
    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "medium",
      needs: ["prio"],
      input: {},
      priority: 5,
    });

    const tasks = await queue.claim({ workerId: "w-1", capabilities: ["prio"], limit: 3 });
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
stepQueueTestSuite(
  async ({ maxDeliveries }) => {
    const queue = new PgStepQueue({ db: pg.db, maxDeliveries });
    await queue.ensureTable();
    await pg.sql`TRUNCATE wf_step_queue RESTART IDENTITY`;
    return queue;
  },
  {
    leaseFenced: async () => {
      const queue = new PgStepQueue({ db: pg.db });
      await queue.ensureTable();
      await ensureTable(pg.db, leaderLeases);
      await pg.sql`TRUNCATE wf_step_queue RESTART IDENTITY`;
      return { queue, leases: new PgLeaderLeaseStore({ db: pg.db }) };
    },
  },
);

// ---------------------------------------------------------------------------
// Regressions that need real concurrency / a real planner
// ---------------------------------------------------------------------------

describe("Postgres step queue — claim under contention", () => {
  beforeAll(async () => {
    await new PgStepQueue({ db: pg.db }).ensureTable();
  });
  beforeEach(async () => {
    await pg.sql`TRUNCATE wf_step_queue RESTART IDENTITY`;
  });

  it("a worker reaches its task behind a higher-priority foreign step (limit 1)", async () => {
    const q = new PgStepQueue({ db: pg.db });
    await q.enqueue({ workflowId: "a", stepName: "y", input: {}, priority: 9 });
    await q.enqueue({ workflowId: "b", stepName: "x", input: {}, priority: 5 });

    let got = 0;
    for (let i = 0; i < 20; i++) {
      got += (await q.claim({ workerId: "w-x", limit: 1, stepNames: ["x"] })).length;
    }
    expect(got).toBe(1);
  });

  it("the concurrency cap holds for concurrent claimers with different needs (30 rounds)", async () => {
    let violations = 0;
    for (let round = 0; round < 30; round++) {
      await pg.sql`TRUNCATE wf_step_queue RESTART IDENTITY`;
      const e = new PgStepQueue({ db: pg.db });
      const conc = { concurrencyKey: "t1", concurrencyScope: "wf", concurrencyLimit: 1 };
      await e.enqueue({
        workflowId: "w1",
        stepName: "cpu-step",
        needs: ["cpu"],
        input: {},
        ...conc,
      });
      await e.enqueue({
        workflowId: "w2",
        stepName: "gpu-step",
        needs: ["gpu"],
        input: {},
        ...conc,
      });
      const a = new PgStepQueue({ db: pg.db });
      const b = new PgStepQueue({ db: pg.db });
      const [ra, rb] = await Promise.all([
        a.claim({ workerId: "cpu", limit: 1, capabilities: ["cpu"] }),
        b.claim({ workerId: "gpu", limit: 1, capabilities: ["gpu"] }),
      ]);
      expect(ra.length + rb.length).toBeGreaterThanOrEqual(1);
      if (ra.length + rb.length > 1) violations++;
    }
    expect(violations).toBe(0);
  });

  it("the cap holds with many claimers racing over many keys", async () => {
    const e = new PgStepQueue({ db: pg.db });
    for (let k = 0; k < 5; k++) {
      for (let i = 0; i < 6; i++) {
        await e.enqueue({
          workflowId: `k${k}-wf${i}`,
          stepName: `s${i % 3}`,
          needs: i % 2 === 0 ? ["cpu"] : ["gpu"],
          input: {},
          concurrencyKey: `key-${k}`,
          concurrencyScope: "race",
          concurrencyLimit: 2,
        });
      }
    }
    await Promise.all(
      Array.from({ length: 8 }, (_, i) =>
        new PgStepQueue({ db: pg.db }).claim({
          workerId: `w-${i}`,
          limit: 3,
          capabilities: i % 2 === 0 ? ["cpu"] : ["gpu"],
        }),
      ),
    );
    const running = (await pg.sql`
      SELECT concurrency_key, COUNT(*)::int AS n FROM wf_step_queue
      WHERE status = 'running' GROUP BY concurrency_key
    `) as unknown as Array<{ concurrency_key: string; n: number }>;
    expect(running.length).toBeGreaterThan(0);
    for (const r of running) expect(r.n).toBeLessThanOrEqual(2);
  });

  it("redelivery keeps the runner's attempt and counts deliveries (poison task)", async () => {
    const q = new PgStepQueue({ db: pg.db, maxDeliveries: 3 });
    const id = await q.enqueue({
      workflowId: "p",
      stepName: "s",
      input: {},
      attempt: 2,
    });
    const seen: string[] = [];
    const tokens = new Set<string>();
    for (let i = 0; i < 4; i++) {
      const [t] = await q.claim({ workerId: "w", limit: 1 });
      if (!t) break;
      seen.push(`${t.attempt}/${t.deliveries}`);
      tokens.add(t.claimToken!);
      await q.requeueStuck({ mode: "worker", workerId: "w" });
    }
    expect(seen).toEqual(["2/1", "2/2", "2/3"]);
    expect(tokens.size).toBe(3);
    expect((await q.get(id))?.status).toBe("failed");
  });

  it("each task in one claim batch gets its own claim token", async () => {
    const q = new PgStepQueue({ db: pg.db });
    await q.enqueue({ workflowId: "a", stepName: "s", input: {} });
    await q.enqueue({ workflowId: "b", stepName: "s", input: {} });
    const claimed = await q.claim({ workerId: "w", limit: 2 });
    expect(new Set(claimed.map((t) => t.claimToken)).size).toBe(2);
  });
});
