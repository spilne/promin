import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { FakeWallClock, type WallClock } from "@promin/workflow";
import { stepQueueTestSuite } from "@promin/workflow/testing";
import { SqliteStepQueue } from "../sqlite-step-queue.ts";

function makeQueue(options: { maxDeliveries?: number; clock?: WallClock } = {}) {
  return SqliteStepQueue.make({ db: new Database(":memory:"), ...options });
}

// ---- conformance suite ----

stepQueueTestSuite(({ maxDeliveries, clock }) => makeQueue({ maxDeliveries, clock }), {
  fakeClock: true,
});

// ---- SQLite-specific tests ----

describe("SqliteStepQueue", () => {
  it("adds the heartbeat details column to a table created before it, and opening twice is harmless", async () => {
    const db = new Database(":memory:");
    SqliteStepQueue.make({ db }); // current layout
    db.run("ALTER TABLE promin_step_tasks DROP COLUMN heartbeat_details");
    const queue = SqliteStepQueue.make({ db });
    SqliteStepQueue.make({ db });
    await queue.enqueue({ workflowId: "wf", stepName: "s", input: {} });
    const [task] = await queue.claim({ workerId: "w", limit: 1 });
    expect(await queue.heartbeat({ taskId: task!.id, details: { n: 1 } })).toBe(true);
    expect((await queue.get(task!.id))?.lastHeartbeatDetails).toEqual({ n: 1 });
  });

  it("adds the step-group columns to a table created before them, and reopens idempotently", async () => {
    const db = new Database(":memory:");
    // A task table as an earlier release created it: no group columns.
    db.run(`
      CREATE TABLE promin_step_tasks (
        id TEXT NOT NULL PRIMARY KEY, workflow_id TEXT NOT NULL, step_name TEXT NOT NULL,
        needs TEXT NOT NULL DEFAULT '[]', priority INTEGER NOT NULL DEFAULT 5, input TEXT NOT NULL,
        deps TEXT NOT NULL DEFAULT '{}', depends_on TEXT NOT NULL DEFAULT '[]', timeout_ms INTEGER,
        attempt INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'pending', version TEXT,
        namespace TEXT, metadata TEXT, created_at INTEGER NOT NULL, claimed_at INTEGER,
        claim_token TEXT, completed_at INTEGER, result TEXT, error TEXT, error_tag TEXT,
        step_metadata TEXT, duration_ms INTEGER, last_heartbeat INTEGER, active_key TEXT,
        concurrency_key TEXT, concurrency_scope TEXT, concurrency_limit INTEGER, claimed_by TEXT,
        deliveries INTEGER NOT NULL DEFAULT 0, run INTEGER NOT NULL DEFAULT 1, consumed_at INTEGER
      )
    `);
    db.run(
      `INSERT INTO promin_step_tasks (id, workflow_id, step_name, input, created_at, active_key)
       VALUES ('old', 'wf-old', 's', '{}', 1, 'wf-old::s')`,
    );

    const q = SqliteStepQueue.make({ db });
    SqliteStepQueue.make({ db });
    await q.enqueue({
      workflowId: "wf-new",
      stepName: "s",
      input: {},
      groupName: "g",
      groupMaxConcurrent: 1,
    });
    const claimed = await q.claim({ workerId: "w", limit: 5 });
    // The pre-existing ungrouped row and the grouped one both work.
    expect(claimed.map((t) => t.workflowId).sort()).toEqual(["wf-new", "wf-old"]);
    expect(claimed.find((t) => t.workflowId === "wf-new")!.groupName).toBe("g");
  });

  it("persists across instances sharing the same db", async () => {
    const db = new Database(":memory:");
    const q1 = SqliteStepQueue.make({ db });
    const id = await q1.enqueue({
      workflowId: "wf-1",
      stepName: "charge",
      input: { amount: 100 },
    });

    const q2 = SqliteStepQueue.make({ db });
    const tasks = await q2.claim({ workerId: "w-1", limit: 10 });
    expect(tasks).toHaveLength(1);
    expect(tasks[0]!.id).toBe(id);
  });

  it("custom table name avoids conflicts", async () => {
    const db = new Database(":memory:");
    const a = SqliteStepQueue.make({ db, table: "tasks_a" });
    const b = SqliteStepQueue.make({ db, table: "tasks_b" });

    await a.enqueue({ workflowId: "wf", stepName: "s", input: {} });
    const tasksA = await a.claim({ workerId: "w-1", limit: 10 });
    const tasksB = await b.claim({ workerId: "w-1", limit: 10 });

    expect(tasksA).toHaveLength(1);
    expect(tasksB).toHaveLength(0);
  });

  it("heartbeat resets the stale timeout", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const q = SqliteStepQueue.make({ db: new Database(":memory:"), clock });
    await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {} });
    const [task] = await q.claim({ workerId: "w-1", limit: 1 });

    clock.advance(400);
    expect(await q.heartbeat({ taskId: task!.id, claimToken: task!.claimToken })).toBe(true);

    // 800ms after the claim but 400ms after the heartbeat: not stale at 500ms.
    clock.advance(400);
    expect((await q.requeueStuck({ mode: "stale", olderThanMs: 500 })).requeued).toBe(0);

    // Once the heartbeat itself is more than 500ms old, the task is requeued.
    clock.advance(101);
    expect((await q.requeueStuck({ mode: "stale", olderThanMs: 500 })).requeued).toBe(1);
  });

  it("metrics returns zero counts for empty queue", async () => {
    const q = makeQueue();
    const m = await q.metrics({ since: new Date(Date.now() - 60_000) });
    expect(m.pending).toBe(0);
    expect(m.running).toBe(0);
    expect(m.completed).toBe(0);
    expect(m.failed).toBe(0);
    expect(m.avgWaitMs).toBe(0);
    expect(m.avgExecMs).toBe(0);
    expect(m.p95ExecMs).toBe(0);
  });

  it("version is stored and returned", async () => {
    const q = makeQueue();
    await q.enqueue({
      workflowId: "wf-v",
      stepName: "s",
      input: {},
      version: "3",
    });
    const [task] = await q.claim({ workerId: "w-1", limit: 1 });
    expect(task!.version).toBe("3");
  });

  it("needs round-trips correctly", async () => {
    const q = makeQueue();
    await q.enqueue({
      workflowId: "wf-n",
      stepName: "transcode",
      input: {},
      needs: ["gpu", "nvme"],
    });
    const [task] = await q.claim({ workerId: "w-1", capabilities: ["gpu", "nvme"], limit: 1 });
    expect(task!.needs).toEqual(["gpu", "nvme"]);
  });
});
