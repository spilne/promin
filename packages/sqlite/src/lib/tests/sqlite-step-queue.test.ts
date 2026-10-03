import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { stepQueueTestSuite } from "@promin/workflow/testing";
import { SqliteStepQueue } from "../sqlite-step-queue.ts";

function makeQueue(options: { maxDeliveries?: number } = {}) {
  return SqliteStepQueue.make({ db: new Database(":memory:"), ...options });
}

// ---- conformance suite ----

stepQueueTestSuite(({ maxDeliveries }) => makeQueue({ maxDeliveries }));

// ---- SQLite-specific tests ----

describe("SqliteStepQueue", () => {
  it("persists across instances sharing the same db", async () => {
    const db = new Database(":memory:");
    const q1 = SqliteStepQueue.make({ db });
    const id = await q1.enqueue({
      workflowId: "wf-1",
      stepName: "charge",
      input: { amount: 100 },
      prevResults: {},
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

    await a.enqueue({ workflowId: "wf", stepName: "s", input: {}, prevResults: {} });
    const tasksA = await a.claim({ workerId: "w-1", limit: 10 });
    const tasksB = await b.claim({ workerId: "w-1", limit: 10 });

    expect(tasksA).toHaveLength(1);
    expect(tasksB).toHaveLength(0);
  });

  it("heartbeat resets the stale timeout", async () => {
    const q = makeQueue();
    await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {}, prevResults: {} });
    const [task] = await q.claim({ workerId: "w-1", limit: 1 });

    await new Promise((r) => setTimeout(r, 20));
    await q.heartbeat({ taskId: task!.id });

    // Cutoff = 500ms ago — heartbeat was <500ms ago, should NOT requeue
    const { requeued } = await q.requeueStuck({ mode: "stale", olderThanMs: 500 });
    expect(requeued).toBe(0);
  });

  it("upgrades a table created before claimed_by / deliveries and namespace-free active keys", async () => {
    const db = new Database(":memory:");
    db.run(`
      CREATE TABLE promin_step_tasks (
        id TEXT NOT NULL PRIMARY KEY, workflow_id TEXT NOT NULL, step_name TEXT NOT NULL,
        needs TEXT NOT NULL DEFAULT '[]', priority INTEGER NOT NULL DEFAULT 5,
        input TEXT NOT NULL, prev_results TEXT NOT NULL DEFAULT '{}',
        attempt INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'pending',
        version TEXT, namespace TEXT, created_at INTEGER NOT NULL, claimed_at INTEGER,
        completed_at INTEGER, result TEXT, error TEXT, duration_ms INTEGER,
        last_heartbeat INTEGER, active_key TEXT
      )
    `);
    db.run(
      `INSERT INTO promin_step_tasks (id, workflow_id, step_name, input, created_at, active_key)
       VALUES ('old-1', 'wf', 's', '{}', 1, 'ns::wf::s')`,
    );

    const q = SqliteStepQueue.make({ db });
    // The pre-existing active task still dedupes under the new key.
    expect(await q.enqueue({ workflowId: "wf", stepName: "s", input: {}, prevResults: {} })).toBe(
      "old-1",
    );
    const [task] = await q.claim({ workerId: "w-9", limit: 1 });
    expect(task?.deliveries).toBe(1);
    expect((await q.get("old-1"))?.claimedBy).toBe("w-9");
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
      prevResults: {},
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
      prevResults: {},
      needs: ["gpu", "nvme"],
    });
    const [task] = await q.claim({ workerId: "w-1", capabilities: ["gpu", "nvme"], limit: 1 });
    expect(task!.needs).toEqual(["gpu", "nvme"]);
  });
});
