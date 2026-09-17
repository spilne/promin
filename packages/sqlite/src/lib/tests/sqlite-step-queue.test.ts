import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { stepQueueTestSuite } from "@promin/workflow/testing";
import { SqliteStepQueue } from "../sqlite-step-queue.ts";

function makeQueue() {
  return SqliteStepQueue.make({ db: new Database(":memory:") });
}

// ---- conformance suite ----

stepQueueTestSuite(makeQueue);

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
    const tasks = await q2.claim({ limit: 10 });
    expect(tasks).toHaveLength(1);
    expect(tasks[0]!.id).toBe(id);
  });

  it("custom table name avoids conflicts", async () => {
    const db = new Database(":memory:");
    const a = SqliteStepQueue.make({ db, table: "tasks_a" });
    const b = SqliteStepQueue.make({ db, table: "tasks_b" });

    await a.enqueue({ workflowId: "wf", stepName: "s", input: {}, prevResults: {} });
    const tasksA = await a.claim({ limit: 10 });
    const tasksB = await b.claim({ limit: 10 });

    expect(tasksA).toHaveLength(1);
    expect(tasksB).toHaveLength(0);
  });

  it("heartbeat resets the stale timeout", async () => {
    const q = makeQueue();
    await q.enqueue({ workflowId: "wf-1", stepName: "s1", input: {}, prevResults: {} });
    const [task] = await q.claim({ limit: 1 });

    await new Promise((r) => setTimeout(r, 20));
    await q.heartbeat({ taskId: task!.id });

    // Cutoff = 500ms ago — heartbeat was <500ms ago, should NOT requeue
    const requeued = await q.requeueStuck({ staleTimeoutMs: 500 });
    expect(requeued).toBe(0);
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
    const [task] = await q.claim({ limit: 1 });
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
    const [task] = await q.claim({ capabilities: ["gpu", "nvme"], limit: 1 });
    expect(task!.needs).toEqual(["gpu", "nvme"]);
  });
});
