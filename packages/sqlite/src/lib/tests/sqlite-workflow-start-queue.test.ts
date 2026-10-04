// ---------------------------------------------------------------------------
// SqliteWorkflowStartQueue — conformance against the shared suite, plus
// persistence-specific behaviour (survives a fresh queue on the same db).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { FakeWallClock } from "@promin/workflow";
import {
  workflowStartQueueTestSuite,
  type WorkflowStartQueueSuiteFactoryParams,
} from "@promin/workflow/testing";
import { SqliteWorkflowStartQueue } from "../sqlite-workflow-start-queue.ts";

let counter = 0;
function freshQueue(params: WorkflowStartQueueSuiteFactoryParams): SqliteWorkflowStartQueue {
  const db = new Database(":memory:");
  return SqliteWorkflowStartQueue.make({
    db,
    tableName: `promin_workflow_starts_${++counter}`,
    clock: params.clock,
    reclaimAfterMs: params.reclaimAfterMs,
  });
}

workflowStartQueueTestSuite(freshQueue);

describe("SqliteWorkflowStartQueue — persistence", () => {
  it("pending starts survive a fresh queue instance over the same db", async () => {
    const db = new Database(":memory:");
    const q1 = SqliteWorkflowStartQueue.make({ db });
    await q1.enqueue({ workflowId: "wf-1", workflowName: "hello", input: { n: 1 } });

    // New instance pointed at the same db → existing pending start
    // must still be claimable. This is the shape that matters: a
    // dashboard restart with no worker connected doesn't lose the
    // queued trigger.
    const q2 = SqliteWorkflowStartQueue.make({ db });
    const claimed = await q2.claim({
      workflowSpecs: [{ name: "hello", versions: [] }],
      workerId: "w1",
      limit: 10,
    });
    expect(claimed.length).toBe(1);
    expect(claimed[0]?.workflowId).toBe("wf-1");
  });

  it("stale claims auto-recover after reclaimAfterMs", async () => {
    const db = new Database(":memory:");
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const q = SqliteWorkflowStartQueue.make({ db, reclaimAfterMs: 50, clock });
    await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
    const first = await q.claim({
      workflowSpecs: [{ name: "wf", versions: [] }],
      workerId: "dead-worker",
      limit: 1,
    });
    expect(first.length).toBe(1);

    // No completion — simulate worker crash.
    clock.advance(80);

    // Next claim() recovers the stale row and hands it to the new worker.
    const second = await q.claim({
      workflowSpecs: [{ name: "wf", versions: [] }],
      workerId: "live-worker",
      limit: 1,
    });
    expect(second.length).toBe(1);
    expect(second[0]?.claimedBy).toBe("live-worker");
  });
});

describe("SqliteWorkflowStartQueue — schema upgrade", () => {
  it("adds the claim fencing columns to a table created without them", async () => {
    const db = new Database(":memory:");
    db.run(`
      CREATE TABLE promin_workflow_starts (
        id            TEXT PRIMARY KEY,
        workflow_id   TEXT NOT NULL,
        workflow_name TEXT NOT NULL,
        version       TEXT,
        input         TEXT NOT NULL,
        metadata      TEXT,
        enqueued_at   INTEGER NOT NULL,
        claimed_at    INTEGER,
        claimed_by    TEXT,
        status        TEXT NOT NULL DEFAULT 'pending'
      )
    `);
    db.run(
      `INSERT INTO promin_workflow_starts (id, workflow_id, workflow_name, input, enqueued_at)
       VALUES ('old-1', 'wf-old', 'wf', '{}', 1)`,
    );

    const q = SqliteWorkflowStartQueue.make({ db });
    const [rec] = await q.claim({
      workflowSpecs: [{ name: "wf", versions: [] }],
      workerId: "w1",
      limit: 1,
    });
    expect(rec?.id).toBe("old-1");
    expect(typeof rec?.claimToken).toBe("string");
    expect(await q.heartbeat({ id: "old-1", claimToken: rec!.claimToken! })).toBe(true);
    expect(await q.complete({ id: "old-1", claimToken: rec!.claimToken! })).toBe(true);
    expect(await q.list()).toEqual([]);
  });
});
