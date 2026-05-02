// ---------------------------------------------------------------------------
// SqliteWorkflowStartQueue — conformance against the shared suite, plus
// persistence-specific behaviour (survives a fresh queue on the same db).
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { workflowStartQueueTestSuite } from "@promin/workflow";
import { SqliteWorkflowStartQueue } from "../sqlite-workflow-start-queue.ts";

let counter = 0;
function freshQueue(): SqliteWorkflowStartQueue {
  const db = new Database(":memory:");
  return SqliteWorkflowStartQueue.make({
    db,
    tableName: `promin_workflow_starts_${++counter}`,
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
    const q = SqliteWorkflowStartQueue.make({ db, reclaimAfterMs: 50 });
    await q.enqueue({ workflowId: "a", workflowName: "wf", input: {} });
    const first = await q.claim({
      workflowSpecs: [{ name: "wf", versions: [] }],
      workerId: "dead-worker",
      limit: 1,
    });
    expect(first.length).toBe(1);

    // No completion — simulate worker crash.
    await new Promise((r) => setTimeout(r, 80));

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
