// ---------------------------------------------------------------------------
// PgWorkflowStartQueue — conformance against the shared WorkflowStartQueue
// suite, plus Pg-specific behaviour (concurrent claim correctness via
// SELECT FOR UPDATE SKIP LOCKED).
// ---------------------------------------------------------------------------

import { beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { workflowStartQueueTestSuite } from "@promin/workflow/testing";
import { PgWorkflowStartQueue } from "../pg-workflow-start-queue.ts";
import { postgresDescribe } from "../test-utils.ts";

postgresDescribe("PgWorkflowStartQueue conformance", (pg) => {
  beforeAll(async () => {
    await new PgWorkflowStartQueue({ db: pg.db }).ensureTable();
  });
  beforeEach(async () => {
    await pg.db.execute(sql`TRUNCATE TABLE wf_workflow_starts`);
  });

  const factory = async () => new PgWorkflowStartQueue({ db: pg.db });

  workflowStartQueueTestSuite(factory);

  describe("PgWorkflowStartQueue — Pg-specific", () => {
    it("100 concurrent claims race for 100 starts → exactly 100 claims, no double-claims (SKIP LOCKED)", async () => {
      const q = new PgWorkflowStartQueue({ db: pg.db });
      await q.ensureTable();
      // Enqueue 100 starts.
      for (let i = 0; i < 100; i++) {
        await q.enqueue({
          workflowId: `wf-${i}`,
          workflowName: "race",
          input: { i },
        });
      }
      // Fire 10 concurrent claimers each asking for 10 records.
      // SKIP LOCKED means the txns get disjoint candidate sets; the
      // total claimed across all 10 callers must be exactly 100 with
      // no id appearing twice.
      const calls = Array.from({ length: 10 }, (_, n) =>
        q.claim({
          workflowSpecs: [{ name: "race", versions: [] }],
          workerId: `worker-${n}`,
          limit: 10,
        }),
      );
      const results = await Promise.all(calls);
      const allIds = results.flat().map((r) => r.id);
      expect(allIds.length).toBe(100);
      expect(new Set(allIds).size).toBe(100);
    });

    it("two start queues on the same DB share state — multi-replica enqueue/claim", async () => {
      const a = new PgWorkflowStartQueue({ db: pg.db });
      await a.ensureTable();
      const b = new PgWorkflowStartQueue({ db: pg.db });

      // Enqueue from queue A; claim from queue B — must see A's enqueue.
      await a.enqueue({ workflowId: "wf-1", workflowName: "shared", input: {} });
      const claimed = await b.claim({
        workflowSpecs: [{ name: "shared", versions: [] }],
        workerId: "worker-b",
        limit: 5,
      });
      expect(claimed).toHaveLength(1);
      expect(claimed[0]!.workflowId).toBe("wf-1");
      // Complete from B; A's list should reflect the deletion.
      await b.complete(claimed[0]!.id);
      const remaining = await a.list();
      expect(remaining).toHaveLength(0);
    });

    it("stale claims drop back to pending after reclaimAfterMs and another worker can pick them up", async () => {
      // Tight reclaim window so the test runs in milliseconds. Real
      // deployments use 60s+; we use 50ms here.
      const q = new PgWorkflowStartQueue({ db: pg.db, reclaimAfterMs: 50 });
      await q.ensureTable();
      await q.enqueue({ workflowId: "stuck-1", workflowName: "wf", input: {} });

      // Worker A claims, then "dies" (never calls complete).
      const firstClaim = await q.claim({
        workflowSpecs: [{ name: "wf", versions: [] }],
        workerId: "worker-a",
        limit: 1,
      });
      expect(firstClaim).toHaveLength(1);

      // Wait past the reclaim window.
      await new Promise((r) => setTimeout(r, 80));

      // Worker B re-claims the stale row.
      const secondClaim = await q.claim({
        workflowSpecs: [{ name: "wf", versions: [] }],
        workerId: "worker-b",
        limit: 1,
      });
      expect(secondClaim).toHaveLength(1);
      expect(secondClaim[0]!.id).toBe(firstClaim[0]!.id);
      expect(secondClaim[0]!.claimedBy).toBe("worker-b");
    });
  });
});
