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

    it("version-filtered concurrent claims — wide window, no double-claims, only matching versions claimed", async () => {
      // Stress test for the wide-window path. When at least one spec has
      // a pinned version, claim() switches to a 4× candidate window so
      // the JS-side filter has room to discard mismatches. This test
      // verifies that path under concurrent load:
      //   - 200 starts: 100 v1, 100 v2, randomly interleaved
      //   - 10 workers, each pinned to a single version (5 to v1, 5 to v2)
      //   - all fire claim() concurrently
      //
      // What's deterministic: every claimed row's version matches the
      // claiming worker's spec; no row is claimed twice. Total throughput
      // depends on lock contention so we assert a reasonable floor
      // rather than an exact count.
      const q = new PgWorkflowStartQueue({ db: pg.db });
      await q.ensureTable();

      // Deterministic interleaving so the test doesn't drift between runs.
      const versions: ("1" | "2")[] = [];
      for (let i = 0; i < 200; i++) versions.push(i % 2 === 0 ? "1" : "2");

      for (let i = 0; i < versions.length; i++) {
        await q.enqueue({
          workflowId: `wf-${i}`,
          workflowName: "race",
          input: { i },
          version: versions[i]!,
        });
      }

      // 10 workers — half pinned to v1, half to v2. Each asks for 15
      // matching rows. With 100 v1 + 100 v2 in the queue and 5 workers
      // racing for each, we expect:
      //   - every claim's row.version matches the spec
      //   - no row claimed twice across all workers
      const calls = Array.from({ length: 10 }, (_, n) => {
        const targetVersion = n < 5 ? "1" : "2";
        return q.claim({
          workflowSpecs: [{ name: "race", versions: [targetVersion] }],
          workerId: `worker-${n}-v${targetVersion}`,
          limit: 15,
        });
      });
      const results = await Promise.all(calls);

      // Pair each claim with its worker's pinned version so we can
      // verify the JS-side filter held under contention.
      const flat: Array<{
        claimedBy: string;
        pinnedVersion: "1" | "2";
        record: { id: string; version?: string };
      }> = [];
      for (let n = 0; n < 10; n++) {
        const pinned = n < 5 ? ("1" as const) : ("2" as const);
        for (const rec of results[n]!) {
          flat.push({
            claimedBy: `worker-${n}-v${pinned}`,
            pinnedVersion: pinned,
            record: rec,
          });
        }
      }

      // Correctness invariants — these must hold regardless of timing.
      // 1. No row claimed twice.
      const ids = flat.map((c) => c.record.id);
      expect(new Set(ids).size).toBe(ids.length);

      // 2. Every row a worker claimed matches its pinned version. This
      //    is the version-filter contract — if it ever broke, the wide
      //    window would let a v2 row sneak through to a v1-pinned worker.
      for (const c of flat) {
        expect(c.record.version).toBe(c.pinnedVersion);
      }

      // 3. Each worker claimed at most `limit` rows.
      for (let n = 0; n < 10; n++) {
        expect(results[n]!.length).toBeLessThanOrEqual(15);
      }

      // 4. Throughput floor — enough claims happened that we know the
      //    wide window did its job. With 200 rows and 10 concurrent
      //    workers asking for 15 each (max possible: 150), we should
      //    saturate at least 50 in a single burst even under heavy
      //    contention. Lower bound is conservative.
      expect(flat.length).toBeGreaterThanOrEqual(50);
      // Log actual throughput so a future regression in the wide-window
      // path stands out (test passes but throughput drops noticeably).
      // eslint-disable-next-line no-console
      console.log(
        `[stress] version-filtered: ${flat.length}/150 claimed across 10 workers (5×v1 + 5×v2)`,
      );
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
