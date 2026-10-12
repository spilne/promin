// ---------------------------------------------------------------------------
// evalRunStoreTestSuite — shared conformance suite for EvalRunStore.
// Every backend (InMemory / Sqlite / Postgres) runs this exact suite.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { EvalCaseResult } from "../types.ts";
import type { EvalRunStore, StartEvalRunParams } from "./types.ts";

function startParams(over: Partial<StartEvalRunParams> = {}): StartEvalRunParams {
  return {
    runId: "run-1",
    targetId: "t",
    datasetId: "d",
    samplesPerCase: 1,
    totalCases: 3,
    startedAt: 1_000,
    ...over,
  };
}

function caseResult(caseId: string, passed: boolean): EvalCaseResult {
  return {
    caseId,
    passRate: passed ? 1 : 0,
    passed,
    samples: [
      {
        sampleIndex: 0,
        output: { output: caseId, metrics: { latencyMs: 3 } },
        scores: [
          {
            scorerId: "s",
            value: passed ? 1 : 0,
            passed,
            threshold: 1,
            required: true,
            reason: "because",
          },
        ],
        passed,
      },
    ],
  };
}

/** Run the EvalRunStore conformance suite against a fresh store per test. */
export function evalRunStoreTestSuite(makeStore: () => EvalRunStore | Promise<EvalRunStore>): void {
  describe("EvalRunStore conformance", () => {
    it("starts a running run with no cases", async () => {
      const store = await makeStore();
      await store.start(
        startParams({
          targetVersion: "v2",
          suiteId: "suite",
          suiteVersion: "v1",
          metadata: { by: "test" },
        }),
      );
      const got = await store.get("run-1");
      expect(got).toEqual({
        runId: "run-1",
        status: "running",
        targetId: "t",
        targetVersion: "v2",
        datasetId: "d",
        suiteId: "suite",
        suiteVersion: "v1",
        samplesPerCase: 1,
        totalCases: 3,
        completedCases: 0,
        startedAt: 1_000,
        metadata: { by: "test" },
        caseResults: [],
      });
    });

    it("start is idempotent: a restarted run keeps its recorded cases", async () => {
      const store = await makeStore();
      await store.start(startParams());
      await store.recordCase({ runId: "run-1", caseIndex: 0, result: caseResult("a", true) });
      await store.start(startParams({ totalCases: 99, startedAt: 5 }));
      const got = await store.get("run-1");
      expect(got?.totalCases).toBe(3);
      expect(got?.startedAt).toBe(1_000);
      expect(got?.caseResults.map((c) => c.caseId)).toEqual(["a"]);
    });

    it("records cases as they finish and returns them in dataset order", async () => {
      const store = await makeStore();
      await store.start(startParams());
      await store.recordCase({ runId: "run-1", caseIndex: 2, result: caseResult("c", false) });
      await store.recordCase({ runId: "run-1", caseIndex: 0, result: caseResult("a", true) });
      const got = await store.get("run-1");
      expect(got?.completedCases).toBe(2);
      expect(got?.caseResults.map((c) => c.caseId)).toEqual(["a", "c"]);
      expect(got?.caseResults[0]).toEqual(caseResult("a", true));
      // Re-recording a case replaces it.
      await store.recordCase({ runId: "run-1", caseIndex: 2, result: caseResult("c", true) });
      const again = await store.get("run-1");
      expect(again?.completedCases).toBe(2);
      expect(again?.caseResults[1]?.passed).toBe(true);
    });

    it("finishes a run with its totals, or with an error", async () => {
      const store = await makeStore();
      await store.start(startParams());
      await store.finish({
        runId: "run-1",
        status: "completed",
        finishedAt: 2_000,
        totals: {
          passRate: 0.5,
          perScorer: { s: { meanValue: 0.5, passRate: 0.5 } },
          scoredCases: 2,
        },
      });
      const done = await store.get("run-1");
      expect(done?.status).toBe("completed");
      expect(done?.finishedAt).toBe(2_000);
      expect(done?.totals).toEqual({
        passRate: 0.5,
        perScorer: { s: { meanValue: 0.5, passRate: 0.5 } },
        scoredCases: 2,
      });

      await store.start(startParams({ runId: "run-2" }));
      await store.finish({ runId: "run-2", status: "failed", finishedAt: 3_000, error: "boom" });
      const failed = await store.get("run-2");
      expect(failed?.status).toBe("failed");
      expect(failed?.error).toBe("boom");
      expect(failed?.totals).toBeUndefined();
    });

    it("leaves an ended run as it is: the first ending wins, stragglers don't land", async () => {
      const store = await makeStore();
      await store.start(startParams());
      await store.recordCase({ runId: "run-1", caseIndex: 0, result: caseResult("a", true) });
      await store.finish({ runId: "run-1", status: "cancelled", finishedAt: 2_000 });
      await store.recordCase({ runId: "run-1", caseIndex: 1, result: caseResult("b", true) });
      await store.finish({
        runId: "run-1",
        status: "completed",
        finishedAt: 3_000,
        totals: { passRate: 1, perScorer: {}, scoredCases: 2 },
      });
      const got = await store.get("run-1");
      expect(got?.status).toBe("cancelled");
      expect(got?.finishedAt).toBe(2_000);
      expect(got?.totals).toBeUndefined();
      expect(got?.caseResults.map((c) => c.caseId)).toEqual(["a"]);
    });

    it("refuses to record into or finish an unknown run", async () => {
      const store = await makeStore();
      await expect(
        store.recordCase({ runId: "ghost", caseIndex: 0, result: caseResult("a", true) }),
      ).rejects.toThrow();
      await expect(
        store.finish({ runId: "ghost", status: "cancelled", finishedAt: 1 }),
      ).rejects.toThrow();
    });

    it("returns null for an unknown id", async () => {
      expect(await (await makeStore()).get("nope")).toBeNull();
    });

    it("lists newest first, without case results, and filters", async () => {
      const store = await makeStore();
      await store.start(startParams({ runId: "a1", targetId: "a", startedAt: 1 }));
      await store.start(
        startParams({ runId: "a3", targetId: "a", targetVersion: "v2", startedAt: 3 }),
      );
      await store.start(
        startParams({
          runId: "b2",
          targetId: "b",
          suiteId: "s",
          suiteVersion: "v1",
          namespaceId: "acme",
          startedAt: 2,
        }),
      );
      await store.recordCase({ runId: "a3", caseIndex: 0, result: caseResult("x", true) });
      await store.finish({ runId: "a3", status: "completed", finishedAt: 4 });

      const all = await store.list();
      expect(all.map((r) => r.runId)).toEqual(["a3", "b2", "a1"]);
      expect(all[0]?.completedCases).toBe(1);
      expect("caseResults" in (all[0] ?? {})).toBe(false);
      expect((await store.list({ targetId: "a" })).map((r) => r.runId)).toEqual(["a3", "a1"]);
      expect((await store.list({ targetVersion: "v2" })).map((r) => r.runId)).toEqual(["a3"]);
      expect((await store.list({ suiteId: "s" })).map((r) => r.runId)).toEqual(["b2"]);
      expect((await store.list({ suiteId: "s", suiteVersion: "v2" })).map((r) => r.runId)).toEqual(
        [],
      );
      expect((await store.list({ status: "completed" })).map((r) => r.runId)).toEqual(["a3"]);
      expect((await store.list({ namespaceId: "acme" })).map((r) => r.runId)).toEqual(["b2"]);
      expect((await store.get("b2"))?.namespaceId).toBe("acme");
      expect((await store.list({ datasetId: "d", limit: 1 })).map((r) => r.runId)).toEqual(["a3"]);
    });

    it("deletes a run and its cases", async () => {
      const store = await makeStore();
      await store.start(startParams());
      await store.recordCase({ runId: "run-1", caseIndex: 0, result: caseResult("a", true) });
      await store.delete("run-1");
      expect(await store.get("run-1")).toBeNull();
      // The id can be started afresh.
      await store.start(startParams());
      expect((await store.get("run-1"))?.caseResults).toEqual([]);
    });
  });
}
