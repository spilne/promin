// ---------------------------------------------------------------------------
// evalRunStoreTestSuite — shared conformance suite for EvalRunStore.
// Every backend (InMemory / Sqlite / Postgres) runs this exact suite.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { EvalRunSummary } from "../types.ts";
import type { EvalRunStore } from "./types.ts";

/** Build a minimal run summary, overriding only the fields a test cares about. */
function summary(over: Partial<EvalRunSummary> = {}): EvalRunSummary {
  return {
    targetId: "t",
    datasetId: "d",
    ranAt: 1000,
    totalCases: 1,
    passRate: 1,
    perScorer: {},
    samplesPerCase: 1,
    caseResults: [],
    ...over,
  };
}

/** Run the EvalRunStore conformance suite against a fresh store per test. */
export function evalRunStoreTestSuite(makeStore: () => EvalRunStore): void {
  describe("EvalRunStore conformance", () => {
    it("saves and gets a run by its composed id", async () => {
      const store = makeStore();
      const runId = await store.save(summary({ ranAt: 5 }));
      const got = await store.get(runId);
      expect(got?.summary.ranAt).toBe(5);
      expect(got?.runId).toBe(runId);
    });

    it("save is idempotent on the (target, version, dataset, ranAt) key", async () => {
      const store = makeStore();
      const first = await store.save(summary({ ranAt: 7, passRate: 1 }));
      const second = await store.save(summary({ ranAt: 7, passRate: 0 }));
      expect(first).toBe(second);
      const all = await store.list();
      expect(all.length).toBe(1);
      expect(all[0]?.summary.passRate).toBe(0); // last write wins
    });

    it("returns null for an unknown id", async () => {
      expect(await makeStore().get("nope")).toBeNull();
    });

    it("lists newest-first and filters by target", async () => {
      const store = makeStore();
      await store.save(summary({ targetId: "a", ranAt: 1 }));
      await store.save(summary({ targetId: "a", ranAt: 3 }));
      await store.save(summary({ targetId: "b", ranAt: 2 }));
      const all = await store.list();
      expect(all.map((r) => r.summary.ranAt)).toEqual([3, 2, 1]);
      const onlyA = await store.list({ targetId: "a" });
      expect(onlyA.length).toBe(2);
      expect(onlyA.every((r) => r.summary.targetId === "a")).toBe(true);
    });

    it("honours the list limit", async () => {
      const store = makeStore();
      await store.save(summary({ ranAt: 1 }));
      await store.save(summary({ ranAt: 2 }));
      expect((await store.list({ limit: 1 })).length).toBe(1);
    });

    it("deletes a run", async () => {
      const store = makeStore();
      const runId = await store.save(summary());
      await store.delete(runId);
      expect(await store.get(runId)).toBeNull();
    });
  });
}
