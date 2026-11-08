// ---------------------------------------------------------------------------
// evalDatasetStoreTestSuite — shared conformance suite for EvalDatasetStore.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { EvalCase } from "../types.ts";
import type { EvalDatasetStore } from "./types.ts";

const CASES: ReadonlyArray<EvalCase> = [
  { id: "1", input: "a", expected: "A" },
  { id: "2", input: "b" },
];

/** Run the EvalDatasetStore conformance suite against a fresh store per test. */
export function evalDatasetStoreTestSuite(makeStore: () => EvalDatasetStore): void {
  describe("EvalDatasetStore conformance", () => {
    it("saves and gets a dataset", async () => {
      const store = makeStore();
      await store.save({ datasetId: "ds", cases: CASES });
      const got = await store.get("ds");
      expect(got?.length).toBe(2);
      expect(got?.[0]?.expected).toBe("A");
    });

    it("returns null for an unknown dataset", async () => {
      expect(await makeStore().get("nope")).toBeNull();
    });

    it("save replaces the existing cases", async () => {
      const store = makeStore();
      await store.save({ datasetId: "ds", cases: CASES });
      await store.save({ datasetId: "ds", cases: [{ id: "x", input: "z" }] });
      const got = await store.get("ds");
      expect(got?.length).toBe(1);
      expect(got?.[0]?.id).toBe("x");
    });

    it("appends concurrently and ignores a repeated case id", async () => {
      const store = makeStore();
      await Promise.all(
        Array.from({ length: 20 }, (_, i) =>
          store.appendCase("feedback", { id: String(i), input: `input ${i}` }),
        ),
      );
      await store.appendCase("feedback", { id: "3", input: "duplicate" });
      const cases = await store.get("feedback");
      expect(cases).toHaveLength(20);
      expect(cases?.find((item) => item.id === "3")?.input).toBe("input 3");
    });

    it("lists dataset ids in ascending order", async () => {
      const store = makeStore();
      await store.save({ datasetId: "b", cases: CASES });
      await store.save({ datasetId: "a", cases: CASES });
      expect(await store.list()).toEqual(["a", "b"]);
    });

    it("deletes a dataset", async () => {
      const store = makeStore();
      await store.save({ datasetId: "ds", cases: CASES });
      await store.delete("ds");
      expect(await store.get("ds")).toBeNull();
    });
  });
}
