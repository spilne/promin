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

    it("appends concurrently, reporting whether each case was inserted", async () => {
      const store = makeStore();
      const inserted = await Promise.all(
        Array.from({ length: 20 }, (_, i) =>
          store.appendCase("feedback", { id: String(i), input: `input ${i}` }),
        ),
      );
      expect(inserted.every(Boolean)).toBe(true);
      expect(await store.appendCase("feedback", { id: "3", input: "duplicate" })).toBe(false);
      const cases = await store.get("feedback");
      expect(cases).toHaveLength(20);
      expect(cases?.find((item) => item.id === "3")?.input).toBe("input 3");
    });

    it("replaces and deletes single cases in place", async () => {
      const store = makeStore();
      expect(await store.replaceCase("ds", { id: "a", input: "x" })).toBe(false);
      expect(await store.deleteCase("ds", "a")).toBe(false);
      for (const id of ["a", "b", "c"]) await store.appendCase("ds", { id, input: id });
      expect(await store.replaceCase("ds", { id: "b", input: "new", expected: { n: 1 } })).toBe(
        true,
      );
      expect(await store.replaceCase("ds", { id: "zz", input: "x" })).toBe(false);
      expect((await store.get("ds"))?.map((item) => item.input)).toEqual(["a", "new", "c"]);
      expect((await store.get("ds"))?.[1]?.expected).toEqual({ n: 1 });
      expect(await store.deleteCase("ds", "a")).toBe(true);
      expect(await store.deleteCase("ds", "a")).toBe(false);
      expect((await store.get("ds"))?.map((item) => item.id)).toEqual(["b", "c"]);
      await store.deleteCase("ds", "b");
      await store.deleteCase("ds", "c");
      expect(await store.get("ds")).toEqual([]);
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
