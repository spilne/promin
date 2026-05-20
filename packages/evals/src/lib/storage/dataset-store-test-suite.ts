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
      await store.save("ds", CASES);
      const got = await store.get("ds");
      expect(got?.length).toBe(2);
      expect(got?.[0]?.expected).toBe("A");
    });

    it("returns null for an unknown dataset", async () => {
      expect(await makeStore().get("nope")).toBeNull();
    });

    it("save replaces the existing cases", async () => {
      const store = makeStore();
      await store.save("ds", CASES);
      await store.save("ds", [{ id: "x", input: "z" }]);
      const got = await store.get("ds");
      expect(got?.length).toBe(1);
      expect(got?.[0]?.id).toBe("x");
    });

    it("lists dataset ids in ascending order", async () => {
      const store = makeStore();
      await store.save("b", CASES);
      await store.save("a", CASES);
      expect(await store.list()).toEqual(["a", "b"]);
    });

    it("deletes a dataset", async () => {
      const store = makeStore();
      await store.save("ds", CASES);
      await store.delete("ds");
      expect(await store.get("ds")).toBeNull();
    });
  });
}
