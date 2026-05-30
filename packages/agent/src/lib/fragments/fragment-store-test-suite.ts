// ---------------------------------------------------------------------------
// Portable `FragmentStore` conformance suite. Every implementation
// (in-memory, SQLite, future Postgres) must pass. Mirrors
// `skillRegistryTestSuite`.
//
// Usage:
//   import { fragmentStoreTestSuite } from "@promin/agent/testing";
//   fragmentStoreTestSuite(() => new InMemoryFragmentStore());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { FragmentStore } from "./types.ts";

export function fragmentStoreTestSuite(factory: () => FragmentStore | Promise<FragmentStore>) {
  async function make(): Promise<FragmentStore> {
    return factory();
  }

  describe("FragmentStore conformance", () => {
    describe("loadAll + set", () => {
      it("starts empty", async () => {
        const s = await make();
        expect(await s.loadAll()).toEqual([]);
      });

      it("persists a fragment and returns it from loadAll", async () => {
        const s = await make();
        await s.set("review-checklist", "## Review\n- correctness");
        const all = await s.loadAll();
        expect(all.map((f) => f.key)).toEqual(["review-checklist"]);
        expect(all[0]!.content).toContain("correctness");
      });

      it("replaces on set with the same key (last write wins)", async () => {
        const s = await make();
        await s.set("k", "v1");
        await s.set("k", "v2");
        const all = await s.loadAll();
        expect(all).toHaveLength(1);
        expect(all[0]!.content).toBe("v2");
      });

      it("stores many fragments", async () => {
        const s = await make();
        for (const k of ["a", "b", "c"]) await s.set(k, `body-${k}`);
        const all = await s.loadAll();
        expect(all.map((f) => f.key).sort()).toEqual(["a", "b", "c"]);
      });
    });

    describe("delete", () => {
      it("removes a fragment by key", async () => {
        const s = await make();
        await s.set("k", "v");
        await s.delete("k");
        expect(await s.loadAll()).toEqual([]);
      });

      it("is a no-op when the key isn't present", async () => {
        const s = await make();
        await s.delete("never-existed");
      });

      it("leaves other entries intact", async () => {
        const s = await make();
        await s.set("a", "1");
        await s.set("b", "2");
        await s.delete("a");
        const all = await s.loadAll();
        expect(all.map((f) => f.key)).toEqual(["b"]);
      });
    });
  });
}
