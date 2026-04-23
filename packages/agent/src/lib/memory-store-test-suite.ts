// ---------------------------------------------------------------------------
// Portable MemoryStore test suite
//
// Usage:
//   import { memoryStoreTestSuite } from "@promin/agent/testing";
//   memoryStoreTestSuite(() => new InMemoryMemoryStore());
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { MemoryStore } from "./memory-store.ts";

export function memoryStoreTestSuite(factory: () => MemoryStore | Promise<MemoryStore>) {
  async function make(): Promise<MemoryStore> {
    return factory();
  }

  describe("MemoryStore conformance", () => {
    // -------------------------------------------------------------------
    // save + list
    // -------------------------------------------------------------------

    describe("save + list", () => {
      it("save returns a non-empty string id", async () => {
        const s = await make();
        const id = await s.save({ content: "hello" });
        expect(typeof id).toBe("string");
        expect(id.length).toBeGreaterThan(0);
      });

      it("save returns a unique id per entry", async () => {
        const s = await make();
        const a = await s.save({ content: "a" });
        const b = await s.save({ content: "b" });
        expect(a).not.toBe(b);
      });

      it("list returns all saved entries", async () => {
        const s = await make();
        await s.save({ content: "one" });
        await s.save({ content: "two" });
        const entries = await s.list();
        expect(entries.length).toBeGreaterThanOrEqual(2);
      });

      it("list returns entries with correct shape", async () => {
        const s = await make();
        const id = await s.save({ content: "check shape" });
        const entries = await s.list();
        const entry = entries.find((e) => e.id === id);
        expect(entry).toBeDefined();
        expect(entry!.content).toBe("check shape");
        expect(entry!.createdAt).toBeInstanceOf(Date);
      });

      it("list respects limit", async () => {
        const s = await make();
        for (let i = 0; i < 5; i++) await s.save({ content: `entry ${i}` });
        const limited = await s.list(3);
        expect(limited.length).toBeLessThanOrEqual(3);
      });

      it("round-trips metadata", async () => {
        const s = await make();
        const id = await s.save({
          content: "with metadata",
          metadata: { source: "test", score: 0.9 },
        });
        const entries = await s.list();
        const entry = entries.find((e) => e.id === id);
        expect(entry!.metadata).toEqual({ source: "test", score: 0.9 });
      });

      it("metadata is undefined when not provided", async () => {
        const s = await make();
        const id = await s.save({ content: "no metadata" });
        const entries = await s.list();
        const entry = entries.find((e) => e.id === id);
        expect(entry!.metadata).toBeUndefined();
      });
    });

    // -------------------------------------------------------------------
    // delete
    // -------------------------------------------------------------------

    describe("delete", () => {
      it("removes the entry", async () => {
        const s = await make();
        const id = await s.save({ content: "to delete" });
        await s.delete(id);
        const entries = await s.list();
        expect(entries.find((e) => e.id === id)).toBeUndefined();
      });

      it("delete is a no-op for unknown id", async () => {
        const s = await make();
        await expect(s.delete("nonexistent-id")).resolves.toBeUndefined();
      });
    });

    // -------------------------------------------------------------------
    // update
    // -------------------------------------------------------------------

    describe("update", () => {
      it("updates content in place", async () => {
        const s = await make();
        const id = await s.save({ content: "original content" });
        await s.update(id, { content: "revised content" });
        const [entry] = await s.list();
        expect(entry!.content).toBe("revised content");
        expect(entry!.id).toBe(id);
      });

      it("updates metadata in place", async () => {
        const s = await make();
        const id = await s.save({ content: "hello", metadata: { v: 1 } });
        await s.update(id, { metadata: { v: 2 } });
        const [entry] = await s.list();
        expect(entry!.metadata).toEqual({ v: 2 });
      });

      it("sets updatedAt on update", async () => {
        const s = await make();
        const id = await s.save({ content: "hello" });
        await s.update(id, { content: "world" });
        const [entry] = await s.list();
        expect(entry!.updatedAt).toBeInstanceOf(Date);
      });

      it("throws for unknown id", async () => {
        const s = await make();
        await expect(s.update("no-such-id", { content: "x" })).rejects.toThrow();
      });
    });

    // -------------------------------------------------------------------
    // search
    // -------------------------------------------------------------------

    describe("search", () => {
      it("returns entries matching query keywords", async () => {
        const s = await make();
        await s.save({ content: "The capital of France is Paris" });
        await s.save({ content: "Berlin is the capital of Germany" });
        await s.save({ content: "Unrelated content about mountains" });

        const results = await s.search("capital France");
        expect(results.length).toBeGreaterThanOrEqual(1);
        const contents = results.map((e) => e.content);
        expect(contents.some((c) => c.includes("France"))).toBe(true);
      });

      it("returns empty array when nothing matches", async () => {
        const s = await make();
        await s.save({ content: "something completely different" });
        const results = await s.search("quantum entanglement superposition");
        expect(results).toHaveLength(0);
      });

      it("respects limit", async () => {
        const s = await make();
        for (let i = 0; i < 8; i++) {
          await s.save({ content: `The quick brown fox jumps over the lazy dog ${i}` });
        }
        const results = await s.search("quick brown fox", 3);
        expect(results.length).toBeLessThanOrEqual(3);
      });

      it("returns results with correct entry shape", async () => {
        const s = await make();
        await s.save({ content: "France capital Paris geography" });
        const results = await s.search("France capital");
        expect(results.length).toBeGreaterThan(0);
        expect(results[0]!.id).toBeDefined();
        expect(results[0]!.createdAt).toBeInstanceOf(Date);
      });
    });

    // -------------------------------------------------------------------
    // scope isolation
    // -------------------------------------------------------------------

    describe("scope isolation", () => {
      it("unscoped entries are isolated from namespaced entries", async () => {
        const s = await make();
        await s.save({ content: "global" });
        await s.save({ content: "namespaced" }, { namespaceId: "ns-1" });

        const global = await s.list();
        const ns1 = await s.list(undefined, { namespaceId: "ns-1" });

        expect(global.every((e) => e.content !== "namespaced")).toBe(true);
        expect(ns1.every((e) => e.content !== "global")).toBe(true);
      });

      it("different namespaces are isolated", async () => {
        const s = await make();
        await s.save({ content: "ns-a entry" }, { namespaceId: "ns-a" });
        await s.save({ content: "ns-b entry" }, { namespaceId: "ns-b" });

        const a = await s.list(undefined, { namespaceId: "ns-a" });
        const b = await s.list(undefined, { namespaceId: "ns-b" });

        expect(a).toHaveLength(1);
        expect(a[0]!.content).toBe("ns-a entry");
        expect(b).toHaveLength(1);
        expect(b[0]!.content).toBe("ns-b entry");
      });

      it("sessionId sub-scopes within namespace", async () => {
        const s = await make();
        await s.save({ content: "session-1" }, { namespaceId: "ns", sessionId: "s1" });
        await s.save({ content: "session-2" }, { namespaceId: "ns", sessionId: "s2" });

        const s1 = await s.list(undefined, { namespaceId: "ns", sessionId: "s1" });
        const s2 = await s.list(undefined, { namespaceId: "ns", sessionId: "s2" });

        expect(s1).toHaveLength(1);
        expect(s1[0]!.content).toBe("session-1");
        expect(s2).toHaveLength(1);
        expect(s2[0]!.content).toBe("session-2");
      });

      it("search is scoped to the same namespace", async () => {
        const s = await make();
        await s.save({ content: "capital of France is Paris" }, { namespaceId: "ns-a" });
        await s.save({ content: "capital of Germany is Berlin" }, { namespaceId: "ns-b" });

        const results = await s.search("capital France", 10, { namespaceId: "ns-a" });
        expect(results).toHaveLength(1);
        expect(results[0]!.content).toContain("France");
      });

      it("delete crosses scope — removes by id regardless of scope", async () => {
        const s = await make();
        const id = await s.save({ content: "scoped entry" }, { namespaceId: "ns" });
        await s.delete(id);
        const remaining = await s.list(undefined, { namespaceId: "ns" });
        expect(remaining.find((e) => e.id === id)).toBeUndefined();
      });
    });
  });
}
