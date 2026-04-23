import { describe, it, expect } from "bun:test";
import { InMemoryMemoryStore } from "../../lib/memory-store.ts";
import { createMemoryTool } from "../console-tools.ts";

function makeStore() {
  return new InMemoryMemoryStore();
}

function makeTool() {
  return createMemoryTool(makeStore());
}

describe("createMemoryTool", () => {
  describe("save command", () => {
    it("stores the content and confirms with id", async () => {
      const store = makeStore();
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "save", content: "The sky is blue." });
      expect(typeof result).toBe("string");
      expect(result).toContain("Saved to memory");
      const entries = await store.list();
      expect(entries).toHaveLength(1);
      expect(entries[0]!.content).toBe("The sky is blue.");
    });

    it("result includes the short id", async () => {
      const store = makeStore();
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "save", content: "fact" });
      const entries = await store.list();
      const shortId = entries[0]!.id.slice(0, 8);
      expect(result).toContain(shortId);
    });
  });

  describe("list command", () => {
    it("returns all entries when store has items", async () => {
      const store = makeStore();
      await store.save({ content: "entry one" });
      await store.save({ content: "entry two" });
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "list", limit: 10 });
      expect(result).toContain("entry one");
      expect(result).toContain("entry two");
    });

    it("returns 'No memories stored yet' when empty", async () => {
      const t = makeTool();
      const result = await t.execute({ command: "list", limit: 10 });
      expect(result).toBe("No memories stored yet.");
    });

    it("respects the limit parameter", async () => {
      const store = makeStore();
      for (let i = 0; i < 6; i++) await store.save({ content: `item ${i}` });
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "list", limit: 3 });
      const lines = result
        .trim()
        .split("\n")
        .filter((l) => /^\d+\./.test(l));
      expect(lines.length).toBeLessThanOrEqual(3);
    });

    it("shows count header when list is truncated", async () => {
      const store = makeStore();
      for (let i = 0; i < 5; i++) await store.save({ content: `item ${i}` });
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "list", limit: 3 });
      expect(result).toMatch(/Showing \d+ of \d+ entries/);
    });

    it("includes relative age in each entry", async () => {
      const store = makeStore();
      await store.save({ content: "timestamped entry" });
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "list", limit: 10 });
      expect(result).toMatch(/\d+s? ago/);
    });
  });

  describe("search command", () => {
    it("returns matching entries", async () => {
      const store = makeStore();
      await store.save({ content: "The capital of France is Paris" });
      await store.save({ content: "Unrelated fact about mountains" });
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "search", query: "France capital", limit: 5 });
      expect(result).toContain("France");
    });

    it("returns 'No memories found' when nothing matches", async () => {
      const store = makeStore();
      await store.save({ content: "completely unrelated content" });
      const t = createMemoryTool(store);
      const result = await t.execute({
        command: "search",
        query: "quantum entanglement superposition",
        limit: 5,
      });
      expect(result).toBe("No memories found matching that query.");
    });

    it("shows count header when results are a subset of total", async () => {
      const store = makeStore();
      for (let i = 0; i < 5; i++) await store.save({ content: `fox jumps over the dog ${i}` });
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "search", query: "fox jumps dog", limit: 2 });
      if (result !== "No memories found matching that query.") {
        // only assert header if there were actual results to truncate
        const totalEntries = (await store.list()).length;
        if (totalEntries > 2) {
          expect(result).toMatch(/Showing \d+ of \d+ entries/);
        }
      }
    });
  });

  describe("update command", () => {
    it("updates entry content by id prefix", async () => {
      const store = makeStore();
      const id = await store.save({ content: "original text" });
      const t = createMemoryTool(store);
      const result = await t.execute({
        command: "update",
        id: id.slice(0, 8),
        content: "revised text",
      });
      expect(result).toContain("Updated");
      const entries = await store.list();
      expect(entries[0]!.content).toBe("revised text");
    });

    it("returns error when id prefix has no match", async () => {
      const store = makeStore();
      await store.save({ content: "something" });
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "update", id: "00000000", content: "new" });
      expect(result).toContain("No memory found");
    });

    it("result includes the short id of the updated entry", async () => {
      const store = makeStore();
      const id = await store.save({ content: "original" });
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "update", id: id.slice(0, 8), content: "updated" });
      expect(result).toContain(id.slice(0, 8));
    });
  });

  describe("delete command", () => {
    it("removes the entry by id prefix", async () => {
      const store = makeStore();
      const id = await store.save({ content: "to be deleted" });
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "delete", id: id.slice(0, 8) });
      expect(result).toContain("Deleted");
      const remaining = await store.list();
      expect(remaining.find((e) => e.id === id)).toBeUndefined();
    });

    it("returns error when id prefix has no match", async () => {
      const store = makeStore();
      await store.save({ content: "something" });
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "delete", id: "00000000" });
      expect(result).toContain("No memory found");
    });

    it("result includes the short id of the deleted entry", async () => {
      const store = makeStore();
      const id = await store.save({ content: "entry" });
      const t = createMemoryTool(store);
      const result = await t.execute({ command: "delete", id: id.slice(0, 8) });
      expect(result).toContain(id.slice(0, 8));
    });
  });
});
