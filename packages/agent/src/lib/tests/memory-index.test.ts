import { describe, it, expect } from "bun:test";
import { InMemoryMemoryIndex } from "../memory-index.ts";
import type { EmbeddingProvider } from "../memory-index.ts";

describe("InMemoryMemoryIndex", () => {
  describe("save / list", () => {
    it("returns saved entries via list, most recent first", async () => {
      const store = new InMemoryMemoryIndex();
      await store.save({ content: "first" });
      await store.save({ content: "second" });
      await store.save({ content: "third" });

      const entries = await store.list();
      expect(entries.map((e) => e.content)).toEqual(["third", "second", "first"]);
    });

    it("list respects limit", async () => {
      const store = new InMemoryMemoryIndex();
      for (let i = 0; i < 5; i++) await store.save({ content: `entry-${i}` });

      const entries = await store.list(3);
      expect(entries).toHaveLength(3);
    });

    it("save returns a unique id each time", async () => {
      const store = new InMemoryMemoryIndex();
      const id1 = await store.save({ content: "a" });
      const id2 = await store.save({ content: "b" });
      expect(id1).not.toBe(id2);
    });

    it("persists metadata", async () => {
      const store = new InMemoryMemoryIndex();
      await store.save({ content: "fact", metadata: { sessionId: "s1", turn: 3 } });

      const [entry] = await store.list();
      expect(entry?.metadata).toEqual({ sessionId: "s1", turn: 3 });
    });
  });

  describe("delete", () => {
    it("removes the entry", async () => {
      const store = new InMemoryMemoryIndex();
      const id = await store.save({ content: "to delete" });
      await store.delete(id);

      const entries = await store.list();
      expect(entries).toHaveLength(0);
    });

    it("is a no-op for unknown ids", async () => {
      const store = new InMemoryMemoryIndex();
      await store.save({ content: "keep" });
      await store.delete("nonexistent");

      expect(await store.list()).toHaveLength(1);
    });
  });

  describe("keyword search (no embeddings)", () => {
    it("returns matching entries ranked by overlap", async () => {
      const store = new InMemoryMemoryIndex();
      await store.save({ content: "the user prefers dark mode in their editor" });
      await store.save({ content: "the user likes TypeScript and Bun runtime" });
      await store.save({ content: "completely unrelated content about cooking" });

      const results = await store.search("TypeScript Bun");
      expect(results[0]?.content).toContain("TypeScript");
    });

    it("excludes entries with zero keyword overlap", async () => {
      const store = new InMemoryMemoryIndex();
      await store.save({ content: "quantum physics" });
      await store.save({ content: "cooking recipes" });

      const results = await store.search("TypeScript workflows");
      expect(results).toHaveLength(0);
    });

    it("respects limit", async () => {
      const store = new InMemoryMemoryIndex();
      for (let i = 0; i < 10; i++) {
        await store.save({ content: `workflow step execution engine result ${i}` });
      }
      const results = await store.search("workflow", 3);
      expect(results).toHaveLength(3);
    });

    it("returns empty array when store is empty", async () => {
      const store = new InMemoryMemoryIndex();
      expect(await store.search("anything")).toEqual([]);
    });
  });

  describe("semantic search (with embeddings)", () => {
    function mockEmbeddings(map: Record<string, number[]>): EmbeddingProvider {
      return {
        embed: async (text) => {
          for (const [key, vec] of Object.entries(map)) {
            if (text.includes(key)) return vec;
          }
          return [0, 0, 0];
        },
      };
    }

    it("ranks by cosine similarity", async () => {
      const embeddings = mockEmbeddings({
        apples: [1, 0, 0],
        bananas: [0, 1, 0],
        fruits: [0.7, 0.7, 0],
      });

      const store = new InMemoryMemoryIndex({ embeddings });
      await store.save({ content: "I like apples" });
      await store.save({ content: "I like bananas" });

      // query embedding similar to apples vector
      const results = await store.search("fruits apples", 2);
      expect(results[0]?.content).toContain("apples");
    });

    it("returns all results when all have zero similarity", async () => {
      const embeddings: EmbeddingProvider = { embed: async () => [0, 0, 0] };
      const store = new InMemoryMemoryIndex({ embeddings });
      await store.save({ content: "entry one" });
      await store.save({ content: "entry two" });

      const results = await store.search("anything", 10);
      expect(results).toHaveLength(2);
    });
  });
});
