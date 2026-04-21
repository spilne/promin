import { describe, it, expect } from "bun:test";
import { createSearchMemoryTool, createSaveMemoryTool, createMemoryTools } from "../tools/memory-tools.ts";
import { InMemoryMemoryStore } from "../memory-store.ts";

function makeStore() {
  return new InMemoryMemoryStore();
}

describe("createSaveMemoryTool", () => {
  it("stores content and returns id confirmation", async () => {
    const store = makeStore();
    const t = createSaveMemoryTool({ store });
    const result = await t.execute({ content: "Paris is the capital of France." });
    expect(result).toContain("Saved to memory");
    const entries = await store.list(10);
    expect(entries).toHaveLength(1);
    expect(entries[0]!.content).toBe("Paris is the capital of France.");
  });

  it("never returns the raw content in the result (just id)", async () => {
    const store = makeStore();
    const t = createSaveMemoryTool({ store });
    const secret = "super-private-content-xyz";
    const result = await t.execute({ content: secret });
    expect(result).not.toContain(secret);
  });

  it("tool name is saveMemory", () => {
    expect(createSaveMemoryTool({ store: makeStore() }).name).toBe("saveMemory");
  });
});

describe("createSearchMemoryTool", () => {
  it("returns 'No memories found' when store is empty", async () => {
    const t = createSearchMemoryTool({ store: makeStore() });
    const result = await t.execute({ query: "anything", limit: 5 });
    expect(result).toBe("No memories found matching that query.");
  });

  it("returns numbered entries for matching memories", async () => {
    const store = makeStore();
    await store.save({ content: "Bun is a fast JavaScript runtime." });
    await store.save({ content: "Effect is a TypeScript FP library." });
    const t = createSearchMemoryTool({ store });
    const result = await t.execute({ query: "Bun", limit: 5 });
    expect(result).toMatch(/^1\./);
    expect(result).toContain("Bun is a fast JavaScript runtime.");
  });

  it("respects the limit parameter", async () => {
    const store = makeStore();
    for (let i = 0; i < 10; i++) await store.save({ content: `fact ${i}` });
    const t = createSearchMemoryTool({ store });
    const result = await t.execute({ query: "fact", limit: 3 });
    const lines = result.trim().split("\n");
    expect(lines.length).toBeLessThanOrEqual(3);
  });

  it("tool name is searchMemory", () => {
    expect(createSearchMemoryTool({ store: makeStore() }).name).toBe("searchMemory");
  });
});

describe("createMemoryTools", () => {
  it("returns both tools keyed by name", () => {
    const tools = createMemoryTools({ store: makeStore() });
    expect(tools.searchMemory.name).toBe("searchMemory");
    expect(tools.saveMemory.name).toBe("saveMemory");
  });

  it("save and search round-trip", async () => {
    const store = makeStore();
    const { saveMemory, searchMemory } = createMemoryTools({ store });
    await saveMemory.execute({ content: "The capital of Japan is Tokyo." });
    const result = await searchMemory.execute({ query: "Japan", limit: 5 });
    expect(result).toContain("Tokyo");
  });
});
