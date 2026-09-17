// ---------------------------------------------------------------------------
// `createLayeredMemoryTool` — model-facing tool over `MemoryStore`.
//
// Pins:
//   - set writes to thread or resource scope; default is thread
//   - resource writes refused when no resourceId is bound to the tool
//   - setWorking overwrites the markdown scratchpad at thread/resource
//   - recall finds keyword matches across facts + episodes
//   - recall scopes default to thread + resource; namespace requires opt-in
//   - recall ranks episodes by 0.5 * keyword + 0.5 * salience
//   - recall delegates to SemanticRecall capability when present
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { createLayeredMemoryTool } from "../tools/layered-memory-tools.ts";
import { InMemoryMemoryStore } from "../memory/in-memory-memory-store.ts";
import type { MemoryStore, RecallHit, ThreadKey } from "../memory/types.ts";

const NS = "acme";
const ALICE = "alice";
const T = "alice-default";

function makeTool(opts: { withResource?: boolean; store?: MemoryStore } = {}) {
  const store = opts.store ?? new InMemoryMemoryStore();
  const tool = createLayeredMemoryTool({
    store,
    namespaceId: NS,
    resourceId: opts.withResource ? ALICE : undefined,
    threadId: T,
  });
  return { store, tool };
}

describe("layered memory tool — set", () => {
  it("default scope is thread; persists fact under thread key", async () => {
    const { store, tool } = makeTool({ withResource: true });
    const out = await tool.execute({ command: "set", text: "user prefers dark mode" });
    expect(out).toContain("Saved to thread memory");
    const facts = await store.listThreadFacts({ namespaceId: NS, threadId: T });
    expect(facts.map((f) => f.text)).toEqual(["user prefers dark mode"]);
  });

  it("explicit resource scope persists under resource key", async () => {
    const { store, tool } = makeTool({ withResource: true });
    await tool.execute({ command: "set", scope: "resource", text: "alice is in PST" });
    const facts = await store.listResourceFacts({ namespaceId: NS, resourceId: ALICE });
    expect(facts.map((f) => f.text)).toEqual(["alice is in PST"]);
  });

  it("rejects resource scope when no resourceId is bound", async () => {
    const { store, tool } = makeTool({ withResource: false });
    const out = await tool.execute({
      command: "set",
      scope: "resource",
      text: "should be rejected",
    });
    expect(out).toContain("Cannot write resource-scope");
    expect(await store.listResourceFacts({ namespaceId: NS, resourceId: ALICE })).toEqual([]);
  });
});

describe("layered memory tool — setWorking", () => {
  it("default scope thread updates thread.workingMemory", async () => {
    const { store, tool } = makeTool({ withResource: true });
    await tool.execute({ command: "setWorking", markdown: "current focus: tax filing" });
    const t = await store.getThread({ namespaceId: NS, threadId: T });
    expect(t?.workingMemory).toBe("current focus: tax filing");
  });

  it("resource scope updates resource.workingMemory", async () => {
    const { store, tool } = makeTool({ withResource: true });
    await tool.execute({
      command: "setWorking",
      scope: "resource",
      markdown: "alice is on parental leave",
    });
    const r = await store.getResource({ namespaceId: NS, resourceId: ALICE });
    expect(r?.workingMemory).toBe("alice is on parental leave");
  });
});

describe("layered memory tool — recall (keyword fallback)", () => {
  it("returns 'No memories matched' when nothing matches", async () => {
    const { tool } = makeTool({ withResource: true });
    const out = await tool.execute({ command: "recall", query: "nothing here", limit: 5 });
    expect(out).toContain("No memories matched");
  });

  it("finds facts by keyword overlap across thread + resource scopes by default", async () => {
    const { store, tool } = makeTool({ withResource: true });
    await store.appendResourceFact(
      { namespaceId: NS, resourceId: ALICE },
      "alice loves typescript",
    );
    await store.appendThreadFact({ namespaceId: NS, threadId: T }, "discussed kafka topics today");
    const out = await tool.execute({ command: "recall", query: "typescript", limit: 5 });
    expect(out).toContain("alice loves typescript");
    expect(out).not.toContain("kafka");
  });

  it("ranks episodes by combined keyword + salience", async () => {
    const { store, tool } = makeTool({ withResource: true });
    const rkey = { namespaceId: NS, resourceId: ALICE };
    await store.appendResourceEpisode(rkey, {
      summary: "low salience match",
      salience: 0.1,
    });
    await store.appendResourceEpisode(rkey, {
      summary: "high salience match",
      salience: 0.9,
    });
    const out = await tool.execute({ command: "recall", query: "match", limit: 5 });
    const idxHigh = out.indexOf("high salience");
    const idxLow = out.indexOf("low salience");
    expect(idxHigh).toBeGreaterThan(-1);
    expect(idxLow).toBeGreaterThan(idxHigh); // high comes first
  });

  it("respects an explicit `scopes: ['namespace']` request", async () => {
    const { store, tool } = makeTool({ withResource: true });
    await store.appendNamespaceFact(NS, "company default currency is USD");
    await store.appendThreadFact({ namespaceId: NS, threadId: T }, "discussed USD pricing");
    const out = await tool.execute({
      command: "recall",
      query: "currency",
      scopes: ["namespace"],
      limit: 5,
    });
    expect(out).toContain("default currency is USD");
    expect(out).not.toContain("discussed USD pricing");
  });

  it("respects a sources filter", async () => {
    const { store, tool } = makeTool({ withResource: true });
    const rkey = { namespaceId: NS, resourceId: ALICE };
    await store.appendResourceFact(rkey, "alice prefers postgres");
    await store.appendResourceEpisode(rkey, {
      summary: "set up postgres last week",
      salience: 0.7,
    });
    const out = await tool.execute({
      command: "recall",
      query: "postgres",
      sources: ["fact"],
      limit: 5,
    });
    expect(out).toContain("alice prefers postgres");
    expect(out).not.toContain("set up postgres last week");
  });
});

describe("layered memory tool — recall (SemanticRecall delegation)", () => {
  it("uses the store's recall() when SemanticRecall is implemented", async () => {
    const calls: Array<{ key: ThreadKey; query: string }> = [];
    class StoreWithRecall extends InMemoryMemoryStore {
      readonly hasSemanticRecall = true as const;
      async recall(key: ThreadKey, query: string): Promise<RecallHit[]> {
        calls.push({ key, query });
        return [
          {
            source: "fact",
            scope: "resource",
            id: "stub-1",
            text: "from semantic recall",
            score: 0.95,
            createdAt: 0,
          },
        ];
      }
    }
    const store = new StoreWithRecall();
    const tool = createLayeredMemoryTool({
      store,
      namespaceId: NS,
      resourceId: ALICE,
      threadId: T,
    });
    const out = await tool.execute({ command: "recall", query: "anything", limit: 5 });
    expect(out).toContain("from semantic recall");
    expect(calls).toHaveLength(1);
    expect(calls[0]!.query).toBe("anything");
    expect(calls[0]!.key.threadId).toBe(T);
  });
});

describe("layered memory tool — name + shape", () => {
  it("exposes the tool under name 'memory'", () => {
    const { tool } = makeTool();
    expect(tool.name).toBe("memory");
  });
});
