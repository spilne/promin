// ---------------------------------------------------------------------------
// DefaultConsolidator behavior — mock LLM, real InMemoryMemoryStore.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";
import { DefaultConsolidator } from "../consolidator.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";

function fixedLLM(envelope: object): LLMProvider {
  return {
    chat: async (): Promise<LLMResponse> => ({
      content: JSON.stringify(envelope),
      finishReason: "stop",
      usage: { inputTokens: 30, outputTokens: 20 },
    }),
  };
}

describe("DefaultConsolidator", () => {
  it("distillThread writes a ResourceEpisode + saved facts pointing at the source thread", async () => {
    const store = new InMemoryMemoryStore();
    const key = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
    await store.appendMessages(key, [
      { role: "user", content: "Hi I'm Anton, I live in Kyiv" },
      { role: "assistant", content: "Nice to meet you, Anton!" },
      { role: "user", content: "I prefer terse answers" },
      { role: "assistant", content: "Got it." },
    ]);

    const consolidator = new DefaultConsolidator({
      store,
      llm: fixedLLM({
        summary: "Anton introduced himself; user lives in Kyiv and prefers terse replies.",
        outcome: null,
        salience: 0.9,
        facts: ["user's name is Anton", "user lives in Kyiv", "user prefers terse replies"],
      }),
    });

    const ep = await consolidator.distillThread(key);

    expect(ep.summary).toContain("Anton");
    expect(ep.salience).toBeGreaterThan(0.5);
    expect(ep.sourceThreadId).toBe("t1");
    expect(ep.sourceMessageRange?.fromSeq).toBe(1);
    expect(ep.sourceMessageRange?.toSeq).toBe(4);

    // Facts persisted at resource scope so future threads pick them up.
    const facts = await store.listResourceFacts({ namespaceId: "acme", resourceId: "alice" });
    expect(facts.map((f) => f.text)).toEqual([
      "user's name is Anton",
      "user lives in Kyiv",
      "user prefers terse replies",
    ]);

    // Episode is at resource scope, not thread scope.
    const resourceEps = await store.listResourceEpisodes({
      namespaceId: "acme",
      resourceId: "alice",
    });
    expect(resourceEps.length).toBe(1);
    expect(resourceEps[0]!.id).toBe(ep.id);
  });

  it("dedupes facts across re-distillation even when the LLM rephrases (apostrophe / casing / trailing punctuation)", async () => {
    const store = new InMemoryMemoryStore();
    const key = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
    await store.appendMessages(key, [
      { role: "user", content: "Hi I'm Anton" },
      { role: "assistant", content: "Nice to meet you Anton" },
    ]);

    // First distill emits "User name is Anton"; second emits the same
    // fact with an apostrophe + trailing period. Without normalization
    // both would land in the resource layer.
    const llmRun1 = fixedLLM({
      summary: "introduction",
      outcome: null,
      salience: 0.8,
      facts: ["User name is Anton"],
    });
    const llmRun2 = fixedLLM({
      summary: "introduction",
      outcome: null,
      salience: 0.8,
      facts: ["User's name is Anton."],
    });

    const consolidator1 = new DefaultConsolidator({ store, llm: llmRun1 });
    await consolidator1.distillThread(key);

    const consolidator2 = new DefaultConsolidator({ store, llm: llmRun2 });
    await consolidator2.distillThread(key, { force: true });

    const facts = await store.listResourceFacts({ namespaceId: "acme", resourceId: "alice" });
    expect(facts).toHaveLength(1);
    expect(facts[0]!.text).toBe("User name is Anton");
  });

  it("distillThread is idempotent on the same thread (no force)", async () => {
    const store = new InMemoryMemoryStore();
    const key = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
    await store.appendMessages(key, [
      { role: "user", content: "hi" },
      { role: "assistant", content: "hello" },
    ]);
    const consolidator = new DefaultConsolidator({
      store,
      llm: fixedLLM({ summary: "trivial", outcome: null, salience: 0.2, facts: [] }),
    });
    const a = await consolidator.distillThread(key);
    const b = await consolidator.distillThread(key);
    expect(b.id).toBe(a.id); // same row, dedupe by sourceThreadId
  });

  it("distillThread re-runs when force: true", async () => {
    const store = new InMemoryMemoryStore();
    const key = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
    await store.appendMessages(key, [
      { role: "user", content: "hi" },
      { role: "assistant", content: "hello" },
    ]);
    const consolidator = new DefaultConsolidator({
      store,
      llm: fixedLLM({ summary: "v1", outcome: null, salience: 0.3, facts: [] }),
    });
    const a = await consolidator.distillThread(key);
    const b = await consolidator.distillThread(key, { force: true });
    expect(b.id).not.toBe(a.id);
  });

  it("compactThread writes a ThreadEpisode covering the trimmed range", async () => {
    const store = new InMemoryMemoryStore();
    const key = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
    const messages = Array.from({ length: 12 }, (_, i) => ({
      role: (i % 2 === 0 ? "user" : "assistant") as "user" | "assistant",
      content: `msg ${i}`,
    }));
    await store.appendMessages(key, messages);

    const consolidator = new DefaultConsolidator({
      store,
      llm: fixedLLM({ summary: "compacted", outcome: null, salience: 0.4, facts: [] }),
    });
    const ep = await consolidator.compactThread(key, { keepRecent: 4 });

    expect(ep.summary).toBe("compacted");
    expect(ep.sourceThreadId).toBe("t1");
    expect(ep.sourceMessageRange?.fromSeq).toBe(1);
    expect(ep.sourceMessageRange?.toSeq).toBe(8); // 8 trimmed (seq 1..8), 4 kept (seq 9..12)

    const threadEps = await store.listThreadEpisodes(key);
    expect(threadEps.length).toBe(1);

    // Did NOT pollute the resource layer with a thread-scope rollup.
    const resourceEps = await store.listResourceEpisodes({
      namespaceId: "acme",
      resourceId: "alice",
    });
    expect(resourceEps.length).toBe(0);
  });

  it("custom salienceFn overrides the default heuristic", async () => {
    const store = new InMemoryMemoryStore();
    const key = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
    await store.appendMessages(key, [
      { role: "user", content: "hi" },
      { role: "assistant", content: "hello" },
    ]);
    const consolidator = new DefaultConsolidator({
      store,
      llm: fixedLLM({ summary: "x", outcome: null, salience: 0.99, facts: [] }),
      // Always pin salience to 0.05, ignore the model's number entirely.
      salienceFn: () => 0.05,
    });
    const ep = await consolidator.distillThread(key);
    expect(ep.salience).toBe(0.05);
  });

  it("custom distillPrompt is passed to the LLM as the system message", async () => {
    const store = new InMemoryMemoryStore();
    const key = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
    await store.appendMessages(key, [
      { role: "user", content: "hi" },
      { role: "assistant", content: "hello" },
    ]);
    let capturedSystem = "";
    const sniffer: LLMProvider = {
      chat: async (params) => {
        const sys = params.messages.find((m) => m.role === "system");
        capturedSystem = sys && typeof sys.content === "string" ? sys.content : "";
        return {
          content: JSON.stringify({ summary: "ok", outcome: null, salience: 0.5, facts: [] }),
          finishReason: "stop",
        };
      },
    };
    const consolidator = new DefaultConsolidator({
      store,
      llm: sniffer,
      distillPrompt: "CUSTOM PROMPT — emit JSON",
    });
    await consolidator.distillThread(key);
    expect(capturedSystem).toContain("CUSTOM PROMPT");
  });

  it("malformed LLM JSON falls back to a low-salience stub instead of throwing", async () => {
    const store = new InMemoryMemoryStore();
    const key = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
    await store.appendMessages(key, [
      { role: "user", content: "hi" },
      { role: "assistant", content: "hello" },
    ]);
    const consolidator = new DefaultConsolidator({
      store,
      llm: {
        chat: async () => ({ content: "not json at all", finishReason: "stop" }),
      },
    });
    const ep = await consolidator.distillThread(key);
    expect(ep.summary).toContain("not json at all");
    // Default salience for malformed = 0.1, then heuristic adds for short
    // threads etc. Just check it stays low.
    expect(ep.salience).toBeLessThan(0.3);
  });

  describe("maxResourceFacts retention cap", () => {
    async function setupResourceWithFacts(count: number) {
      const store = new InMemoryMemoryStore();
      const resourceKey = { namespaceId: "acme", resourceId: "alice" };
      for (let i = 0; i < count; i++) {
        await store.appendResourceFact(resourceKey, `fact ${i}`);
        // Tiny delay so createdAt timestamps are distinct (in-memory store
        // uses Date.now()) — eviction order is by createdAt asc.
        await new Promise((r) => setTimeout(r, 1));
      }
      return { store, resourceKey };
    }

    it("evicts oldest facts when distilling pushes count over the cap", async () => {
      const { store } = await setupResourceWithFacts(5);
      const threadKey = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
      await store.appendMessages(threadKey, [
        { role: "user", content: "I have a new fact" },
        { role: "assistant", content: "noted" },
      ]);

      const consolidator = new DefaultConsolidator({
        store,
        maxResourceFacts: 3,
        llm: fixedLLM({
          summary: "introduced new facts",
          outcome: null,
          salience: 0.5,
          facts: ["new fact A", "new fact B"],
        }),
      });

      await consolidator.distillThread(threadKey);

      const after = await store.listResourceFacts({ namespaceId: "acme", resourceId: "alice" });
      // 5 existing + 2 new = 7, capped to 3 → keep the 3 most recent.
      expect(after.map((f) => f.text)).toEqual(["fact 4", "new fact A", "new fact B"]);
    });

    it("doesn't evict newly-appended facts in the same call (current turn wins)", async () => {
      const { store } = await setupResourceWithFacts(2);
      const threadKey = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
      await store.appendMessages(threadKey, [
        { role: "user", content: "lots of new" },
        { role: "assistant", content: "ok" },
      ]);

      const consolidator = new DefaultConsolidator({
        store,
        maxResourceFacts: 3,
        llm: fixedLLM({
          summary: "many facts",
          outcome: null,
          salience: 0.5,
          facts: ["new A", "new B", "new C"],
        }),
      });

      await consolidator.distillThread(threadKey);
      const after = await store.listResourceFacts({ namespaceId: "acme", resourceId: "alice" });
      // 2 existing + 3 new = 5, capped to 3 → both existing evicted, all 3 new kept.
      expect(after.map((f) => f.text)).toEqual(["new A", "new B", "new C"]);
    });

    it("is a no-op when count stays at or below the cap", async () => {
      const { store } = await setupResourceWithFacts(1);
      const threadKey = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
      await store.appendMessages(threadKey, [
        { role: "user", content: "small update" },
        { role: "assistant", content: "ok" },
      ]);

      const consolidator = new DefaultConsolidator({
        store,
        maxResourceFacts: 5,
        llm: fixedLLM({
          summary: "minor",
          outcome: null,
          salience: 0.5,
          facts: ["new one"],
        }),
      });

      await consolidator.distillThread(threadKey);
      const after = await store.listResourceFacts({ namespaceId: "acme", resourceId: "alice" });
      expect(after.map((f) => f.text)).toEqual(["fact 0", "new one"]);
    });

    it("retention cap doesn't leak across resources", async () => {
      const store = new InMemoryMemoryStore();
      // Stand up two resources, each at the cap.
      for (const owner of ["alice", "bob"]) {
        for (let i = 0; i < 3; i++) {
          await store.appendResourceFact(
            { namespaceId: "acme", resourceId: owner },
            `${owner}-fact-${i}`,
          );
        }
      }
      const threadKey = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
      await store.appendMessages(threadKey, [
        { role: "user", content: "alice update" },
        { role: "assistant", content: "ok" },
      ]);

      const consolidator = new DefaultConsolidator({
        store,
        maxResourceFacts: 3,
        llm: fixedLLM({
          summary: "alice's update",
          outcome: null,
          salience: 0.5,
          facts: ["alice-new"],
        }),
      });
      await consolidator.distillThread(threadKey);

      // Alice over-cap → eviction.
      const aliceFacts = await store.listResourceFacts({
        namespaceId: "acme",
        resourceId: "alice",
      });
      expect(aliceFacts.map((f) => f.text)).toEqual(["alice-fact-1", "alice-fact-2", "alice-new"]);
      // Bob untouched — different resource.
      const bobFacts = await store.listResourceFacts({ namespaceId: "acme", resourceId: "bob" });
      expect(bobFacts).toHaveLength(3);
      expect(bobFacts.map((f) => f.text)).toEqual(["bob-fact-0", "bob-fact-1", "bob-fact-2"]);
    });

    it("cap is unbounded when maxResourceFacts is unset", async () => {
      const { store } = await setupResourceWithFacts(50);
      const threadKey = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
      await store.appendMessages(threadKey, [
        { role: "user", content: "yet another" },
        { role: "assistant", content: "ok" },
      ]);

      const consolidator = new DefaultConsolidator({
        store,
        // maxResourceFacts NOT set
        llm: fixedLLM({
          summary: "another",
          outcome: null,
          salience: 0.5,
          facts: ["new fact"],
        }),
      });
      await consolidator.distillThread(threadKey);
      const after = await store.listResourceFacts({ namespaceId: "acme", resourceId: "alice" });
      expect(after).toHaveLength(51);
    });
  });
});
