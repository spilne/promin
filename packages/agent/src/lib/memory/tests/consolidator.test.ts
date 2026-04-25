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
});
