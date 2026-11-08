// ---------------------------------------------------------------------------
// distillThread keeps the confidence the model states for each fact and
// writes the relations it extracts to the resource's entity graph.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { DefaultConsolidator } from "../consolidator.ts";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";
import { MAX_RELATIONS_PER_RESOURCE } from "../relations.ts";
import type { LLMProvider } from "../../llm-provider.ts";

const RESOURCE = { namespaceId: "acme", resourceId: "alice" };
const THREAD = { ...RESOURCE, threadId: "t1" };

async function distill(params: { reply: Record<string, unknown>; extractRelations?: boolean }) {
  const store = new InMemoryMemoryStore();
  await store.appendMessages(THREAD, [
    { role: "user", content: "I work at Acme in Berlin" },
    { role: "assistant", content: "Nice." },
  ]);
  const llm: LLMProvider = {
    chat: async () => ({
      content: JSON.stringify({ summary: "chat", outcome: null, salience: 0.6, ...params.reply }),
      finishReason: "stop",
    }),
  };
  await new DefaultConsolidator({
    store,
    llm,
    minDistillMessages: 1,
    reconcileFacts: false,
    ...(params.extractRelations !== undefined && { extractRelations: params.extractRelations }),
  }).distillThread(THREAD);
  return store;
}

describe("distilled fact confidence", () => {
  it("stores the stated confidence (capped for the model) and defaults the rest", async () => {
    const store = await distill({
      reply: {
        facts: [
          { text: "works at Acme", confidence: 0.95 },
          { text: "probably likes jazz", confidence: 0.3 },
          { text: "lives in Berlin" },
          "drinks tea",
          { text: "bad confidence", confidence: "high" },
        ],
      },
    });
    const byText = new Map(
      (await store.listResourceFacts(RESOURCE)).map((f) => [f.text, f.confidence]),
    );
    expect(byText.get("works at Acme")).toBe(0.9);
    expect(byText.get("probably likes jazz")).toBe(0.3);
    expect(byText.get("lives in Berlin")).toBe(0.6);
    expect(byText.get("drinks tea")).toBe(0.6);
    expect(byText.get("bad confidence")).toBe(0.6);
  });
});

describe("distilled relations", () => {
  it("writes valid relations as model relations and drops malformed ones", async () => {
    const store = await distill({
      reply: {
        relations: [
          { subject: "Anton", predicate: "works at", object: "Acme", confidence: 0.8 },
          { subject: "anton", predicate: "Works At", object: "ACME" },
          { subject: "Acme", predicate: "is based in", object: "Berlin" },
          { subject: "Acme", predicate: "is", object: "acme" },
          { subject: "", predicate: "knows", object: "Bob" },
          { subject: "x".repeat(200), predicate: "knows", object: "Bob" },
          { subject: "Anton", predicate: 7, object: "Bob" },
          "not an object",
        ],
      },
    });
    const relations = await store.listResourceRelations(RESOURCE);
    expect(relations.map((r) => `${r.subject}|${r.predicate}|${r.object}|${r.source}`)).toEqual([
      "Anton|works at|Acme|model",
      "Acme|is based in|Berlin|model",
    ]);
    expect(relations[0]!.confidence).toBe(0.8);
  });

  it("can be switched off, and never grows a resource past the cap", async () => {
    const reply = { relations: [{ subject: "A", predicate: "knows", object: "B" }] };
    const off = await distill({ reply, extractRelations: false });
    expect(await off.listResourceRelations(RESOURCE)).toEqual([]);

    const store = new InMemoryMemoryStore();
    for (let i = 0; i < MAX_RELATIONS_PER_RESOURCE; i++) {
      await store.appendResourceRelation(RESOURCE, {
        subject: `s${i}`,
        predicate: "knows",
        object: `o${i}`,
        source: "user",
      });
    }
    await store.appendMessages(THREAD, [
      { role: "user", content: "hello there" },
      { role: "assistant", content: "hi" },
    ]);
    await new DefaultConsolidator({
      store,
      llm: {
        chat: async () => ({
          content: JSON.stringify({
            summary: "s",
            relations: [
              { subject: "new", predicate: "knows", object: "thing" },
              { subject: "s0", predicate: "knows", object: "o0" },
            ],
          }),
          finishReason: "stop",
        }),
      },
      minDistillMessages: 1,
    }).distillThread(THREAD);
    const after = await store.listResourceRelations(RESOURCE);
    expect(after).toHaveLength(MAX_RELATIONS_PER_RESOURCE);
    expect(after.some((r) => r.subject === "new")).toBe(false);
  });
});
