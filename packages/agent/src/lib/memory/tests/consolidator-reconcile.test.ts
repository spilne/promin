// ---------------------------------------------------------------------------
// distillThread retires model facts that a newly distilled fact contradicts.
// The reconcile call is best effort and can only name facts it was shown.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { DefaultConsolidator } from "../consolidator.ts";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";

const THREAD = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };
const RESOURCE = { namespaceId: "acme", resourceId: "alice" };

/** Answers the distill call with `facts` and the reconcile call with `reconcile`. */
function scriptedLLM(params: { facts: string[]; reconcile: () => string }): {
  llm: LLMProvider;
  reconcileCalls: string[];
} {
  const reconcileCalls: string[] = [];
  const reply = (content: string): LLMResponse => ({ content, finishReason: "stop" });
  return {
    reconcileCalls,
    llm: {
      chat: async ({ messages }) => {
        const system = typeof messages[0]!.content === "string" ? messages[0]!.content : "";
        if (system.includes("keep a user's saved facts consistent")) {
          reconcileCalls.push(String(messages[1]!.content));
          return reply(params.reconcile());
        }
        return reply(
          JSON.stringify({ summary: "moved", outcome: null, salience: 0.8, facts: params.facts }),
        );
      },
    },
  };
}

async function setup(params: {
  facts: string[];
  reconcile: () => string;
  reconcileFacts?: boolean;
}) {
  const store = new InMemoryMemoryStore();
  await store.appendMessages(THREAD, [
    { role: "user", content: "I moved to Lisbon" },
    { role: "assistant", content: "Congrats." },
  ]);
  const { llm, reconcileCalls } = scriptedLLM(params);
  const consolidator = new DefaultConsolidator({
    store,
    llm,
    minDistillMessages: 1,
    ...(params.reconcileFacts !== undefined && { reconcileFacts: params.reconcileFacts }),
  });
  return { store, consolidator, reconcileCalls };
}

const currentTexts = async (store: InMemoryMemoryStore) =>
  (await store.listResourceFacts(RESOURCE)).map((f) => f.text);

describe("distillThread fact reconciliation", () => {
  it("retires the contradicted model fact, leaves user facts alone, and ignores numbers it was not shown", async () => {
    const { store, consolidator, reconcileCalls } = await setup({
      facts: ["lives in Lisbon", "has a dog"],
      // Candidates are the model facts only: 1 = Berlin, 2 = Python. 3 is the user fact and is not offered.
      reconcile: () =>
        JSON.stringify({
          replaces: [
            { new: 1, old: 1 },
            { new: 2, old: 3 },
            { new: 9, old: 2 },
          ],
        }),
    });
    await store.appendResourceFact(RESOURCE, { text: "lives in Berlin", source: "model" });
    await store.appendResourceFact(RESOURCE, { text: "writes Python", source: "model" });
    await store.appendResourceFact(RESOURCE, { text: "is vegetarian", source: "user" });

    await consolidator.distillThread(THREAD);

    expect(await currentTexts(store)).toEqual([
      "writes Python",
      "is vegetarian",
      "lives in Lisbon",
      "has a dog",
    ]);
    expect(reconcileCalls[0]).not.toContain("vegetarian");
    const history = await store.listResourceFacts(RESOURCE, { includeRetired: true });
    expect(history.filter((f) => f.retiredAt !== null).map((f) => f.text)).toEqual([
      "lives in Berlin",
    ]);
  });

  it("appends the new facts unchanged when the reconcile answer is junk or the call fails", async () => {
    for (const reconcile of [
      () => "not json",
      () => {
        throw new Error("provider down");
      },
    ]) {
      const { store, consolidator } = await setup({ facts: ["lives in Lisbon"], reconcile });
      await store.appendResourceFact(RESOURCE, { text: "lives in Berlin", source: "model" });

      await consolidator.distillThread(THREAD);

      expect(await currentTexts(store)).toEqual(["lives in Berlin", "lives in Lisbon"]);
    }
  });

  it("makes no reconcile call when disabled, when no model fact exists, or when nothing is new", async () => {
    const off = await setup({
      facts: ["lives in Lisbon"],
      reconcile: () => "{}",
      reconcileFacts: false,
    });
    await off.store.appendResourceFact(RESOURCE, { text: "lives in Berlin", source: "model" });
    await off.consolidator.distillThread(THREAD);

    const noModelFacts = await setup({ facts: ["lives in Lisbon"], reconcile: () => "{}" });
    await noModelFacts.store.appendResourceFact(RESOURCE, {
      text: "is vegetarian",
      source: "user",
    });
    await noModelFacts.consolidator.distillThread(THREAD);

    const duplicate = await setup({ facts: ["Lives in Berlin."], reconcile: () => "{}" });
    await duplicate.store.appendResourceFact(RESOURCE, {
      text: "lives in Berlin",
      source: "model",
    });
    await duplicate.consolidator.distillThread(THREAD);

    expect(off.reconcileCalls).toEqual([]);
    expect(noModelFacts.reconcileCalls).toEqual([]);
    expect(duplicate.reconcileCalls).toEqual([]);
  });

  it("shows the model only the newest 100 model facts, numbered oldest to newest", async () => {
    const { store, consolidator, reconcileCalls } = await setup({
      facts: ["lives in Lisbon"],
      reconcile: () => JSON.stringify({ replaces: [{ new: 1, old: 100 }] }),
    });
    for (let i = 0; i < 105; i++) {
      await store.appendResourceFact(RESOURCE, { text: `saved fact ${i}`, source: "model" });
    }

    await consolidator.distillThread(THREAD);

    const prompt = reconcileCalls[0]!;
    expect(prompt).not.toContain("saved fact 4\n");
    expect(prompt).toMatch(/\b1\. saved fact 5\b/);
    expect(prompt).toMatch(/\b100\. saved fact 104\b/);
    // Number 100 is the newest shown, so that is the fact it retires.
    const texts = await currentTexts(store);
    expect(texts).not.toContain("saved fact 104");
    expect(texts).toContain("saved fact 103");
    expect(texts).toContain("lives in Lisbon");
  });
});
