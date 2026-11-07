// ---------------------------------------------------------------------------
// Prompt customization: operator instructions are appended to the prompt of
// the call they name, and a full replacement is used as given.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  DEFAULT_COMPACT_PROMPT,
  DEFAULT_DISTILL_PROMPT,
  DEFAULT_RECONCILE_PROMPT,
  DefaultConsolidator,
  type DefaultConsolidatorConfig,
} from "../consolidator.ts";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";
import type { LLMProvider } from "../../llm-provider.ts";

const THREAD = { namespaceId: "acme", resourceId: "alice", threadId: "t1" };

async function systemPrompts(
  config: Partial<DefaultConsolidatorConfig>,
  run: "distill" | "compact",
): Promise<string[]> {
  const store = new InMemoryMemoryStore();
  const messages = Array.from({ length: 6 }, (_, i) => ({
    role: i % 2 === 0 ? ("user" as const) : ("assistant" as const),
    content: `message ${i} about Acme and Berlin`,
  }));
  await store.appendMessages(THREAD, messages);
  const seen: string[] = [];
  const llm: LLMProvider = {
    chat: async ({ messages: sent }) => {
      const system = String(sent[0]!.content);
      seen.push(system);
      return {
        content: system.includes("keep a user's saved facts consistent")
          ? '{"replaces":[]}'
          : JSON.stringify({ summary: "s", facts: ["lives in Berlin"] }),
        finishReason: "stop",
      };
    },
  };
  const consolidator = new DefaultConsolidator({
    store,
    llm,
    minDistillMessages: 1,
    ...config,
  });
  if (run === "distill") {
    await store.appendResourceFact(
      { namespaceId: "acme", resourceId: "alice" },
      { text: "lives in Paris", source: "model" },
    );
    await consolidator.distillThread(THREAD);
  } else {
    await consolidator.compactThread(THREAD, { keepRecent: 2 });
  }
  return seen;
}

describe("consolidator prompts", () => {
  it("appends instructions to each default prompt and leaves it alone without them", async () => {
    const plain = await systemPrompts({}, "distill");
    expect(plain).toEqual([DEFAULT_DISTILL_PROMPT, DEFAULT_RECONCILE_PROMPT]);

    const guided = await systemPrompts(
      {
        distillInstructions: "Keep matter names as entities.",
        reconcileInstructions: "Be strict.",
      },
      "distill",
    );
    expect(guided[0]!.startsWith(DEFAULT_DISTILL_PROMPT)).toBe(true);
    expect(guided[0]).toContain("Keep matter names as entities.");
    expect(guided[1]!.startsWith(DEFAULT_RECONCILE_PROMPT)).toBe(true);
    expect(guided[1]).toContain("Be strict.");

    const compact = await systemPrompts({ compactInstructions: "Keep ticket ids." }, "compact");
    expect(compact[0]!.startsWith(DEFAULT_COMPACT_PROMPT)).toBe(true);
    expect(compact[0]).toContain("Keep ticket ids.");
  });

  it("uses a replacement prompt as given, with instructions still appended to it", async () => {
    const replaced = await systemPrompts({ distillPrompt: "MY PROMPT" }, "distill");
    expect(replaced[0]).toBe("MY PROMPT");
    const both = await systemPrompts(
      { distillPrompt: "MY PROMPT", distillInstructions: "extra" },
      "distill",
    );
    expect(both[0]!.startsWith("MY PROMPT")).toBe(true);
    expect(both[0]).toContain("extra");
    expect(both[0]).not.toContain(DEFAULT_DISTILL_PROMPT);
  });

  it("treats blank instructions as none", async () => {
    const out = await systemPrompts({ distillInstructions: "   " }, "distill");
    expect(out[0]).toBe(DEFAULT_DISTILL_PROMPT);
  });
});
