import { describe, expect, it } from "bun:test";
import { isToolResultStep, routerLLM, twoSpeedLLM } from "../router-llm.ts";
import type { LLMChatParams, LLMProvider, LLMStreamChunk } from "../../llm-provider.ts";

function named(name: string): LLMProvider {
  return {
    chat: async () => ({
      content: name,
      finishReason: "stop",
      thinkingBlocks: [{ thinking: `${name} thought`, signature: "sig" }],
    }),
  };
}

const userTurn: LLMChatParams = {
  messages: [
    { role: "system", content: "sys" },
    { role: "user", content: "hi" },
  ],
};
const toolTurn: LLMChatParams = {
  messages: [
    { role: "user", content: "hi" },
    { role: "assistant", content: null, toolCalls: [{ id: "c1", name: "t", input: {} }] },
    { role: "tool", toolCallId: "c1", content: "result" },
    { role: "system", content: "late system note" },
  ],
};

describe("routerLLM", () => {
  it("routes to the first matching route, else the last one", async () => {
    const llm = routerLLM([
      { when: (p) => p.messages.length > 3, use: named("long") },
      { when: () => false, use: named("never") },
      { when: () => false, use: named("last") },
    ]);
    expect((await llm.chat(toolTurn)).content).toBe("long");
    expect((await llm.chat(userTurn)).content).toBe("last");
  });

  it("streams over a chat-only provider without losing thinking blocks", async () => {
    const llm = routerLLM([{ when: () => true, use: named("only") }]);
    const chunks: LLMStreamChunk[] = [];
    for await (const c of llm.chatStream!(userTurn)) chunks.push(c);
    expect(chunks.map((c) => c.delta).join("")).toBe("only");
    expect(chunks.at(-1)?.thinkingBlocks).toEqual([{ thinking: "only thought", signature: "sig" }]);
  });
});

describe("twoSpeedLLM", () => {
  it("isToolResultStep looks at the last non-system message", () => {
    expect(isToolResultStep(toolTurn)).toBe(true);
    expect(isToolResultStep(userTurn)).toBe(false);
    expect(isToolResultStep({ messages: [] })).toBe(false);
  });

  it("sends tool-result steps to fast and everything else to capable", async () => {
    const llm = twoSpeedLLM({ fast: named("fast"), capable: named("capable") });
    expect((await llm.chat(toolTurn)).content).toBe("fast");
    expect((await llm.chat(userTurn)).content).toBe("capable");
  });

  it("honours a custom `when`", async () => {
    const llm = twoSpeedLLM({ fast: named("fast"), capable: named("capable"), when: () => true });
    expect((await llm.chat(userTurn)).content).toBe("fast");
  });
});
