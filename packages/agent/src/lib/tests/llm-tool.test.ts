import { describe, it, expect } from "bun:test";
import { createLlmTool } from "../tools/llm-tool.ts";
import type { LLMProvider, LLMResponse } from "../llm-provider.ts";

function makeLlm(response: Partial<LLMResponse> = {}): LLMProvider & { calls: { messages: unknown[] }[] } {
  const calls: { messages: unknown[] }[] = [];
  return {
    calls,
    async chat(params) {
      calls.push({ messages: params.messages });
      return { content: "mock response", finishReason: "stop", ...response };
    },
  };
}

describe("createLlmTool", () => {
  it("calls the sub-LLM with the provided prompt as a user message", async () => {
    const llm = makeLlm();
    const t = createLlmTool({ name: "test", description: "d", llm });
    await t.execute({ prompt: "hello from main" });
    expect(llm.calls).toHaveLength(1);
    const msgs = llm.calls[0]!.messages as Array<{ role: string; content: string }>;
    expect(msgs[msgs.length - 1]).toEqual({ role: "user", content: "hello from main" });
  });

  it("returns the sub-LLM content as the tool result", async () => {
    const llm = makeLlm({ content: "sub-llm answer" });
    const t = createLlmTool({ name: "test", description: "d", llm });
    expect(await t.execute({ prompt: "q" })).toBe("sub-llm answer");
  });

  it("prepends systemPrompt when provided", async () => {
    const llm = makeLlm();
    const t = createLlmTool({ name: "test", description: "d", llm, systemPrompt: "You are a summariser." });
    await t.execute({ prompt: "summarise this" });
    const msgs = llm.calls[0]!.messages as Array<{ role: string; content: string }>;
    expect(msgs[0]).toEqual({ role: "system", content: "You are a summariser." });
    expect(msgs[1]).toEqual({ role: "user", content: "summarise this" });
  });

  it("omits system message when systemPrompt is not set", async () => {
    const llm = makeLlm();
    const t = createLlmTool({ name: "test", description: "d", llm });
    await t.execute({ prompt: "hi" });
    const msgs = llm.calls[0]!.messages as Array<{ role: string }>;
    expect(msgs).toHaveLength(1);
    expect(msgs[0]!.role).toBe("user");
  });

  it("returns empty string when sub-LLM content is null", async () => {
    const llm = makeLlm({ content: null });
    const t = createLlmTool({ name: "test", description: "d", llm });
    expect(await t.execute({ prompt: "tool-only response" })).toBe("");
  });

  it("tool name matches config", () => {
    const t = createLlmTool({ name: "cheapSummarize", llm: makeLlm() });
    expect(t.name).toBe("cheapSummarize");
  });

  it("does not set requireApproval", () => {
    const t = createLlmTool({ llm: makeLlm() });
    expect(t.requireApproval).toBeFalsy();
  });
});

describe("createLlmTool — simple overload", () => {
  it("accepts a bare LLMProvider and defaults name to 'llm'", () => {
    const t = createLlmTool(makeLlm());
    expect(t.name).toBe("llm");
  });

  it("accepts a bare provider with options override", () => {
    const t = createLlmTool(makeLlm(), { name: "deepReason", description: "Hard logic." });
    expect(t.name).toBe("deepReason");
  });

  it("calls the provider correctly via simple overload", async () => {
    const llm = makeLlm({ content: "42" });
    const t = createLlmTool(llm);
    const result = await t.execute({ prompt: "what is 6x7?" });
    expect(result).toBe("42");
    expect(llm.calls).toHaveLength(1);
  });

  it("has a non-empty default description", () => {
    const t = createLlmTool(makeLlm());
    expect(t.description.length).toBeGreaterThan(10);
  });
});
