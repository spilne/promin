import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop, MaxIterationsError } from "../agent-loop.ts";
import { tool } from "../tool.ts";
import type { LLMProvider, LLMResponse } from "../llm-provider.ts";
import type { LLMChatParams } from "../llm-provider.ts";

function mockLLM(responses: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async (_params: LLMChatParams): Promise<LLMResponse> => {
      const resp = responses[i++];
      if (!resp) throw new Error("Mock LLM exhausted responses");
      return resp;
    },
  };
}

function makeRunner() {
  const storage = new InMemoryWorkflowStorage();
  return { storage, runner: createWorkflowRunner({ storage }) };
}

describe("agentLoop", () => {
  it("returns final answer when LLM stops immediately", async () => {
    const agent = agentLoop({
      name: "test-agent",
      llm: mockLLM([{ content: "The answer is 42.", finishReason: "stop" }]),
    });

    const { runner } = makeRunner();
    const handle = await runner.start({
      workflow: agent,
      workflowId: "w-1",
      input: { task: "What is 6 * 7?" },
    });
    const result = await handle.result({ timeoutMs: 5_000 });

    expect(result.answer).toBe("The answer is 42.");
    expect(result.iterations).toBe(1);
  });

  it("executes tools then returns final answer", async () => {
    const searchTool = tool({
      description: "Search the web",
      parameters: z.object({ query: z.string() }),
      execute: async ({ query }) => `Results for: ${query}`,
    });

    const agent = agentLoop({
      name: "tool-agent",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-1", name: "search", input: { query: "TypeScript" } }],
        },
        { content: "TypeScript is great.", finishReason: "stop" },
      ]),
      tools: { search: searchTool },
    });

    const { runner } = makeRunner();
    const handle = await runner.start({
      workflow: agent,
      workflowId: "w-2",
      input: { task: "Tell me about TypeScript" },
    });
    const result = await handle.result({ timeoutMs: 5_000 });

    expect(result.answer).toBe("TypeScript is great.");
    expect(result.iterations).toBe(2);

    const toolMsg = result.messages.find((m) => m.role === "tool");
    expect(toolMsg?.content).toBe("Results for: TypeScript");
  });

  it("validates tool input with Zod schema", async () => {
    const strictTool = tool({
      description: "A strict tool",
      parameters: z.object({ count: z.number().int().positive() }),
      execute: async ({ count }) => `count=${count}`,
    });

    const agent = agentLoop({
      name: "strict-agent",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-1", name: "strictTool", input: { count: "not-a-number" } }],
        },
      ]),
      tools: { strictTool: strictTool },
    });

    const { runner } = makeRunner();
    const { data, error } = await runner.runSafe({
      workflow: agent,
      workflowId: "w-3",
      input: { task: "run it" },
    });

    expect(data).toBeNull();
    expect(error).not.toBeNull();
  });

  it("handles unknown tool gracefully", async () => {
    const agent = agentLoop({
      name: "unknown-tool-agent",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-1", name: "nonexistent", input: {} }],
        },
        { content: "I couldn't use that tool.", finishReason: "stop" },
      ]),
    });

    const { runner } = makeRunner();
    const handle = await runner.start({
      workflow: agent,
      workflowId: "w-4",
      input: { task: "use a tool" },
    });
    const result = await handle.result({ timeoutMs: 5_000 });

    const toolMsg = result.messages.find((m) => m.role === "tool");
    expect(toolMsg?.content).toContain("unknown tool");
  });

  it("throws MaxIterationsError when loop exceeds limit", async () => {
    const infiniteResponses: LLMResponse[] = Array.from({ length: 5 }, (_, i) => ({
      content: null,
      finishReason: "tool_calls" as const,
      toolCalls: [{ id: `tc-${i}`, name: "search", input: { query: "loop" } }],
    }));

    const searchTool = tool({
      description: "Search",
      parameters: z.object({ query: z.string() }),
      execute: async () => "still searching...",
    });

    const agent = agentLoop({
      name: "infinite-agent",
      llm: mockLLM(infiniteResponses),
      tools: { search: searchTool },
      maxIterations: 3,
    });

    const { runner } = makeRunner();
    const { data, error } = await runner.runSafe({
      workflow: agent,
      workflowId: "w-5",
      input: { task: "loop forever" },
    });

    expect(data).toBeNull();
    expect(String(error)).toContain("maximum iterations");
  });

  it("stops early via onIteration hook", async () => {
    const agent = agentLoop({
      name: "hookable-agent",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-1", name: "search", input: { query: "q" } }],
        },
        { content: "Done.", finishReason: "stop" },
      ]),
      tools: {
        search: tool({
          description: "Search",
          parameters: z.object({ query: z.string() }),
          execute: async () => "result",
        }),
      },
      onIteration: ({ iteration }) => {
        if (iteration === 0) return { continue: false };
      },
    });

    const { runner } = makeRunner();
    const handle = await runner.start({
      workflow: agent,
      workflowId: "w-6",
      input: { task: "search something" },
    });
    const result = await handle.result({ timeoutMs: 5_000 });

    expect(result.iterations).toBe(1);
  });

  it("seeds conversation with prior messages", async () => {
    let capturedMessages: unknown[] = [];

    const agent = agentLoop({
      name: "seeded-agent",
      llm: {
        chat: async (params) => {
          capturedMessages = params.messages;
          return { content: "Continuing.", finishReason: "stop" };
        },
      },
    });

    const { runner } = makeRunner();
    await runner.start({
      workflow: agent,
      workflowId: "w-7",
      input: {
        task: "continue",
        messages: [
          { role: "user", content: "Hello" },
          { role: "assistant", content: "Hi there!", toolCalls: undefined },
        ],
      },
    });

    expect(capturedMessages).toHaveLength(3);
    expect((capturedMessages[0] as { role: string }).role).toBe("user");
    expect((capturedMessages[1] as { role: string }).role).toBe("assistant");
    expect((capturedMessages[2] as { role: string; content: string }).content).toBe("continue");
  });
});
