import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentAction } from "../agent-action.ts";
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

describe("agentAction", () => {
  it("returns final answer when LLM stops immediately", async () => {
    const agent = agentAction({
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
    expect(result.steps).toBe(1);
  });

  it("executes tools then returns final answer", async () => {
    const searchTool = tool({
      name: "search",
      description: "Search the web",
      parameters: z.object({ query: z.string() }),
      execute: async ({ query }) => `Results for: ${query}`,
    });

    const agent = agentAction({
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
    expect(result.steps).toBe(2);

    const toolMsg = result.messages.find((m) => m.role === "tool");
    expect(toolMsg?.content).toBe("Results for: TypeScript");
  });

  it("validates tool input with Zod schema — parse error is fed back to LLM as tool result", async () => {
    const strictTool = tool({
      name: "strictTool",
      description: "A strict tool",
      parameters: z.object({ count: z.number().int().positive() }),
      execute: async ({ count }) => `count=${count}`,
    });

    let toolResultContent = "";
    const agent = agentAction({
      name: "strict-agent",
      llm: {
        chat: async (params: LLMChatParams) => {
          const toolResult = params.messages.find((m) => m.role === "tool");
          if (toolResult) {
            toolResultContent = (toolResult as { content: string }).content;
            return { content: "Got an error.", finishReason: "stop" as const };
          }
          return {
            content: null,
            finishReason: "tool_calls" as const,
            toolCalls: [{ id: "tc-1", name: "strictTool", input: { count: "not-a-number" } }],
          };
        },
      },
      tools: { strictTool: strictTool },
    });

    const { runner } = makeRunner();
    const handle = await runner.start({
      workflow: agent,
      workflowId: "w-3",
      input: { task: "run it" },
    });
    const result = await handle.result({ timeoutMs: 5_000 });

    expect(result.answer).toBe("Got an error.");
    expect(toolResultContent).toContain("Invalid input");
    expect(toolResultContent).toContain("count");
  });

  it("handles unknown tool gracefully", async () => {
    const agent = agentAction({
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

  it("throws MaxStepsError when loop exceeds limit", async () => {
    const infiniteResponses: LLMResponse[] = Array.from({ length: 5 }, (_, i) => ({
      content: null,
      finishReason: "tool_calls" as const,
      toolCalls: [{ id: `tc-${i}`, name: "search", input: { query: "loop" } }],
    }));

    const searchTool = tool({
      name: "search",
      description: "Search",
      parameters: z.object({ query: z.string() }),
      execute: async () => "still searching...",
    });

    const agent = agentAction({
      name: "infinite-agent",
      llm: mockLLM(infiniteResponses),
      tools: { search: searchTool },
      maxSteps: 3,
    });

    const { runner } = makeRunner();
    const { data, error } = await runner.runSafe({
      workflow: agent,
      workflowId: "w-5",
      input: { task: "loop forever" },
    });

    expect(data).toBeNull();
    expect(String(error)).toContain("maximum steps");
  });

  it("stops early via onStep hook", async () => {
    const agent = agentAction({
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
          name: "search",
          description: "Search",
          parameters: z.object({ query: z.string() }),
          execute: async () => "result",
        }),
      },
      onStep: ({ step }) => {
        if (step === 0) return { continue: false };
      },
    });

    const { runner } = makeRunner();
    const handle = await runner.start({
      workflow: agent,
      workflowId: "w-6",
      input: { task: "search something" },
    });
    const result = await handle.result({ timeoutMs: 5_000 });

    expect(result.steps).toBe(1);
  });

  it("onStep receives isReplay=false on a fresh run (plumbing check)", async () => {
    const seen: boolean[] = [];
    const agent = agentAction({
      name: "isreplay-agent",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      onStep: ({ isReplay }) => {
        seen.push(isReplay);
      },
    });

    const { runner } = makeRunner();
    await runner.start({
      workflow: agent,
      workflowId: "w-isreplay",
      input: { task: "go" },
    });

    // Plumbing check: the flag arrives as `false` on the very first body
    // pass. Replay-mode behaviour (true after a body re-execution) is
    // covered at the workflow-runner level in journaled-step.test.ts —
    // agentAction is one-shot so the runner doesn't re-execute the body
    // once it completes.
    expect(seen).toEqual([false]);
  });

  it("processors.beforeLLM ctx.isReplay is plumbed (false on fresh run)", async () => {
    const replayValuesSeen: boolean[] = [];
    const agent = agentAction({
      name: "isreplay-procs",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      processors: {
        beforeLLM: (msgs, ctx) => {
          replayValuesSeen.push(ctx.isReplay);
          return msgs;
        },
      },
    });

    const { runner } = makeRunner();
    await runner.start({
      workflow: agent,
      workflowId: "w-isreplay-procs",
      input: { task: "go" },
    });

    expect(replayValuesSeen).toEqual([false]);
  });

  it("seeds conversation with prior messages", async () => {
    let capturedMessages: unknown[] = [];

    const agent = agentAction({
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
