import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import { tool } from "../tool.ts";
import { InMemoryMemoryIndex } from "../memory-index.ts";
import type { LLMProvider, LLMResponse } from "../llm-provider.ts";

function mockLLM(responses: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async () => {
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

async function makeSession(config: Parameters<typeof agentLoop>[0], sessionId = "s-1") {
  const { runner } = makeRunner();
  const session = await agentLoop(config).session({ runner, sessionId });
  return session;
}

describe("agentLoop session", () => {
  it("send returns the LLM answer", async () => {
    const session = await makeSession({
      name: "test",
      llm: mockLLM([{ content: "Hello!", finishReason: "stop" }]),
    });

    const answer = await session.send("hi");
    expect(answer).toBe("Hello!");
    session.close();
  });

  it("multi-turn conversation maintains context (LLM receives prior messages)", async () => {
    const captured: unknown[][] = [];

    const session = await makeSession({
      name: "multi-turn",
      llm: {
        chat: async (params) => {
          captured.push(params.messages);
          return { content: `turn-${captured.length}`, finishReason: "stop" };
        },
      },
    });

    await session.send("first");
    await session.send("second");
    session.close();

    // Second call should include the first user+assistant exchange
    const secondCallMessages = captured[1]!;
    expect(secondCallMessages.length).toBeGreaterThan(2);
    expect(secondCallMessages.some((m: any) => m.content === "first")).toBe(true);
  });

  it("executes a tool and returns the final answer", async () => {
    const echoTool = tool({
      name: "echo",
      description: "Echoes input",
      parameters: z.object({ text: z.string() }),
      execute: async ({ text }) => `echoed: ${text}`,
    });

    const session = await makeSession({
      name: "tool-session",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-1", name: "echo", input: { text: "ping" } }],
        },
        { content: "Done: echoed: ping", finishReason: "stop" },
      ]),
      tools: { echo: echoTool },
    });

    const answer = await session.send("echo ping");
    expect(answer).toBe("Done: echoed: ping");
    session.close();
  });

  it("returns tool error to LLM when input validation fails", async () => {
    const strictTool = tool({
      name: "strict",
      description: "Requires a prompt string",
      parameters: z.object({ prompt: z.string() }),
      execute: async ({ prompt }) => `result: ${prompt}`,
    });

    let toolResultContent = "";
    const session = await makeSession({
      name: "validation-error-session",
      llm: {
        chat: async (params) => {
          const toolResult = params.messages.find((m: any) => m.role === "tool");
          if (toolResult) {
            toolResultContent = (toolResult as any).content;
            return { content: "I see an error occurred.", finishReason: "stop" };
          }
          // LLM sends invalid input — missing required `prompt`
          return {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-bad", name: "strict", input: {} }],
          };
        },
      },
      tools: { strict: strictTool },
    });

    const answer = await session.send("call strict tool badly");
    expect(answer).toBe("I see an error occurred.");
    expect(toolResultContent).toContain("Invalid input");
    expect(toolResultContent).toContain("prompt");
    session.close();
  });

  it("returns tool error to LLM when execute throws", async () => {
    const faultyTool = tool({
      name: "faulty",
      description: "Always throws",
      parameters: z.object({ x: z.string() }),
      execute: async () => {
        throw new Error("upstream service unavailable");
      },
    });

    let toolResultContent = "";
    const session = await makeSession({
      name: "execute-error-session",
      llm: {
        chat: async (params) => {
          const toolResult = params.messages.find((m: any) => m.role === "tool");
          if (toolResult) {
            toolResultContent = (toolResult as any).content;
            return { content: "Tool failed.", finishReason: "stop" };
          }
          return {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-faulty", name: "faulty", input: { x: "anything" } }],
          };
        },
      },
      tools: { faulty: faultyTool },
    });

    const answer = await session.send("run faulty tool");
    expect(answer).toBe("Tool failed.");
    expect(toolResultContent).toContain("Tool execution failed");
    expect(toolResultContent).toContain("upstream service unavailable");
    session.close();
  });

  it("handles unknown tool gracefully", async () => {
    const session = await makeSession({
      name: "unknown-tool-session",
      llm: {
        chat: async (params) => {
          const hasToolResult = params.messages.some((m: any) => m.role === "tool");
          if (hasToolResult) return { content: "Could not use tool.", finishReason: "stop" };
          return {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "tc-1", name: "nonexistent", input: {} }],
          };
        },
      },
    });

    const answer = await session.send("use missing tool");
    expect(answer).toBe("Could not use tool.");
    session.close();
  });

  it("close prevents further sends", async () => {
    const session = await makeSession({
      name: "close-test",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
    });

    await session.send("first");
    session.close();

    await expect(session.send("second")).rejects.toThrow("closed");
  });

  it("send() rejects (not hangs) when the LLM throws", async () => {
    const session = await makeSession({
      name: "llm-throw-test",
      llm: {
        chat: async () => {
          throw new Error("LLM unavailable");
        },
      },
    });

    await expect(session.send("hello")).rejects.toThrow("LLM unavailable");
    session.close();
  });

  describe("context compaction", () => {
    it("triggers compaction when non-system messages exceed maxMessages", async () => {
      const calls: string[] = [];

      const session = await makeSession({
        name: "compact-test",
        llm: {
          chat: async (params) => {
            const systemMsg = params.messages.find((m: any) => m.role === "system");
            const isCompaction =
              typeof systemMsg?.content === "string" &&
              systemMsg.content.includes("Summarize the following");
            calls.push(isCompaction ? "compact" : "chat");
            if (isCompaction) return { content: "summary of earlier turns", finishReason: "stop" };
            return { content: "reply", finishReason: "stop" };
          },
        },
        context: { maxMessages: 4, keepMessages: 2, summarize: true },
      });

      // Each turn adds 2 non-system messages (user + assistant) → 3 turns = 6 messages → triggers
      await session.send("turn 1");
      await session.send("turn 2");
      await session.send("turn 3");

      expect(calls).toContain("compact");
      session.close();
    });

    it("injects summary as system message after compaction", async () => {
      let messagesAfterCompaction: unknown[] = [];
      let compactionDone = false;

      const session = await makeSession({
        name: "compact-inject-test",
        llm: {
          chat: async (params) => {
            const isCompaction = params.messages.some(
              (m: any) => m.role === "system" && m.content?.includes("Summarize the following"),
            );
            if (isCompaction) {
              compactionDone = true;
              return { content: "key facts: user asked about turns 1 and 2", finishReason: "stop" };
            }
            if (compactionDone) {
              messagesAfterCompaction = params.messages;
            }
            return { content: "reply", finishReason: "stop" };
          },
        },
        context: { maxMessages: 4, keepMessages: 2, summarize: true },
      });

      await session.send("turn 1");
      await session.send("turn 2");
      await session.send("turn 3"); // triggers compaction after emit
      await session.send("turn 4"); // first turn with compacted context

      const hasSummary = messagesAfterCompaction.some(
        (m: any) => m.role === "system" && m.content?.includes("Earlier conversation summary"),
      );
      expect(hasSummary).toBe(true);
      session.close();
    });
  });

  describe("memory integration", () => {
    it("injects memories from store at session start", async () => {
      const store = new InMemoryMemoryIndex();
      await store.save({ content: "user prefers concise answers" });

      let firstCallMessages: unknown[] = [];

      const session = await makeSession({
        name: "memory-inject-test",
        llm: {
          chat: async (params) => {
            if (firstCallMessages.length === 0) firstCallMessages = params.messages;
            return { content: "ok", finishReason: "stop" };
          },
        },
        memory: { store, injectLimit: 5, searchQuery: "user preferences" },
      });

      await session.send("hello");

      const hasMemory = firstCallMessages.some(
        (m: any) => m.role === "system" && m.content?.includes("user prefers concise answers"),
      );
      expect(hasMemory).toBe(true);
      session.close();
    });

    it("skips memory injection when store is empty", async () => {
      const store = new InMemoryMemoryIndex();
      let callCount = 0;

      const session = await makeSession({
        name: "empty-memory-test",
        llm: {
          chat: async () => {
            callCount++;
            return { content: "ok", finishReason: "stop" };
          },
        },
        memory: { store, injectLimit: 5 },
      });

      await session.send("hello");
      // Only the chat call should happen — no extra LLM call for memory search
      expect(callCount).toBe(1);
      session.close();
    });

    it("saves compaction summary to memory store", async () => {
      const store = new InMemoryMemoryIndex();

      const session = await makeSession({
        name: "memory-save-test",
        llm: {
          chat: async (params) => {
            const isCompaction = params.messages.some(
              (m: any) => m.role === "system" && m.content?.includes("Summarize the following"),
            );
            if (isCompaction) return { content: "compaction summary text", finishReason: "stop" };
            return { content: "reply", finishReason: "stop" };
          },
        },
        context: { maxMessages: 4, keepMessages: 2, summarize: true },
        memory: { store, saveOnCompact: true },
      });

      await session.send("turn 1");
      await session.send("turn 2");
      await session.send("turn 3"); // triggers compaction + save

      const memories = await store.list();
      expect(memories.some((m) => m.content === "compaction summary text")).toBe(true);
      session.close();
    });

    it("does not save when saveOnCompact is false", async () => {
      const store = new InMemoryMemoryIndex();

      const session = await makeSession({
        name: "no-save-test",
        llm: {
          chat: async (params) => {
            const isCompaction = params.messages.some(
              (m: any) => m.role === "system" && m.content?.includes("Summarize the following"),
            );
            if (isCompaction) return { content: "summary", finishReason: "stop" };
            return { content: "reply", finishReason: "stop" };
          },
        },
        context: { maxMessages: 4, keepMessages: 2, summarize: true },
        memory: { store, saveOnCompact: false },
      });

      await session.send("turn 1");
      await session.send("turn 2");
      await session.send("turn 3");

      expect(await store.list()).toHaveLength(0);
      session.close();
    });
  });

  describe("token-based RECAP compaction", () => {
    // Use compactionLlm as a separate mock so we can verify it was called
    // independently of the main LLM. keepMessages=1 ensures messages from
    // earlier turns are in the "dropped" window when RECAP fires.
    function makeRecapSession(opts: {
      name: string;
      mainTokens: number;
      compactionCalls: string[];
      store?: InMemoryMemoryIndex;
      saveOnCompact?: boolean;
    }) {
      const { name, mainTokens, compactionCalls } = opts;
      return makeSession({
        name,
        llm: {
          chat: async () => ({
            content: "reply",
            finishReason: "stop" as const,
            usage: { inputTokens: mainTokens, outputTokens: 50 },
          }),
        },
        compactionLlm: {
          chat: async () => {
            compactionCalls.push("compact");
            return { content: "recap summary text", finishReason: "stop" as const };
          },
        },
        context: { contextLimit: 1000, compressAt: 0.6, maxMessages: 100, keepMessages: 1 },
        memory:
          opts.store !== undefined
            ? { store: opts.store, saveOnCompact: opts.saveOnCompact ?? true }
            : undefined,
      });
    }

    it("fires a compress activity when inputTokens reaches contextLimit * compressAt", async () => {
      const compactionCalls: string[] = [];
      const session = await makeRecapSession({
        name: "recap-trigger",
        mainTokens: 700, // 700 >= 1000 * 0.6 = 600 → triggers
        compactionCalls,
      });

      // 2 warm-up turns to build history so compact() has messages to drop.
      await session.send("turn 1");
      await session.send("turn 2");
      // Third turn triggers RECAP; by now history=[user1,asst1,user2,asst2,user3] (5 msgs),
      // keepMessages=1 → drops first 4, calls compactionLlm.
      await session.send("turn 3");

      expect(compactionCalls.length).toBeGreaterThan(0);
      session.close();
    });

    it("does not compact when inputTokens is below contextLimit * compressAt", async () => {
      const compactionCalls: string[] = [];
      const session = await makeRecapSession({
        name: "recap-no-trigger",
        mainTokens: 300, // 300 < 600 → no trigger
        compactionCalls,
      });

      await session.send("turn 1");
      await session.send("turn 2");
      await session.send("turn 3");

      expect(compactionCalls).toHaveLength(0);
      session.close();
    });

    it("does not compact when contextLimit is not set even with high token counts", async () => {
      const compactionCalls: string[] = [];
      const session = await makeSession({
        name: "no-limit-test",
        llm: {
          chat: async () => ({
            content: "reply",
            finishReason: "stop" as const,
            usage: { inputTokens: 999_999, outputTokens: 50 },
          }),
        },
        compactionLlm: {
          chat: async () => {
            compactionCalls.push("compact");
            return { content: "recap", finishReason: "stop" as const };
          },
        },
        context: { keepMessages: 1, maxMessages: 100 },
      });

      await session.send("turn 1");
      await session.send("turn 2");
      await session.send("turn 3");

      expect(compactionCalls).toHaveLength(0);
      session.close();
    });

    it("saves recap summary to memory store when saveOnCompact is true", async () => {
      const store = new InMemoryMemoryIndex();
      const compactionCalls: string[] = [];
      const session = await makeRecapSession({
        name: "recap-memory",
        mainTokens: 700,
        compactionCalls,
        store,
        saveOnCompact: true,
      });

      await session.send("turn 1");
      await session.send("turn 2");
      await session.send("turn 3");

      const memories = await store.list();
      const recap = memories.find((m) => m.content === "recap summary text");
      expect(recap).toBeDefined();
      expect(recap!.metadata?.type).toBe("recap-summary");
      session.close();
    });

    it("does not save to memory when saveOnCompact is false", async () => {
      const store = new InMemoryMemoryIndex();
      const compactionCalls: string[] = [];
      const session = await makeRecapSession({
        name: "recap-no-save",
        mainTokens: 700,
        compactionCalls,
        store,
        saveOnCompact: false,
      });

      await session.send("turn 1");
      await session.send("turn 2");
      await session.send("turn 3");

      expect(await store.list()).toHaveLength(0);
      session.close();
    });

    it("uses a custom recapPrompt when provided", async () => {
      const capturedSystemPrompts: string[] = [];
      const session = await makeSession({
        name: "recap-custom-prompt",
        llm: {
          chat: async () => ({
            content: "reply",
            finishReason: "stop" as const,
            usage: { inputTokens: 700, outputTokens: 50 },
          }),
        },
        compactionLlm: {
          chat: async (params) => {
            const sys = params.messages.find((m: { role: string }) => m.role === "system");
            if (sys) capturedSystemPrompts.push((sys as { content: string }).content);
            return { content: "summary", finishReason: "stop" as const };
          },
        },
        context: {
          contextLimit: 1000,
          compressAt: 0.6,
          keepMessages: 1,
          maxMessages: 100,
          recapPrompt: "CUSTOM RECAP PROMPT",
        },
      });

      await session.send("turn 1");
      await session.send("turn 2");
      await session.send("turn 3");
      session.close();

      expect(capturedSystemPrompts.length).toBeGreaterThan(0);
      expect(capturedSystemPrompts.every((p) => p === "CUSTOM RECAP PROMPT")).toBe(true);
    });
  });
});
