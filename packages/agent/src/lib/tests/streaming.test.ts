import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalAgent } from "../agent/local-agent.ts";
import type { AgentEngineConfig } from "../agent-action.ts";
import { tool } from "../tool.ts";
import type { LLMProvider, LLMResponse, LLMStreamChunk, LLMFinishReason } from "../llm-provider.ts";
import type { ToolCall, ThinkingBlock } from "../message.ts";

// ---------------------------------------------------------------------------
// Mock helpers
// ---------------------------------------------------------------------------

interface StreamingTurn {
  chunks?: string[];
  thinkingChunks?: string[];
  thinkingBlocks?: ThinkingBlock[];
  toolCalls?: ToolCall[];
  finishReason?: LLMFinishReason;
}

function mockStreamingLLM(turns: StreamingTurn[]): LLMProvider {
  let callIndex = 0;
  return {
    chat: async () => {
      throw new Error("chat() should not be called when chatStream is present");
    },
    chatStream(_params) {
      const turn = turns[callIndex++];
      if (!turn) throw new Error("mockStreamingLLM: exhausted turns");
      return {
        [Symbol.asyncIterator](): AsyncIterator<LLMStreamChunk> {
          const thinkingChunks = turn.thinkingChunks ?? [];
          const textChunks = turn.chunks ?? [];
          let phase: "thinking" | "text" | "final" = thinkingChunks.length ? "thinking" : "text";
          let pos = 0;
          return {
            async next(): Promise<IteratorResult<LLMStreamChunk>> {
              if (phase === "thinking" && pos < thinkingChunks.length) {
                return { value: { delta: "", thinkingDelta: thinkingChunks[pos++]! }, done: false };
              }
              if (phase === "thinking") {
                phase = "text";
                pos = 0;
              }
              if (phase === "text" && pos < textChunks.length) {
                return { value: { delta: textChunks[pos++]! }, done: false };
              }
              if (phase !== "final") {
                phase = "final";
                return {
                  value: {
                    delta: "",
                    toolCalls: turn.toolCalls,
                    thinkingBlocks: turn.thinkingBlocks,
                    finishReason:
                      turn.finishReason ?? (turn.toolCalls?.length ? "tool_calls" : "stop"),
                  },
                  done: false,
                };
              }
              return { value: undefined as never, done: true };
            },
          };
        },
      };
    },
  };
}

function mockPlainLLM(responses: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async () => {
      const r = responses[i++];
      if (!r) throw new Error("mockPlainLLM: exhausted responses");
      return r;
    },
  };
}

/** A thread of a LocalAgent running `config`. */
async function makeThread(config: AgentEngineConfig) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const agent = new LocalAgent({ agent: config, runner, namespaceId: "ns" });
  return agent.thread(`t-${Math.random()}`);
}

async function collectStream(iter: AsyncIterable<string>): Promise<string[]> {
  const chunks: string[] = [];
  for await (const chunk of iter) {
    chunks.push(chunk);
  }
  return chunks;
}

// ---------------------------------------------------------------------------
// thread.stream() — with chatStream
// ---------------------------------------------------------------------------

describe("thread.stream() — with chatStream", () => {
  it("yields deltas in order and terminates", async () => {
    const thread = await makeThread({
      name: "stream-basic",
      llm: mockStreamingLLM([{ chunks: ["Hello", " world"], finishReason: "stop" }]),
    });

    const chunks = await collectStream(thread.stream({ task: "hi" }).textStream);

    expect(chunks).toEqual(["Hello", " world"]);
  });

  it("works across a multi-step turn (tool call → final text)", async () => {
    const echoTool = tool({
      name: "echo",
      description: "Echo the input",
      parameters: z.object({ text: z.string() }),
      execute: async ({ text }) => text,
    });

    const thread = await makeThread({
      name: "stream-multistep",
      llm: mockStreamingLLM([
        // Step 1: tool call only — no text deltas
        {
          chunks: [],
          toolCalls: [{ id: "tc-1", name: "echo", input: { text: "ping" } }],
          finishReason: "tool_calls",
        },
        // Step 2: final text answer
        {
          chunks: ["done"],
          finishReason: "stop",
        },
      ]),
      tools: { echo: echoTool },
    });

    const chunks = await collectStream(thread.stream({ task: "go" }).textStream);

    expect(chunks).toEqual(["done"]);
  });

  it("stream ends after the turn; second turn yields its own deltas independently", async () => {
    const thread = await makeThread({
      name: "stream-two-turns",
      llm: mockStreamingLLM([
        { chunks: ["turn1"], finishReason: "stop" },
        { chunks: ["turn2"], finishReason: "stop" },
      ]),
    });

    const firstChunks = await collectStream(thread.stream({ task: "first" }).textStream);
    const secondChunks = await collectStream(thread.stream({ task: "second" }).textStream);

    expect(firstChunks).toEqual(["turn1"]);
    expect(secondChunks).toEqual(["turn2"]);
  });
});

// ---------------------------------------------------------------------------
// thread.stream() — fallback without chatStream
// ---------------------------------------------------------------------------

describe("thread.stream() — fallback without chatStream", () => {
  it("emits full content as a single chunk when chatStream is absent", async () => {
    const thread = await makeThread({
      name: "stream-fallback",
      llm: mockPlainLLM([{ content: "The full answer.", finishReason: "stop" }]),
    });

    const chunks = await collectStream(thread.stream({ task: "tell me something" }).textStream);

    expect(chunks).toEqual(["The full answer."]);
  });
});

// ---------------------------------------------------------------------------
// thread.stream() — extended thinking via onThinking callback
// ---------------------------------------------------------------------------

describe("thread.stream() — extended thinking", () => {
  it("delivers thinking deltas to onThinking and text deltas to the stream", async () => {
    const thinkingDeltas: string[] = [];
    const thread = await makeThread({
      name: "stream-thinking",
      onThinking: (delta) => thinkingDeltas.push(delta),
      llm: mockStreamingLLM([
        {
          thinkingChunks: ["Let me", " think..."],
          thinkingBlocks: [{ thinking: "Let me think...", signature: "sig-abc" }],
          chunks: ["The answer"],
          finishReason: "stop",
        },
      ]),
    });

    const textChunks = await collectStream(thread.stream({ task: "solve this" }).textStream);

    expect(textChunks).toEqual(["The answer"]);
    expect(thinkingDeltas).toEqual(["Let me", " think..."]);
  });

  it("stores thinking blocks on the assistant message in history", async () => {
    const thread = await makeThread({
      name: "stream-thinking-history",
      llm: mockStreamingLLM([
        {
          thinkingChunks: ["reasoning"],
          thinkingBlocks: [{ thinking: "reasoning", signature: "sig-xyz" }],
          chunks: ["answer"],
          finishReason: "stop",
        },
      ]),
    });

    await collectStream(thread.stream({ task: "question" }).textStream);
    const msgs = await thread.messages();

    const assistant = msgs.find((m) => m.role === "assistant");
    expect(assistant?.thinkingBlocks).toEqual([{ thinking: "reasoning", signature: "sig-xyz" }]);
  });

  it("an unaborted signal leaves the stream untouched", async () => {
    const thread = await makeThread({
      name: "stream-thinking-signal",
      llm: mockStreamingLLM([{ chunks: ["ok"], finishReason: "stop" }]),
    });

    const ac = new AbortController();
    const chunks = await collectStream(
      thread.stream({ task: "hi" }, { signal: ac.signal }).textStream,
    );

    expect(chunks).toEqual(["ok"]);
  });
});
