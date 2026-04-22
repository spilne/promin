import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import { broadcast } from "../broadcast.ts";
import { tool } from "../tool.ts";
import type { LLMProvider, LLMResponse, LLMStreamChunk, LLMFinishReason } from "../llm-provider.ts";
import type { ToolCall } from "../message.ts";

// ---------------------------------------------------------------------------
// Mock helpers
// ---------------------------------------------------------------------------

interface StreamingTurn {
  chunks?: string[];
  toolCalls?: ToolCall[];
  finishReason?: LLMFinishReason;
}

function mockStreamingLLM(turns: StreamingTurn[]): LLMProvider {
  let callIndex = 0;
  return {
    chat: async () => {
      throw new Error("chat() should not be called when chatStream is present");
    },
    chatStream(params) {
      const turn = turns[callIndex++];
      if (!turn) throw new Error("mockStreamingLLM: exhausted turns");
      return {
        [Symbol.asyncIterator](): AsyncIterator<LLMStreamChunk> {
          const textChunks = turn.chunks ?? [];
          let pos = 0;
          let finalEmitted = false;
          return {
            async next(): Promise<IteratorResult<LLMStreamChunk>> {
              if (pos < textChunks.length) {
                return { value: { delta: textChunks[pos++]! }, done: false };
              }
              if (!finalEmitted) {
                finalEmitted = true;
                return {
                  value: {
                    delta: "",
                    toolCalls: turn.toolCalls,
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

async function makeSession(config: Parameters<typeof agentLoop>[0]) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  return agentLoop(config).session({ runner, sessionId: `s-${Math.random()}` });
}

async function collectStream(iter: AsyncIterable<string>): Promise<string[]> {
  const chunks: string[] = [];
  for await (const chunk of iter) {
    chunks.push(chunk);
  }
  return chunks;
}

// ---------------------------------------------------------------------------
// session.stream() — with chatStream
// ---------------------------------------------------------------------------

describe("session.stream() — with chatStream", () => {
  it("yields deltas in order and terminates", async () => {
    const session = await makeSession({
      name: "stream-basic",
      llm: mockStreamingLLM([{ chunks: ["Hello", " world"], finishReason: "stop" }]),
    });

    const chunks = await collectStream(session.stream("hi"));
    await session.close();

    expect(chunks).toEqual(["Hello", " world"]);
  });

  it("works across a multi-step turn (tool call → final text)", async () => {
    const echoTool = tool({
      name: "echo",
      description: "Echo the input",
      parameters: z.object({ text: z.string() }),
      execute: async ({ text }) => text,
    });

    const session = await makeSession({
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

    const chunks = await collectStream(session.stream("go"));
    await session.close();

    expect(chunks).toEqual(["done"]);
  });

  it("stream ends after the turn; second turn yields its own deltas independently", async () => {
    const session = await makeSession({
      name: "stream-two-turns",
      llm: mockStreamingLLM([
        { chunks: ["turn1"], finishReason: "stop" },
        { chunks: ["turn2"], finishReason: "stop" },
      ]),
    });

    const firstChunks = await collectStream(session.stream("first"));
    const secondChunks = await collectStream(session.stream("second"));
    await session.close();

    expect(firstChunks).toEqual(["turn1"]);
    expect(secondChunks).toEqual(["turn2"]);
  });
});

// ---------------------------------------------------------------------------
// session.stream() — fallback without chatStream
// ---------------------------------------------------------------------------

describe("session.stream() — fallback without chatStream", () => {
  it("emits full content as a single chunk when chatStream is absent", async () => {
    const session = await makeSession({
      name: "stream-fallback",
      llm: mockPlainLLM([{ content: "The full answer.", finishReason: "stop" }]),
    });

    const chunks = await collectStream(session.stream("tell me something"));
    await session.close();

    expect(chunks).toEqual(["The full answer."]);
  });
});

// ---------------------------------------------------------------------------
// broadcast()
// ---------------------------------------------------------------------------

describe("broadcast()", () => {
  async function* makeSource(items: string[]): AsyncIterable<string> {
    for (const item of items) {
      yield item;
    }
  }

  it("single subscriber receives all items", async () => {
    const hub = broadcast(makeSource(["a", "b", "c"]));
    const result = await collectStream(hub.subscribe());
    expect(result).toEqual(["a", "b", "c"]);
  });

  it("two concurrent subscribers each receive the full sequence", async () => {
    const hub = broadcast(makeSource(["x", "y", "z"]));
    const [r1, r2] = await Promise.all([
      collectStream(hub.subscribe()),
      collectStream(hub.subscribe()),
    ]);
    expect(r1).toEqual(["x", "y", "z"]);
    expect(r2).toEqual(["x", "y", "z"]);
  });

  it("late subscriber receives all buffered items from the start", async () => {
    const hub = broadcast(makeSource(["p", "q", "r"]));

    // First subscriber consumes entirely — this drains the source
    const first = await collectStream(hub.subscribe());

    // Late subscriber subscribes after source is done; must still get all items
    const late = await collectStream(hub.subscribe());

    expect(first).toEqual(["p", "q", "r"]);
    expect(late).toEqual(["p", "q", "r"]);
  });

  it("source that ends early: subscribers terminate cleanly after all items", async () => {
    // Verifies that once the source iterator signals done, every subscriber
    // also terminates — even one that subscribes after the source finished.
    const hub = broadcast(makeSource(["only", "these"]));

    // First subscriber — concurrently with second
    const [r1, r2] = await Promise.all([
      collectStream(hub.subscribe()),
      collectStream(hub.subscribe()),
    ]);

    // Third subscriber added well after source done
    const r3 = await collectStream(hub.subscribe());

    expect(r1).toEqual(["only", "these"]);
    expect(r2).toEqual(["only", "these"]);
    expect(r3).toEqual(["only", "these"]);
  });
});
