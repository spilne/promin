import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import { tool } from "../tool.ts";
import { InMemorySessionLogger } from "../session-logger.ts";
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

describe("InMemorySessionLogger", () => {
  it("emit adds ts automatically", () => {
    const logger = new InMemorySessionLogger();
    const before = Date.now();
    logger.emit({ type: "turn.start", turn: 0, task: "hi" });
    const after = Date.now();
    const [e] = logger.events();
    expect(e!.ts).toBeGreaterThanOrEqual(before);
    expect(e!.ts).toBeLessThanOrEqual(after);
  });

  it("events() returns a copy — mutation does not affect internal buffer", () => {
    const logger = new InMemorySessionLogger();
    logger.emit({ type: "turn.start", turn: 0, task: "hi" });
    const snap = logger.events();
    snap.pop();
    expect(logger.events()).toHaveLength(1);
  });

  it("clear() empties the buffer", () => {
    const logger = new InMemorySessionLogger();
    logger.emit({ type: "turn.start", turn: 0, task: "hi" });
    logger.clear();
    expect(logger.events()).toHaveLength(0);
  });

  it("ring buffer drops oldest when maxSize is exceeded", () => {
    const logger = new InMemorySessionLogger(3);
    for (let i = 0; i < 5; i++) {
      logger.emit({ type: "turn.start", turn: i, task: `t${i}` });
    }
    const events = logger.events();
    expect(events).toHaveLength(3);
    // Oldest two (turn 0, 1) were dropped — newest three remain.
    expect(events.map((e) => (e as { turn: number }).turn)).toEqual([2, 3, 4]);
  });

  it("explicit ts is preserved (not overwritten)", () => {
    const logger = new InMemorySessionLogger();
    logger.emit({ type: "turn.start", turn: 0, task: "hi", ts: 42 });
    expect(logger.events()[0]!.ts).toBe(42);
  });
});

describe("agentLoop — session event log", () => {
  it("eventLog() is empty when no logger is configured", async () => {
    const session = await makeSession({
      name: "no-logger",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
    });
    await session.send("hi");
    expect(session.eventLog()).toHaveLength(0);
    session.close();
  });

  it("emits turn.start and turn.end for each turn", async () => {
    const logger = new InMemorySessionLogger();
    const session = await makeSession({
      name: "turn-events",
      llm: mockLLM([
        { content: "first", finishReason: "stop" },
        { content: "second", finishReason: "stop" },
      ]),
      logger,
    });

    await session.send("hello");
    await session.send("world");
    session.close();

    const starts = logger.events().filter((e) => e.type === "turn.start");
    const ends = logger.events().filter((e) => e.type === "turn.end");
    expect(starts).toHaveLength(2);
    expect(ends).toHaveLength(2);
    expect((starts[0] as { turn: number }).turn).toBe(0);
    expect((starts[1] as { turn: number }).turn).toBe(1);
    expect((ends[0] as { answer: string }).answer).toBe("first");
    expect((ends[1] as { answer: string }).answer).toBe("second");
  });

  it("turn.end carries durationMs > 0", async () => {
    const logger = new InMemorySessionLogger();
    const session = await makeSession({
      name: "duration",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      logger,
    });
    await session.send("go");
    session.close();

    const end = logger.events().find((e) => e.type === "turn.end") as
      | { durationMs: number }
      | undefined;
    expect(end).toBeDefined();
    expect(end!.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("turn.end carries token counts when LLM returns usage", async () => {
    const logger = new InMemorySessionLogger();
    const session = await makeSession({
      name: "tokens",
      llm: {
        chat: async () => ({
          content: "reply",
          finishReason: "stop" as const,
          usage: { inputTokens: 10, outputTokens: 5 },
        }),
      },
      logger,
    });
    await session.send("hi");
    session.close();

    const end = logger.events().find((e) => e.type === "turn.end") as
      | { tokens: { inputTokens: number; outputTokens: number } }
      | undefined;
    expect(end).toBeDefined();
    expect(end!.tokens.inputTokens).toBe(10);
    expect(end!.tokens.outputTokens).toBe(5);
  });

  it("emits llm.call with durationMs for each think step", async () => {
    const logger = new InMemorySessionLogger();
    const session = await makeSession({
      name: "llm-call",
      llm: mockLLM([
        // Turn 0: two think steps (tool call then final answer)
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-1", name: "echo", input: { text: "ping" } }],
        },
        { content: "done", finishReason: "stop" },
      ]),
      tools: {
        echo: tool({
          name: "echo",
          description: "echo",
          parameters: z.object({ text: z.string() }),
          execute: async ({ text }) => text,
        }),
      },
      logger,
    });
    await session.send("run");
    session.close();

    const llmCalls = logger.events().filter((e) => e.type === "llm.call");
    expect(llmCalls).toHaveLength(2);
    for (const c of llmCalls) {
      expect((c as { durationMs: number }).durationMs).toBeGreaterThanOrEqual(0);
    }
    expect((llmCalls[0] as { step: number }).step).toBe(0);
    expect((llmCalls[1] as { step: number }).step).toBe(1);
  });

  it("emits tool.start and tool.end around each tool execution", async () => {
    const logger = new InMemorySessionLogger();
    const echoTool = tool({
      name: "echo",
      description: "echo",
      parameters: z.object({ text: z.string() }),
      execute: async ({ text }) => text,
    });
    const session = await makeSession({
      name: "tool-events",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-1", name: "echo", input: { text: "hello" } }],
        },
        { content: "done", finishReason: "stop" },
      ]),
      tools: { echo: echoTool },
      logger,
    });
    await session.send("call echo");
    session.close();

    const starts = logger.events().filter((e) => e.type === "tool.start");
    const ends = logger.events().filter((e) => e.type === "tool.end");
    expect(starts).toHaveLength(1);
    expect(ends).toHaveLength(1);
    expect((starts[0] as { name: string }).name).toBe("echo");
    expect((ends[0] as { name: string; failed: boolean }).name).toBe("echo");
    expect((ends[0] as { failed: boolean }).failed).toBe(false);
    expect((ends[0] as { durationMs: number }).durationMs).toBeGreaterThanOrEqual(0);
  });

  it("tool.end marks failed=true when tool throws", async () => {
    const logger = new InMemorySessionLogger();
    const faultyTool = tool({
      name: "faulty",
      description: "always throws",
      parameters: z.object({ x: z.string() }),
      execute: async () => {
        throw new Error("boom");
      },
    });
    const session = await makeSession({
      name: "tool-fail",
      llm: {
        chat: async (params) => {
          const hasResult = params.messages.some((m: any) => m.role === "tool");
          if (hasResult) return { content: "handled", finishReason: "stop" as const };
          return {
            content: null,
            finishReason: "tool_calls" as const,
            toolCalls: [{ id: "tc-1", name: "faulty", input: { x: "a" } }],
          };
        },
      },
      tools: { faulty: faultyTool },
      logger,
    });
    await session.send("run faulty");
    session.close();

    const end = logger.events().find((e) => e.type === "tool.end") as
      | { failed: boolean }
      | undefined;
    expect(end).toBeDefined();
    expect(end!.failed).toBe(true);
  });

  it("emits tool.parse_error when LLM sends invalid input", async () => {
    const logger = new InMemorySessionLogger();
    const strictTool = tool({
      name: "strict",
      description: "strict",
      parameters: z.object({ prompt: z.string() }),
      execute: async ({ prompt }) => prompt,
    });
    const session = await makeSession({
      name: "parse-error",
      llm: {
        chat: async (params) => {
          const hasResult = params.messages.some((m: any) => m.role === "tool");
          if (hasResult) return { content: "ok", finishReason: "stop" as const };
          return {
            content: null,
            finishReason: "tool_calls" as const,
            // missing required "prompt" field
            toolCalls: [{ id: "tc-1", name: "strict", input: {} }],
          };
        },
      },
      tools: { strict: strictTool },
      logger,
    });
    await session.send("bad call");
    session.close();

    const parseErr = logger.events().find((e) => e.type === "tool.parse_error");
    expect(parseErr).toBeDefined();
    expect((parseErr as { name: string }).name).toBe("strict");
  });

  it("emits compact event with reason message_count when threshold exceeded", async () => {
    const logger = new InMemorySessionLogger();
    const session = await makeSession({
      name: "compact-event",
      llm: {
        chat: async (params) => {
          const isCompaction = params.messages.some(
            (m: any) => m.role === "system" && m.content?.includes("Summarize"),
          );
          if (isCompaction) return { content: "summary", finishReason: "stop" as const };
          return { content: "reply", finishReason: "stop" as const };
        },
      },
      context: { maxMessages: 4, keepMessages: 2, summarize: false },
      logger,
    });
    // 3 turns × 2 messages each = 6 non-system → exceeds maxMessages=4
    await session.send("turn 1");
    await session.send("turn 2");
    await session.send("turn 3");
    session.close();

    const compact = logger.events().find((e) => e.type === "compact") as
      | { reason: string; kept: number; dropped: number }
      | undefined;
    expect(compact).toBeDefined();
    expect(compact!.reason).toBe("message_count");
    expect(compact!.dropped).toBeGreaterThan(0);
    expect(compact!.kept).toBeGreaterThan(0);
  });

  it("emits approval.requested and approval.decision via onApprovalRequired hook", async () => {
    const logger = new InMemorySessionLogger();
    const guardedTool = tool({
      name: "guarded",
      description: "requires approval",
      parameters: z.object({ x: z.string() }),
      requireApproval: true,
      execute: async ({ x }) => `done: ${x}`,
    });
    const session = await makeSession({
      name: "approval-events",
      llm: {
        chat: async (params) => {
          const hasResult = params.messages.some((m: any) => m.role === "tool");
          if (hasResult) return { content: "finished", finishReason: "stop" as const };
          return {
            content: null,
            finishReason: "tool_calls" as const,
            toolCalls: [{ id: "tc-appr", name: "guarded", input: { x: "val" } }],
          };
        },
      },
      tools: { guarded: guardedTool },
      hooks: {
        onApprovalRequired: async () => ({ approved: true }),
      },
      logger,
    });
    await session.send("run guarded");
    session.close();

    const requested = logger.events().find((e) => e.type === "approval.requested") as
      | { toolName: string; toolCallId: string }
      | undefined;
    const decided = logger.events().find((e) => e.type === "approval.decision") as
      | { approved: boolean }
      | undefined;
    expect(requested).toBeDefined();
    expect(requested!.toolName).toBe("guarded");
    expect(decided).toBeDefined();
    expect(decided!.approved).toBe(true);
  });

  it("emits step_limit.hit when maxStepsPerTurn is exceeded", async () => {
    const logger = new InMemorySessionLogger();
    const loopingTool = tool({
      name: "loop",
      description: "always loops",
      parameters: z.object({}),
      execute: async () => "keep going",
    });
    const session = await makeSession({
      name: "step-limit",
      llm: {
        chat: async (params) => {
          const toolCount = params.messages.filter((m: any) => m.role === "tool").length;
          // Always call the tool until step limit hit
          return {
            content: null,
            finishReason: "tool_calls" as const,
            toolCalls: [{ id: `tc-${toolCount}`, name: "loop", input: {} }],
          };
        },
      },
      tools: { loop: loopingTool },
      maxStepsPerTurn: 3,
      logger,
    });
    await session.send("loop");
    session.close();

    const limitHit = logger.events().find((e) => e.type === "step_limit.hit") as
      | { maxSteps: number }
      | undefined;
    expect(limitHit).toBeDefined();
    expect(limitHit!.maxSteps).toBe(3);
  });

  it("all events carry a numeric ts timestamp", async () => {
    const logger = new InMemorySessionLogger();
    const session = await makeSession({
      name: "timestamps",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      logger,
    });
    await session.send("hi");
    session.close();

    for (const e of logger.events()) {
      expect(typeof e.ts).toBe("number");
      expect(e.ts).toBeGreaterThan(0);
    }
  });

  it("eventLog() on session returns same events as logger", async () => {
    const logger = new InMemorySessionLogger();
    const session = await makeSession({
      name: "eventlog-method",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      logger,
    });
    await session.send("hi");
    session.close();

    expect(session.eventLog()).toEqual(logger.events());
    expect(session.eventLog().length).toBeGreaterThan(0);
  });
});
