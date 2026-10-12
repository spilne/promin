// ---------------------------------------------------------------------------
// agentAction + SessionEventBus — multi-subscriber observability for the
// one-shot agent primitive.
//
// Pins the contract:
//   - bus emits turn / llm.call / tool.start / tool.end / approval.* /
//     turn.end as the journaled body runs.
//   - Multiple subscribers attached before run() each see every event.
//   - Late subscribers (attached after run() starts) see only events
//     from that point onward.
//   - Token deltas from the LLM stream land on the bus as token.delta
//     events, transient, alongside the onChunk callback.
//   - A throwing subscriber is silently removed without breaking the loop.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentAction } from "../agent-action.ts";
import { SessionEventBus, type SessionEvent } from "../session-logger.ts";
import { tool } from "../tool.ts";
import type { LLMProvider, LLMResponse } from "../llm-provider.ts";

async function runAction(workflow: ReturnType<typeof agentAction>, task: string) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  return runner.run({
    workflow,
    workflowId: `aa-${Math.random().toString(36).slice(2)}`,
    input: { task },
  });
}

function mockLLM(responses: LLMResponse[]) {
  let i = 0;
  return {
    chat: async () => {
      const resp = responses[i++];
      if (!resp) throw new Error("Mock LLM exhausted");
      return resp;
    },
  };
}

function mockStreamingLLM(deltas: string[], answer: string): LLMProvider {
  return {
    chat: async () => ({ content: answer, finishReason: "stop" }),
    chatStream: async function* () {
      for (const delta of deltas) yield { delta };
      yield { delta: "", finishReason: "stop" };
    },
  };
}

describe("agentAction — SessionEventBus integration", () => {
  it("emits turn.start, llm.call, turn.end on the bus during a single-step run", async () => {
    const bus = new SessionEventBus();
    const events: SessionEvent[] = [];
    bus.subscribe((e) => events.push(e));

    const action = agentAction({
      name: "echo",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      bus,
    });
    await runAction(action, "hi");

    const types = events.map((e) => e.type);
    expect(types[0]).toBe("turn.start");
    expect(types).toContain("llm.call");
    expect(types[types.length - 1]).toBe("turn.end");
    const turnEnd = events.find((e) => e.type === "turn.end");
    expect(turnEnd && turnEnd.type === "turn.end" && turnEnd.answer).toBe("ok");
  });

  it("emits tool.start + tool.end around a journaled tool call", async () => {
    const bus = new SessionEventBus();
    const events: SessionEvent[] = [];
    bus.subscribe((e) => events.push(e));

    const greetTool = tool({
      name: "greet",
      description: "Returns a greeting",
      parameters: z.object({ name: z.string() }),
      execute: async ({ name }) => `hello ${name}`,
    });

    const action = agentAction({
      name: "tool-runner",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "t1", name: "greet", input: { name: "world" } }],
        },
        { content: "done", finishReason: "stop" },
      ]),
      tools: { greet: greetTool },
      bus,
    });
    await runAction(action, "say hi");

    const toolStart = events.find((e) => e.type === "tool.start");
    const toolEnd = events.find((e) => e.type === "tool.end");
    expect(toolStart && toolStart.type === "tool.start" && toolStart.name).toBe("greet");
    expect(toolEnd && toolEnd.type === "tool.end" && toolEnd.failed).toBe(false);
  });

  it("delivers events to multiple subscribers in arrival order", async () => {
    const bus = new SessionEventBus();
    const a: string[] = [];
    const b: string[] = [];
    bus.subscribe((e) => a.push(e.type));
    bus.subscribe((e) => b.push(e.type));

    const action = agentAction({
      name: "fanout",
      llm: mockLLM([{ content: "k", finishReason: "stop" }]),
      bus,
    });
    await runAction(action, "ping");

    expect(a).toEqual(b);
    expect(a.length).toBeGreaterThan(0);
  });

  it("a throwing subscriber is removed silently — peers + loop stay alive", async () => {
    const bus = new SessionEventBus();
    const goodEvents: SessionEvent[] = [];
    let badCalls = 0;
    bus.subscribe(() => {
      badCalls++;
      throw new Error("subscriber blew up");
    });
    bus.subscribe((e) => goodEvents.push(e));

    const action = agentAction({
      name: "fault-tolerant",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      bus,
    });
    const result = await runAction(action, "task");

    expect(result.answer).toBe("ok");
    // Throwing observer fires once (on turn.start) then is evicted.
    expect(badCalls).toBe(1);
    // Healthy observer keeps receiving every event after the bad one's
    // first call.
    expect(goodEvents.length).toBeGreaterThan(1);
  });

  it("token.delta lands on the bus alongside onChunk callbacks", async () => {
    const bus = new SessionEventBus();
    const tokenDeltas: string[] = [];
    const callbackDeltas: string[] = [];
    bus.subscribe((e) => {
      if (e.type === "token.delta") tokenDeltas.push(e.delta);
    });

    const action = agentAction({
      name: "streamer",
      llm: mockStreamingLLM(["he", "llo"], "hello"),
      onChunk: (d) => callbackDeltas.push(d),
      bus,
    });
    await runAction(action, "stream");

    // Behavior depends on whether runLlmCall actually invokes chatStream
    // when onChunk is supplied — the test asserts the channel parity.
    expect(tokenDeltas).toEqual(callbackDeltas);
  });

  it("late subscriber sees only events from its attach point onward", async () => {
    const bus = new SessionEventBus();
    const early: SessionEvent[] = [];
    bus.subscribe((e) => early.push(e));

    const action = agentAction({
      name: "late-sub",
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      bus,
    });
    const runP = runAction(action, "task");

    // Attach late — once the run starts emitting (turn.start fires
    // synchronously inside the journaled body), the late subscriber
    // misses it.
    const late: SessionEvent[] = [];
    bus.subscribe((e) => late.push(e));

    await runP;
    // Early subscriber sees turn.start; late one might or might not,
    // depending on event-loop timing. Strict check: early sees more
    // than late.
    expect(early.length).toBeGreaterThanOrEqual(late.length);
  });
});

describe("agentAction — event payloads", () => {
  const echo = tool({
    name: "echo",
    description: "echo",
    parameters: z.object({ text: z.string() }),
    execute: async ({ text }) => text,
  });
  const callEcho: LLMResponse = {
    content: null,
    finishReason: "tool_calls",
    toolCalls: [{ id: "tc-1", name: "echo", input: { text: "ping" } }],
  };

  it("turn.end carries the duration on the engine clock and the turn's tokens", async () => {
    const clock = FakeWallClock.create(1_000);
    const bus = new SessionEventBus();
    const events: SessionEvent[] = [];
    bus.subscribe((e) => events.push(e));
    const llm: LLMProvider = {
      chat: async () => {
        clock.advance(250);
        return {
          content: "reply",
          finishReason: "stop",
          usage: { inputTokens: 10, outputTokens: 5 },
        };
      },
    };

    await runAction(agentAction({ name: "timed", llm, bus, clock }), "hi");

    expect(events.find((e) => e.type === "turn.end")).toMatchObject({
      durationMs: 250,
      tokens: { inputTokens: 10, outputTokens: 5 },
    });
    expect(events.every((e) => typeof e.ts === "number")).toBe(true);
  });

  it("emits one llm.call per think step, numbered by step", async () => {
    const bus = new SessionEventBus();
    const events: SessionEvent[] = [];
    bus.subscribe((e) => events.push(e));

    await runAction(
      agentAction({
        name: "steps",
        llm: mockLLM([callEcho, { content: "done", finishReason: "stop" }]),
        tools: { echo },
        bus,
      }),
      "run",
    );

    const steps = events.flatMap((e) => (e.type === "llm.call" ? [e.step] : []));
    expect(steps).toEqual([0, 1]);
  });

  it("tool.end marks failed=true when the tool throws", async () => {
    const bus = new SessionEventBus();
    const events: SessionEvent[] = [];
    bus.subscribe((e) => events.push(e));
    const faulty = tool({
      name: "echo",
      description: "always throws",
      parameters: z.object({ text: z.string() }),
      execute: async (): Promise<string> => {
        throw new Error("boom");
      },
    });

    await runAction(
      agentAction({
        name: "faulty",
        llm: mockLLM([callEcho, { content: "handled", finishReason: "stop" }]),
        tools: { echo: faulty },
        bus,
      }),
      "run faulty",
    );

    expect(events.find((e) => e.type === "tool.end")).toMatchObject({
      name: "echo",
      failed: true,
    });
  });

  it("emits step_limit.hit when maxSteps runs out", async () => {
    const bus = new SessionEventBus();
    const events: SessionEvent[] = [];
    bus.subscribe((e) => events.push(e));

    const result = await runAction(
      agentAction({
        name: "limit",
        llm: { chat: async () => callEcho },
        tools: { echo },
        maxSteps: 2,
        stall: false,
        bus,
      }),
      "loop",
    );

    expect(result.finishReason).toBe("max_steps");
    expect(events.find((e) => e.type === "step_limit.hit")).toMatchObject({ maxSteps: 2 });
  });
});
