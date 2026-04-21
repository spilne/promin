import { describe, it, expect } from "bun:test";
import { FakeClock } from "@promin/core";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import type { LLMResponse } from "../llm-provider.ts";

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

async function makeSession(config: Parameters<typeof agentLoop>[0], sessionId = "idle-s1") {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  return agentLoop(config).session({ runner, sessionId });
}

describe("agentLoop onIdle hook", () => {
  it("fires onIdle after idleTimeoutMs with no new send()", async () => {
    const clock = FakeClock.create(0);
    const idleFires: number[] = [];

    const session = await makeSession({
      name: "idle-test",
      clock,
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      hooks: {
        idleTimeoutMs: 1000,
        onIdle: async (idleMs) => {
          idleFires.push(idleMs);
        },
      },
    });

    await session.send("hello");
    expect(idleFires).toHaveLength(0);

    // Advance past idleTimeoutMs — timer should fire
    clock.advance(1001);
    await Promise.resolve(); // allow microtasks to flush

    expect(idleFires).toHaveLength(1);
    expect(idleFires[0]).toBeGreaterThanOrEqual(1000);
    await session.close();
  });

  it("does not fire if send() arrives before timeout", async () => {
    const clock = FakeClock.create(0);
    const idleFires: number[] = [];

    const session = await makeSession({
      name: "no-idle-test",
      clock,
      llm: mockLLM([
        { content: "reply 1", finishReason: "stop" },
        { content: "reply 2", finishReason: "stop" },
      ]),
      hooks: {
        idleTimeoutMs: 1000,
        onIdle: async (idleMs) => {
          idleFires.push(idleMs);
        },
      },
    });

    await session.send("first");
    clock.advance(500); // only 500ms — not yet idle
    await session.send("second"); // resets the timer
    clock.advance(500); // total 1000ms since first, but only 500ms since second
    await Promise.resolve();

    expect(idleFires).toHaveLength(0);
    await session.close();
  });

  it("close() cancels the idle timer", async () => {
    const clock = FakeClock.create(0);
    const idleFires: number[] = [];

    const session = await makeSession({
      name: "close-cancels-idle",
      clock,
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      hooks: {
        idleTimeoutMs: 1000,
        onIdle: async (idleMs) => {
          idleFires.push(idleMs);
        },
      },
    });

    await session.send("hello");
    await session.close(); // clear timer before it fires
    clock.advance(2000);
    await Promise.resolve();

    expect(idleFires).toHaveLength(0);
  });

  it("onIdle fires once per idle window, not repeatedly", async () => {
    const clock = FakeClock.create(0);
    const idleFires: number[] = [];

    const session = await makeSession({
      name: "once-per-idle",
      clock,
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
      hooks: {
        idleTimeoutMs: 500,
        onIdle: async (idleMs) => {
          idleFires.push(idleMs);
        },
      },
    });

    await session.send("hello");
    clock.advance(600);
    await Promise.resolve();
    clock.advance(600); // second advance — no new timer was set
    await Promise.resolve();

    expect(idleFires).toHaveLength(1);
    await session.close();
  });

  it("no timer is set when hooks.onIdle is not provided", async () => {
    const clock = FakeClock.create(0);

    const session = await makeSession({
      name: "no-idle-hook",
      clock,
      llm: mockLLM([{ content: "ok", finishReason: "stop" }]),
    });

    await session.send("hello");
    clock.advance(99999);
    await Promise.resolve();
    // No error — just completes normally
    await session.close();
  });
});
