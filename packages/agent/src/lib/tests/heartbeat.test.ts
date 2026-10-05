// ---------------------------------------------------------------------------
// Heartbeat injection — verifies the agent loop emits keepalive events
// when no other event has fired for `heartbeatMs` while a turn is in
// flight. The heartbeat pump runs on the loop's `clock`: the tests pass a
// FakeWallClock and advance it while the LLM call is held open, so the
// number of heartbeats is exact. The workflow runner stays on the system
// clock; nothing in it waits on time here.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import type { LLMProvider, LLMResponse } from "../llm-provider.ts";
import type { SessionEvent } from "../session-logger.ts";

/**
 * LLM that holds the chat() promise until you call `resolve()`; `called`
 * resolves once chat() has been entered.
 */
function deferredLLM(): {
  llm: LLMProvider;
  called: Promise<void>;
  resolve: (r?: LLMResponse) => void;
} {
  let resolveFn: (r: LLMResponse) => void = () => {};
  let markCalled!: () => void;
  const called = new Promise<void>((r) => (markCalled = r));
  const llm: LLMProvider = {
    chat: async (): Promise<LLMResponse> =>
      new Promise<LLMResponse>((resolve) => {
        resolveFn = resolve;
        markCalled();
      }),
  };
  return {
    llm,
    called,
    resolve: (r) => resolveFn(r ?? { content: "done", finishReason: "stop" }),
  };
}

/** Let in-flight async work settle: a bounded number of macrotask turns. */
async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

/** Advance the clock `ms` in `stepMs` steps, settling work after each. */
async function elapse(clock: FakeWallClock, ms: number, stepMs = 10): Promise<void> {
  for (let t = 0; t < ms; t += stepMs) {
    clock.advance(stepMs);
    await flush();
  }
}

async function bootSession(opts: { llm: LLMProvider; heartbeatMs?: number }) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const events: SessionEvent[] = [];
  const session = await agentLoop({
    llm: opts.llm,
    clock,
    ...(opts.heartbeatMs !== undefined ? { heartbeatMs: opts.heartbeatMs } : {}),
  }).session({ runner, sessionId: "hb-test" });
  session.subscribe((e) => events.push(e));
  return { clock, session, events };
}

const heartbeatsIn = (events: SessionEvent[]) => events.filter((e) => e.type === "heartbeat");

describe("agentLoop heartbeat", () => {
  it("emits one heartbeat per idle interval while the turn is in flight", async () => {
    const { llm, called, resolve } = deferredLLM();
    const { clock, session, events } = await bootSession({ llm, heartbeatMs: 50 });

    const sent = session.send("hi").catch(() => {});
    await called;
    await flush();

    // 49ms idle: nothing yet. At 50, 100 and 150ms: one heartbeat each.
    await elapse(clock, 40);
    clock.advance(9);
    await flush();
    expect(heartbeatsIn(events)).toHaveLength(0);
    clock.advance(1);
    await flush();
    expect(heartbeatsIn(events)).toHaveLength(1);
    await elapse(clock, 100);
    expect(heartbeatsIn(events)).toHaveLength(3);
    expect(heartbeatsIn(events)[0]).toMatchObject({ type: "heartbeat", turn: 0 });

    // Resolve so the workflow can finish cleanly.
    resolve();
    await sent;
    await session.close();
  });

  it("heartbeatMs: 0 disables heartbeat entirely", async () => {
    const { llm, called, resolve } = deferredLLM();
    const { clock, session, events } = await bootSession({ llm, heartbeatMs: 0 });

    const sent = session.send("hi").catch(() => {});
    await called;
    await elapse(clock, 150);

    expect(heartbeatsIn(events)).toHaveLength(0);

    resolve();
    await sent;
    await session.close();
  });

  it("stops emitting after the turn ends", async () => {
    const { llm, called, resolve } = deferredLLM();
    const { clock, session, events } = await bootSession({ llm, heartbeatMs: 50 });

    const sendPromise = session.send("hi");
    await called;
    await elapse(clock, 50);
    const duringTurn = heartbeatsIn(events).length;
    expect(duringTurn).toBe(1);

    resolve();
    await sendPromise;

    // Turn is done — stopHeartbeat() ran. Idling now must NOT fire heartbeats.
    await elapse(clock, 150);
    expect(heartbeatsIn(events)).toHaveLength(duringTurn);

    await session.close();
  });
});
