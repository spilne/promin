// ---------------------------------------------------------------------------
// Heartbeat injection — verifies the agent loop emits keepalive events
// when no other event has fired for `heartbeatMs` while a turn is in
// flight. Uses real timers with a short interval (50ms) — driving this
// via FakeClock against the real workflow runner is fragile because
// the runner's promise resolution doesn't synchronize with FakeClock
// advances.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import type { LLMProvider, LLMResponse } from "../llm-provider.ts";
import type { SessionEvent } from "../session-logger.ts";

/** LLM that holds the chat() promise until you call `resolve()`. */
function deferredLLM(): { llm: LLMProvider; resolve: (r?: LLMResponse) => void } {
  let resolveFn: (r: LLMResponse) => void = () => {};
  const llm: LLMProvider = {
    chat: async (): Promise<LLMResponse> =>
      new Promise<LLMResponse>((resolve) => {
        resolveFn = resolve;
      }),
  };
  return {
    llm,
    resolve: (r) => resolveFn(r ?? { content: "done", finishReason: "stop" }),
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function bootSession(opts: { llm: LLMProvider; heartbeatMs?: number }) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const events: SessionEvent[] = [];
  const session = await agentLoop({
    llm: opts.llm,
    ...(opts.heartbeatMs !== undefined ? { heartbeatMs: opts.heartbeatMs } : {}),
  }).session({ runner, sessionId: "hb-test" });
  session.subscribe((e) => events.push(e));
  return { session, events };
}

describe("agentLoop heartbeat", () => {
  it("emits heartbeat events while the turn is idle past the threshold", async () => {
    const { llm, resolve } = deferredLLM();
    const { session, events } = await bootSession({ llm, heartbeatMs: 50 });

    void session.send("hi").catch(() => {});
    // Wait long enough for ~3 heartbeats to fire while the LLM is hung.
    await sleep(200);

    const heartbeats = events.filter((e) => e.type === "heartbeat");
    expect(heartbeats.length).toBeGreaterThanOrEqual(2);
    expect(heartbeats[0]).toMatchObject({ type: "heartbeat", turn: 0 });

    // Resolve so the workflow can finish cleanly.
    resolve();
    await sleep(20);
    await session.close();
  });

  it("heartbeatMs: 0 disables heartbeat entirely", async () => {
    const { llm, resolve } = deferredLLM();
    const { session, events } = await bootSession({ llm, heartbeatMs: 0 });

    void session.send("hi").catch(() => {});
    await sleep(150);

    expect(events.filter((e) => e.type === "heartbeat")).toHaveLength(0);

    resolve();
    await sleep(20);
    await session.close();
  });

  it("stops emitting after the turn ends", async () => {
    const { llm, resolve } = deferredLLM();
    const { session, events } = await bootSession({ llm, heartbeatMs: 50 });

    const sendPromise = session.send("hi");
    // Let the workflow reach the LLM-call suspension, then resolve so
    // the turn completes immediately.
    await sleep(20);
    resolve();
    await sendPromise;

    const beforeIdle = events.filter((e) => e.type === "heartbeat").length;
    // Turn is done — stopHeartbeat() ran. Idling now must NOT fire heartbeats.
    await sleep(150);
    const afterIdle = events.filter((e) => e.type === "heartbeat").length;
    expect(afterIdle).toBe(beforeIdle);

    await session.close();
  });
});
