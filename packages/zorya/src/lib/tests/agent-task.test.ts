// ---------------------------------------------------------------------------
// runAgentTask — wraps agentAction with stream registration on a worker.
//
// This test pins the wiring contract:
//   1. runAgentTask registers a stream on `worker.streams[workflowId]`
//      before the agent body runs, so observers attached at any point
//      see events from turn.start onward.
//   2. Events emitted on the agent's internal bus reach an observer
//      subscribed via the registered stream's subscribe fn.
//   3. After completion (success or failure), the stream is
//      unregistered from the worker — the entry leaves `worker.streams`.
//
// The full SSE-through-WS round-trip is covered by agent-stream.test.ts
// (promin-o8dj). Keeping this test focused on the AgentWorker pattern.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { tool } from "@promin/agent";
import type { LLMResponse } from "@promin/agent";
import {
  createWorkflowRunner,
  InMemoryWorkflowStorage,
  type WorkflowRunner,
} from "@promin/workflow";
import { runAgentTask } from "@promin/zorya-client";

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

/**
 * Minimal stand-in for ZoryaWorker. runAgentTask only touches `runner` and
 * `registerStream`, so the test doesn't need a full worker / WS / control
 * socket. The streams map mirrors what the real worker keeps.
 */
function fakeWorker(): {
  runner: WorkflowRunner;
  streams: Map<string, (observer: (event: unknown) => void) => () => void>;
  registerStream: (
    workflowId: string,
    subscribe: (observer: (event: unknown) => void) => () => void,
  ) => () => void;
} {
  const streams = new Map<string, (observer: (event: unknown) => void) => () => void>();
  const storage = new InMemoryWorkflowStorage();
  return {
    runner: createWorkflowRunner({ storage }),
    streams,
    registerStream: (workflowId, subscribe) => {
      streams.set(workflowId, subscribe);
      return () => {
        if (streams.get(workflowId) === subscribe) streams.delete(workflowId);
      };
    },
  };
}

describe("runAgentTask — wraps agentAction with worker.streams registration", () => {
  it("registers a stream before the run, fans events to observers, unregisters on completion", async () => {
    const worker = fakeWorker();

    const greetTool = tool({
      name: "greet",
      description: "Returns a greeting",
      input: z.object({ name: z.string() }),
      run: async ({ name }) => `hello ${name}`,
    });

    // Race: the moment runAgentTask is called it registers the stream
    // synchronously (before any await). We attach the observer right
    // after `registerStream` lands, before the LLM mock fires.
    const events: Array<{ type: string }> = [];

    const taskPromise = runAgentTask(
      // biome-ignore lint/suspicious/noExplicitAny: minimal test stub
      worker as any,
      {
        name: "greet-agent",
        llm: mockLLM([
          {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "t1", name: "greet", input: { name: "world" } }],
          },
          { content: "All done.", finishReason: "stop" },
        ]),
        tools: { greet: greetTool },
      },
      { workflowId: "agent-run-1", input: { task: "say hi" } },
    );

    // Stream is registered synchronously before the runner is awaited —
    // attach an observer that captures every emit.
    const subscribe = worker.streams.get("agent-run-1");
    expect(subscribe).toBeDefined();
    let unsubObserver: (() => void) | undefined;
    if (subscribe) {
      unsubObserver = subscribe((event) => {
        const e = event as { type: string };
        events.push({ type: e.type });
      });
    }

    const result = await taskPromise;
    unsubObserver?.();

    // Agent ran to completion.
    expect(result.answer).toBe("All done.");

    // Event taxonomy reached the observer.
    const types = events.map((e) => e.type);
    expect(types).toContain("turn.start");
    expect(types).toContain("llm.call");
    expect(types).toContain("tool.start");
    expect(types).toContain("tool.end");
    expect(types).toContain("turn.end");

    // Stream unregistered after completion — the worker's map no longer
    // points at this run.
    expect(worker.streams.has("agent-run-1")).toBe(false);
  });

  it("unregisters the stream even when the agent throws", async () => {
    const worker = fakeWorker();

    const taskPromise = runAgentTask(
      // biome-ignore lint/suspicious/noExplicitAny: minimal test stub
      worker as any,
      {
        name: "fail-agent",
        // No LLM responses — the mock throws on first call.
        llm: mockLLM([]),
      },
      { workflowId: "agent-run-2", input: { task: "doomed" } },
    );

    await expect(taskPromise).rejects.toBeDefined();
    expect(worker.streams.has("agent-run-2")).toBe(false);
  });
});
