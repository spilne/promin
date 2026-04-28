// ---------------------------------------------------------------------------
// LocalAgentThread.resume — drives a real agent loop until it suspends on
// `approve:<callId>`, then verifies resume() delivers the signal and
// continues to completion. Covers the rejection path too (decision
// surfaces as a rejected tool result).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalAgent } from "../local-agent.ts";
import { tool } from "../../tool.ts";
import type { LLMResponse } from "../../llm-provider.ts";

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

const guarded = tool({
  name: "guarded",
  description: "Requires approval before running",
  parameters: z.object({ x: z.string() }),
  execute: async ({ x }) => `ran: ${x}`,
  requireApproval: true,
});

function buildAgent(llmResponses: LLMResponse[]) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const agent = new LocalAgent({
    namespaceId: "acme",
    resourceId: "alice",
    runner,
    agent: {
      name: "approval-resume-test",
      llm: mockLLM(llmResponses),
      tools: { guarded },
    },
  });
  return { agent, storage };
}

describe("LocalAgentThread.resume", () => {
  it("delivers an approve decision and lets the workflow continue to completion", async () => {
    const { agent } = buildAgent([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "tc-1", name: "guarded", input: { x: "hello" } }],
      },
      { content: "all done", finishReason: "stop" },
    ]);
    const thread = await agent.thread("t-resume-1");

    // The first send() rejects with WorkflowSuspendedError when the agent
    // hits the approval gate — that's how the runner signals "I stopped
    // waiting for input". The gateway / chat backend swallows that error
    // and surfaces "waiting_approval" to the UI.
    await expect(thread.send({ task: "Do the thing" })).rejects.toThrow(/suspended|signal/i);
    await waitForApproval(agent, thread.id, "tc-1");

    const out = await thread.resume!("tc-1", { approved: true });
    const text = await out.text;
    expect(text).toBe("all done");
  });

  it("propagates a reject decision into a tool-result rejection", async () => {
    const { agent } = buildAgent([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "tc-2", name: "guarded", input: { x: "no" } }],
      },
      { content: "stopped because rejected", finishReason: "stop" },
    ]);
    const thread = await agent.thread("t-resume-2");

    await expect(thread.send({ task: "Try the thing" })).rejects.toThrow(/suspended|signal/i);
    await waitForApproval(agent, thread.id, "tc-2");

    const out = await thread.resume!("tc-2", { approved: false, reason: "no thanks" });
    expect(await out.text).toBe("stopped because rejected");
  });

  it("throws when no suspended approval matches the (thread, callId) pair", async () => {
    const { agent } = buildAgent([{ content: "hi", finishReason: "stop" }]);
    const thread = await agent.thread("t-resume-3");
    await thread.send({ task: "Hi" });

    await expect(thread.resume!("nonexistent", { approved: true })).rejects.toThrow(
      /No suspended approval/,
    );
  });
});

async function waitForApproval(
  agent: LocalAgent,
  threadId: string,
  callId: string,
  timeoutMs = 2000,
) {
  // Poll the underlying storage by fishing the runner's storage out of the
  // agent's first thread — easier than threading the storage in. We only
  // need the side-effect of "wait until visible".
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    // biome-ignore lint/suspicious/noExplicitAny: test introspection
    const storage = (agent as any).config.runner.storage as InMemoryWorkflowStorage;
    const list = await storage.listWorkflows({ status: "suspended", limit: 50 });
    const hit = list.find(
      (w) =>
        w.workflowId.startsWith(`${threadId}-`) &&
        Object.values(w.steps).some(
          (s) => s.status === "waiting_for_signal" && s.signalName === `approve:${callId}`,
        ),
    );
    if (hit) return;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`Timed out waiting for approve:${callId} on ${threadId}`);
}
