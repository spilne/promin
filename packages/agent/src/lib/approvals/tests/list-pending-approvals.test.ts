// ---------------------------------------------------------------------------
// listPendingApprovals tests — combine an integration spec that drives the
// real agent loop until it suspends on `approve:<id>` (verifies the helper
// reads what the loop actually writes) with fixture-based specs for the
// filter / shape edges (namespace scoping, non-approval signals, limits).
// Fixtures use `createWorkflow` + `saveStepResult` + `suspendWorkflow`
// directly so we can exercise namespace + non-approval cases without
// plumbing them through agent-loop config.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../../agent-loop.ts";
import { tool } from "../../tool.ts";
import { listPendingApprovals } from "../list-pending-approvals.ts";
import type { AgentSession } from "../../agent-loop.ts";
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

async function waitForStatus(session: AgentSession, target: string, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if ((await session.status()) === target) return;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`Timed out waiting for status "${target}"`);
}

/** Stamp a fixture pending-approval workflow row directly into storage. */
async function fixturePending(opts: {
  storage: InMemoryWorkflowStorage;
  workflowId: string;
  toolCallId: string;
  toolName: string;
  toolInput: unknown;
  namespace?: string;
  workflowName?: string;
}) {
  const startedAt = new Date();
  await opts.storage.createWorkflow({
    workflowId: opts.workflowId,
    workflowName: opts.workflowName ?? "fixture",
    input: undefined,
    ...(opts.namespace !== undefined && { namespace: opts.namespace }),
  });
  // The lc-approval-start activity records its result on the journal,
  // not in wf.steps — match what the real agent loop does.
  await opts.storage.appendEntry({
    workflowId: opts.workflowId,
    stepName: "conversation",
    activityIndex: 0,
    activityName: `lc-0-approval-${opts.toolCallId}-start`,
    exit: { tag: "Success", value: { toolName: opts.toolName, toolInput: opts.toolInput } },
  });
  // The conversation step suspended waiting for the approve signal.
  await opts.storage.suspendWorkflow(opts.workflowId, "conversation", {
    status: "waiting_for_signal",
    signalName: `approve:${opts.toolCallId}`,
    stepType: "signal",
    startedAt,
  });
}

describe("listPendingApprovals — integration with the real agent loop", () => {
  it("surfaces a session that's actually suspended on an approval gate", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = await agentLoop({
      name: "approval-test",
      llm: mockLLM([
        {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "tc-real", name: "guarded", input: { x: "hello" } }],
        },
        { content: "ok", finishReason: "stop" },
      ]),
      tools: { guarded },
    }).session({ runner, sessionId: "wf-real" });

    void session.send("call the guarded tool please");
    await waitForStatus(session, "waiting_approval");

    const pending = await listPendingApprovals(storage);
    expect(pending).toHaveLength(1);
    expect(pending[0]).toMatchObject({
      workflowId: "wf-real",
      toolCallId: "tc-real",
      toolName: "guarded",
      toolInput: { x: "hello" },
      workflowName: "approval-test",
    });
    expect(pending[0]!.suspendedAt).toBeInstanceOf(Date);

    // Approving resolves the suspension — helper drops the row.
    await session.approve("tc-real");
    expect(await listPendingApprovals(storage)).toEqual([]);
    await session.close();
  });
});

describe("listPendingApprovals — fixture coverage", () => {
  it("filters by namespace", async () => {
    const storage = new InMemoryWorkflowStorage();
    await fixturePending({
      storage,
      workflowId: "wf-acme",
      toolCallId: "tc-a",
      toolName: "guarded",
      toolInput: { x: "a" },
      namespace: "acme",
    });
    await fixturePending({
      storage,
      workflowId: "wf-globex",
      toolCallId: "tc-b",
      toolName: "guarded",
      toolInput: { x: "b" },
      namespace: "globex",
    });

    const acme = await listPendingApprovals(storage, { namespace: "acme" });
    expect(acme.map((p) => p.workflowId)).toEqual(["wf-acme"]);
    expect(acme[0]?.namespace).toBe("acme");

    const globex = await listPendingApprovals(storage, { namespace: "globex" });
    expect(globex.map((p) => p.workflowId)).toEqual(["wf-globex"]);
  });

  it("ignores suspended workflows whose signal isn't an approval", async () => {
    const storage = new InMemoryWorkflowStorage();
    // Real approval — should appear.
    await fixturePending({
      storage,
      workflowId: "wf-yes",
      toolCallId: "tc-y",
      toolName: "guarded",
      toolInput: {},
    });
    // Non-approval suspension — different signal name.
    await storage.createWorkflow({
      workflowId: "wf-other",
      workflowName: "other",
      input: undefined,
    });
    await storage.suspendWorkflow("wf-other", "wait-for-thing", {
      status: "waiting_for_signal",
      signalName: "ext:custom-thing",
      stepType: "signal",
      startedAt: new Date(),
    });

    const pending = await listPendingApprovals(storage);
    expect(pending.map((p) => p.workflowId)).toEqual(["wf-yes"]);
  });

  it("returns empty when no workflows are suspended", async () => {
    const storage = new InMemoryWorkflowStorage();
    expect(await listPendingApprovals(storage)).toEqual([]);
  });

  it("falls back to undefined toolName/toolInput when the lc-start step is missing", async () => {
    // Simulates a row that suspended on `approve:` but whose lc-start
    // step wasn't journaled (older runs from before the result tweak,
    // or hand-stamped fixtures).
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({
      workflowId: "wf-bare",
      workflowName: "bare",
      input: undefined,
    });
    await storage.suspendWorkflow("wf-bare", "conversation", {
      status: "waiting_for_signal",
      signalName: "approve:tc-bare",
      stepType: "signal",
      startedAt: new Date(),
    });

    const pending = await listPendingApprovals(storage);
    expect(pending).toHaveLength(1);
    expect(pending[0]).toMatchObject({
      workflowId: "wf-bare",
      toolCallId: "tc-bare",
      toolName: undefined,
      toolInput: undefined,
    });
  });

  it("respects the limit parameter", async () => {
    const storage = new InMemoryWorkflowStorage();
    await fixturePending({
      storage,
      workflowId: "wf-l1",
      toolCallId: "tc-l1",
      toolName: "guarded",
      toolInput: {},
    });
    await fixturePending({
      storage,
      workflowId: "wf-l2",
      toolCallId: "tc-l2",
      toolName: "guarded",
      toolInput: {},
    });

    const limited = await listPendingApprovals(storage, { limit: 1 });
    expect(limited).toHaveLength(1);
  });
});
