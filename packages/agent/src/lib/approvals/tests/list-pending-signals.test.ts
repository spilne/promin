// ---------------------------------------------------------------------------
// listPendingSignals tests — combine an integration spec that drives a
// real agent turn until it suspends on `approve:<id>` (verifies the helper
// reads the tool metadata the engine actually writes) with fixture-based
// specs for the filter / shape edges (namespace scoping, non-approval
// signals, limits).
// Fixtures use `createWorkflow` + `appendEntry` + `suspendWorkflow`
// directly so we can exercise namespace + non-approval cases without
// plumbing them through engine config.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, completeSignal, createWorkflowRunner } from "@promin/workflow";
import { agentAction } from "../../agent-action.ts";
import { tool } from "../../tool.ts";
import { listPendingSignals } from "../list-pending-signals.ts";
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
  // The approval-start activity records its result on the journal,
  // not in wf.steps — match what the engine does.
  await opts.storage.appendEntry({
    workflowId: opts.workflowId,
    stepName: "agent",
    activityIndex: 0,
    activityName: `step-0-approval-${opts.toolCallId}-start`,
    exit: { tag: "Success", value: { toolName: opts.toolName, toolInput: opts.toolInput } },
  });
  // The agent step suspended waiting for the approve signal.
  await opts.storage.suspendWorkflow({
    workflowId: opts.workflowId,
    stepName: "agent",
    stepUpdate: {
      status: "waiting_for_signal",
      signalName: `approve:${opts.toolCallId}`,
      stepType: "signal",
      startedAt,
    },
  });
}

describe("listPendingSignals — integration with a real agent turn", () => {
  it("surfaces a turn that's actually suspended on an approval gate", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const llm = mockLLM([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "tc-real", name: "guarded", input: { x: "hello" } }],
      },
      { content: "ok", finishReason: "stop" },
    ]);
    const turn = () => agentAction({ name: "approval-test", llm, tools: { guarded } });

    const first = await runner.runSafe({
      workflow: turn(),
      workflowId: "wf-real",
      input: { task: "call the guarded tool please" },
    });
    expect(first.error).toBeTruthy();

    const pending = await listPendingSignals(storage);
    expect(pending).toHaveLength(1);
    expect(pending[0]).toMatchObject({
      workflowId: "wf-real",
      toolCallId: "tc-real",
      toolName: "guarded",
      toolInput: { x: "hello" },
      workflowName: "approval-test",
      isApproval: true,
      signalName: "approve:tc-real",
    });
    expect(pending[0]!.suspendedAt).toBeInstanceOf(Date);

    // Approving resolves the suspension; the finished turn waits on nothing.
    await completeSignal({
      storage,
      workflowId: "wf-real",
      stepName: pending[0]!.stepName,
      signalName: "approve:tc-real",
      value: { approved: true },
    });
    const result = await runner.run({
      workflow: turn(),
      workflowId: "wf-real",
      input: { task: "call the guarded tool please" },
    });
    expect(result.answer).toBe("ok");
    expect(await listPendingSignals(storage)).toEqual([]);
  });
});

describe("listPendingSignals — fixture coverage", () => {
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

    const acme = await listPendingSignals(storage, { namespace: "acme" });
    expect(acme.map((p) => p.workflowId)).toEqual(["wf-acme"]);
    expect(acme[0]?.namespace).toBe("acme");

    const globex = await listPendingSignals(storage, { namespace: "globex" });
    expect(globex.map((p) => p.workflowId)).toEqual(["wf-globex"]);
  });

  it("lists non-approval suspensions too, flagged isApproval: false", async () => {
    const storage = new InMemoryWorkflowStorage();
    // Approval suspension.
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
    await storage.suspendWorkflow({
      workflowId: "wf-other",
      stepName: "wait-for-thing",
      stepUpdate: {
        status: "waiting_for_signal",
        signalName: "ext:custom-thing",
        stepType: "signal",
        startedAt: new Date(),
      },
    });

    const pending = await listPendingSignals(storage);
    const byId = new Map(pending.map((p) => [p.workflowId, p]));
    expect([...byId.keys()].sort()).toEqual(["wf-other", "wf-yes"]);
    expect(byId.get("wf-yes")).toMatchObject({ isApproval: true, toolCallId: "tc-y" });
    expect(byId.get("wf-other")).toMatchObject({
      isApproval: false,
      signalName: "ext:custom-thing",
      stepName: "wait-for-thing",
    });
    expect(byId.get("wf-other")?.toolCallId).toBeUndefined();
  });

  it("returns empty when no workflows are suspended", async () => {
    const storage = new InMemoryWorkflowStorage();
    expect(await listPendingSignals(storage)).toEqual([]);
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
    await storage.suspendWorkflow({
      workflowId: "wf-bare",
      stepName: "conversation",
      stepUpdate: {
        status: "waiting_for_signal",
        signalName: "approve:tc-bare",
        stepType: "signal",
        startedAt: new Date(),
      },
    });

    const pending = await listPendingSignals(storage);
    expect(pending).toHaveLength(1);
    expect(pending[0]).toMatchObject({
      workflowId: "wf-bare",
      toolCallId: "tc-bare",
      isApproval: true,
    });
    expect(pending[0]!.toolName).toBeUndefined();
    expect(pending[0]!.toolInput).toBeUndefined();
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

    const limited = await listPendingSignals(storage, { limit: 1 });
    expect(limited).toHaveLength(1);
  });
});
