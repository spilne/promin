// ---------------------------------------------------------------------------
// LocalAgentThread.resume — drives a real agent turn until it suspends on
// `approve:<callId>`, then verifies resume() finds it through the pending-
// approval index (exact namespace + thread + call key), records the
// decision with the approver, delivers it and continues to completion.
// Covers rejection, expiry, unrelated suspended workflows, thread ids that
// prefix each other, namespaces sharing a thread id, and refusing a new
// send while a turn waits for approval.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import {
  FakeWallClock,
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  workflow,
} from "@promin/workflow";
import { LocalAgent } from "../local-agent.ts";
import { tool } from "../../tool.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import { InMemoryPendingApprovalStore } from "../../approvals/in-memory-pending-approval-store.ts";
import {
  ApprovalAlreadyDecidedError,
  ApprovalExpiredError,
  ApprovalNotFoundError,
  ThreadAwaitingApprovalError,
} from "../../approvals/pending-approval-store.ts";

function mockLLM(responses: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async () => {
      const resp = responses[i++];
      if (!resp) throw new Error("Mock LLM exhausted");
      return resp;
    },
  };
}

/** One LLM shared by every thread: calls `guarded` with call id `callId`, then answers. */
function guardedThenAnswer(callId: string, answer: string): LLMResponse[] {
  return [
    {
      content: null,
      finishReason: "tool_calls",
      toolCalls: [{ id: callId, name: "guarded", input: { x: "hello" } }],
    },
    { content: answer, finishReason: "stop" },
  ];
}

const guarded = tool({
  name: "guarded",
  description: "Requires approval before running",
  parameters: z.object({ x: z.string() }),
  execute: async ({ x }) => `ran: ${x}`,
  requireApproval: true,
});

function buildAgent(params: {
  llmResponses: LLMResponse[];
  approvalTtlMs?: number;
  clock?: FakeWallClock;
}) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const memory = new InMemoryMemoryStore();
  const approvals = new InMemoryPendingApprovalStore(params.clock ? { clock: params.clock } : {});
  const agent = new LocalAgent({
    namespaceId: "acme",
    resourceId: "alice",
    runner,
    memory,
    approvals,
    ...(params.approvalTtlMs !== undefined && { approvalTtlMs: params.approvalTtlMs }),
    agent: {
      name: "approval-resume-test",
      llm: mockLLM(params.llmResponses),
      tools: { guarded },
    },
  });
  return { agent, storage, runner, memory, approvals };
}

describe("LocalAgentThread.resume", () => {
  it("delivers an approve decision and lets the workflow continue to completion", async () => {
    const { agent, approvals } = buildAgent({
      llmResponses: guardedThenAnswer("tc-1", "all done"),
    });
    const thread = await agent.thread("t-resume-1");

    // The first send() rejects with WorkflowSuspendedError when the agent
    // hits the approval gate. The index row is written before it suspends.
    await expect(thread.send({ task: "Do the thing" })).rejects.toThrow(/suspended|signal/i);
    const [pending] = await thread.pendingApprovals!();
    expect(pending).toMatchObject({
      namespaceId: "acme",
      threadId: "t-resume-1",
      toolCallId: "tc-1",
      resourceId: "alice",
      agentId: "approval-resume-test",
      toolName: "guarded",
      input: { x: "hello" },
      status: "pending",
    });

    const out = await thread.resume!("tc-1", { approved: true, by: "bob" });
    expect(await out.text).toBe("all done");
    expect(await out.toolResults).toEqual([
      { toolCallId: "tc-1", name: "guarded", content: "ran: hello", failed: false },
    ]);
    const decided = await approvals.get({
      namespaceId: "acme",
      threadId: "t-resume-1",
      toolCallId: "tc-1",
    });
    expect(decided).toMatchObject({ status: "approved", decidedBy: "bob" });
    expect(await thread.pendingApprovals!()).toEqual([]);
  });

  it("propagates a reject decision into a tool-result rejection", async () => {
    const { agent } = buildAgent({
      llmResponses: guardedThenAnswer("tc-2", "stopped because rejected"),
    });
    const thread = await agent.thread("t-resume-2");

    await expect(thread.send({ task: "Try the thing" })).rejects.toThrow(/suspended|signal/i);

    const out = await thread.resume!("tc-2", { approved: false, reason: "no thanks" });
    expect(await out.text).toBe("stopped because rejected");
    const [result] = await out.toolResults;
    expect(result).toMatchObject({ toolCallId: "tc-2", name: "guarded", failed: true });
  });

  it("throws ApprovalNotFoundError when the thread is not waiting on the call", async () => {
    const { agent } = buildAgent({ llmResponses: [{ content: "hi", finishReason: "stop" }] });
    const thread = await agent.thread("t-resume-3");
    await thread.send({ task: "Hi" });

    await expect(thread.resume!("nonexistent", { approved: true })).rejects.toBeInstanceOf(
      ApprovalNotFoundError,
    );
  });

  it("a second decision on the same approval answers ApprovalAlreadyDecidedError", async () => {
    const { agent } = buildAgent({ llmResponses: guardedThenAnswer("tc-4", "ok") });
    const thread = await agent.thread("t-resume-4");
    await expect(thread.send({ task: "go" })).rejects.toThrow(/suspended|signal/i);
    const out = await thread.resume!("tc-4", { approved: true, by: "bob" });
    expect(await out.text).toBe("ok");
    await expect(thread.resume!("tc-4", { approved: false, by: "mallory" })).rejects.toBeInstanceOf(
      ApprovalAlreadyDecidedError,
    );
  });

  it("an expired approval can be rejected but not approved", async () => {
    const clock = FakeWallClock.create(1_700_000_000_000);
    const { agent } = buildAgent({
      llmResponses: guardedThenAnswer("tc-5", "rejected late"),
      approvalTtlMs: 60_000,
      clock,
    });
    const thread = await agent.thread("t-resume-5");
    await expect(thread.send({ task: "go" })).rejects.toThrow(/suspended|signal/i);
    clock.advance(60_001);
    await expect(thread.resume!("tc-5", { approved: true })).rejects.toBeInstanceOf(
      ApprovalExpiredError,
    );
    const out = await thread.resume!("tc-5", { approved: false, reason: "expired" });
    expect(await out.text).toBe("rejected late");
  });

  it("finds the turn among >100 unrelated suspended workflows", async () => {
    const { agent, runner } = buildAgent({ llmResponses: guardedThenAnswer("tc-6", "found") });
    const waiter = workflow<{ n: number }>({ name: "unrelated" })
      .journaled("wait", function* (ctx) {
        return yield* ctx.signal("approve:tc-6");
      })
      .build();
    // Started first so they are older; a scan capped at 100 rows by
    // start time would also have missed the turn when started after.
    for (let i = 0; i < 110; i++) {
      await expect(
        runner.run({ workflow: waiter, workflowId: `t-resume-6-${i}`, input: { n: i } }),
      ).rejects.toThrow(/suspended|signal/i);
    }
    const thread = await agent.thread("t-resume-6");
    await expect(thread.send({ task: "go" })).rejects.toThrow(/suspended|signal/i);
    for (let i = 0; i < 110; i++) {
      await expect(
        runner.run({ workflow: waiter, workflowId: `t-resume-6-late-${i}`, input: { n: i } }),
      ).rejects.toThrow(/suspended|signal/i);
    }
    const out = await thread.resume!("tc-6", { approved: true });
    expect(await out.text).toBe("found");
  });

  it("keeps thread ids that prefix each other apart (t vs t-1)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const approvals = new InMemoryPendingApprovalStore();
    const make = (responses: LLMResponse[]) =>
      new LocalAgent({
        namespaceId: "acme",
        runner,
        approvals,
        agent: { name: "prefix", llm: mockLLM(responses), tools: { guarded } },
      });
    const longer = await make(guardedThenAnswer("tc-7", "from t-1")).thread("t-1");
    const shorter = await make(guardedThenAnswer("tc-7", "from t")).thread("t");
    await expect(longer.send({ task: "go" })).rejects.toThrow(/suspended|signal/i);

    // `t` has no pending approval, though `t-1` waits on the same call id.
    await expect(shorter.resume!("tc-7", { approved: true })).rejects.toBeInstanceOf(
      ApprovalNotFoundError,
    );
    await expect(shorter.send({ task: "go" })).rejects.toThrow(/suspended|signal/i);
    expect(await (await shorter.resume!("tc-7", { approved: true })).text).toBe("from t");
    expect(await (await longer.resume!("tc-7", { approved: true })).text).toBe("from t-1");
  });

  it("keeps two namespaces with the same thread id apart", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const approvals = new InMemoryPendingApprovalStore();
    const make = (namespaceId: string, responses: LLMResponse[]) =>
      new LocalAgent({
        namespaceId,
        runner,
        approvals,
        agent: { name: "ns", llm: mockLLM(responses), tools: { guarded } },
      });
    const acme = await make("acme", guardedThenAnswer("tc-8", "acme done")).thread("shared");
    const globex = await make("globex", guardedThenAnswer("tc-8", "globex done")).thread("shared");
    await expect(acme.send({ task: "go" })).rejects.toThrow(/suspended|signal/i);
    await expect(globex.resume!("tc-8", { approved: true })).rejects.toBeInstanceOf(
      ApprovalNotFoundError,
    );
    await expect(globex.send({ task: "go" })).rejects.toThrow(/suspended|signal/i);
    expect(await (await globex.resume!("tc-8", { approved: true })).text).toBe("globex done");
    expect(await (await acme.resume!("tc-8", { approved: true })).text).toBe("acme done");
  });

  it("refuses a new send while a turn waits, then lands the resumed output in order", async () => {
    const { agent, memory } = buildAgent({
      llmResponses: [
        ...guardedThenAnswer("tc-9", "first done"),
        { content: "second done", finishReason: "stop" },
      ],
    });
    const thread = await agent.thread("t-order");
    await expect(thread.send({ task: "first" })).rejects.toThrow(/suspended|signal/i);
    // The suspended turn has recorded its user message.
    const key = { namespaceId: "acme", resourceId: "alice", threadId: "t-order" };
    expect((await memory.getMessages(key)).map((m) => m.content)).toEqual(["first"]);

    await expect(thread.send({ task: "second" })).rejects.toBeInstanceOf(
      ThreadAwaitingApprovalError,
    );
    await thread.resume!("tc-9", { approved: true });
    await thread.send({ task: "second" });
    const stored = await memory.getMessages(key);
    expect(stored.map((m) => [m.role, m.content])).toEqual([
      ["user", "first"],
      ["assistant", null],
      ["tool", "ran: hello"],
      ["assistant", "first done"],
      ["user", "second"],
      ["assistant", "second done"],
    ]);
  });

  it("a resume that crashed after recording its decision is finished by resuming again", async () => {
    const { agent, approvals } = buildAgent({ llmResponses: guardedThenAnswer("tc-10", "ok") });
    const thread = await agent.thread("t-resume-10");
    await expect(thread.send({ task: "go" })).rejects.toThrow(/suspended|signal/i);
    // Simulate the crash: the decision is recorded, the turn never resumed.
    await approvals.decide({
      namespaceId: "acme",
      threadId: "t-resume-10",
      toolCallId: "tc-10",
      approved: true,
      decidedBy: "bob",
    });
    await expect(thread.resume!("tc-10", { approved: false })).rejects.toBeInstanceOf(
      ApprovalAlreadyDecidedError,
    );
    const out = await thread.resume!("tc-10", { approved: true, by: "bob" });
    expect(await out.text).toBe("ok");
  });
});
