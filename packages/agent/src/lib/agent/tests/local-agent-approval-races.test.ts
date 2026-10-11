// ---------------------------------------------------------------------------
// LocalAgent approvals at the edges of a turn's life:
//   - the window between recording an approval and suspending on it: a
//     concurrent send is refused (the live approval is not closed as
//     stale), and an approve sent in that window waits for the suspension
//   - a resume interrupted after recording its decision: the thread stays
//     blocked, the approval stays listed, resuming again finishes it
//   - an expired approval is closed as `expired` by the next send, which
//     finishes the old turn first so history stays in order
//   - an approval whose turn ended out of band no longer blocks the thread
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalAgent } from "../local-agent.ts";
import type { AgentRunOutput } from "../types.ts";
import { tool } from "../../tool.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import { InMemoryPendingApprovalStore } from "../../approvals/in-memory-pending-approval-store.ts";
import {
  ThreadAwaitingApprovalError,
  type PendingApproval,
  type RecordPendingApprovalParams,
} from "../../approvals/pending-approval-store.ts";

const guarded = tool({
  name: "guarded",
  description: "Requires approval before running",
  parameters: z.object({ x: z.string() }),
  execute: async ({ x }) => `ran: ${x}`,
  requireApproval: true,
});

function scripted(responses: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async () => {
      const r = responses[i++];
      if (!r) throw new Error("scripted LLM exhausted");
      return r;
    },
  };
}

const callGuarded: LLMResponse = {
  content: null,
  finishReason: "tool_calls",
  toolCalls: [{ id: "tc-1", name: "guarded", input: { x: "h" } }],
};

/** Runs `onRecorded` once, right after the approval row is written (before the turn suspends). */
class HookedApprovals extends InMemoryPendingApprovalStore {
  onRecorded?: () => Promise<void>;
  override async record(params: RecordPendingApprovalParams): Promise<PendingApproval> {
    const row = await super.record(params);
    const hook = this.onRecorded;
    this.onRecorded = undefined;
    if (hook) await hook();
    return row;
  }
}

/** Fails the next `failLocks` lock attempts with a transient error. */
class FlakyStorage extends InMemoryWorkflowStorage {
  failLocks = 0;
  override async tryLockAndLoad(
    params: Parameters<InMemoryWorkflowStorage["tryLockAndLoad"]>[0],
  ): ReturnType<InMemoryWorkflowStorage["tryLockAndLoad"]> {
    if (this.failLocks > 0) {
      this.failLocks--;
      throw new Error("transient storage error");
    }
    return super.tryLockAndLoad(params);
  }
}

function build(params: {
  responses: LLMResponse[];
  clock?: FakeWallClock;
  ttlMs?: number;
  storage?: InMemoryWorkflowStorage;
}) {
  const storage = params.storage ?? new InMemoryWorkflowStorage();
  const approvals = new HookedApprovals(params.clock ? { clock: params.clock } : {});
  const memory = new InMemoryMemoryStore();
  const agent = new LocalAgent({
    namespaceId: "acme",
    resourceId: "alice",
    runner: createWorkflowRunner({ storage }),
    memory,
    approvals,
    ...(params.ttlMs !== undefined && { approvalTtlMs: params.ttlMs }),
    agent: { name: "races", llm: scripted(params.responses), tools: { guarded } },
  });
  return { agent, approvals, memory, storage };
}

const KEY = { namespaceId: "acme", threadId: "t", toolCallId: "tc-1" };

async function history(memory: InMemoryMemoryStore): Promise<Array<[string, unknown]>> {
  return (await memory.getMessages({ namespaceId: "acme", threadId: "t" })).map((m) => [
    m.role,
    m.content,
  ]);
}

describe("LocalAgent approvals — record-before-suspend window", () => {
  it("refuses a concurrent send instead of closing the live approval", async () => {
    const { agent, approvals } = build({
      responses: [callGuarded, { content: "after approval", finishReason: "stop" }],
    });
    const first = await agent.thread("t");
    const second = await agent.thread("t");
    let concurrent: unknown;
    approvals.onRecorded = async () => {
      concurrent = await second.send({ task: "unrelated" }).catch((err: unknown) => err);
    };
    await expect(first.send({ task: "do it" })).rejects.toThrow(/suspend|signal/i);

    expect(concurrent).toBeInstanceOf(ThreadAwaitingApprovalError);
    expect((await approvals.get(KEY))?.status).toBe("pending");
    const out = await first.resume!("tc-1", { approved: true, by: "bob" });
    expect(await out.text).toBe("after approval");
  });

  it("an approve sent before the turn suspends waits for the suspension", async () => {
    const { agent, approvals } = build({
      responses: [callGuarded, { content: "done", finishReason: "stop" }],
    });
    const first = await agent.thread("t");
    const approver = await agent.thread("t");
    let early: Promise<AgentRunOutput> | undefined;
    approvals.onRecorded = async () => {
      // The approver reacts to approval.requested; not awaited by the turn.
      early = approver.resume!("tc-1", { approved: true, by: "bot" });
    };
    await expect(first.send({ task: "do it" })).rejects.toThrow(/suspend|signal/i);
    const out = await early!;
    expect(await out.text).toBe("done");
    expect(await approvals.get(KEY)).toMatchObject({ status: "approved", decidedBy: "bot" });
  });
});

describe("LocalAgent approvals — interrupted resume", () => {
  it("keeps the thread blocked and finishes the decision on the next resume", async () => {
    const { agent, approvals, memory } = build({
      responses: [
        callGuarded,
        { content: "old turn finished", finishReason: "stop" },
        { content: "new turn", finishReason: "stop" },
      ],
    });
    const thread = await agent.thread("t");
    await expect(thread.send({ task: "first" })).rejects.toThrow(/suspend|signal/i);
    // A resume recorded its decision, then crashed before delivering it.
    await approvals.decide({ ...KEY, approved: true, decidedBy: "bob" });

    expect((await thread.pendingApprovals!()).map((p) => [p.toolCallId, p.status])).toEqual([
      ["tc-1", "approved"],
    ]);
    await expect(thread.send({ task: "second" })).rejects.toBeInstanceOf(
      ThreadAwaitingApprovalError,
    );

    const finished = await thread.resume!("tc-1", { approved: true, by: "bob" });
    expect(await finished.text).toBe("old turn finished");
    expect(await thread.pendingApprovals!()).toEqual([]);
    await thread.send({ task: "second" });
    expect(await history(memory)).toEqual([
      ["user", "first"],
      ["assistant", null],
      ["tool", "ran: h"],
      ["assistant", "old turn finished"],
      ["user", "second"],
      ["assistant", "new turn"],
    ]);
  });
});

describe("LocalAgent approvals — resume interrupted before the turn consumes the decision", () => {
  it("leaves the approval open when the re-run fails before the turn consumes it", async () => {
    const storage = new FlakyStorage();
    const { agent, memory } = build({
      storage,
      responses: [
        callGuarded,
        { content: "old turn finished", finishReason: "stop" },
        { content: "new turn", finishReason: "stop" },
      ],
    });
    const thread = await agent.thread("t");
    await expect(thread.send({ task: "first" })).rejects.toThrow(/suspend|signal/i);

    // The decision is recorded and delivered, then re-running the turn
    // fails on a transient storage error before the turn consumes it.
    storage.failLocks = 1;
    await expect(thread.resume!("tc-1", { approved: true, by: "bob" })).rejects.toThrow(
      /transient storage error/,
    );
    expect((await thread.pendingApprovals!()).map((p) => [p.toolCallId, p.status])).toEqual([
      ["tc-1", "approved"],
    ]);
    await expect(thread.send({ task: "second" })).rejects.toBeInstanceOf(
      ThreadAwaitingApprovalError,
    );

    const finished = await thread.resume!("tc-1", { approved: true, by: "bob" });
    expect(await finished.text).toBe("old turn finished");
    await thread.send({ task: "second" });
    expect(await history(memory)).toEqual([
      ["user", "first"],
      ["assistant", null],
      ["tool", "ran: h"],
      ["assistant", "old turn finished"],
      ["user", "second"],
      ["assistant", "new turn"],
    ]);
  });
});

describe("LocalAgent approvals — expiry and ended turns", () => {
  it("the next send closes an expired approval as expired and finishes its turn first", async () => {
    const clock = FakeWallClock.create(1_700_000_000_000);
    const { agent, approvals, memory } = build({
      responses: [
        callGuarded,
        { content: "gave up on the tool", finishReason: "stop" },
        { content: "new turn", finishReason: "stop" },
      ],
      clock,
      ttlMs: 60_000,
    });
    const thread = await agent.thread("t");
    await expect(thread.send({ task: "first" })).rejects.toThrow(/suspend|signal/i);
    clock.advance(60_001);

    const out = await thread.send({ task: "second" });
    expect(await out.text).toBe("new turn");
    expect(await approvals.get(KEY)).toMatchObject({ status: "expired", decidedBy: "system" });
    expect((await approvals.get(KEY))?.deliveredAt).not.toBeNull();
    const roles = (await history(memory)).map(([role, content]) =>
      role === "tool" ? "tool" : `${role}:${String(content)}`,
    );
    expect(roles).toEqual([
      "user:first",
      "assistant:null",
      "tool",
      "assistant:gave up on the tool",
      "user:second",
      "assistant:new turn",
    ]);
  });

  it("an approval whose turn ended out of band no longer blocks the thread", async () => {
    const { agent, approvals, storage } = build({
      responses: [callGuarded, { content: "fresh", finishReason: "stop" }],
    });
    const thread = await agent.thread("t");
    await expect(thread.send({ task: "first" })).rejects.toThrow(/suspend|signal/i);
    const row = await approvals.get(KEY);
    await storage.failWorkflow({ workflowId: row!.workflowId, error: "cancelled by operator" });

    const out = await thread.send({ task: "second" });
    expect(await out.text).toBe("fresh");
    expect(await approvals.get(KEY)).toMatchObject({ status: "rejected", decidedBy: "system" });
  });
});
