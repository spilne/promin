// ---------------------------------------------------------------------------
// Lifecycle of an approval carried up from a sub-agent, judged by the ROOT
// turn of the thread, never by the child run alone:
//
//   - while a resume re-runs the root, the child may already have ended:
//     a concurrent send is still refused until the root has committed;
//   - a child ended out of band while the root waits on it: the next send
//     lets the root finish first, then goes on;
//   - a root ended out of band (cancelled without cascade): the approval is
//     closed and the orphaned child cancelled, so the thread moves on;
//   - two sub-agents waiting in parallel: approving one after the other
//     finishes the turn and leaves no open approval, also when the two
//     resumes race;
//   - a transient error loading the child on resume does not cancel it: a
//     second resume finishes the turn.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { z } from "zod";
import { agentAction } from "../../agent-action.ts";
import { LocalAgent } from "../../agent/local-agent.ts";
import { InMemoryPendingApprovalStore } from "../../approvals/in-memory-pending-approval-store.ts";
import { ThreadAwaitingApprovalError } from "../../approvals/pending-approval-store.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import type { Message, ToolCall } from "../../message.ts";
import { deferred, lastMessage, reactiveLLM, until } from "../../tests/engine-harness.ts";
import type { AgentTool } from "../../tool.ts";
import { subAgentTool } from "../sub-agent-tool.ts";

const KEY = { namespaceId: "acme", resourceId: "alice", threadId: "th" };

function lastContent(messages: ReadonlyArray<Message>): string {
  const last = lastMessage(messages);
  return typeof last?.content === "string" ? last.content : "";
}

function setup(params?: {
  leadCalls?: ToolCall[];
  /** Replaces the lead's final answer (e.g. to hold it on a gate). */
  leadAnswer?: (messages: ReadonlyArray<Message>) => LLMResponse | Promise<LLMResponse>;
  helper?: (tools: Record<string, AgentTool<any, any>>) => AgentTool<any, any>;
}) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const memory = new InMemoryMemoryStore();
  const approvals = new InMemoryPendingApprovalStore();
  const runs: string[] = [];
  const guarded: AgentTool<{ what: string }, string> = {
    name: "guarded",
    description: "needs approval",
    parameters: z.object({ what: z.string() }),
    requireApproval: true,
    execute: async ({ what }) => {
      runs.push(what);
      return `ran ${what}`;
    },
  };
  const helperLlm: LLMProvider = reactiveLLM((messages) =>
    lastMessage(messages)?.role === "tool"
      ? { content: `helper: ${lastContent(messages)}`, finishReason: "stop" }
      : {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "g1", name: "guarded", input: { what: lastContent(messages) } }],
        },
  );
  const helper = params?.helper
    ? params.helper({ guarded })
    : subAgentTool({
        name: "helper",
        description: "h",
        agent: { name: "helper", llm: helperLlm, tools: { guarded } },
      });
  const leadLlm = reactiveLLM((messages) => {
    if (lastMessage(messages)?.role === "tool") {
      if (params?.leadAnswer) return params.leadAnswer(messages);
      const results = messages.filter((m) => m.role === "tool").map((m) => m.content);
      return { content: `lead: ${results.join(" | ")}`, finishReason: "stop" };
    }
    if (lastContent(messages) === "next") return { content: "next answer", finishReason: "stop" };
    return {
      content: null,
      finishReason: "tool_calls",
      toolCalls: params?.leadCalls ?? [{ id: "c1", name: "helper", input: { task: "A" } }],
    };
  });
  const agent = new LocalAgent({
    namespaceId: "acme",
    resourceId: "alice",
    runner,
    memory,
    approvals,
    agent: { name: "lead", llm: leadLlm, tools: { helper } },
  });
  return { storage, runner, memory, approvals, runs, agent, helperLlm, guarded };
}

async function suspended(h: ReturnType<typeof setup>) {
  const thread = await h.agent.thread("th");
  await expect(thread.send({ task: "go" })).rejects.toThrow(/suspended|waiting/i);
  return thread;
}

async function history(h: ReturnType<typeof setup>): Promise<string[]> {
  return (await h.memory.getMessages(KEY)).map((m) => `${m.role}:${String(m.content ?? "")}`);
}

describe("sub-agent approvals — judged by the root turn", () => {
  it("refuses a send while the resumed root is still finishing after its child ended", async () => {
    const gate = deferred();
    let finishing = false;
    const h = setup({
      leadAnswer: async (messages) => {
        finishing = true;
        await gate.promise;
        return { content: `lead: ${lastContent(messages)}`, finishReason: "stop" };
      },
    });
    const thread = await suspended(h);
    const [row] = await thread.pendingApprovals!();

    const resuming = thread.resume!(row!.toolCallId, { approved: true }).then((o) => o.text);
    await until(() => finishing);
    expect((await h.storage.loadWorkflow(row!.workflowId))?.status).toBe("completed");
    await expect(thread.send({ task: "next" })).rejects.toBeInstanceOf(ThreadAwaitingApprovalError);

    gate.resolve();
    expect(await resuming).toBe("lead: helper: ran A");
    const out = await thread.send({ task: "next" });
    expect(await out.text).toBe("next answer");
    expect(await history(h)).toEqual([
      "user:go",
      "assistant:",
      "tool:helper: ran A",
      "assistant:lead: helper: ran A",
      "user:next",
      "assistant:next answer",
    ]);
  });

  it("lets the root finish first when its child ended out of band", async () => {
    const h = setup();
    const thread = await suspended(h);
    const [row] = await thread.pendingApprovals!();
    const child = await h.storage.loadWorkflow(row!.workflowId);
    await h.storage.cancelWorkflow({ workflowId: row!.workflowId, cascade: true });

    const out = await thread.send({ task: "next" });

    expect(await out.text).toBe("next answer");
    expect((await h.storage.loadWorkflow(child!.parentWorkflowId!))?.status).toBe("completed");
    expect(await thread.pendingApprovals!()).toEqual([]);
    const lines = await history(h);
    expect(lines[0]).toBe("user:go");
    expect(lines.at(-2)).toBe("user:next");
    expect(h.runs).toEqual([]);
  });

  it("closes the approval and cancels the orphaned child when the root ended out of band", async () => {
    const h = setup();
    const thread = await suspended(h);
    const [row] = await thread.pendingApprovals!();
    const child = await h.storage.loadWorkflow(row!.workflowId);
    await h.storage.cancelWorkflow({ workflowId: child!.parentWorkflowId!, cascade: false });

    await expect(thread.resume!(row!.toolCallId, { approved: true })).rejects.toThrow(
      /No pending approval/,
    );
    expect(await thread.pendingApprovals!()).toEqual([]);
    const childAfter = await h.storage.loadWorkflow(row!.workflowId);
    expect(childAfter?.status).toBe("failed");
    const out = await thread.send({ task: "next" });
    expect(await out.text).toBe("next answer");
    expect(h.runs).toEqual([]);
  });

  it("finishes two parallel sub-agent approvals decided one after the other", async () => {
    const h = setup({
      leadCalls: [
        { id: "a", name: "helper", input: { task: "A" } },
        { id: "b", name: "helper", input: { task: "B" } },
      ],
    });
    const thread = await suspended(h);
    const rows = await thread.pendingApprovals!();
    expect(rows).toHaveLength(2);

    await thread.resume!(rows[0]!.toolCallId, { approved: true }).catch(() => undefined);
    const out = await thread.resume!(rows[1]!.toolCallId, { approved: false, reason: "no" });

    expect(await out.text).toBe("lead: helper: ran A | helper: Rejected: no");
    expect(await thread.pendingApprovals!()).toEqual([]);
    expect(h.runs).toEqual(["A"]);
  });

  it("leaves no open approval when two resumes of parallel sub-agents race", async () => {
    const h = setup({
      leadCalls: [
        { id: "a", name: "helper", input: { task: "A" } },
        { id: "b", name: "helper", input: { task: "B" } },
      ],
    });
    const thread = await suspended(h);
    const rows = await thread.pendingApprovals!();

    await Promise.allSettled(
      rows.map((r) => thread.resume!(r.toolCallId, { approved: true }).then((o) => o.text)),
    );

    // Whatever the race left behind is settled by the next send.
    const out = await thread.send({ task: "next" });
    expect(await out.text).toBe("next answer");
    expect(await thread.pendingApprovals!()).toEqual([]);
    expect([...h.runs].sort()).toEqual(["A", "B"]);
  });

  it("does not cancel the child on a transient load error; a second resume finishes", async () => {
    let failNext = false;
    let loads = 0;
    const h = setup({
      helper: (tools) =>
        subAgentTool({
          name: "helper",
          description: "h",
          parameters: z.object({ task: z.string() }),
          route: async ({ input }) => ({
            ok: true,
            route: { agentId: "helper", input: { task: input.task } },
          }),
          load: async ({ signal, wiring }) => {
            loads++;
            if (failNext) {
              failNext = false;
              throw new Error("ECONNRESET registry");
            }
            return agentAction({
              name: "helper",
              llm: reactiveLLM((messages) =>
                lastMessage(messages)?.role === "tool"
                  ? { content: `helper: ${lastContent(messages)}`, finishReason: "stop" }
                  : {
                      content: null,
                      finishReason: "tool_calls",
                      toolCalls: [{ id: "g1", name: "guarded", input: { what: "A" } }],
                    },
              ),
              tools: tools,
              signal,
              ...(wiring.bus && { bus: wiring.bus }),
              ...(wiring.approvalIndex && { approvalIndex: wiring.approvalIndex }),
            });
          },
        }),
    });
    const thread = await suspended(h);
    const [row] = await thread.pendingApprovals!();

    failNext = true;
    await expect(thread.resume!(row!.toolCallId, { approved: true })).rejects.toThrow("ECONNRESET");
    expect((await h.storage.loadWorkflow(row!.workflowId))?.status).toBe("suspended");
    expect(await thread.pendingApprovals!()).toHaveLength(1);

    const out = await thread.resume!(row!.toolCallId, { approved: true });
    expect(await out.text).toBe("lead: helper: ran A");
    expect(h.runs).toEqual(["A"]);
    expect(loads).toBe(3);
    expect(await thread.pendingApprovals!()).toEqual([]);
  });
});
