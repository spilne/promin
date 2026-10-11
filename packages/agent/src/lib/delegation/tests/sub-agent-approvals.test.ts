// ---------------------------------------------------------------------------
// Approvals inside a sub-agent are carried up to the thread at the root:
//
//   - the child's approval is indexed under the ROOT thread, with an id
//     scoped to the child's run (child tool-call ids can repeat the
//     caller's), so `thread.pendingApprovals()` shows it and the thread
//     refuses a new turn until it is decided;
//   - the child's approval and tool events reach the root turn's stream;
//   - `thread.resume(id)` delivers the decision to the CHILD run, then
//     re-runs the ROOT turn, which re-attaches the child: the child's tool
//     runs once with the child's scope, the root turn finishes and commits
//     its transcript once;
//   - a rejection reaches the child the same way;
//   - a resume interrupted between delivering the decision and re-running
//     the root is finished by resuming again.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  type WorkflowRunner,
} from "@promin/workflow";
import { z } from "zod";
import { LocalAgent } from "../../agent/local-agent.ts";
import type { AgentEvent } from "../../agent/types.ts";
import { InMemoryPendingApprovalStore } from "../../approvals/in-memory-pending-approval-store.ts";
import { ThreadAwaitingApprovalError } from "../../approvals/pending-approval-store.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import { lastMessage, reactiveLLM } from "../../tests/engine-harness.ts";
import type { AgentTool, ToolScope } from "../../tool.ts";
import { subAgentTool } from "../sub-agent-tool.ts";

function setup(params?: { runner?: (runner: WorkflowRunner) => WorkflowRunner }) {
  const storage = new InMemoryWorkflowStorage();
  const baseRunner = createWorkflowRunner({ storage });
  const runner = params?.runner ? params.runner(baseRunner) : baseRunner;
  const memory = new InMemoryMemoryStore();
  const approvals = new InMemoryPendingApprovalStore();
  const runs: Array<ToolScope | undefined> = [];
  const guarded: AgentTool<{ what: string }, string> = {
    name: "guarded",
    description: "needs approval",
    parameters: z.object({ what: z.string() }),
    requireApproval: true,
    execute: async ({ what }, ctx) => {
      runs.push(ctx?.scope);
      return `guarded ran: ${what}`;
    },
  };
  // The child's tool call reuses the caller's id ("c1") on purpose.
  const helperLlm = reactiveLLM((messages) => {
    const last = lastMessage(messages);
    return last?.role === "tool"
      ? { content: `helper: ${last.content}`, finishReason: "stop" }
      : {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "c1", name: "guarded", input: { what: "deploy" } }],
        };
  });
  const leadLlm = reactiveLLM((messages) => {
    const last = lastMessage(messages);
    return last?.role === "tool"
      ? { content: `lead: ${last.content}`, finishReason: "stop" }
      : {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "c1", name: "helper", input: { task: "deploy it" } }],
        };
  });
  const helper = subAgentTool({
    name: "helper",
    description: "deploys",
    agent: { name: "helper", llm: helperLlm, tools: { guarded } },
  });
  const agent = new LocalAgent({
    namespaceId: "acme",
    resourceId: "alice",
    runner,
    memory,
    approvals,
    agent: { name: "lead", llm: leadLlm, tools: { helper } },
  });
  return { agent, storage, memory, approvals, runs, leadLlm, helperLlm };
}

/** Start a turn that suspends on the sub-agent's approval; its events and the open row. */
async function suspendOnChildApproval(h: ReturnType<typeof setup>) {
  const thread = await h.agent.thread("th");
  const out = thread.stream({ task: "ship it" });
  // The turn suspends: every view of its output rejects with the suspension.
  for (const view of [out.text, out.output, out.toolCalls, out.toolResults, out.steps]) {
    view.catch(() => undefined);
  }
  for (const view of [out.usage, out.finishReason, out.messages]) view.catch(() => undefined);
  const events: AgentEvent[] = [];
  try {
    for await (const e of out.fullStream) events.push(e);
  } catch {
    // The stream ends with the suspension.
  }
  const [pending] = await thread.pendingApprovals!();
  return { thread, events, pending: pending! };
}

describe("subAgentTool — approvals carried up to the root thread", () => {
  it("indexes the child's approval under the root thread and resumes the whole chain", async () => {
    const h = setup();
    const { thread, events, pending } = await suspendOnChildApproval(h);

    const child = await h.storage.loadWorkflow(pending.workflowId);
    expect(child?.parentWorkflowId).toBeDefined();
    expect(child?.status).toBe("suspended");
    expect(pending).toMatchObject({
      namespaceId: "acme",
      threadId: "th",
      toolCallId: `${pending.workflowId}:c1`,
      toolName: "guarded",
      input: { what: "deploy" },
      status: "pending",
    });
    // The root turn's stream saw the child's approval request, under the same id.
    expect(events).toContainEqual(
      expect.objectContaining({ type: "approval-requested", toolCallId: pending.toolCallId }),
    );
    // The thread waits for the decision.
    await expect(thread.send({ task: "next" })).rejects.toBeInstanceOf(ThreadAwaitingApprovalError);

    const out = await thread.resume!(pending.toolCallId, { approved: true, by: "bob" });

    expect(await out.text).toBe("lead: helper: guarded ran: deploy");
    expect(h.runs).toHaveLength(1);
    expect(h.runs[0]).toMatchObject({ namespaceId: "acme", threadId: "th", agentId: "helper" });
    expect(await thread.pendingApprovals!()).toEqual([]);
    expect(h.leadLlm.calls).toHaveLength(2);
    expect(h.helperLlm.calls).toHaveLength(2);
    const history = await h.memory.getMessages({
      namespaceId: "acme",
      resourceId: "alice",
      threadId: "th",
    });
    expect(history.map((m) => m.role)).toEqual(["user", "assistant", "tool", "assistant"]);
    expect(history.at(-1)?.content).toBe("lead: helper: guarded ran: deploy");
  });

  it("delivers a rejection to the child", async () => {
    const h = setup();
    const { thread, pending } = await suspendOnChildApproval(h);

    const out = await thread.resume!(pending.toolCallId, { approved: false, reason: "no" });

    expect(await out.text).toBe("lead: helper: Rejected: no");
    expect(h.runs).toHaveLength(0);
    expect(await thread.pendingApprovals!()).toEqual([]);
  });

  it("finishes a resume interrupted after the decision reached the child", async () => {
    let failRoot: string | undefined;
    const h = setup({
      runner: (base) =>
        new Proxy(base, {
          get(target, prop, receiver) {
            if (prop !== "run") return Reflect.get(target, prop, receiver);
            return (params: Parameters<WorkflowRunner["run"]>[0]) => {
              if (failRoot !== undefined && params.workflowId === failRoot) {
                failRoot = undefined;
                return Promise.reject(new Error("process died"));
              }
              return target.run(params);
            };
          },
        }),
    });
    const { thread, pending } = await suspendOnChildApproval(h);
    const child = await h.storage.loadWorkflow(pending.workflowId);
    failRoot = child!.parentWorkflowId;

    await expect(thread.resume!(pending.toolCallId, { approved: true })).rejects.toThrow(
      "process died",
    );
    // Decided, delivered to the child, but the root never ran: still open.
    const [open] = await thread.pendingApprovals!();
    expect(open).toMatchObject({ toolCallId: pending.toolCallId, status: "approved" });

    const out = await thread.resume!(pending.toolCallId, { approved: true });
    expect(await out.text).toBe("lead: helper: guarded ran: deploy");
    expect(h.runs).toHaveLength(1);
    expect(await thread.pendingApprovals!()).toEqual([]);
  });
});
