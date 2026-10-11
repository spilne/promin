// ---------------------------------------------------------------------------
// subAgentTool — a sub-agent call runs as a journaled child workflow of the
// calling turn:
//
//   - the child's id is `<caller workflow id>.<toolCallId>`, it is linked
//     to the caller by `parentWorkflowId`, and its input carries the
//     delegation frame (depth + chain);
//   - a crash of the caller while the child runs re-attaches to the same
//     child on recovery: one child run, each child LLM step called once;
//   - cancelling the caller stops the child; a time limit stops the child
//     and fails the call; a failed child is a tool error;
//   - depth and cycle checks hold across hops;
//   - the tool is never run through `execute`, and works in `agentLoop` too.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  ChildWorkflowGoneError,
  FakeWallClock,
  completeSignal,
  isCancelledRun,
} from "@promin/workflow";
import { z } from "zod";
import { agentAction, type AgentActionConfig } from "../../agent-action.ts";
import { agentLoop } from "../../agent-loop.ts";
import type { LLMResponse } from "../../llm-provider.ts";
import type { Message, ToolResultMessage } from "../../message.ts";
import type { AgentTool } from "../../tool.ts";
import { CHILD_TRACE_META_KEY } from "../../trace.ts";
import {
  crashAndRecover,
  deferred,
  freshRunner,
  lastMessage,
  reactiveLLM,
  scriptedLLM,
  until,
} from "../../tests/engine-harness.ts";
import { subAgentTool } from "../sub-agent-tool.ts";

/** Delegates `task` through `tool` once, then answers with the tool result. */
function delegatingLLM(params: { tool: string; input: unknown; id?: string }) {
  return reactiveLLM((messages) => {
    const last = lastMessage(messages);
    return last?.role === "tool"
      ? { content: `lead got: ${last.content}`, finishReason: "stop" }
      : {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: params.id ?? "call-1", name: params.tool, input: params.input }],
        };
  });
}

function toolResultOf(messages: ReadonlyArray<Message>): ToolResultMessage | undefined {
  return messages.find((m): m is ToolResultMessage => m.role === "tool");
}

const answer = (content: string): LLMResponse => ({ content, finishReason: "stop" });

describe("subAgentTool — child workflow", () => {
  it("runs the sub-agent as a child run linked to the caller, then replays it from the journal", async () => {
    const { runner, storage } = freshRunner();
    const helperLlm = scriptedLLM([answer("helper answer")]);
    const helper = subAgentTool({
      name: "helper",
      description: "helps",
      agent: { name: "helper", llm: helperLlm },
    });
    const leadLlm = delegatingLLM({ tool: "helper", input: { task: "sum it up" } });
    const lead = agentAction({ name: "lead", llm: leadLlm, tools: { helper } });

    const result = await runner.run({
      workflow: lead,
      workflowId: "lead-1",
      input: { task: "go" },
    });

    expect(result.answer).toBe("lead got: helper answer");
    const toolResult = toolResultOf(result.messages);
    expect(toolResult?.content).toBe("helper answer");
    expect(toolResult?.metadata?.[CHILD_TRACE_META_KEY]).toBeDefined();
    const child = await storage.loadWorkflow("lead-1.0.call-1");
    expect(child?.parentWorkflowId).toBe("lead-1");
    expect(child?.workflowName).toBe("helper");
    expect(child?.input).toEqual({ task: "sum it up", delegation: { depth: 1, chain: ["lead"] } });
    expect(helperLlm.calls[0]?.messages.at(-1)).toEqual({ role: "user", content: "sum it up" });

    // Replay: the child's outcome is journaled, its LLM is not called again.
    const again = await runner.run({ workflow: lead, workflowId: "lead-1", input: { task: "go" } });
    expect(again.answer).toBe(result.answer);
    expect(helperLlm.calls).toHaveLength(1);
    expect(await storage.listWorkflows({ parentId: "lead-1" })).toHaveLength(1);
  });

  it("re-attaches to the same child after a crash: one child run, each child step's LLM called once", async () => {
    const gate = deferred();
    let reads = 0;
    const read: AgentTool<{ path: string }, string> = {
      name: "read",
      description: "read a file",
      parameters: z.object({ path: z.string() }),
      idempotent: true,
      execute: async () => {
        reads++;
        // Worker A stalls here, inside the child's tool call, and loses its lease.
        if (reads === 1) await gate.promise;
        return "file contents";
      },
    };
    const helperLlm = reactiveLLM((messages) =>
      lastMessage(messages)?.role === "tool"
        ? answer("summary of the file")
        : {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "read-1", name: "read", input: { path: "a.txt" } }],
          },
    );
    const leadLlm = delegatingLLM({ tool: "helper", input: { task: "read a.txt" } });
    const build = () =>
      agentAction({
        name: "lead",
        llm: leadLlm,
        tools: {
          helper: subAgentTool({
            name: "helper",
            description: "reads files",
            agent: { name: "helper", llm: helperLlm, tools: { read } },
          }),
        },
      });

    const { result, storage } = await crashAndRecover({
      build,
      input: { task: "go" },
      workflowId: "lead-crash",
      stalled: () => reads === 1,
      release: () => gate.resolve(),
    });

    expect(result.answer).toBe("lead got: summary of the file");
    // One child run, resumed by the recovering worker.
    const children = await storage.listWorkflows({ parentId: "lead-crash" });
    expect(children.map((c) => c.workflowId)).toEqual(["lead-crash.0.call-1"]);
    expect(children[0]?.status).toBe("completed");
    // Each LLM step ran once: lead think-0 / think-1, helper think-0 / think-1.
    expect(leadLlm.calls).toHaveLength(2);
    expect(helperLlm.calls).toHaveLength(2);
    // The idempotent tool inside the child ran again on the recovering worker.
    expect(reads).toBe(2);
  });

  it("stops the child when the caller's turn is cancelled", async () => {
    const { runner, storage } = freshRunner();
    const controller = new AbortController();
    let helperCalls = 0;
    const helperLlm = reactiveLLM(
      (_messages, params) =>
        new Promise<LLMResponse>((_resolve, reject) => {
          helperCalls++;
          params.signal?.addEventListener("abort", () => reject(params.signal?.reason), {
            once: true,
          });
        }),
    );
    const leadLlm = delegatingLLM({ tool: "helper", input: { task: "work slowly" } });
    const lead = agentAction({
      name: "lead",
      llm: leadLlm,
      signal: controller.signal,
      tools: {
        helper: subAgentTool({
          name: "helper",
          description: "slow",
          agent: { name: "helper", llm: helperLlm, llmRetry: false },
        }),
      },
    });

    const running = runner.run({ workflow: lead, workflowId: "lead-c", input: { task: "go" } });
    await until(() => helperCalls === 1);
    controller.abort();
    const result = await running;

    expect(result.finishReason).toBe("cancelled");
    const child = await storage.loadWorkflow("lead-c.0.call-1");
    expect(child?.status).toBe("completed");
    expect((child?.result as { finishReason?: string } | undefined)?.finishReason).toBe(
      "cancelled",
    );
    const toolResult = toolResultOf(result.messages);
    expect(toolResult?.isError).toBe(true);
    expect(toolResult?.content).toContain("cancelled");
    expect(leadLlm.calls).toHaveLength(1);
  });

  it("stops a child that runs past the time limit and fails the call", async () => {
    const { runner } = freshRunner();
    const clock = FakeWallClock.create(0);
    let helperCalls = 0;
    const helperLlm = reactiveLLM(
      (_messages, params) =>
        new Promise<LLMResponse>((_resolve, reject) => {
          helperCalls++;
          params.signal?.addEventListener("abort", () => reject(params.signal?.reason), {
            once: true,
          });
        }),
    );
    const leadLlm = delegatingLLM({ tool: "helper", input: { task: "never ends" } });
    const lead = agentAction({
      name: "lead",
      llm: leadLlm,
      clock,
      tools: {
        helper: subAgentTool({
          name: "helper",
          description: "slow",
          timeoutMs: 5_000,
          agent: { name: "helper", llm: helperLlm, llmRetry: false },
        }),
      },
    });

    const running = runner.run({ workflow: lead, workflowId: "lead-t", input: { task: "go" } });
    await until(() => helperCalls === 1 && clock.pendingCount() > 0);
    clock.advance(5_000);
    const result = await running;

    // The caller goes on: the timed-out call is a tool error, the turn answers.
    expect(result.finishReason).toBe("stop");
    const toolResult = toolResultOf(result.messages);
    expect(toolResult?.isError).toBe(true);
    expect(toolResult?.content).toContain("5000ms time limit");
  });

  it("reports a failed child to the caller's model as a tool error", async () => {
    const { runner, storage } = freshRunner();
    const helperLlm = reactiveLLM(() => {
      throw new Error("model exploded");
    });
    const leadLlm = delegatingLLM({ tool: "helper", input: { task: "try" } });
    const lead = agentAction({
      name: "lead",
      llm: leadLlm,
      tools: {
        helper: subAgentTool({
          name: "helper",
          description: "fragile",
          agent: { name: "helper", llm: helperLlm, llmRetry: false },
        }),
      },
    });

    const result = await runner.run({
      workflow: lead,
      workflowId: "lead-f",
      input: { task: "go" },
    });

    expect(result.finishReason).toBe("stop");
    const toolResult = toolResultOf(result.messages);
    expect(toolResult?.isError).toBe(true);
    expect(toolResult?.content).toContain('Sub-agent "helper" failed');
    expect(toolResult?.content).toContain("model exploded");
    expect((await storage.loadWorkflow("lead-f.0.call-1"))?.status).toBe("failed");
  });

  it("refuses a call deeper than maxDepth and a call back into an agent on the chain", async () => {
    const { runner, storage } = freshRunner();
    // a → b → a: b's call back into a is a cycle. b → c at depth 2 with
    // maxDepth 1 is refused for depth.
    const tools: Record<string, Record<string, AgentTool<any, any>>> = { a: {}, b: {} };
    const engine = (name: string, llm: AgentActionConfig["llm"]): AgentActionConfig => ({
      name,
      llm,
      tools: tools[name] ?? {},
    });
    const aLlm = delegatingLLM({ tool: "callB", input: { task: "b, do it" } });
    const bLlm = reactiveLLM((messages) => {
      const results = messages.filter((m) => m.role === "tool");
      if (results.length === 2) {
        return answer(results.map((m) => m.content).join(" | "));
      }
      return {
        content: null,
        finishReason: "tool_calls",
        toolCalls:
          results.length === 0
            ? [{ id: "back", name: "callA", input: { task: "a again" } }]
            : [{ id: "deeper", name: "callC", input: { task: "c, go" } }],
      };
    });
    const cLlm = scriptedLLM([answer("c answer")]);
    Object.assign(tools.a!, {
      callB: subAgentTool({ name: "callB", description: "b", agent: engine("b", bLlm) }),
    });
    Object.assign(tools.b!, {
      callA: subAgentTool({ name: "callA", description: "a", agent: engine("a", aLlm) }),
      callC: subAgentTool({
        name: "callC",
        description: "c",
        maxDepth: 1,
        agent: engine("c", cLlm),
      }),
    });

    const result = await runner.run({
      workflow: agentAction({ name: "a", llm: aLlm, tools: tools.a ?? {} }),
      workflowId: "chain-1",
      input: { task: "start" },
    });

    expect(result.answer).toContain("cycle");
    expect(result.answer).toContain("a → b → a");
    expect(result.answer).toContain("depth 2 exceeds the limit of 1");
    expect(cLlm.calls).toHaveLength(0);
    expect(aLlm.calls).toHaveLength(2);
    expect((await storage.listWorkflows({ parentId: "chain-1.0.call-1" })).length).toBe(0);
  });

  it("cannot be run outside an agent engine", async () => {
    const helper = subAgentTool({
      name: "helper",
      description: "helps",
      agent: { name: "helper", llm: scriptedLLM([]) },
    });
    expect(helper.idempotent).toBe(true);
    await expect(helper.execute({ task: "x" })).rejects.toThrow("durable agent turn");
  });
});

describe("subAgentTool — agentLoop", () => {
  it("runs the sub-agent as a child run of the session", async () => {
    const { runner, storage } = freshRunner();
    const helperLlm = scriptedLLM([answer("from helper")]);
    const loop = agentLoop({
      name: "loop-lead",
      llm: delegatingLLM({ tool: "helper", input: { task: "help me" } }),
      tools: {
        helper: subAgentTool({
          name: "helper",
          description: "helps",
          agent: { name: "helper", llm: helperLlm },
        }),
      },
    });
    const session = await loop.session({ runner, sessionId: "loop-1" });
    try {
      expect(await session.send("go")).toBe("lead got: from helper");
    } finally {
      await session.close();
    }
    const child = await storage.loadWorkflow("loop-1.0.0.call-1");
    expect(child?.parentWorkflowId).toBe("loop-1");
    expect(helperLlm.calls).toHaveLength(1);
  });
});

describe("subAgentTool — ids, frames, deadlines and re-attach", () => {
  const echoHelper = () =>
    reactiveLLM((messages) => answer(`echo:${String(lastMessage(messages)?.content ?? "")}`));

  it("gives each call its own child when the provider repeats or omits tool-call ids", async () => {
    const { runner, storage } = freshRunner();
    const responses: LLMResponse[] = [
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "c", name: "helper", input: { task: "A" } }],
      },
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "c", name: "helper", input: { task: "B" } }],
      },
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [
          { id: "", name: "helper", input: { task: "C" } },
          { id: "", name: "helper", input: { task: "D" } },
        ],
      },
      answer("done"),
    ];
    const lead = agentAction({
      name: "lead",
      llm: scriptedLLM(responses),
      tools: {
        helper: subAgentTool({
          name: "helper",
          description: "h",
          agent: { name: "helper", llm: echoHelper() },
        }),
      },
    });

    const result = await runner.run({ workflow: lead, workflowId: "ids", input: { task: "go" } });

    const results = result.messages.filter((m): m is ToolResultMessage => m.role === "tool");
    expect(results.map((m) => m.content)).toEqual(["echo:A", "echo:B", "echo:C", "echo:D"]);
    expect(new Set(results.map((m) => m.toolCallId)).size).toBe(4);
    expect((await storage.listWorkflows({ parentId: "ids" })).length).toBe(4);
  });

  it("fails a run whose input carries a malformed delegation frame", async () => {
    for (const delegation of [
      { depth: "x", chain: [] },
      { depth: -100, chain: [] },
      { depth: 0, chain: "lead" },
      { depth: 2, chain: ["only-one"] },
    ]) {
      const { runner } = freshRunner();
      const llm = scriptedLLM([answer("never")]);
      const lead = agentAction({ name: "lead", llm });
      await expect(
        runner.run({
          workflow: lead,
          workflowId: "forged",
          input: { task: "go", delegation: delegation as never },
        }),
      ).rejects.toThrow("invalid delegation frame");
      expect(llm.calls).toHaveLength(0);
    }
  });

  it("keeps the time limit from the call's journaled start across a suspension", async () => {
    const { runner, storage } = freshRunner();
    const clock = FakeWallClock.create(0);
    let runs = 0;
    const guarded: AgentTool<Record<string, never>, string> = {
      name: "guarded",
      description: "needs approval",
      parameters: z.object({}),
      requireApproval: true,
      execute: async () => {
        runs++;
        return "ran";
      },
    };
    const helperLlm = reactiveLLM((messages) =>
      lastMessage(messages)?.role === "tool"
        ? answer("helper done")
        : {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "g1", name: "guarded", input: {} }],
          },
    );
    const lead = agentAction({
      name: "lead",
      llm: delegatingLLM({ tool: "helper", input: { task: "t" } }),
      clock,
      tools: {
        helper: subAgentTool({
          name: "helper",
          description: "h",
          timeoutMs: 5_000,
          agent: { name: "helper", llm: helperLlm, tools: { guarded }, clock },
        }),
      },
    });
    await expect(
      runner.run({ workflow: lead, workflowId: "dl", input: { task: "go" } }),
    ).rejects.toThrow(/suspended|waiting/i);
    const childId = "dl.0.call-1";
    await completeSignal({
      storage,
      workflowId: childId,
      stepName: "agent",
      signalName: "approve:g1",
      value: { approved: true },
    });

    // The approval came after the limit: the resumed child is out of time.
    clock.set(6_000);
    const result = await runner.run({ workflow: lead, workflowId: "dl", input: { task: "go" } });

    expect(runs).toBe(0);
    const toolResult = toolResultOf(result.messages);
    expect(toolResult?.isError).toBe(true);
    expect(toolResult?.content).toContain("5000ms time limit");
  });

  it("fails the call and cancels the child when its callee changed before a re-attach", async () => {
    const { runner, storage } = freshRunner();
    let gone = false;
    const guarded: AgentTool<Record<string, never>, string> = {
      name: "guarded",
      description: "needs approval",
      parameters: z.object({}),
      requireApproval: true,
      execute: async () => "ran",
    };
    const helperLlm = reactiveLLM(() => ({
      content: null,
      finishReason: "tool_calls",
      toolCalls: [{ id: "g1", name: "guarded", input: {} }],
    }));
    const helper = subAgentTool({
      name: "helper",
      description: "h",
      parameters: z.object({ task: z.string() }),
      route: async ({ input }) => ({
        ok: true,
        route: { agentId: "helper", input: { task: input.task }, data: { version: "v1" } },
      }),
      load: async ({ signal }) => {
        if (gone) throw new ChildWorkflowGoneError({ message: "agent_version_gone: helper v1" });
        return agentAction({ name: "helper", llm: helperLlm, tools: { guarded }, signal });
      },
    });
    const lead = agentAction({
      name: "lead",
      llm: delegatingLLM({ tool: "helper", input: { task: "t" } }),
      tools: { helper },
    });
    await expect(
      runner.run({ workflow: lead, workflowId: "gone", input: { task: "go" } }),
    ).rejects.toThrow(/suspended|waiting/i);

    gone = true;
    const result = await runner.run({ workflow: lead, workflowId: "gone", input: { task: "go" } });

    const toolResult = toolResultOf(result.messages);
    expect(toolResult?.isError).toBe(true);
    expect(toolResult?.content).toContain("agent_version_gone");
    const child = await storage.loadWorkflow("gone.0.call-1");
    expect(isCancelledRun(child!)).toBe(true);
  });

  it("fails the turn, not the call, when routing hits an infrastructure error", async () => {
    const { runner } = freshRunner();
    const helper = subAgentTool({
      name: "helper",
      description: "h",
      parameters: z.object({ task: z.string() }),
      route: async () => {
        throw new Error("registry unreachable");
      },
      load: async () => {
        throw new Error("never loaded");
      },
    });
    const llm = delegatingLLM({ tool: "helper", input: { task: "t" } });
    const lead = agentAction({ name: "lead", llm, tools: { helper } });

    await expect(
      runner.run({ workflow: lead, workflowId: "infra", input: { task: "go" } }),
    ).rejects.toThrow("registry unreachable");
    // The model never heard the store error as an answer.
    expect(llm.calls).toHaveLength(1);
  });
});
