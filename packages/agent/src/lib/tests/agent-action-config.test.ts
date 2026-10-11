// ---------------------------------------------------------------------------
// Config parity: every engine setting works on agentAction and reaches it
// through LocalAgent (invoke and thread turns) — thinking / maxTokens,
// metrics, per-turn hooks, onLifecycle, onStep feedback, outputSchema,
// toolRegistry, cancellation. Also: listPendingSignals reads the tool name
// and input agentAction journals before an approval suspend.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { agentAction, type AgentTurnLifecycleEvent } from "../agent-action.ts";
import { LocalAgent } from "../agent/local-agent.ts";
import { listPendingSignals } from "../approvals/list-pending-signals.ts";
import { InMemoryMemoryStore } from "../memory/in-memory-memory-store.ts";
import { InMemoryAgentMetrics } from "../metrics/types.ts";
import type { ToolRegistry } from "../tool-registry.ts";
import { tool } from "../tool.ts";
import {
  collectingBus,
  freshRunner,
  lastMessage,
  reactiveLLM,
  scriptedLLM,
} from "./engine-harness.ts";

const lookup = tool({
  name: "lookup",
  description: "look something up",
  parameters: z.object({ key: z.string() }),
  requireApproval: true,
  execute: async ({ key }) => `value of ${key}`,
});

/** Looks up once, then answers with the tool's output. */
function lookupThenAnswer() {
  return reactiveLLM((messages) => {
    const last = lastMessage(messages);
    return last?.role === "tool"
      ? { content: `answer: ${last.content}`, finishReason: "stop" }
      : {
          content: null,
          finishReason: "tool_calls",
          toolCalls: [{ id: "c-1", name: "lookup", input: { key: "k" } }],
          usage: { inputTokens: 10, outputTokens: 5 },
        };
  });
}

describe("agentAction — config parity", () => {
  it("forwards thinkingBudgetTokens and maxTokens to every LLM call", async () => {
    const llm = scriptedLLM([{ content: "ok", finishReason: "stop" }]);
    const { runner } = freshRunner();
    await runner.run({
      workflow: agentAction({ name: "think", llm, thinkingBudgetTokens: 4_000, maxTokens: 9_000 }),
      workflowId: "think-1",
      input: { task: "hi" },
    });
    expect(llm.calls[0]).toMatchObject({ thinkingBudgetTokens: 4_000, maxTokens: 9_000 });
  });

  it("records chat, tool and approval metrics", async () => {
    const metrics = new InMemoryAgentMetrics();
    const { runner } = freshRunner();
    await runner.run({
      workflow: agentAction({
        name: "metrics",
        llm: lookupThenAnswer(),
        tools: { lookup },
        onApprovalRequired: async () => ({ approved: true }),
        metrics,
        llmProvider: "test",
        llmModel: "m",
        metricLabels: { agent: "a" },
      }),
      workflowId: "metrics-1",
      input: { task: "hi" },
    });
    expect(metrics.counterTotal("llm.calls")).toBe(2);
    expect(metrics.counterTotal("tool.calls")).toBe(1);
    expect(metrics.counterTotal("approval.requests")).toBe(1);
    expect(metrics.counterSamples("approval.decisions")[0]?.labels).toEqual({
      agent: "a",
      outcome: "approved",
    });
  });

  it("runs per-turn hooks and reports lifecycle transitions once each", async () => {
    const lifecycle: Array<Pick<AgentTurnLifecycleEvent, "event" | "from" | "to">> = [];
    let afterTurn: { answer: string; finishReason: string } | undefined;
    const llm = lookupThenAnswer();
    const { runner } = freshRunner();
    await runner.run({
      workflow: agentAction({
        name: "hooks",
        llm,
        tools: { lookup },
        onApprovalRequired: async () => ({ approved: true }),
        hooks: {
          beforeTurn: async ({ messages }) => [
            ...messages,
            { role: "system", content: "injected context" },
          ],
          afterTurn: async ({ answer, finishReason }) => {
            afterTurn = { answer, finishReason };
          },
        },
        onLifecycle: ({ event, from, to }) => {
          lifecycle.push({ event, from, to });
        },
      }),
      workflowId: "hooks-1",
      input: { task: "hi" },
    });
    expect(llm.calls[0]?.messages.some((m) => m.content === "injected context")).toBe(true);
    expect(afterTurn).toEqual({ answer: "answer: value of k", finishReason: "stop" });
    expect(lifecycle).toEqual([
      { event: "message", from: "idle", to: "thinking" },
      { event: "approval-required", from: "thinking", to: "waiting_approval" },
      { event: "approved", from: "waiting_approval", to: "thinking" },
      { event: "done", from: "thinking", to: "idle" },
    ]);
  });

  it("injects onStep feedback before the next think and runs onStep once per step", async () => {
    let stepCalls = 0;
    const llm = scriptedLLM([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "c-1", name: "lookup", input: { key: "k" } }],
      },
      { content: "brief answer", finishReason: "stop" },
    ]);
    const { runner } = freshRunner();
    await runner.run({
      workflow: agentAction({
        name: "feedback",
        llm,
        tools: { lookup },
        autoApprove: true,
        onStep: ({ step }) => {
          stepCalls++;
          return step === 0 ? { continue: true, feedback: "be brief" } : undefined;
        },
      }),
      workflowId: "feedback-1",
      input: { task: "hi" },
    });
    expect(stepCalls).toBe(2);
    expect(lastMessage(llm.calls[1]!.messages)).toEqual({ role: "system", content: "be brief" });
  });

  it("onStep stop closes the turn without dangling tool calls", async () => {
    let executed = false;
    const noisy = tool({
      name: "lookup",
      description: "x",
      parameters: z.object({ key: z.string() }),
      execute: async () => {
        executed = true;
        return "v";
      },
    });
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({
        name: "stop",
        llm: lookupThenAnswer(),
        tools: { lookup: noisy },
        onStep: () => ({ continue: false }),
      }),
      workflowId: "stop-1",
      input: { task: "hi" },
    });
    expect(executed).toBe(false);
    expect(result.messages.find((m) => m.role === "tool")).toMatchObject({
      toolCallId: "c-1",
      isError: true,
    });
    expect(lastMessage(result.messages)?.role).toBe("assistant");
  });
});

describe("LocalAgent — runs every engine setting through agentAction", () => {
  it("one-shot invoke honors thinking, maxTokens, hooks, metrics and outputSchema", async () => {
    const metrics = new InMemoryAgentMetrics();
    let afterTurnRan = false;
    const llm = scriptedLLM([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r", name: "_respond", input: { n: 1 } }],
      },
    ]);
    const { runner } = freshRunner();
    const agent = new LocalAgent({
      agent: {
        name: "parity",
        llm,
        thinkingBudgetTokens: 1_000,
        maxTokens: 2_000,
        outputSchema: z.object({ n: z.number() }),
        metrics,
        llmProvider: "p",
        llmModel: "m",
        hooks: {
          afterTurn: async () => {
            afterTurnRan = true;
          },
        },
      },
      runner,
    });
    const out = await agent.invoke({ task: "go" });
    expect(await out.output).toEqual({ n: 1 });
    expect(await out.finishReason).toBe("stop");
    expect(llm.calls[0]?.maxTokens).toBe(2_000);
    expect(afterTurnRan).toBe(true);
    expect(metrics.counterTotal("llm.calls")).toBe(1);
  });

  it("thread turns honor maxTokens and a toolRegistry, with the memory tool merged in", async () => {
    let registryReads = 0;
    const registry: ToolRegistry = {
      getTools: () => {
        registryReads++;
        return { lookup: { ...lookup, requireApproval: false } };
      },
      close: () => {},
    };
    const llm = lookupThenAnswer();
    const { runner } = freshRunner();
    const agent = new LocalAgent({
      agent: { name: "reg", llm, toolRegistry: registry, maxTokens: 777 },
      runner,
      memory: new InMemoryMemoryStore(),
      namespaceId: "ns",
    });
    const thread = await agent.thread("t-1");
    const out = await thread.send({ task: "go" });
    expect(await out.text).toBe("answer: value of k");
    expect(registryReads).toBeGreaterThan(0);
    expect(llm.calls[0]?.maxTokens).toBe(777);
    expect(llm.calls[0]?.tools?.map((t) => t.name).sort()).toEqual(["lookup", "memory"]);
  });

  it("cancels a run through the caller's signal", async () => {
    const controller = new AbortController();
    const llm = reactiveLLM(() => {
      controller.abort();
      return {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "c", name: "lookup", input: { key: "k" } }],
      };
    });
    const { runner } = freshRunner();
    const agent = new LocalAgent({
      agent: { name: "cancel", llm, tools: { lookup: { ...lookup, requireApproval: false } } },
      runner,
    });
    const out = await agent.invoke({ task: "go" }, { signal: controller.signal });
    expect(await out.finishReason).toBe("cancelled");
    expect(llm.calls).toHaveLength(1);
  });
});

describe("listPendingSignals — agentAction approvals", () => {
  it("reports the tool name and input of a suspended approval", async () => {
    const { runner, storage } = freshRunner();
    await runner.runSafe({
      workflow: agentAction({ name: "pending", llm: lookupThenAnswer(), tools: { lookup } }),
      workflowId: "pending-1",
      input: { task: "go" },
    });
    const [pending] = await listPendingSignals(storage);
    expect(pending).toMatchObject({
      workflowId: "pending-1",
      isApproval: true,
      toolCallId: "c-1",
      toolName: "lookup",
      toolInput: { key: "k" },
    });
  });
});

describe("LocalAgent — thread persistence with a beforeTurn hook", () => {
  it("persists exactly each turn's task and answer when the hook trims context", async () => {
    let n = 0;
    const llm = reactiveLLM(() => ({ content: `answer ${++n}`, finishReason: "stop" }));
    const { runner } = freshRunner();
    const memory = new InMemoryMemoryStore();
    const agent = new LocalAgent({
      agent: {
        name: "trim",
        llm,
        systemPrompt: "persona",
        hooks: {
          // Keep the system prompt and the last two messages.
          beforeTurn: async ({ messages }) => [messages[0]!, ...messages.slice(-2)],
        },
      },
      runner,
      memory,
      namespaceId: "ns",
    });
    const thread = await agent.thread("th-1");
    for (const task of ["q1", "q2", "q3"]) {
      await (
        await thread.send({ task })
      ).text;
    }
    const history = (await thread.messages()).map((m) => `${m.role}:${m.content}`);
    expect(history).toEqual([
      "user:q1",
      "assistant:answer 1",
      "user:q2",
      "assistant:answer 2",
      "user:q3",
      "assistant:answer 3",
    ]);
  });
});

describe("agentAction — failed turns", () => {
  it("emits turn.failed and a failed lifecycle transition when the turn throws", async () => {
    const lifecycle: string[] = [];
    const { bus, events } = collectingBus();
    const { runner } = freshRunner();
    const { error } = await runner.runSafe({
      workflow: agentAction({
        name: "fails",
        llm: scriptedLLM([{ content: "ok", finishReason: "stop" }]),
        bus,
        hooks: {
          afterTurn: async () => {
            throw new Error("hook exploded");
          },
        },
        onLifecycle: ({ event }) => {
          lifecycle.push(event);
        },
      }),
      workflowId: "fails-1",
      input: { task: "hi" },
    });
    expect(String(error)).toContain("hook exploded");
    expect(events.find((e) => e.type === "turn.failed")).toMatchObject({ error: "hook exploded" });
    expect(events.map((e) => e.type)).not.toContain("turn.end");
    expect(lifecycle).toEqual(["message", "failed"]);
  });
});
