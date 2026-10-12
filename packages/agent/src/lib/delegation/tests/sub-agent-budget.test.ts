// ---------------------------------------------------------------------------
// Call-tree budgets: a delegating turn passes its remaining budget down in
// the child's input (split between the sub-agent calls of one step) and is
// charged what the child spent, from the child's journaled result — so a
// caller never spends past its budget through its sub-agents.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { ChildWorkflowGoneError } from "@promin/workflow";
import { z } from "zod";
import { agentAction, BUDGET_EXCEEDED_ANSWER } from "../../agent-action.ts";
import type { LLMResponse } from "../../llm-provider.ts";
import { freshRunner, lastMessage, reactiveLLM } from "../../tests/engine-harness.ts";
import { subAgentTool } from "../sub-agent-tool.ts";

/** A model priced at $0, so turns under a cost budget can run. */
const FREE_MODEL = {
  llmProvider: "p",
  llmModel: "m",
  costs: { rates: () => ({ inputPerMillion: 0 }) },
} as const;

const usage = (inputTokens: number): LLMResponse["usage"] => ({ inputTokens, outputTokens: 0 });

/** A sub-agent that answers in one call reporting `tokens` of input, priced at $0. */
function helperTool(params: { name: string; tokens: number }) {
  const llm = reactiveLLM(() => ({
    content: `${params.name} done`,
    finishReason: "stop",
    usage: usage(params.tokens),
  }));
  return {
    llm,
    tool: subAgentTool({
      name: params.name,
      description: "helps",
      agent: { name: params.name, llm, ...FREE_MODEL },
    }),
  };
}

/** Delegates to each of `tools` in one step, then answers; every call reports 100 tokens. */
function leadLLM(tools: ReadonlyArray<string>) {
  return reactiveLLM((messages) =>
    lastMessage(messages)?.role === "tool"
      ? { content: "lead done", finishReason: "stop", usage: usage(100) }
      : {
          content: null,
          finishReason: "tool_calls",
          toolCalls: tools.map((name, i) => ({ id: `call-${i}`, name, input: { task: "do it" } })),
          usage: usage(100),
        },
  );
}

describe("subAgentTool — call-tree budget", () => {
  it("passes the remaining budget down and charges the child's spend to the caller", async () => {
    const { runner, storage } = freshRunner();
    const helper = helperTool({ name: "helper", tokens: 300 });
    const lead = agentAction({
      name: "lead",
      llm: leadLLM(["helper"]),
      tools: { helper: helper.tool },
      budget: { maxTotalTokens: 1_000, maxToolCalls: 4 },
    });

    const result = await runner.run({
      workflow: lead,
      workflowId: "tree-1",
      input: { task: "go" },
    });

    const child = await storage.loadWorkflow("tree-1.0.call-0");
    // 1000 - the lead's first call (100); 4 - the delegation call itself.
    expect((child?.input as { budget?: unknown } | undefined)?.budget).toEqual({
      maxTotalTokens: 900,
      maxToolCalls: 3,
    });
    expect(result.finishReason).toBe("stop");
    // Lead: two calls of 100; helper: 300; one tool call (the delegation).
    expect(result.spend).toMatchObject({ inputTokens: 500, llmCalls: 3, toolCalls: 1 });
    expect(result.trace?.spend).toEqual(result.spend);
  });

  it("splits the remaining budget between the sub-agent calls of one step", async () => {
    const { runner, storage } = freshRunner();
    const a = helperTool({ name: "a", tokens: 10 });
    const b = helperTool({ name: "b", tokens: 20 });
    const lead = agentAction({
      name: "lead",
      llm: leadLLM(["a", "b"]),
      tools: { a: a.tool, b: b.tool },
      budget: { maxTotalTokens: 1_001, maxCostUsd: 3 },
      ...FREE_MODEL,
    });

    const result = await runner.run({
      workflow: lead,
      workflowId: "tree-2",
      input: { task: "go" },
    });

    for (const id of ["tree-2.0.call-0", "tree-2.0.call-1"]) {
      const child = await storage.loadWorkflow(id);
      // (1001 - 100) / 2, rounded down; cost split exactly.
      expect((child?.input as { budget?: unknown } | undefined)?.budget).toEqual({
        maxTotalTokens: 450,
        maxCostUsd: 1.5,
      });
    }
    expect(result.spend?.inputTokens).toBe(230);
  });

  it("stops the caller once a child's spend uses up its budget", async () => {
    const { runner } = freshRunner();
    const helper = helperTool({ name: "helper", tokens: 895 });
    const llm = leadLLM(["helper"]);
    const lead = agentAction({
      name: "lead",
      llm,
      tools: { helper: helper.tool },
      budget: { maxTotalTokens: 1_000 },
    });

    const result = await runner.run({
      workflow: lead,
      workflowId: "tree-3",
      input: { task: "go" },
    });

    // 100 (lead) + 895 (helper) leaves too little for the lead's next prompt.
    expect(llm.calls).toHaveLength(1);
    expect(result.finishReason).toBe("budget_exceeded");
    expect(result.answer).toBe(BUDGET_EXCEEDED_ANSWER);
    expect(result.spend?.inputTokens).toBe(995);
  });

  it("ends a child that gets no budget left without calling its model", async () => {
    const { runner } = freshRunner();
    const helper = helperTool({ name: "helper", tokens: 10 });
    const lead = agentAction({
      name: "lead",
      llm: leadLLM(["helper"]),
      tools: { helper: helper.tool },
      budget: { maxLlmCalls: 1 },
    });

    const result = await runner.run({
      workflow: lead,
      workflowId: "tree-4",
      input: { task: "go" },
    });

    expect(helper.llm.calls).toHaveLength(0);
    const toolResult = result.turnMessages.find((m) => m.role === "tool");
    expect(toolResult?.content).toBe(BUDGET_EXCEEDED_ANSWER);
    expect(result.finishReason).toBe("budget_exceeded");
  });

  it("charges a child that failed without a result its whole share", async () => {
    const { runner } = freshRunner();
    const broken = subAgentTool({
      name: "broken",
      description: "fails",
      route: async () => ({
        ok: true as const,
        route: { agentId: "broken", input: { task: "x" } },
      }),
      parameters: z.object({ task: z.string() }),
      load: async () => {
        throw new ChildWorkflowGoneError({ message: "gone" });
      },
    });
    const lead = agentAction({
      name: "lead",
      llm: leadLLM(["broken"]),
      tools: { broken },
      budget: { maxTotalTokens: 1_000 },
    });

    const result = await runner.run({
      workflow: lead,
      workflowId: "tree-5",
      input: { task: "go" },
    });

    // The lead's first call (100) plus the child's whole share (900): nothing is left.
    expect(result.finishReason).toBe("budget_exceeded");
    expect(result.spend?.inputTokens).toBe(1_000);
  });
});
