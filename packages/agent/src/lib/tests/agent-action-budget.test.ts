// ---------------------------------------------------------------------------
// agentAction — per-turn budgets.
//
//   - Running out mid-turn ends the turn with `budget_exceeded` and the
//     sentinel answer, between steps: every tool call has its result.
//   - The projected check stops a turn before an LLM call that can't fit;
//     tool-call and cost limits hold too.
//   - Budget decisions replay from the journal: a turn resumed under an
//     edited budget or edited prices follows the recorded path and spend.
//   - Spend is recorded once per committed turn (`usageRecorder`).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { completeSignal } from "@promin/workflow";
import { agentAction, BUDGET_EXCEEDED_ANSWER } from "../agent-action.ts";
import type { LLMResponse } from "../llm-provider.ts";
import type { AssistantMessage, Message } from "../message.ts";
import { staticCostRegistry } from "../metrics/types.ts";
import { tool } from "../tool.ts";
import { collectingBus, freshRunner, reactiveLLM } from "./engine-harness.ts";

/** Every tool call of `messages` has a result right after its assistant message. */
function expectToolPairsIntact(messages: ReadonlyArray<Message>): void {
  messages.forEach((m, i) => {
    if (m.role !== "assistant" || !m.toolCalls?.length) return;
    const ids = m.toolCalls.map((c) => c.id);
    const results = messages
      .slice(i + 1, i + 1 + ids.length)
      .map((r) => (r.role === "tool" ? r.toolCallId : null));
    expect(results).toEqual(ids);
  });
}

function lookupTool() {
  let runs = 0;
  const lookup = tool({
    name: "lookup",
    description: "look something up",
    parameters: z.object({ q: z.string() }),
    execute: async () => {
      runs++;
      return "found";
    },
  });
  return { lookup, runs: () => runs };
}

/** Calls `lookup` on every step, reporting `usage`. */
function loopingLLM(usage: LLMResponse["usage"], callsPerStep = 1) {
  let step = 0;
  return reactiveLLM(() => {
    step++;
    return {
      content: null,
      finishReason: "tool_calls",
      toolCalls: Array.from({ length: callsPerStep }, (_, k) => ({
        id: `c${step}-${k}`,
        name: "lookup",
        input: { q: "x" },
      })),
      ...(usage && { usage }),
    };
  });
}

describe("agentAction — budget", () => {
  it("ends a turn that ran out mid-way cleanly: tool calls closed, sentinel answer, event, trace", async () => {
    const { runner } = freshRunner();
    const { lookup, runs } = lookupTool();
    const llm = loopingLLM({ inputTokens: 1_000, outputTokens: 10 });
    const { bus, events } = collectingBus();

    const result = await runner.run({
      workflow: agentAction({
        name: "budgeted",
        llm,
        tools: { lookup },
        budget: { maxTotalTokens: 1_500 },
        bus,
      }),
      workflowId: "budget-mid",
      input: { task: "go" },
    });

    // Step 0 fits (1010 tokens); step 1 brings the turn to 2020 > 1500, so
    // its tool call is closed without running.
    expect(llm.calls).toHaveLength(2);
    expect(runs()).toBe(1);
    expect(result.finishReason).toBe("budget_exceeded");
    expect(result.answer).toBe(BUDGET_EXCEEDED_ANSWER);
    expectToolPairsIntact(result.turnMessages);
    expectToolPairsIntact(result.messages);
    const last = result.turnMessages.at(-1) as AssistantMessage;
    expect(last).toMatchObject({ role: "assistant", content: BUDGET_EXCEEDED_ANSWER });
    const skipped = result.turnMessages.find((m) => m.role === "tool" && m.toolCallId === "c2-0");
    expect(skipped).toMatchObject({ isError: true });

    expect(result.spend).toMatchObject({
      inputTokens: 2_000,
      outputTokens: 20,
      llmCalls: 2,
      toolCalls: 1,
    });
    expect(result.trace?.budget).toEqual({
      limits: { maxTotalTokens: 1_500 },
      exceeded: { dimension: "maxTotalTokens", limit: 1_500, used: 2_020, step: 1 },
    });
    expect(events.find((e) => e.type === "budget.exceeded")).toMatchObject({
      step: 1,
      dimension: "maxTotalTokens",
      limit: 1_500,
      used: 2_020,
    });
    expect(events.at(-1)?.type).toBe("turn.end");
  });

  it("stops before an LLM call the budget can't fit", async () => {
    const { runner } = freshRunner();
    const { lookup, runs } = lookupTool();
    const llm = loopingLLM({ inputTokens: 5, outputTokens: 5 });

    const result = await runner.run({
      workflow: agentAction({ name: "calls", llm, tools: { lookup }, budget: { maxLlmCalls: 2 } }),
      workflowId: "budget-calls",
      input: { task: "go" },
    });

    expect(llm.calls).toHaveLength(2);
    expect(runs()).toBe(2);
    expect(result.finishReason).toBe("budget_exceeded");
    expect(result.trace?.budget?.exceeded).toMatchObject({
      dimension: "maxLlmCalls",
      limit: 2,
      used: 3,
      step: 2,
    });
    expectToolPairsIntact(result.turnMessages);
    expect(result.turnMessages.at(-1)).toEqual({
      role: "assistant",
      content: BUDGET_EXCEEDED_ANSWER,
    });
  });

  it("projects the prompt of the next call: a prompt bigger than the input budget never goes out", async () => {
    const { runner } = freshRunner();
    const llm = reactiveLLM(() => ({ content: "answer", finishReason: "stop" }));

    const result = await runner.run({
      workflow: agentAction({ name: "projected", llm, budget: { maxInputTokens: 10 } }),
      workflowId: "budget-projected",
      input: { task: "x".repeat(400) },
    });

    expect(llm.calls).toHaveLength(0);
    expect(result.finishReason).toBe("budget_exceeded");
    expect(result.trace?.budget?.exceeded?.dimension).toBe("maxInputTokens");
  });

  it("caps a call's output to what the budget leaves instead of refusing it", async () => {
    const { runner } = freshRunner();
    const llm = reactiveLLM(() => ({
      content: "ok",
      finishReason: "stop",
      usage: { inputTokens: 10, outputTokens: 10 },
    }));

    const result = await runner.run({
      workflow: agentAction({
        name: "cap",
        llm,
        maxTokens: 4_096,
        budget: { maxOutputTokens: 2_000 },
      }),
      workflowId: "budget-cap",
      input: { task: "hi" },
    });

    expect(result.finishReason).toBe("stop");
    expect(llm.calls).toHaveLength(1);
    expect(llm.calls[0]?.maxTokens).toBe(2_000);
  });

  it("caps output by the total budget and by price, without a configured maxTokens", async () => {
    const { runner } = freshRunner();
    const llm = reactiveLLM(() => ({ content: "ok", finishReason: "stop" }));

    await runner.run({
      workflow: agentAction({
        name: "cap-cost",
        llm,
        llmProvider: "acme",
        llmModel: "m1",
        costs: staticCostRegistry({ "acme/m1": { outputPerMillion: 1_000_000 } }),
        budget: { maxTotalTokens: 100_000, maxCostUsd: 1_000 },
      }),
      workflowId: "budget-cap-cost",
      input: { task: "hi" },
    });

    // $1 per output token: $1000 buys 1000 tokens, fewer than the total limit leaves.
    expect(llm.calls[0]?.maxTokens).toBe(1_000);
  });

  it("does not make a call that could produce fewer than minCallTokens", async () => {
    const { runner } = freshRunner();
    const llm = reactiveLLM(() => ({ content: "ok", finishReason: "stop" }));

    const result = await runner.run({
      workflow: agentAction({
        name: "too-small",
        llm,
        minCallTokens: 300,
        budget: { maxOutputTokens: 299 },
      }),
      workflowId: "budget-too-small",
      input: { task: "hi" },
    });

    expect(llm.calls).toHaveLength(0);
    expect(result.finishReason).toBe("budget_exceeded");
    expect(result.trace?.budget?.exceeded).toMatchObject({
      dimension: "maxOutputTokens",
      limit: 299,
      used: 300,
    });
  });

  it("does not start tool calls past maxToolCalls", async () => {
    const { runner } = freshRunner();
    const { lookup, runs } = lookupTool();
    const llm = loopingLLM(undefined, 2);

    const result = await runner.run({
      workflow: agentAction({ name: "tools", llm, tools: { lookup }, budget: { maxToolCalls: 3 } }),
      workflowId: "budget-tools",
      input: { task: "go" },
    });

    // Step 0 runs two calls; step 1's two would make four.
    expect(runs()).toBe(2);
    expect(result.finishReason).toBe("budget_exceeded");
    expect(result.trace?.budget?.exceeded).toMatchObject({ dimension: "maxToolCalls", used: 4 });
    expectToolPairsIntact(result.turnMessages);
  });

  it("prices calls with the engine's costs and stops at maxCostUsd", async () => {
    const { runner } = freshRunner();
    const { lookup } = lookupTool();
    const llm = loopingLLM({ inputTokens: 1_000_000, outputTokens: 0 });

    const result = await runner.run({
      workflow: agentAction({
        name: "cost",
        llm,
        tools: { lookup },
        llmProvider: "acme",
        llmModel: "m1",
        costs: staticCostRegistry({ "acme/m1": { inputPerMillion: 2 } }),
        budget: { maxCostUsd: 5 },
      }),
      workflowId: "budget-cost",
      input: { task: "go" },
    });

    // $2 per step: the third step's projection still fits ($4 + a tiny
    // prompt), its call brings the turn to $6 > $5.
    expect(llm.calls).toHaveLength(3);
    expect(result.finishReason).toBe("budget_exceeded");
    expect(result.spend?.costUsd).toBeCloseTo(6, 9);
    expect(result.trace?.budget?.exceeded?.dimension).toBe("maxCostUsd");
  });

  it("fails a turn under a cost budget whose model has no price", async () => {
    const { runner } = freshRunner();
    const llm = reactiveLLM(() => ({ content: "hi", finishReason: "stop" }));
    const run = await runner.runSafe({
      workflow: agentAction({ name: "unpriced", llm, budget: { maxCostUsd: 1 } }),
      workflowId: "budget-unpriced",
      input: { task: "go" },
    });
    expect(String(run.error)).toContain("maxCostUsd");
    expect(run.error).toMatchObject({ code: "model_unpriced" });
    expect(llm.calls).toHaveLength(0);
  });

  it("takes the tighter of the config budget and the input's ceiling, and rejects a malformed one", async () => {
    const { runner } = freshRunner();
    const { lookup } = lookupTool();
    const llm = loopingLLM(undefined);

    const result = await runner.run({
      workflow: agentAction({
        name: "ceiling",
        llm,
        tools: { lookup },
        budget: { maxLlmCalls: 5 },
      }),
      workflowId: "budget-ceiling",
      input: { task: "go", budget: { maxLlmCalls: 1 } },
    });
    expect(llm.calls).toHaveLength(1);
    expect(result.trace?.budget?.limits).toEqual({ maxLlmCalls: 1 });

    const bad = await runner.runSafe({
      workflow: agentAction({ name: "ceiling", llm, tools: { lookup } }),
      workflowId: "budget-ceiling-bad",
      input: { task: "go", budget: { maxLlmCalls: -1 } },
    });
    expect(String(bad.error)).toContain("invalid budget");
  });

  it("records the spend of a turn that fails", async () => {
    const { runner } = freshRunner();
    const recorded: Array<{ workflowId: string; llmCalls: number; inputTokens: number }> = [];
    let calls = 0;
    const { lookup } = lookupTool();
    const llm = reactiveLLM(() => {
      calls++;
      if (calls === 2) throw new Error("provider down");
      return {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "c1", name: "lookup", input: { q: "x" } }],
        usage: { inputTokens: 70, outputTokens: 0 },
      };
    });

    const run = await runner.runSafe({
      workflow: agentAction({
        name: "fails",
        llm,
        llmRetry: false,
        tools: { lookup },
        usageRecorder: {
          record: async ({ workflowId, usage }) => {
            recorded.push({ workflowId, llmCalls: usage.llmCalls, inputTokens: usage.inputTokens });
          },
        },
      }),
      workflowId: "rec-failed",
      input: { task: "go" },
    });

    expect(run.error).toBeTruthy();
    expect(recorded).toEqual([{ workflowId: "rec-failed", llmCalls: 1, inputTokens: 70 }]);
  });

  it("records the committed turn's spend once", async () => {
    const { runner } = freshRunner();
    const recorded: Array<{ workflowId: string; tokens: number }> = [];
    const llm = reactiveLLM(() => ({
      content: "hi",
      finishReason: "stop",
      usage: { inputTokens: 7, outputTokens: 3 },
    }));
    const build = () =>
      agentAction({
        name: "recorded",
        llm,
        usageRecorder: {
          record: async ({ workflowId, usage }) => {
            recorded.push({ workflowId, tokens: usage.inputTokens + usage.outputTokens });
          },
        },
      });

    await runner.run({ workflow: build(), workflowId: "rec-1", input: { task: "go" } });
    // A replay of the finished turn reads the journal: no second record.
    await runner.run({ workflow: build(), workflowId: "rec-1", input: { task: "go" } });
    expect(recorded).toEqual([{ workflowId: "rec-1", tokens: 10 }]);
  });
});

describe("agentAction — budget replay", () => {
  it("resumes under the journaled budget and prices, whatever the config says now", async () => {
    const { runner, storage } = freshRunner();
    const deploy = tool({
      name: "deploy",
      description: "deploy",
      parameters: z.object({ env: z.string() }),
      requireApproval: true,
      execute: async () => "deployed",
    });
    const llm = reactiveLLM((messages) =>
      messages.at(-1)?.role === "tool"
        ? { content: "done", finishReason: "stop", usage: { inputTokens: 100, outputTokens: 0 } }
        : {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "call-1", name: "deploy", input: { env: "prod" } }],
            usage: { inputTokens: 100, outputTokens: 0 },
          },
    );
    const build = (params: { maxLlmCalls: number; inputPerMillion: number }) =>
      agentAction({
        name: "resume",
        llm,
        tools: { deploy },
        llmProvider: "acme",
        llmModel: "m1",
        costs: staticCostRegistry({ "acme/m1": { inputPerMillion: params.inputPerMillion } }),
        budget: { maxLlmCalls: params.maxLlmCalls },
      });

    const first = await runner.runSafe({
      workflow: build({ maxLlmCalls: 2, inputPerMillion: 10_000 }),
      workflowId: "resume-1",
      input: { task: "ship" },
    });
    expect(first.error).toBeTruthy();
    const wf = await storage.loadWorkflow("resume-1");
    const waiting = Object.values(wf!.steps).find((s) => s.status === "waiting_for_signal")!;
    await completeSignal({
      storage,
      workflowId: "resume-1",
      stepName: waiting.stepName,
      signalName: "approve:call-1",
      value: { approved: true },
    });

    // Edited while the turn waited: one LLM call allowed, prices doubled.
    const result = await runner.run({
      workflow: build({ maxLlmCalls: 1, inputPerMillion: 20_000 }),
      workflowId: "resume-1",
      input: { task: "ship" },
    });

    expect(result.finishReason).toBe("stop");
    expect(result.answer).toBe("done");
    expect(result.trace?.budget?.limits).toEqual({ maxLlmCalls: 2 });
    // The first call keeps its recorded price ($1); the second is priced now ($2).
    expect(result.spend?.costUsd).toBeCloseTo(3, 9);
    expect(result.spend?.llmCalls).toBe(2);
  });
});
