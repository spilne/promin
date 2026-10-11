// ---------------------------------------------------------------------------
// Structured output: `_respond` forced through tool_choice, other tool
// calls in the same response answered (never left dangling), schema errors
// fed back to the model for a bounded number of repairs.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { RESPOND_TOOL_NAME, StructuredOutputParseError, agentAction } from "../agent-action.ts";
import type { Message } from "../message.ts";
import { tool } from "../tool.ts";
import { freshRunner, lastMessage, scriptedLLM } from "./engine-harness.ts";

const Schema = z.object({ count: z.number() });

/** Every tool call in `messages` has a tool result. */
function everyCallAnswered(messages: ReadonlyArray<Message>): boolean {
  const answered = new Set(messages.flatMap((m) => (m.role === "tool" ? [m.toolCallId] : [])));
  return messages.every(
    (m) => m.role !== "assistant" || (m.toolCalls ?? []).every((c) => answered.has(c.id)),
  );
}

describe("agentAction — structured output", () => {
  it("forces _respond via tool_choice when the agent has no other tools", async () => {
    const llm = scriptedLLM([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r1", name: RESPOND_TOOL_NAME, input: { count: 3 } }],
      },
    ]);
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({ name: "forced", llm, outputSchema: Schema }),
      workflowId: "forced-1",
      input: { task: "count" },
    });
    expect(llm.calls[0]?.toolChoice).toEqual({ name: RESPOND_TOOL_NAME });
    expect(result.output).toEqual({ count: 3 });
    expect(everyCallAnswered(result.messages)).toBe(true);
    expect(lastMessage(result.messages)).toEqual({ role: "assistant", content: '{"count":3}' });
  });

  it("forces _respond after the model answers in prose, with thinking off for that round", async () => {
    const search = tool({
      name: "search",
      description: "search",
      parameters: z.object({}),
      execute: async () => "r",
    });
    const llm = scriptedLLM([
      { content: "The count is 3.", finishReason: "stop" },
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r1", name: RESPOND_TOOL_NAME, input: { count: 3 } }],
      },
    ]);
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({
        name: "prose",
        llm,
        tools: { search },
        outputSchema: Schema,
        thinkingBudgetTokens: 2_000,
      }),
      workflowId: "prose-1",
      input: { task: "count" },
    });
    expect(llm.calls[0]?.toolChoice).toBeUndefined();
    expect(llm.calls[0]?.thinkingBudgetTokens).toBe(2_000);
    expect(llm.calls[1]?.toolChoice).toEqual({ name: RESPOND_TOOL_NAME });
    expect(llm.calls[1]?.thinkingBudgetTokens).toBeUndefined();
    expect(result.output).toEqual({ count: 3 });
  });

  it("answers other tool calls in a _respond response without running them", async () => {
    let ran = false;
    const sideEffect = tool({
      name: "side_effect",
      description: "do something",
      parameters: z.object({}),
      execute: async () => {
        ran = true;
        return "done";
      },
    });
    const llm = scriptedLLM([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [
          { id: "s1", name: "side_effect", input: {} },
          { id: "r1", name: RESPOND_TOOL_NAME, input: { count: 1 } },
        ],
      },
    ]);
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({
        name: "mixed",
        llm,
        tools: { side_effect: sideEffect },
        outputSchema: Schema,
      }),
      workflowId: "mixed-1",
      input: { task: "count" },
    });
    expect(ran).toBe(false);
    expect(result.output).toEqual({ count: 1 });
    expect(everyCallAnswered(result.messages)).toBe(true);
    expect(result.messages.find((m) => m.role === "tool" && m.toolCallId === "s1")).toMatchObject({
      isError: true,
    });
  });

  it("feeds schema errors back and accepts the repaired response", async () => {
    const llm = scriptedLLM([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r1", name: RESPOND_TOOL_NAME, input: { count: "three" } }],
      },
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r2", name: RESPOND_TOOL_NAME, input: { count: 3 } }],
      },
    ]);
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({ name: "repair", llm, outputSchema: Schema }),
      workflowId: "repair-1",
      input: { task: "count" },
    });
    expect(result.output).toEqual({ count: 3 });
    const feedback = llm.calls[1]?.messages.find((m) => m.role === "tool" && m.toolCallId === "r1");
    expect(feedback).toMatchObject({ isError: true });
    expect(feedback?.role === "tool" && feedback.content).toContain("count");
    expect(everyCallAnswered(result.messages)).toBe(true);
  });

  it("fails with StructuredOutputParseError once the repair budget is spent", async () => {
    const bad = {
      content: null,
      finishReason: "tool_calls" as const,
      toolCalls: [{ id: "r", name: RESPOND_TOOL_NAME, input: { count: "x" } }],
    };
    const llm = scriptedLLM([bad, bad, bad, bad]);
    const { runner } = freshRunner();
    const { error } = await runner.runSafe({
      workflow: agentAction({
        name: "budget",
        llm,
        outputSchema: Schema,
        structuredOutputRepairs: 2,
      }),
      workflowId: "budget-1",
      input: { task: "count" },
    });
    expect(llm.calls).toHaveLength(3);
    expect(error).toBeInstanceOf(StructuredOutputParseError);
  });
});

describe("agentAction — structured output request shape", () => {
  it("never ends a forced round's request on the assistant's prose answer", async () => {
    const search = tool({
      name: "search",
      description: "search",
      parameters: z.object({}),
      execute: async () => "r",
    });
    const llm = scriptedLLM([
      { content: "the count is 3", finishReason: "stop" },
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r1", name: RESPOND_TOOL_NAME, input: { count: 3 } }],
      },
    ]);
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({ name: "nudge", llm, tools: { search }, outputSchema: Schema }),
      workflowId: "nudge-1",
      input: { task: "count" },
    });
    expect(lastMessage(llm.calls[1]!.messages)?.role).toBe("user");
    expect(result.output).toEqual({ count: 3 });
    // The nudge is guidance, not part of the turn's transcript.
    expect(result.turnMessages.filter((m) => m.role === "user")).toEqual([
      { role: "user", content: "count" },
    ]);
  });

  it("keeps the agent's tools available in the round after an invalid _respond", async () => {
    const search = tool({
      name: "search",
      description: "search",
      parameters: z.object({}),
      execute: async () => "r",
    });
    const llm = scriptedLLM([
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r1", name: RESPOND_TOOL_NAME, input: { count: "x" } }],
      },
      {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [{ id: "r2", name: RESPOND_TOOL_NAME, input: { count: 2 } }],
      },
    ]);
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({ name: "repair-tools", llm, tools: { search }, outputSchema: Schema }),
      workflowId: "repair-tools-1",
      input: { task: "count" },
    });
    expect(llm.calls[1]?.toolChoice).toBeUndefined();
    expect(result.output).toEqual({ count: 2 });
  });
});
