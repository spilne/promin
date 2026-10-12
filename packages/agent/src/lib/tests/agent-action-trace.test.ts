// ---------------------------------------------------------------------------
// agentAction — the turn's trace.
//
//   - A turn reports think / approval / tool spans with durations on the
//     engine clock, per-think token usage, the turn's totals and its
//     finish reason (AgentResult.trace).
//   - Span times are journaled: a turn resumed after an approval (a second
//     run replaying the journal) still reports the spans recorded before
//     the suspension, with their original timings.
//   - The committed final assistant message carries the trace, so
//     buildAgentTrace shows it on the turn.
//   - Span order does not depend on which parallel call finished first.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { completeSignal, FakeWallClock } from "@promin/workflow";
import { agentAction, type TurnTranscript } from "../agent-action.ts";
import type { Message } from "../message.ts";
import { tool } from "../tool.ts";
import { buildAgentTrace, TURN_TRACE_META_KEY } from "../trace.ts";
import { deferred, freshRunner, lastMessage, reactiveLLM } from "./engine-harness.ts";

describe("agentAction — turn trace", () => {
  it("traces think, approval and tool spans across an approval resume", async () => {
    const clock = FakeWallClock.create(1_000_000);
    const { runner, storage } = freshRunner();
    const deploy = tool({
      name: "deploy",
      description: "deploy",
      parameters: z.object({ env: z.string() }),
      requireApproval: true,
      execute: async () => {
        clock.advance(40);
        return "deployed";
      },
    });
    const llm = reactiveLLM((messages) => {
      clock.advance(100);
      return lastMessage(messages)?.role === "tool"
        ? { content: "done", finishReason: "stop", usage: { inputTokens: 20, outputTokens: 2 } }
        : {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "call-1", name: "deploy", input: { env: "prod" } }],
            usage: { inputTokens: 10, outputTokens: 5 },
          };
    });
    const committed: Message[] = [];
    const transcript: TurnTranscript = {
      open: async ({ message }) => {
        committed.push(message);
      },
      commit: async ({ messages }) => {
        committed.push(...messages);
      },
    };
    const build = () => agentAction({ name: "trace", llm, tools: { deploy }, clock, transcript });

    const first = await runner.runSafe({
      workflow: build(),
      workflowId: "trace-1",
      input: { task: "ship it" },
    });
    expect(first.error).toBeTruthy();

    // The approval waits five seconds.
    clock.advance(5_000);
    const wf = await storage.loadWorkflow("trace-1");
    const waiting = Object.values(wf!.steps).find((s) => s.status === "waiting_for_signal")!;
    await completeSignal({
      storage,
      workflowId: "trace-1",
      stepName: waiting.stepName,
      signalName: "approve:call-1",
      value: { approved: true },
    });
    const result = await runner.run({
      workflow: build(),
      workflowId: "trace-1",
      input: { task: "ship it" },
    });

    const trace = result.trace!;
    expect(trace).toMatchObject({
      workflowId: "trace-1",
      startedAt: 1_000_000,
      finishReason: "stop",
      steps: 2,
      usage: { inputTokens: 30, outputTokens: 7 },
    });
    expect(trace.durationMs).toBe(trace.endedAt - trace.startedAt);
    expect(trace.durationMs).toBe(100 + 5_000 + 40 + 100);
    expect(trace.spans).toEqual([
      {
        kind: "think",
        step: 0,
        startedAt: 1_000_000,
        durationMs: 100,
        usage: { inputTokens: 10, outputTokens: 5 },
      },
      {
        kind: "approval",
        step: 0,
        toolCallId: "call-1",
        toolName: "deploy",
        approved: true,
        startedAt: 1_000_100,
        durationMs: 5_000,
      },
      {
        kind: "tool",
        step: 0,
        name: "deploy",
        toolCallId: "call-1",
        failed: false,
        startedAt: 1_005_100,
        durationMs: 40,
      },
      {
        kind: "think",
        step: 1,
        startedAt: 1_005_140,
        durationMs: 100,
        usage: { inputTokens: 20, outputTokens: 2 },
      },
    ]);

    // The committed answer carries the trace; the trace view shows it.
    const answer = committed[committed.length - 1]!;
    expect(answer).toMatchObject({ role: "assistant", content: "done" });
    expect((answer as { metadata?: Record<string, unknown> }).metadata).toEqual({
      [TURN_TRACE_META_KEY]: trace,
    });
    // Only the final assistant message is stamped.
    const stamped = committed.filter(
      (m) => m.role === "assistant" && m.metadata?.[TURN_TRACE_META_KEY] !== undefined,
    );
    expect(stamped).toHaveLength(1);
    const view = buildAgentTrace(JSON.parse(JSON.stringify(committed)) as Message[]);
    expect(view.turns).toHaveLength(1);
    expect(view.turns[0]!.timing).toEqual(trace);
  });

  it("marks a failed tool span and reports a step-limited turn's finish reason", async () => {
    const clock = FakeWallClock.create(0);
    const { runner } = freshRunner();
    const flaky = tool({
      name: "flaky",
      description: "fails",
      parameters: z.object({}),
      execute: async (): Promise<string> => {
        throw new Error("boom");
      },
    });
    const llm = reactiveLLM(() => ({
      content: null,
      finishReason: "tool_calls",
      toolCalls: [{ id: "c-1", name: "flaky", input: {} }],
    }));
    const result = await runner.run({
      workflow: agentAction({ name: "limit", llm, tools: { flaky }, clock, maxSteps: 1 }),
      workflowId: "limit-1",
      input: { task: "try" },
    });
    expect(result.trace!.finishReason).toBe("max_steps");
    expect(result.trace!.steps).toBe(1);
    const toolSpan = result.trace!.spans.find((s) => s.kind === "tool");
    expect(toolSpan).toMatchObject({ kind: "tool", name: "flaky", failed: true });
  });

  it("orders spans of parallel calls by call id, not by which finished first", async () => {
    const clock = FakeWallClock.create(0);
    const { runner } = freshRunner();
    const fastDone = deferred();
    const slow = tool({
      name: "slow",
      description: "waits for fast",
      parameters: z.object({}),
      execute: async () => {
        await fastDone.promise;
        return "slow";
      },
    });
    const fast = tool({
      name: "fast",
      description: "answers first",
      parameters: z.object({}),
      execute: async () => {
        fastDone.resolve();
        return "fast";
      },
    });
    const llm = reactiveLLM((messages) =>
      lastMessage(messages)?.role === "tool"
        ? { content: "done", finishReason: "stop" }
        : {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [
              { id: "c-1", name: "slow", input: {} },
              { id: "c-2", name: "fast", input: {} },
            ],
          },
    );
    const result = await runner.run({
      workflow: agentAction({ name: "order", llm, tools: { slow, fast }, clock }),
      workflowId: "order-1",
      input: { task: "go" },
    });
    const tools = result.trace!.spans.filter((s) => s.kind === "tool");
    expect(tools.map((s) => (s.kind === "tool" ? s.toolCallId : ""))).toEqual(["c-1", "c-2"]);
  });
});
