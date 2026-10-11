// ---------------------------------------------------------------------------
// Cancellation and tool time limits.
//
//   - executeToolCall hands the tool `toolCallId`, `signal` and
//     `timeoutMs`, fails the call (isError) on timeout or cancellation
//     whether or not the tool honors the signal.
//   - agentAction threads its `signal` into every LLM call and tool, and
//     a cancelled turn ends with finishReason "cancelled", a turn.aborted
//     event and a transcript that ends with an assistant message.
//   - `AgentTool.timeoutMs` overrides the agent's `toolTimeoutMs`.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { FakeWallClock } from "@promin/workflow";
import { CANCELLED_ANSWER, agentAction } from "../agent-action.ts";
import { executeToolCall } from "../agent-shared.ts";
import type { AgentTool, ToolExecuteContext } from "../tool.ts";
import {
  collectingBus,
  deferred,
  freshRunner,
  lastMessage,
  reactiveLLM,
  until,
} from "./engine-harness.ts";

/** A tool that never finishes on its own and records the ctx it got. */
function hangingTool(params: {
  seen: ToolExecuteContext[];
  timeoutMs?: number;
}): AgentTool<Record<string, never>, string> {
  return {
    name: "hang",
    description: "never returns",
    parameters: z.object({}),
    ...(params.timeoutMs !== undefined && { timeoutMs: params.timeoutMs }),
    execute: (_input, ctx) => {
      if (ctx) params.seen.push(ctx);
      return new Promise<string>(() => {});
    },
  };
}

describe("executeToolCall — context, timeout and cancellation", () => {
  it("passes toolCallId, signal and timeoutMs to the tool", async () => {
    const seen: ToolExecuteContext[] = [];
    const echo: AgentTool<Record<string, never>, string> = {
      name: "echo",
      description: "echo",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        if (ctx) seen.push(ctx);
        return "ok";
      },
    };
    const result = await executeToolCall({
      call: { id: "c-7", name: "echo", input: {} },
      tool: echo,
      timeoutMs: 1_000,
      clock: FakeWallClock.create(0),
    });
    expect(result).toEqual({ role: "tool", toolCallId: "c-7", content: "ok" });
    expect(seen[0]?.toolCallId).toBe("c-7");
    expect(seen[0]?.timeoutMs).toBe(1_000);
    expect(seen[0]?.signal?.aborted).toBe(false);
  });

  it("fails a call that outlives its timeout and aborts the tool's signal", async () => {
    const clock = FakeWallClock.create(0);
    const seen: ToolExecuteContext[] = [];
    const pending = executeToolCall({
      call: { id: "c-1", name: "hang", input: {} },
      tool: hangingTool({ seen }),
      timeoutMs: 500,
      clock,
    });
    await until(() => clock.pendingCount() === 1);
    clock.advance(500);
    const result = await pending;
    expect(result.isError).toBe(true);
    expect(result.content).toBe("Tool timed out after 500ms.");
    expect(seen[0]?.signal?.aborted).toBe(true);
  });

  it("fails a call when the turn's signal aborts", async () => {
    const controller = new AbortController();
    const seen: ToolExecuteContext[] = [];
    const pending = executeToolCall({
      call: { id: "c-1", name: "hang", input: {} },
      tool: hangingTool({ seen }),
      signal: controller.signal,
    });
    await until(() => seen.length === 1);
    controller.abort();
    const result = await pending;
    expect(result.isError).toBe(true);
    expect(result.content).toContain("Tool cancelled");
    expect(seen[0]?.signal?.aborted).toBe(true);
  });
});

describe("agentAction — cancellation", () => {
  it("threads the signal into LLM calls and tools and ends the turn as cancelled", async () => {
    const controller = new AbortController();
    const seen: ToolExecuteContext[] = [];
    const llm = reactiveLLM((messages) =>
      lastMessage(messages)?.role === "tool"
        ? { content: "should not be reached", finishReason: "stop" }
        : {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "c-1", name: "hang", input: {} }],
          },
    );
    const { runner } = freshRunner();
    const { bus, events } = collectingBus();
    const run = runner.run({
      workflow: agentAction({
        name: "cancel",
        llm,
        tools: { hang: hangingTool({ seen }) },
        signal: controller.signal,
        bus,
      }),
      workflowId: "cancel-1",
      input: { task: "go" },
    });
    await until(() => seen.length === 1);
    expect(llm.calls[0]?.signal).toBe(controller.signal);
    expect(seen[0]?.toolCallId).toBe("c-1");
    controller.abort();

    const result = await run;
    expect(result.finishReason).toBe("cancelled");
    expect(result.answer).toBe(CANCELLED_ANSWER);
    expect(llm.calls).toHaveLength(1);
    expect(result.messages.find((m) => m.role === "tool")).toMatchObject({
      toolCallId: "c-1",
      isError: true,
    });
    expect(lastMessage(result.messages)).toEqual({ role: "assistant", content: CANCELLED_ANSWER });
    const types = events.map((e) => e.type);
    expect(types).toContain("turn.aborted");
    expect(types).not.toContain("turn.end");
  });

  it("keeps text streamed before the abort as the answer", async () => {
    const controller = new AbortController();
    const started = deferred();
    const llm = {
      chat: async () => ({ content: "unused", finishReason: "stop" as const }),
      chatStream: async function* () {
        yield { delta: "partial " };
        started.resolve();
        await new Promise<void>((_, reject) =>
          controller.signal.addEventListener("abort", () => {
            const err = new Error("aborted");
            err.name = "AbortError";
            reject(err);
          }),
        );
      },
    };
    const { runner } = freshRunner();
    const run = runner.run({
      workflow: agentAction({
        name: "cancel-stream",
        llm,
        signal: controller.signal,
        onChunk: () => {},
      }),
      workflowId: "cancel-stream-1",
      input: { task: "go" },
    });
    await started.promise;
    controller.abort();
    const result = await run;
    expect(result.finishReason).toBe("cancelled");
    expect(result.answer).toBe("partial ");
  });
});

describe("agentAction — tool time limits", () => {
  it("applies toolTimeoutMs, and a tool's own timeoutMs wins", async () => {
    const clock = FakeWallClock.create(0);
    const seenDefault: ToolExecuteContext[] = [];
    const seenOwn: ToolExecuteContext[] = [];
    const slow = { ...hangingTool({ seen: seenDefault }), name: "slow" };
    const slower = { ...hangingTool({ seen: seenOwn, timeoutMs: 50 }), name: "slower" };
    const llm = reactiveLLM((messages) =>
      lastMessage(messages)?.role === "tool"
        ? { content: "gave up", finishReason: "stop" }
        : {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [
              { id: "a", name: "slow", input: {} },
              { id: "b", name: "slower", input: {} },
            ],
          },
    );
    const { runner } = freshRunner();
    const run = runner.run({
      workflow: agentAction({
        name: "timeouts",
        llm,
        tools: { slow, slower },
        toolTimeoutMs: 1_000,
        clock,
      }),
      workflowId: "timeouts-1",
      input: { task: "go" },
    });
    await until(() => seenDefault.length === 1 && seenOwn.length === 1);
    expect(seenDefault[0]?.timeoutMs).toBe(1_000);
    expect(seenOwn[0]?.timeoutMs).toBe(50);
    clock.advance(1_000);
    const result = await run;
    const toolResults = result.messages.filter((m) => m.role === "tool");
    expect(toolResults.map((m) => m.role === "tool" && m.content)).toEqual([
      "Tool timed out after 1000ms.",
      "Tool timed out after 50ms.",
    ]);
    expect(result.answer).toBe("gave up");
  });
});
