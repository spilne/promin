// ---------------------------------------------------------------------------
// Stall detector: repeated calls with no new text, or one tool called too
// often, force a final no-tools round (tool_choice "none"); a forced round
// that still says nothing ends with the STALLED_ANSWER sentinel.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { STALLED_ANSWER, agentAction } from "../agent-action.ts";
import type { LLMResponse } from "../llm-provider.ts";
import type { Message } from "../message.ts";
import { StallDetector } from "../stall-detector.ts";
import { tool } from "../tool.ts";
import { collectingBus, freshRunner, reactiveLLM } from "./engine-harness.ts";

const call = (id: string, input: unknown) => ({ id, name: "search", input });

describe("StallDetector", () => {
  it("flags repeated identical calls with no new text after maxRepeatRounds", () => {
    const d = new StallDetector({ maxRepeatRounds: 2 });
    expect(d.observe({ content: null, calls: [call("1", { q: "a" })] }).stalled).toBe(false);
    expect(d.observe({ content: null, calls: [call("2", { q: "a" })] }).stalled).toBe(false);
    expect(d.observe({ content: null, calls: [call("3", { q: "a" })] })).toEqual({
      stalled: true,
      reason: "repeated_calls",
      toolName: "search",
    });
  });

  it("treats key order as the same call and resets on new input or new text", () => {
    const d = new StallDetector({ maxRepeatRounds: 2 });
    d.observe({ content: null, calls: [call("1", { a: 1, b: 2 })] });
    expect(d.observe({ content: null, calls: [call("2", { b: 2, a: 1 })] }).stalled).toBe(false);
    expect(d.observe({ content: "new thought", calls: [call("3", { a: 1, b: 2 })] }).stalled).toBe(
      false,
    );
    expect(d.observe({ content: null, calls: [call("4", { a: 2 })] }).stalled).toBe(false);
  });

  it("flags a tool called more than maxCallsPerTool times", () => {
    const d = new StallDetector({ maxCallsPerTool: 2 });
    d.observe({ content: null, calls: [call("1", { q: 1 })] });
    d.observe({ content: null, calls: [call("2", { q: 2 })] });
    expect(d.observe({ content: null, calls: [call("3", { q: 3 })] })).toEqual({
      stalled: true,
      reason: "tool_budget",
      toolName: "search",
    });
  });
});

describe("agentAction — stall detection", () => {
  const search = tool({
    name: "search",
    description: "search",
    parameters: z.object({ q: z.string() }),
    execute: async () => "nothing new",
  });

  /** Repeats the same search until tools are disabled, then answers with `final`. */
  function loopingLLM(final: string | null) {
    let n = 0;
    return reactiveLLM((_messages: ReadonlyArray<Message>, params): LLMResponse => {
      n++;
      if (params.toolChoice === "none") return { content: final, finishReason: "stop" };
      return {
        content: null,
        finishReason: "tool_calls",
        toolCalls: [call(`c${n}`, { q: "same" })],
      };
    });
  }

  it("forces a final answer when the agent loops on the same call", async () => {
    const llm = loopingLLM("best effort answer");
    const { runner } = freshRunner();
    const { bus, events } = collectingBus();
    const result = await runner.run({
      workflow: agentAction({
        name: "stall",
        llm,
        tools: { search },
        stall: { maxRepeatRounds: 2 },
        bus,
      }),
      workflowId: "stall-1",
      input: { task: "find it" },
    });
    expect(result.finishReason).toBe("stalled");
    expect(result.answer).toBe("best effort answer");
    // Round 1 new, rounds 2–3 repeats → stall; round 4 is the forced answer.
    expect(llm.calls).toHaveLength(4);
    expect(llm.calls[3]?.toolChoice).toBe("none");
    expect(llm.calls[3]?.tools?.length).toBeGreaterThan(0);
    expect(events.filter((e) => e.type === "stall.detected")).toHaveLength(1);
  });

  it("ends with the stalled sentinel when the forced round says nothing", async () => {
    const llm = loopingLLM(null);
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({
        name: "stall-empty",
        llm,
        tools: { search },
        stall: { maxRepeatRounds: 1 },
      }),
      workflowId: "stall-2",
      input: { task: "find it" },
    });
    expect(result.finishReason).toBe("stalled");
    expect(result.answer).toBe(STALLED_ANSWER);
    expect(result.messages.at(-1)).toEqual({ role: "assistant", content: STALLED_ANSWER });
  });

  it("stall: false lets the loop run to the step limit", async () => {
    const llm = loopingLLM("unused");
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({
        name: "no-stall",
        llm,
        tools: { search },
        stall: false,
        maxSteps: 5,
      }),
      workflowId: "stall-3",
      input: { task: "find it" },
    });
    expect(result.finishReason).toBe("max_steps");
    expect(llm.calls).toHaveLength(5);
  });
});
