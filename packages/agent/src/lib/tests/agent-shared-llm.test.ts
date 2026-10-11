// ---------------------------------------------------------------------------
// agent-shared — how a turn handles adapter-reported tool-argument errors
// and an LLM call aborted mid-stream.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { executeToolCall, runLlmCall } from "../agent-shared.ts";
import type { AgentTool } from "../tool.ts";
import type { LLMProvider, LLMStreamChunk } from "../llm-provider.ts";
import type { ProcessorContext } from "../processors.ts";

const CTX = { step: 0, turn: 0, workflowId: "wf", isReplay: false } as ProcessorContext;

describe("executeToolCall — inputError", () => {
  it("reports the adapter's argument error to the model without running the tool", async () => {
    let ran = false;
    const search: AgentTool<{ q?: string }, string> = {
      name: "search",
      description: "search",
      parameters: z.object({ q: z.string().optional() }),
      execute: async () => {
        ran = true;
        return "results";
      },
    };
    const result = await executeToolCall(
      { id: "c1", name: "search", input: {}, inputError: "tool arguments are not valid JSON" },
      search,
    );
    expect(ran).toBe(false);
    expect(result.toolCallId).toBe("c1");
    expect(result.content).toBe("Invalid input: tool arguments are not valid JSON");
  });
});

describe("runLlmCall — abort mid-stream", () => {
  function streamingThenAborted(controller: AbortController, deltas: string[]): LLMProvider {
    return {
      chat: async () => ({ content: null, finishReason: "stop" }),
      chatStream: async function* (): AsyncIterable<LLMStreamChunk> {
        for (const delta of deltas) yield { delta };
        controller.abort();
        const err = new Error("aborted");
        err.name = "AbortError";
        throw err;
      },
    };
  }

  it("keeps the text streamed before the abort", async () => {
    const controller = new AbortController();
    const res = await runLlmCall({
      llm: streamingThenAborted(controller, ["Hello", ", wor"]),
      messages: [{ role: "user", content: "hi" }],
      processorCtx: CTX,
      onChunk: () => {},
      signal: controller.signal,
    });
    expect(res.content).toBe("Hello, wor");
    expect(res.toolCalls).toBeUndefined();
    expect(res.finishReason).toBe("stop");
  });

  it("returns null content, not empty text, when nothing was streamed", async () => {
    const controller = new AbortController();
    const res = await runLlmCall({
      llm: streamingThenAborted(controller, []),
      messages: [{ role: "user", content: "hi" }],
      processorCtx: CTX,
      onChunk: () => {},
      signal: controller.signal,
    });
    expect(res.content).toBeNull();
  });
});
