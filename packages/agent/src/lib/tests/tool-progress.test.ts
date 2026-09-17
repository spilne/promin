// ---------------------------------------------------------------------------
// Tool-progress writer — verifies that a tool calling
// `ctx.writer.write(payload)` mid-execution emits `tool.progress`
// SessionEvents labeled with the current turn / step / toolCallId.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { agentLoop } from "../agent-loop.ts";
import { tool } from "../tool.ts";
import type { LLMProvider, LLMResponse } from "../llm-provider.ts";
import type { SessionEvent } from "../session-logger.ts";

function scriptedLLM(...responses: LLMResponse[]): LLMProvider {
  let i = 0;
  return {
    chat: async (): Promise<LLMResponse> => {
      const r = responses[i++];
      if (!r) throw new Error("scripted LLM exhausted");
      return r;
    },
  };
}

describe("tool-progress writer", () => {
  it("writer.write emits tool.progress events labeled with turn / step / toolCallId", async () => {
    const downloadTool = tool({
      name: "download",
      description: "Long-running download with progress.",
      parameters: z.object({ url: z.string() }),
      execute: async ({ url }, ctx) => {
        ctx?.writer?.write({ percent: 25 });
        ctx?.writer?.write({ percent: 50 });
        ctx?.writer?.write({ percent: 100, sha: "abc" });
        return { ok: true, url };
      },
    });

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = await agentLoop({
      heartbeatMs: 0,
      llm: scriptedLLM(
        {
          content: null,
          finishReason: "tool_use",
          toolCalls: [{ id: "t1", name: "download", input: { url: "x" } }],
        },
        { content: "done", finishReason: "stop" },
      ),
      tools: { download: downloadTool },
    }).session({ runner, sessionId: "tp-1" });

    const events: SessionEvent[] = [];
    session.subscribe((e) => events.push(e));
    await session.send("download something");

    const progress = events.filter((e) => e.type === "tool.progress");
    expect(progress).toHaveLength(3);
    expect(progress.map((e) => (e as { payload: unknown }).payload)).toEqual([
      { percent: 25 },
      { percent: 50 },
      { percent: 100, sha: "abc" },
    ]);
    // Labeling
    progress.forEach((e) => {
      expect(e).toMatchObject({
        type: "tool.progress",
        toolCallId: "t1",
        name: "download",
        turn: 0,
      });
    });

    await session.close();
  });

  it("tools that ignore the writer keep working unchanged (backwards-compat)", async () => {
    // Tool defined with the original single-arg signature — no ctx.
    const noProgressTool = tool({
      name: "noProgress",
      description: "Returns immediately without progress.",
      parameters: z.object({ x: z.number() }),
      execute: async ({ x }) => ({ doubled: x * 2 }),
    });

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = await agentLoop({
      heartbeatMs: 0,
      llm: scriptedLLM(
        {
          content: null,
          finishReason: "tool_use",
          toolCalls: [{ id: "t1", name: "noProgress", input: { x: 5 } }],
        },
        { content: "ok", finishReason: "stop" },
      ),
      tools: { noProgress: noProgressTool },
    }).session({ runner, sessionId: "tp-2" });

    const events: SessionEvent[] = [];
    session.subscribe((e) => events.push(e));
    await session.send("call it");

    expect(events.filter((e) => e.type === "tool.progress")).toHaveLength(0);
    expect(events.find((e) => e.type === "tool.end")).toBeDefined();

    await session.close();
  });

  it("writer.write across multiple tool calls in one turn carries distinct toolCallIds", async () => {
    const writerTool = tool({
      name: "ping",
      description: "emits one progress event.",
      parameters: z.object({}).strict(),
      execute: async (_input, ctx) => {
        ctx?.writer?.write({ ok: true });
        return "done";
      },
    });

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const session = await agentLoop({
      heartbeatMs: 0,
      llm: scriptedLLM(
        {
          content: null,
          finishReason: "tool_use",
          toolCalls: [
            { id: "a", name: "ping", input: {} },
            { id: "b", name: "ping", input: {} },
          ],
        },
        { content: "done", finishReason: "stop" },
      ),
      tools: { ping: writerTool },
    }).session({ runner, sessionId: "tp-3" });

    const events: SessionEvent[] = [];
    session.subscribe((e) => events.push(e));
    await session.send("ping twice");

    const progress = events
      .filter((e) => e.type === "tool.progress")
      .map((e) => (e as { toolCallId: string }).toolCallId)
      .sort();
    expect(progress).toEqual(["a", "b"]);

    await session.close();
  });
});
