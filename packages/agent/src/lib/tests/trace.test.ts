// ---------------------------------------------------------------------------
// buildAgentTrace — pure transform from stored Message[] to a turn-tree.
// Pinned cases:
//   1. Empty input → empty trace
//   2. user → assistant → groups into one turn
//   3. user → assistant w/ toolCall → tool result → assistant text:
//      tool-call gets stitched to its result inside the assistant move
//   4. Multi-turn: two user messages produce two turns
//   5. Orphan tool result (no matching call) surfaces as orphan node
//   6. Failure detection on tool result content
//   7. Leading system messages collected separately, don't form a turn
//   8. Synthetic turn 0 when conversation starts with assistant (no user)
//   9. summary counters reflect the trace shape
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { buildAgentTrace } from "../trace.ts";
import type { Message } from "../message.ts";

describe("buildAgentTrace", () => {
  it("empty input → empty trace", () => {
    const trace = buildAgentTrace([]);
    expect(trace.turns).toHaveLength(0);
    expect(trace.orphanSystem).toHaveLength(0);
    expect(trace.summary.turns).toBe(0);
  });

  it("user → assistant groups into one turn with two children", () => {
    const messages: Message[] = [
      { role: "user", content: "hi" },
      { role: "assistant", content: "hello back" },
    ];
    const trace = buildAgentTrace(messages);
    expect(trace.turns).toHaveLength(1);
    const turn = trace.turns[0]!;
    expect(turn.children).toHaveLength(2);
    expect(turn.children[0]!.kind).toBe("user");
    expect(turn.children[1]!.kind).toBe("assistant");
  });

  it("stitches tool calls to their results inside the assistant move", () => {
    const messages: Message[] = [
      { role: "user", content: "search for cats" },
      {
        role: "assistant",
        content: null,
        toolCalls: [{ id: "c1", name: "search", input: { q: "cats" } }],
      },
      { role: "tool", toolCallId: "c1", content: "found 42 results" },
      { role: "assistant", content: "There are 42 results." },
    ];
    const trace = buildAgentTrace(messages);
    expect(trace.turns).toHaveLength(1);
    const turn = trace.turns[0]!;
    // user, assistant (with one tool call inside), assistant
    expect(turn.children.length).toBe(3);
    const firstAssistant = turn.children[1]!;
    expect(firstAssistant.kind).toBe("assistant");
    if (firstAssistant.kind !== "assistant") return;
    expect(firstAssistant.toolCalls).toHaveLength(1);
    expect(firstAssistant.toolCalls[0]!.name).toBe("search");
    expect(firstAssistant.toolCalls[0]!.result?.content).toBe("found 42 results");
    expect(firstAssistant.toolCalls[0]!.result?.failed).toBe(false);
  });

  it("multi-turn: two user messages produce two turns", () => {
    const messages: Message[] = [
      { role: "user", content: "first" },
      { role: "assistant", content: "first reply" },
      { role: "user", content: "second" },
      { role: "assistant", content: "second reply" },
    ];
    const trace = buildAgentTrace(messages);
    expect(trace.turns).toHaveLength(2);
    expect(trace.turns[0]!.turnIndex).toBe(0);
    expect(trace.turns[1]!.turnIndex).toBe(1);
  });

  it("orphan tool result (no matching call) surfaces as a top-level node", () => {
    const messages: Message[] = [
      { role: "user", content: "go" },
      { role: "assistant", content: "ok" },
      { role: "tool", toolCallId: "ghost", content: "stale result" },
    ];
    const trace = buildAgentTrace(messages);
    expect(trace.turns).toHaveLength(1);
    const turn = trace.turns[0]!;
    const orphan = turn.children.find((c) => c.kind === "tool-call");
    expect(orphan).toBeDefined();
    if (orphan?.kind !== "tool-call") return;
    expect(orphan.name).toBe("(orphan-result)");
    expect(orphan.callSeq).toBe(-1);
    expect(trace.summary.orphanedToolResults).toBe(1);
  });

  it("flags tool result content starting with Error: as failed", () => {
    const messages: Message[] = [
      { role: "user", content: "do it" },
      {
        role: "assistant",
        content: null,
        toolCalls: [{ id: "c1", name: "risky", input: {} }],
      },
      { role: "tool", toolCallId: "c1", content: "Error: nope" },
    ];
    const trace = buildAgentTrace(messages);
    const turn = trace.turns[0]!;
    const a = turn.children[1]!;
    if (a.kind !== "assistant") throw new Error("expected assistant");
    expect(a.toolCalls[0]!.result?.failed).toBe(true);
    expect(trace.summary.toolFailures).toBe(1);
  });

  it("leading system messages are collected separately, no turn", () => {
    const messages: Message[] = [
      { role: "system", content: "you are helpful" },
      { role: "user", content: "hi" },
      { role: "assistant", content: "hi" },
    ];
    const trace = buildAgentTrace(messages);
    expect(trace.orphanSystem).toHaveLength(1);
    expect(trace.turns).toHaveLength(1);
  });

  it("conversation starting with assistant produces a synthetic turn", () => {
    const messages: Message[] = [
      { role: "assistant", content: "welcome" },
      { role: "user", content: "thanks" },
      { role: "assistant", content: "you bet" },
    ];
    const trace = buildAgentTrace(messages);
    expect(trace.turns).toHaveLength(2);
    // First turn has no user child.
    const t0 = trace.turns[0]!;
    expect(t0.children.find((c) => c.kind === "user")).toBeUndefined();
    expect(t0.children[0]!.kind).toBe("assistant");
  });

  it("summary counters reflect the trace shape", () => {
    const messages: Message[] = [
      { role: "user", content: "go" },
      {
        role: "assistant",
        content: null,
        toolCalls: [
          { id: "a", name: "tool_a", input: {} },
          { id: "b", name: "tool_b", input: {} },
        ],
      },
      { role: "tool", toolCallId: "a", content: "ok-a" },
      { role: "tool", toolCallId: "b", content: "Error: failed" },
      { role: "assistant", content: "summary" },
    ];
    const trace = buildAgentTrace(messages);
    expect(trace.summary.turns).toBe(1);
    expect(trace.summary.assistantMoves).toBe(2);
    expect(trace.summary.toolCalls).toBe(2);
    expect(trace.summary.toolFailures).toBe(1);
    expect(trace.summary.orphanedToolCalls).toBe(0);
  });

  it("surfaces a callAgent sub-run trace from result-message metadata as childTrace", () => {
    // A peer's run, pre-built by callAgent and stowed on the tool
    // result's metadata under `childTrace`.
    const childTrace = buildAgentTrace([
      { role: "user", content: "draft a haiku" },
      { role: "assistant", content: "branches into the sub-run" },
    ]);
    const messages: Message[] = [
      { role: "user", content: "ask the writer" },
      {
        role: "assistant",
        content: null,
        toolCalls: [{ id: "c1", name: "callAgent", input: { id: "writer", prompt: "x" } }],
      },
      { role: "tool", toolCallId: "c1", content: '{"ok":true}', metadata: { childTrace } },
      { role: "assistant", content: "done" },
    ];
    const assistant = buildAgentTrace(messages).turns[0]!.children.find(
      (c) => c.kind === "assistant",
    );
    const callNode = assistant?.kind === "assistant" ? assistant.toolCalls[0] : undefined;
    expect(callNode?.name).toBe("callAgent");
    expect(callNode?.childTrace?.turns).toHaveLength(1);
  });

  it("ignores result metadata that isn't a well-formed child trace", () => {
    const messages: Message[] = [
      { role: "user", content: "go" },
      {
        role: "assistant",
        content: null,
        toolCalls: [{ id: "c1", name: "someTool", input: {} }],
      },
      { role: "tool", toolCallId: "c1", content: "ok", metadata: { childTrace: "not-a-trace" } },
    ];
    const assistant = buildAgentTrace(messages).turns[0]!.children.find(
      (c) => c.kind === "assistant",
    );
    const callNode = assistant?.kind === "assistant" ? assistant.toolCalls[0] : undefined;
    expect(callNode?.childTrace).toBeUndefined();
  });
});
