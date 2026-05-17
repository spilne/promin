// ---------------------------------------------------------------------------
// trace-graph — transform a thread turn-tree into a flat call-graph DAG.
// Pinned cases:
//   1. Empty trace → no nodes
//   2. user → assistant — assistant depends on the user node
//   3. Tool calls fan out from their assistant; the next assistant move
//      depends on every tool result
//   4. A failed tool result flags the node
//   5. Multi-turn — the next turn's user depends on the prior turn's
//      terminal node(s)
//   6. An orphan top-level tool-call child is flagged orphan
//   7. Labels collapse whitespace and clamp to a card-sized string
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { AgentTraceDto, TraceChildDto } from "../../api/client.ts";
import { buildTraceGraph } from "../trace-graph.ts";

function trace(turns: AgentTraceDto["turns"]): AgentTraceDto {
  return {
    turns,
    orphanSystem: [],
    summary: {
      turns: turns.length,
      assistantMoves: 0,
      toolCalls: 0,
      toolFailures: 0,
      orphanedToolCalls: 0,
      orphanedToolResults: 0,
    },
  };
}

function turn(turnIndex: number, children: TraceChildDto[]): AgentTraceDto["turns"][number] {
  return { kind: "turn", turnIndex, fromSeq: 0, toSeq: 0, children };
}

describe("buildTraceGraph", () => {
  it("returns no nodes for an empty trace", () => {
    expect(buildTraceGraph(trace([])).nodes).toHaveLength(0);
  });

  it("links the assistant move to the user message", () => {
    const m = buildTraceGraph(
      trace([
        turn(0, [
          { kind: "user", seq: 0, content: "hi" },
          { kind: "assistant", seq: 1, content: "hello", toolCalls: [] },
        ]),
      ]),
    );
    expect(m.nodes).toHaveLength(2);
    const [user, assistant] = m.nodes;
    expect(user!.kind).toBe("user");
    expect(assistant!.kind).toBe("assistant");
    expect(assistant!.dependsOn).toEqual([user!.id]);
  });

  it("fans tool calls out of the assistant and rejoins on the next move", () => {
    const m = buildTraceGraph(
      trace([
        turn(0, [
          { kind: "user", seq: 0, content: "search please" },
          {
            kind: "assistant",
            seq: 1,
            content: null,
            toolCalls: [
              {
                kind: "tool-call",
                id: "c1",
                name: "search",
                input: {},
                callSeq: 1,
                result: { seq: 2, content: "ok", failed: false },
              },
              {
                kind: "tool-call",
                id: "c2",
                name: "fetch",
                input: {},
                callSeq: 1,
                result: { seq: 3, content: "ok", failed: false },
              },
            ],
          },
          { kind: "assistant", seq: 4, content: "done", toolCalls: [] },
        ]),
      ]),
    );
    const [user, a1, tc1, tc2, a2] = m.nodes;
    expect([user!.kind, a1!.kind, tc1!.kind, tc2!.kind, a2!.kind]).toEqual([
      "user",
      "assistant",
      "tool-call",
      "tool-call",
      "assistant",
    ]);
    expect(tc1!.dependsOn).toEqual([a1!.id]);
    expect(tc2!.dependsOn).toEqual([a1!.id]);
    // The second assistant move consumed both tool results.
    expect(a2!.dependsOn).toEqual([tc1!.id, tc2!.id]);
  });

  it("flags a failed tool result", () => {
    const m = buildTraceGraph(
      trace([
        turn(0, [
          { kind: "user", seq: 0, content: "go" },
          {
            kind: "assistant",
            seq: 1,
            content: null,
            toolCalls: [
              {
                kind: "tool-call",
                id: "c1",
                name: "search",
                input: {},
                callSeq: 1,
                result: { seq: 2, content: "Error: boom", failed: true },
              },
            ],
          },
        ]),
      ]),
    );
    const tool = m.nodes.find((n) => n.kind === "tool-call")!;
    expect(tool.failed).toBe(true);
  });

  it("chains the next turn's user onto the prior turn's terminal node", () => {
    const m = buildTraceGraph(
      trace([
        turn(0, [
          { kind: "user", seq: 0, content: "first" },
          { kind: "assistant", seq: 1, content: "answer one", toolCalls: [] },
        ]),
        turn(1, [
          { kind: "user", seq: 2, content: "second" },
          { kind: "assistant", seq: 3, content: "answer two", toolCalls: [] },
        ]),
      ]),
    );
    const [, a1, u2] = m.nodes;
    expect(u2!.kind).toBe("user");
    expect(u2!.turnIndex).toBe(1);
    expect(u2!.dependsOn).toEqual([a1!.id]);
  });

  it("flags an orphan top-level tool-call child", () => {
    const m = buildTraceGraph(
      trace([
        turn(0, [
          { kind: "user", seq: 0, content: "hi" },
          {
            kind: "tool-call",
            id: "stray",
            name: "(orphan-result)",
            input: undefined,
            callSeq: -1,
            result: { seq: 1, content: "leftover", failed: false },
          },
        ]),
      ]),
    );
    const orphan = m.nodes.find((n) => n.kind === "tool-call")!;
    expect(orphan.orphan).toBe(true);
  });

  it("collapses whitespace and clamps long labels", () => {
    const long = "x".repeat(200);
    const m = buildTraceGraph(
      trace([turn(0, [{ kind: "user", seq: 0, content: `multi\n   line\ttext ${long}` }])]),
    );
    const label = m.nodes[0]!.label;
    expect(label).not.toContain("\n");
    expect(label).not.toContain("\t");
    expect(label.length).toBeLessThanOrEqual(38);
    expect(label.endsWith("…")).toBe(true);
  });
});
