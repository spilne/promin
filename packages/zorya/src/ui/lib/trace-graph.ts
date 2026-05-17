// ---------------------------------------------------------------------------
// trace-graph — pure transform from a thread's turn-tree (`AgentTraceDto`)
// into a flat DAG of nodes the `graph-layout` engine can place.
//
// The turn-tree the trace endpoint returns is a hierarchy; an operator
// debugging "why did the agent do that" wants the *call graph*:
//
//   user → assistant move → tool call → (results feed) → next assistant
//        → ... → final assistant answer → next turn's user → ...
//
// Each assistant move is one LLM turn; its tool calls fan out from it and
// the next assistant move depends on all of them (the model consumed
// every result before moving on). Turns are chained end-to-end so a
// multi-turn conversation reads as one long graph.
// ---------------------------------------------------------------------------

import type { AgentTraceDto, TraceChildDto } from "../api/client.ts";

export type TraceGraphNodeKind = "user" | "assistant" | "tool-call" | "system";

export interface TraceGraphNode {
  /** Stable, zero-padded id — also the within-rank sort key, so nodes
   *  keep conversation order within a layout column. */
  readonly id: string;
  readonly dependsOn: readonly string[];
  readonly kind: TraceGraphNodeKind;
  readonly turnIndex: number;
  /** Source message sequence number (−1 for orphan tool results). */
  readonly seq: number;
  /** Short label for the node card. */
  readonly label: string;
  /** True for a failed tool call. */
  readonly failed: boolean;
  /** True for an orphan tool call/result (no matching call or result). */
  readonly orphan: boolean;
  /** Raw trace child, for the detail panel. */
  readonly detail: TraceChildDto;
}

export interface TraceGraphModel {
  readonly nodes: ReadonlyArray<TraceGraphNode>;
}

const LABEL_MAX = 38;

/** Build the flat call-graph model from a thread trace. */
export function buildTraceGraph(trace: AgentTraceDto): TraceGraphModel {
  const nodes: TraceGraphNode[] = [];
  let counter = 0;
  const nextId = () => `n${String(counter++).padStart(5, "0")}`;

  // Ids the *next* node should depend on. Seeded empty; carries the
  // previous turn's terminal node(s) across turn boundaries.
  let cursorDeps: string[] = [];

  for (const turn of trace.turns) {
    for (const child of turn.children) {
      if (child.kind === "user") {
        const id = nextId();
        nodes.push({
          id,
          dependsOn: cursorDeps,
          kind: "user",
          turnIndex: turn.turnIndex,
          seq: child.seq,
          label: oneLine(child.content),
          failed: false,
          orphan: false,
          detail: child,
        });
        cursorDeps = [id];
      } else if (child.kind === "system") {
        const id = nextId();
        nodes.push({
          id,
          dependsOn: cursorDeps,
          kind: "system",
          turnIndex: turn.turnIndex,
          seq: child.seq,
          label: "system",
          failed: false,
          orphan: false,
          detail: child,
        });
        cursorDeps = [id];
      } else if (child.kind === "assistant") {
        const assistantId = nextId();
        nodes.push({
          id: assistantId,
          dependsOn: cursorDeps,
          kind: "assistant",
          turnIndex: turn.turnIndex,
          seq: child.seq,
          label: child.content ? oneLine(child.content) : "(tool calls)",
          failed: false,
          orphan: false,
          detail: child,
        });
        if (child.toolCalls.length > 0) {
          const toolIds: string[] = [];
          for (const tc of child.toolCalls) {
            const id = nextId();
            nodes.push({
              id,
              dependsOn: [assistantId],
              kind: "tool-call",
              turnIndex: turn.turnIndex,
              seq: tc.callSeq,
              label: tc.name,
              failed: tc.result?.failed === true,
              orphan: tc.callSeq === -1 || !tc.result,
              detail: tc,
            });
            toolIds.push(id);
          }
          // The next assistant move consumed every tool result.
          cursorDeps = toolIds;
        } else {
          cursorDeps = [assistantId];
        }
      } else {
        // Orphan tool-call surfaced as a top-level turn child.
        const id = nextId();
        nodes.push({
          id,
          dependsOn: cursorDeps,
          kind: "tool-call",
          turnIndex: turn.turnIndex,
          seq: child.callSeq,
          label: child.name,
          failed: child.result?.failed === true,
          orphan: true,
          detail: child,
        });
        cursorDeps = [id];
      }
    }
  }

  return { nodes };
}

/** Collapse whitespace and clamp to a node-card-sized label. */
function oneLine(text: string): string {
  const collapsed = text.replace(/\s+/g, " ").trim();
  if (collapsed.length === 0) return "(empty)";
  return collapsed.length <= LABEL_MAX ? collapsed : collapsed.slice(0, LABEL_MAX - 1) + "…";
}
