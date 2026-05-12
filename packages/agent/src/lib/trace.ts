// ---------------------------------------------------------------------------
// Agent run trace — pure transform from a stored Message[] into a
// turn-structured tree, suitable for rendering as a graph or indented
// tree in the UI.
//
// v0 sources from the persisted message stream alone (role + toolCalls
// + toolCallId linkage). Doesn't yet pull from SessionEvents (which
// today aren't persisted) or workflow journal entries; both can be
// layered in later as additional inputs without breaking the consumer
// shape.
//
// The output is intentionally NOT the SessionEvent stream. SessionEvents
// are a low-level firehose; this is a structured "what did the agent
// actually do" tree shaped around how operators think about a run:
// turn → assistant moves (text / tool calls) → tool results → final answer.
// ---------------------------------------------------------------------------

import type { Message, ToolCall } from "./message.ts";

export type TraceNode =
  | TraceTurnNode
  | TraceUserNode
  | TraceAssistantNode
  | TraceToolCallNode
  | TraceSystemNode;

/**
 * One conversational turn — anchored on a user message (or initial
 * system seed when no user msg precedes the first assistant turn).
 * Holds the assistant move and any tool calls that move triggered.
 */
export interface TraceTurnNode {
  readonly kind: "turn";
  readonly turnIndex: number;
  /** Sequence range covered by this turn (inclusive). */
  readonly fromSeq: number;
  readonly toSeq: number;
  readonly children: ReadonlyArray<TraceNode>;
}

export interface TraceUserNode {
  readonly kind: "user";
  readonly seq: number;
  readonly content: string;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface TraceAssistantNode {
  readonly kind: "assistant";
  readonly seq: number;
  readonly content: string | null;
  readonly thinkingBlocks?: ReadonlyArray<unknown>;
  /**
   * Tool-call children of this assistant move. Each entry pairs a call
   * with its matching tool-result message (when one exists).
   */
  readonly toolCalls: ReadonlyArray<TraceToolCallNode>;
}

export interface TraceToolCallNode {
  readonly kind: "tool-call";
  readonly id: string;
  readonly name: string;
  readonly input: unknown;
  /** Sequence number of the assistant message that issued the call. */
  readonly callSeq: number;
  /** Filled when a matching tool-result message is found. */
  readonly result?: {
    readonly seq: number;
    readonly content: string;
    /**
     * Best-effort failure flag. We don't have structured `failed: true`
     * on stored tool messages today; treat any result whose content
     * starts with `Error:` (the agent runtime's convention for surfaced
     * failures) as a failure for highlighting purposes.
     */
    readonly failed: boolean;
  };
}

export interface TraceSystemNode {
  readonly kind: "system";
  readonly seq: number;
  readonly content: string;
}

export interface TraceSummary {
  readonly turns: number;
  readonly assistantMoves: number;
  readonly toolCalls: number;
  readonly toolFailures: number;
  readonly orphanedToolCalls: number;
  readonly orphanedToolResults: number;
}

export interface AgentTrace {
  readonly turns: ReadonlyArray<TraceTurnNode>;
  /** System-role messages that aren't part of any turn (rare, but possible). */
  readonly orphanSystem: ReadonlyArray<TraceSystemNode>;
  readonly summary: TraceSummary;
}

interface IndexedMessage {
  readonly seq: number;
  readonly msg: Message;
}

/**
 * Pure transform: stored messages → turn-structured trace.
 *
 * Turn boundaries are user messages. The first leading non-user
 * messages (if any) become a synthetic "turn 0" so a chat that starts
 * with a system-seeded assistant turn doesn't lose its leading content.
 */
export function buildAgentTrace(rawMessages: ReadonlyArray<Message>): AgentTrace {
  // Normalise + assign seq if not already present. We accept either the
  // raw `Message` shape (no seq) or any object that already carries one;
  // the gateway returns stored messages with `seq` stripped, so we
  // generate sequential indices here for stable rendering.
  const messages: IndexedMessage[] = rawMessages.map((msg, i) => ({
    seq: (msg as { seq?: number }).seq ?? i,
    msg,
  }));

  // Index tool results by toolCallId for O(1) lookup when stitching
  // calls to results. Tool results CAN appear out of strict order if
  // the agent loop is buggy or if we're rendering a snapshot mid-turn,
  // so we don't assume positional adjacency.
  const resultByCallId = new Map<string, IndexedMessage>();
  for (const im of messages) {
    if (im.msg.role === "tool") {
      resultByCallId.set(im.msg.toolCallId, im);
    }
  }

  const turns: TraceTurnNode[] = [];
  const orphanSystem: TraceSystemNode[] = [];
  let i = 0;

  // Eat any leading system messages — they're not part of a turn.
  while (i < messages.length && messages[i]!.msg.role === "system") {
    const im = messages[i]!;
    orphanSystem.push({
      kind: "system",
      seq: im.seq,
      content: (im.msg as { content: string }).content,
    });
    i += 1;
  }

  let turnIndex = 0;
  while (i < messages.length) {
    const turnStart = i;
    const children: TraceNode[] = [];

    // A turn starts with a user message in the normal case. If the
    // very first non-system message is an assistant move (e.g. a
    // pre-seeded greeting), promote the assistant move to a synthetic
    // turn 0 with no user node.
    if (messages[i]!.msg.role === "user") {
      const userIm = messages[i]!;
      const userMsg = userIm.msg as Extract<Message, { role: "user" }>;
      children.push({
        kind: "user",
        seq: userIm.seq,
        content: userMsg.content,
        ...(userMsg.metadata && { metadata: userMsg.metadata }),
      });
      i += 1;
    }

    // Collect every message that belongs to this turn — assistant +
    // tool results — until we hit the next user message OR end of input.
    while (i < messages.length && messages[i]!.msg.role !== "user") {
      const im = messages[i]!;
      if (im.msg.role === "assistant") {
        const aMsg = im.msg as Extract<Message, { role: "assistant" }>;
        const calls = aMsg.toolCalls ?? [];
        const toolCallNodes: TraceToolCallNode[] = calls.map((call: ToolCall) => {
          const resultIm = resultByCallId.get(call.id);
          if (!resultIm) {
            return {
              kind: "tool-call",
              id: call.id,
              name: call.name,
              input: call.input,
              callSeq: im.seq,
            };
          }
          const resultMsg = resultIm.msg as Extract<Message, { role: "tool" }>;
          return {
            kind: "tool-call",
            id: call.id,
            name: call.name,
            input: call.input,
            callSeq: im.seq,
            result: {
              seq: resultIm.seq,
              content: resultMsg.content,
              failed: detectFailure(resultMsg.content),
            },
          };
        });
        children.push({
          kind: "assistant",
          seq: im.seq,
          content: aMsg.content,
          ...(aMsg.thinkingBlocks && { thinkingBlocks: aMsg.thinkingBlocks }),
          toolCalls: toolCallNodes,
        });
        i += 1;
      } else if (im.msg.role === "tool") {
        // Tool results are nested under their tool-call above. Skip
        // here unless the call is missing (orphan), in which case we
        // surface the result as a top-level node so the operator sees
        // unaccounted-for output rather than silently dropping it.
        const callExists = children.some(
          (c) =>
            c.kind === "assistant" &&
            (c as TraceAssistantNode).toolCalls.some(
              (tc) => tc.id === (im.msg as { toolCallId: string }).toolCallId,
            ),
        );
        if (!callExists) {
          children.push({
            kind: "tool-call",
            id: (im.msg as { toolCallId: string }).toolCallId,
            name: "(orphan-result)",
            input: undefined,
            callSeq: -1,
            result: {
              seq: im.seq,
              content: (im.msg as { content: string }).content,
              failed: detectFailure((im.msg as { content: string }).content),
            },
          });
        }
        i += 1;
      } else {
        // System inside a turn: rare, but pass through.
        orphanSystem.push({
          kind: "system",
          seq: im.seq,
          content: (im.msg as { content: string }).content,
        });
        i += 1;
      }
    }

    if (children.length > 0) {
      turns.push({
        kind: "turn",
        turnIndex,
        fromSeq: messages[turnStart]!.seq,
        toSeq: messages[i - 1]!.seq,
        children,
      });
      turnIndex += 1;
    }
  }

  return {
    turns,
    orphanSystem,
    summary: summarise(turns),
  };
}

function detectFailure(content: string): boolean {
  const trimmed = content.trimStart();
  return (
    trimmed.startsWith("Error:") || trimmed.startsWith("error:") || trimmed.startsWith("[error]")
  );
}

function summarise(turns: ReadonlyArray<TraceTurnNode>): TraceSummary {
  let assistantMoves = 0;
  let toolCalls = 0;
  let toolFailures = 0;
  let orphanedToolCalls = 0;
  let orphanedToolResults = 0;
  for (const t of turns) {
    for (const c of t.children) {
      if (c.kind === "assistant") {
        assistantMoves += 1;
        for (const tc of c.toolCalls) {
          toolCalls += 1;
          if (!tc.result) orphanedToolCalls += 1;
          else if (tc.result.failed) toolFailures += 1;
        }
      } else if (c.kind === "tool-call" && c.callSeq === -1) {
        orphanedToolResults += 1;
      }
    }
  }
  return {
    turns: turns.length,
    assistantMoves,
    toolCalls,
    toolFailures,
    orphanedToolCalls,
    orphanedToolResults,
  };
}
