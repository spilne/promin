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

import type { BudgetOverrun, BudgetUsage, TurnBudget } from "./budget/budget.ts";
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
  /**
   * The engine's timed trace of the turn (think / tool / approval spans,
   * usage, finish reason), when the turn's final assistant message
   * carries one (`TURN_TRACE_META_KEY`).
   */
  readonly timing?: TurnTrace;
}

// ---------------------------------------------------------------------------
// Turn trace — the engine's structured, timed record of one turn
// ---------------------------------------------------------------------------

/** Metadata key on a turn's final assistant message holding its `TurnTrace`. */
export const TURN_TRACE_META_KEY = "turnTrace";

/** Token usage of one think step or of a whole turn. */
export interface TurnTraceUsage {
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly cacheReadTokens?: number;
  readonly cacheWriteTokens?: number;
}

/**
 * One span of a turn. Times are epoch ms on the engine's clock, journaled
 * with the step they time, so a replayed or resumed turn reports the
 * original timings.
 */
export type TurnSpan =
  | {
      /** One LLM call (retries included). */
      readonly kind: "think";
      readonly step: number;
      readonly startedAt: number;
      readonly durationMs: number;
      readonly usage?: TurnTraceUsage;
    }
  | {
      /**
       * One tool call. `startedAt` / `durationMs` are absent for a
       * sub-agent call (its child run has its own trace) and for a call
       * whose outcome a crash lost.
       */
      readonly kind: "tool";
      readonly step: number;
      readonly name: string;
      readonly toolCallId: string;
      readonly failed: boolean;
      readonly delegated?: true;
      readonly startedAt?: number;
      readonly durationMs?: number;
    }
  | {
      /**
       * An in-turn compaction: older steps folded into one summary before
       * this step's think. `summarized` is false for a digest (no LLM call).
       */
      readonly kind: "compaction";
      readonly step: number;
      readonly startedAt: number;
      readonly durationMs: number;
      /** Messages folded into the summary. */
      readonly compacted: number;
      readonly summarized: boolean;
      readonly usage?: TurnTraceUsage;
    }
  | {
      /** A tool approval: from the request to the decision reaching the turn. */
      readonly kind: "approval";
      readonly step: number;
      readonly toolCallId: string;
      readonly toolName: string;
      readonly approved: boolean;
      readonly startedAt: number;
      readonly durationMs: number;
    };

/** The engine's trace of one turn: its spans plus the turn's totals. */
export interface TurnTrace {
  /** The turn's workflow id. */
  readonly workflowId: string;
  readonly startedAt: number;
  readonly endedAt: number;
  readonly durationMs: number;
  /** `stop`, `max_steps`, `stalled`, `cancelled` or `budget_exceeded`. */
  readonly finishReason: string;
  /** Think steps taken. */
  readonly steps: number;
  readonly usage: TurnTraceUsage;
  /**
   * What the turn spent, its sub-agents included: tokens, priced cost, LLM
   * and tool calls.
   */
  readonly spend?: BudgetUsage;
  /**
   * The turn's budget, when it had one, and — when it ran out — the limit
   * it hit and the step it ended at.
   */
  readonly budget?: {
    readonly limits: TurnBudget;
    readonly exceeded?: BudgetOverrun & { readonly step: number };
  };
  /** Spans in start order (calls of one step run in parallel and may overlap). */
  readonly spans: ReadonlyArray<TurnSpan>;
}

/** A `TurnTrace` read back from message metadata (untyped JSON), or undefined. */
export function turnTraceOf(
  metadata: Readonly<Record<string, unknown>> | undefined,
): TurnTrace | undefined {
  const raw = metadata?.[TURN_TRACE_META_KEY];
  if (
    raw &&
    typeof raw === "object" &&
    typeof (raw as { workflowId?: unknown }).workflowId === "string" &&
    Array.isArray((raw as { spans?: unknown }).spans)
  ) {
    return raw as TurnTrace;
  }
  return undefined;
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
    /**
     * The tool's result metadata (e.g. a retriever's `sources`), without
     * a sub-agent's trace — that one is `childTrace`.
     */
    readonly metadata?: Readonly<Record<string, unknown>>;
  };
  /**
   * Sub-agent run trace, present on `callAgent` tool calls that
   * dispatched a peer. `callAgent` builds it from the sub-run's
   * messages and stows it on the result message's `metadata`; this
   * lets the graph view expand the call node into the peer's turns.
   */
  readonly childTrace?: AgentTrace;
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
    let timing: TurnTrace | undefined;
    while (i < messages.length && messages[i]!.msg.role !== "user") {
      const im = messages[i]!;
      if (im.msg.role === "assistant") {
        const aMsg = im.msg as Extract<Message, { role: "assistant" }>;
        timing = turnTraceOf(aMsg.metadata) ?? timing;
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
          const childTrace = extractChildTrace(resultMsg.metadata);
          const metadata = resultMetadata(resultMsg.metadata);
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
              ...(metadata && { metadata }),
            },
            ...(childTrace && { childTrace }),
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
        ...(timing && { timing }),
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

/** Key under which `callAgent` stows the sub-run trace on result metadata. */
export const CHILD_TRACE_META_KEY = "childTrace";

/** A tool result's metadata minus the sub-run trace; undefined when nothing is left. */
function resultMetadata(
  metadata: Readonly<Record<string, unknown>> | undefined,
): Readonly<Record<string, unknown>> | undefined {
  if (metadata === undefined) return undefined;
  const { [CHILD_TRACE_META_KEY]: _childTrace, ...rest } = metadata;
  return Object.keys(rest).length > 0 ? rest : undefined;
}

/**
 * Pull a sub-agent `AgentTrace` out of a tool-result message's metadata,
 * if one was stowed there (by `callAgent`). Guards the shape — the value
 * round-tripped through JSON storage, so it's untyped on the way back.
 */
function extractChildTrace(
  metadata: Readonly<Record<string, unknown>> | undefined,
): AgentTrace | undefined {
  const raw = metadata?.[CHILD_TRACE_META_KEY];
  if (raw && typeof raw === "object" && Array.isArray((raw as { turns?: unknown }).turns)) {
    return raw as AgentTrace;
  }
  return undefined;
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
