import type { BudgetDimension } from "./budget/budget.ts";

export type LogUsage = {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens?: number;
  cacheWriteTokens?: number;
};

type SessionEventBody =
  | { type: "turn.start"; turn: number; task: string }
  | { type: "turn.end"; turn: number; answer: string; durationMs: number; tokens: LogUsage }
  | { type: "turn.aborted"; turn: number; reason: "signal" }
  | {
      /** The turn threw (a permanent LLM error, a hook, invalid structured output). */
      type: "turn.failed";
      turn: number;
      error: string;
    }
  | {
      type: "llm.call";
      turn: number;
      step: number;
      durationMs: number;
      tokens?: LogUsage;
      /** Set for a call that isn't the step's think: `compaction` summarises old steps. */
      purpose?: "compaction";
    }
  | {
      /** A failed LLM call is retried after `delayMs` (see `LlmRetryOptions`). */
      type: "llm.retry";
      turn: number;
      step: number;
      attempt: number;
      delayMs: number;
      errorClass: string;
      error: string;
    }
  | {
      type: "tool.start";
      turn: number;
      step: number;
      toolCallId: string;
      name: string;
      input: unknown;
    }
  | {
      type: "tool.end";
      turn: number;
      step: number;
      toolCallId: string;
      name: string;
      durationMs: number;
      failed: boolean;
    }
  | {
      /**
       * In-turn compaction: older tool steps were folded into one summary
       * because the next think step was estimated at `tokens`, over the
       * engine's `compaction.maxTokens`.
       */
      type: "compact";
      turn: number;
      step: number;
      reason: "token_limit";
      tokens: number;
      /** Messages folded into the summary (the previous summary excluded). */
      compacted: number;
      /** Step messages kept verbatim after the cut. */
      kept: number;
    }
  | {
      /**
       * Emitted once per turn: a compaction left the step estimated at
       * `tokens`, still over `maxTokens` — the seed (system prompt, history,
       * tools) or the kept steps alone are too big. The turn compacts again
       * only every few steps from here on.
       */
      type: "compaction.ineffective";
      turn: number;
      step: number;
      tokens: number;
      maxTokens: number;
    }
  | {
      /**
       * A context-mode retriever failed or timed out while the turn was
       * prepared: the turn runs without its results (the knowledge block
       * notes the missing source).
       */
      type: "retriever.failed";
      turn: number;
      bindingId: string;
      error: string;
      timedOut: boolean;
    }
  | {
      type: "approval.requested";
      turn: number;
      toolCallId: string;
      toolName: string;
      /** The tool call's input, so an approval UI can show what will run. */
      input: unknown;
    }
  | { type: "approval.decision"; turn: number; toolCallId: string; approved: boolean }
  | { type: "step_limit.hit"; turn: number; maxSteps: number }
  | {
      /**
       * The turn's budget ran out at `step`: the turn ends with
       * `budget_exceeded`. `dimension` names the limit (`maxTotalTokens`,
       * `maxCostUsd`, …), `used` what was (or would have been) spent on it.
       */
      type: "budget.exceeded";
      turn: number;
      step: number;
      dimension: BudgetDimension;
      limit: number;
      used: number;
    }
  | {
      /**
       * The stall detector saw no progress — the same tool calls repeated
       * with no new text, or one tool called too often — and forced a
       * final answer.
       */
      type: "stall.detected";
      turn: number;
      step: number;
      reason: "repeated_calls" | "tool_budget";
      toolName: string;
    }
  // ---- High-frequency / streaming events (transient by default) -------------
  | {
      /**
       * One token delta from the LLM stream — the text an agent's
       * `textStream` yields, on the structured channel so cross-process
       * forwarders can carry tokens and structured events on one wire.
       * Always `transient: true`: the final assistant message is part of
       * the turn's result.
       */
      type: "token.delta";
      turn: number;
      delta: string;
    }
  | {
      /**
       * Tool free-form progress event. Tools emit these via the writer that
       * tool handlers receive — e.g. `writer.write({ percent: 30 })` while
       * downloading. Default `transient: true`. Wire-compatible with
       * Mastra's `context.writer` pattern.
       */
      type: "tool.progress";
      turn: number;
      step: number;
      toolCallId: string;
      name: string;
      payload: unknown;
    };

/**
 * Every emitted event carries:
 *  - `ts` — Unix-ms timestamp (added automatically on emit)
 *  - `transient` — when true, the event is meant for live observers only and
 *    should be skipped by anything that persists / replays. Token deltas +
 *    tool progress are transient by default; structured lifecycle events
 *    (turn / llm / tool start-end) are not.
 */
export type SessionEvent = SessionEventBody & { ts: number; transient?: boolean };

function isTransientByDefault(type: SessionEventBody["type"]): boolean {
  return type === "token.delta" || type === "tool.progress";
}

/**
 * Multi-subscriber event bus of an agent turn.
 *
 * Subscribers register at any time and receive every event from that point
 * onward — there's no replay buffer, by design.
 *
 * Subscriber failures are isolated: a throwing subscriber is removed; the
 * rest keep receiving events. This lets a misbehaving observer (e.g., a
 * closed WS sink) self-evict without taking the agent down.
 */
export class SessionEventBus {
  private subscribers = new Set<(event: SessionEvent) => void>();

  subscribe(fn: (event: SessionEvent) => void): () => void {
    this.subscribers.add(fn);
    return () => {
      this.subscribers.delete(fn);
    };
  }

  emit(event: SessionEventBody & { ts?: number; transient?: boolean }): void {
    const transient = event.transient === true || isTransientByDefault(event.type);
    const finalised: SessionEvent = {
      ...event,
      ts: event.ts ?? Date.now(),
      transient,
    } as SessionEvent;
    for (const fn of this.subscribers) {
      try {
        fn(finalised);
      } catch {
        // Drop misbehaving subscriber so one bad observer can't take the
        // agent down. The engine has no way to surface the error
        // upstream — this is the safe local resolution.
        this.subscribers.delete(fn);
      }
    }
  }

  /** For tests / shutdown. */
  clear(): void {
    this.subscribers.clear();
  }
}
