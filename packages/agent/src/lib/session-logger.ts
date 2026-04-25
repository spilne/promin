export type LogUsage = { inputTokens: number; outputTokens: number };

type SessionEventBody =
  | { type: "turn.start"; turn: number; task: string }
  | { type: "turn.end"; turn: number; answer: string; durationMs: number; tokens: LogUsage }
  | { type: "turn.aborted"; turn: number; reason: "signal" | "close" }
  | { type: "llm.call"; turn: number; step: number; durationMs: number; tokens?: LogUsage }
  | { type: "tool.start"; turn: number; step: number; name: string; input: unknown }
  | {
      type: "tool.end";
      turn: number;
      step: number;
      name: string;
      durationMs: number;
      failed: boolean;
    }
  | { type: "tool.parse_error"; turn: number; step: number; name: string; error: string }
  | { type: "subagent.start"; name: string; workflowId: string; task: string }
  | {
      type: "subagent.end";
      name: string;
      workflowId: string;
      durationMs: number;
      timedOut: boolean;
    }
  | {
      type: "compact";
      turn: number;
      reason: "message_count" | "token_limit";
      kept: number;
      dropped: number;
    }
  | { type: "approval.requested"; turn: number; toolCallId: string; toolName: string }
  | { type: "approval.decision"; turn: number; toolCallId: string; approved: boolean }
  | { type: "step_limit.hit"; turn: number; maxSteps: number }
  // ---- High-frequency / streaming events (transient by default) -------------
  | {
      /**
       * One token delta from the LLM stream. Mirrors what
       * `session.stream(task)` yields on its `AsyncIterable<string>`, just on
       * the structured channel so cross-process forwarders can carry tokens
       * + structured events on a single wire. Always `transient: true` —
       * never journaled (the final assistant message is captured by the
       * journaled `emit-${turn}` activity).
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

export interface SessionLogger {
  /** Emit an event — `ts` is added automatically if omitted. */
  emit(event: SessionEventBody & { ts?: number; transient?: boolean }): void;
  events(): SessionEvent[];
  clear(): void;
}

/**
 * In-process ring-buffer `SessionLogger`.
 *
 * Keeps up to `maxSize` events (default 2 000). Older events are dropped when the
 * buffer is full. Pass an instance to `agentLoop({ logger })` or `agentAction` to
 * capture `turn.start`, `llm.call`, `tool.start/end`, `compact`, and `approval`
 * events, then read them back with `events()` or surface them in a `/log` REPL pane.
 *
 * Skips `transient: true` events by default to keep the buffer focused on
 * durable lifecycle events (token deltas would dominate otherwise).
 */
export class InMemorySessionLogger implements SessionLogger {
  private readonly _buf: SessionEvent[] = [];
  private readonly _maxSize: number;
  private readonly _includeTransient: boolean;

  constructor(maxSize = 2000, options: { includeTransient?: boolean } = {}) {
    this._maxSize = maxSize;
    this._includeTransient = options.includeTransient === true;
  }

  emit(event: SessionEventBody & { ts?: number; transient?: boolean }): void {
    const transient = event.transient === true || isTransientByDefault(event.type);
    if (transient && !this._includeTransient) return;
    if (this._buf.length >= this._maxSize) this._buf.shift();
    this._buf.push({ ...event, ts: event.ts ?? Date.now(), transient } as SessionEvent);
  }

  events(): SessionEvent[] {
    return [...this._buf];
  }

  clear(): void {
    this._buf.length = 0;
  }
}

function isTransientByDefault(type: SessionEventBody["type"]): boolean {
  return type === "token.delta" || type === "tool.progress";
}

/**
 * Multi-subscriber event bus for an agent session.
 *
 * Replaces the single-callback `logger` pattern used historically. Subscribers
 * register at any time (idle, mid-stream, between turns) and receive every
 * event from that point onward — there's no replay buffer, by design. The
 * `InMemorySessionLogger` (when configured) is one of the subscribers and
 * keeps the existing ring-buffer semantics for `session.eventLog()`.
 *
 * Subscriber failures are isolated: a throwing subscriber is logged once and
 * removed; the rest keep receiving events. This lets a misbehaving observer
 * (e.g., a closed WS sink) self-evict without taking the agent down.
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
        // agent down. The agent loop has no way to surface the error
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
