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
  | { type: "step_limit.hit"; turn: number; maxSteps: number };

/** Every stored event carries a Unix-ms timestamp added automatically on emit. */
export type SessionEvent = SessionEventBody & { ts: number };

export interface SessionLogger {
  /** Emit an event — `ts` is added automatically if omitted. */
  emit(event: SessionEventBody & { ts?: number }): void;
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
 */
export class InMemorySessionLogger implements SessionLogger {
  private readonly _buf: SessionEvent[] = [];
  private readonly _maxSize: number;

  constructor(maxSize = 2000) {
    this._maxSize = maxSize;
  }

  emit(event: SessionEventBody & { ts?: number }): void {
    if (this._buf.length >= this._maxSize) this._buf.shift();
    this._buf.push({ ...event, ts: event.ts ?? Date.now() } as SessionEvent);
  }

  events(): SessionEvent[] {
    return [...this._buf];
  }

  clear(): void {
    this._buf.length = 0;
  }
}
