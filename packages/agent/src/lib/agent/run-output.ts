// ---------------------------------------------------------------------------
// Run capture and output assembly for `LocalAgent`: a turn's engine events
// become the live `textStream` / `fullStream` and the step list of an
// `AgentRunOutput`; the engine's result fills in the rest.
// ---------------------------------------------------------------------------

import type { AgentResult } from "../agent-action.ts";
import type { Message, ToolCall } from "../message.ts";
import type { SessionEvent } from "../session-logger.ts";
import type {
  AgentEvent,
  AgentRunOutput,
  FinishReason,
  Step,
  ToolResult,
  UsageStats,
} from "./types.ts";

/** What one run's event stream has produced so far. */
export interface RunCapture {
  readonly text: PushChannel<string>;
  readonly events: PushChannel<AgentEvent>;
  readonly steps: Step[];
  /** Aborts the run: `cancel()` and the caller's `signal` both feed it. */
  readonly abort: AbortController;
  finishReason: FinishReason | null;
  /** An `error` event is on `events` already. */
  failed: boolean;
  usage: UsageStats;
  toolStartByCallId: Map<string, { input: unknown }>;
}

export function makeCapture(signal?: AbortSignal): RunCapture {
  const abort = new AbortController();
  if (signal) {
    if (signal.aborted) abort.abort(signal.reason);
    else signal.addEventListener("abort", () => abort.abort(signal.reason), { once: true });
  }
  return {
    text: new PushChannel<string>(),
    events: new PushChannel<AgentEvent>(),
    steps: [],
    abort,
    finishReason: null,
    failed: false,
    usage: { inputTokens: 0, outputTokens: 0 },
    toolStartByCallId: new Map(),
  };
}

/** Fold one engine event into the capture. */
export function recordEvent(capture: RunCapture, e: SessionEvent): void {
  switch (e.type) {
    case "token.delta":
      capture.text.push(e.delta);
      capture.events.push({ type: "text-delta", delta: e.delta });
      break;
    case "tool.start": {
      capture.toolStartByCallId.set(e.toolCallId, { input: e.input });
      capture.events.push({
        type: "tool-call",
        call: { id: e.toolCallId, name: e.name, input: e.input },
      });
      break;
    }
    case "tool.end": {
      const step: Step = {
        type: "tool",
        index: capture.steps.length,
        name: e.name,
        toolCallId: e.toolCallId,
        input: capture.toolStartByCallId.get(e.toolCallId)?.input,
        failed: e.failed,
        durationMs: e.durationMs,
      };
      capture.steps.push(step);
      capture.events.push({ type: "step-end", step });
      break;
    }
    case "llm.call": {
      const step: Step = { type: "llm", index: capture.steps.length, durationMs: e.durationMs };
      capture.steps.push(step);
      if (e.tokens) {
        capture.usage = sumUsage(capture.usage, e.tokens);
      }
      capture.events.push({ type: "step-end", step });
      break;
    }
    case "approval.requested":
      capture.events.push({
        type: "approval-requested",
        toolCallId: e.toolCallId,
        toolName: e.toolName,
        input: e.input,
      });
      break;
    case "step_limit.hit":
      capture.finishReason = "max_steps";
      break;
    case "stall.detected":
      capture.finishReason = "stalled";
      break;
    case "turn.end":
      if (capture.finishReason === null) capture.finishReason = "stop";
      capture.events.push({ type: "finish", reason: capture.finishReason, usage: capture.usage });
      break;
    case "turn.failed":
      capture.failed = true;
      capture.events.push({ type: "error", error: { message: e.error } });
      break;
    case "turn.aborted":
      capture.finishReason = "cancelled";
      capture.events.push({
        type: "finish",
        reason: "cancelled",
        usage: capture.usage,
      });
      break;
    default:
      break;
  }
}

/** Build an AgentRunOutput where every promise is already resolved. */
export function resolvedOutput<TOutput>(params: {
  result: AgentResult;
  capture: RunCapture;
}): AgentRunOutput<TOutput> {
  const { result, capture } = params;
  capture.text.close();
  capture.events.close();
  return {
    textStream: capture.text,
    fullStream: capture.events,
    text: Promise.resolve(result.answer),
    output: Promise.resolve(result.output as TOutput | undefined),
    toolCalls: Promise.resolve(extractToolCalls(result.turnMessages)),
    toolResults: Promise.resolve(extractToolResults(result.turnMessages)),
    steps: Promise.resolve(capture.steps.slice()),
    usage: Promise.resolve(usageOf(result)),
    finishReason: Promise.resolve(result.finishReason),
    messages: Promise.resolve([...result.messages]),
    // Already finished: nothing left to cancel.
    cancel: async () => {},
  };
}

/**
 * Build an AgentRunOutput over a run still in flight. A failed run rejects
 * every result promise; a caller reads the ones it needs, so none of them
 * counts as an unhandled rejection on its own.
 */
export function liveOutput<TOutput>(params: {
  promise: Promise<AgentResult>;
  capture: RunCapture;
}): AgentRunOutput<TOutput> {
  const { promise, capture } = params;
  // Both streams end when the run settles, however it ends.
  // A run that fails ends `fullStream` with an `error` event — the
  // engine's `turn.failed`, or the rejection when the run failed before
  // or outside the turn. A turn suspended on an approval has not failed.
  const settled = promise
    .catch((err: unknown) => {
      const suspended = (err as { _tag?: unknown } | null)?._tag === "WorkflowSuspendedError";
      if (!capture.failed && !suspended) {
        capture.failed = true;
        capture.events.push({
          type: "error",
          error: { message: err instanceof Error ? err.message : String(err) },
        });
      }
      throw err;
    })
    .finally(() => {
      capture.text.close();
      capture.events.close();
    });
  const field = <T>(read: (r: AgentResult) => T): Promise<T> => {
    const p = settled.then(read);
    p.catch(() => {});
    return p;
  };
  return {
    textStream: capture.text,
    fullStream: capture.events,
    text: field((r) => r.answer),
    output: field((r) => r.output as TOutput | undefined),
    toolCalls: field((r) => extractToolCalls(r.turnMessages)),
    toolResults: field((r) => extractToolResults(r.turnMessages)),
    steps: field(() => capture.steps.slice()),
    usage: field(usageOf),
    finishReason: field((r) => r.finishReason),
    messages: field((r) => [...r.messages]),
    // Abort the run (LLM calls and tools see the signal; the engine ends
    // the turn as `cancelled` at its next step) and wait until it has
    // actually stopped.
    cancel: async () => {
      capture.abort.abort();
      await settled.then(
        () => undefined,
        () => undefined,
      );
    },
  };
}

/** Usage of a finished turn, from the engine's result. */
function usageOf(result: AgentResult): UsageStats {
  return {
    inputTokens: result.usage?.inputTokens ?? 0,
    outputTokens: result.usage?.outputTokens ?? 0,
    ...(result.usage?.cacheReadTokens !== undefined && {
      cacheReadTokens: result.usage.cacheReadTokens,
    }),
    ...(result.usage?.cacheWriteTokens !== undefined && {
      cacheWriteTokens: result.usage.cacheWriteTokens,
    }),
  };
}

function extractToolCalls(messages: ReadonlyArray<Message>): ToolCall[] {
  const calls: ToolCall[] = [];
  for (const m of messages) {
    if (m.role === "assistant" && m.toolCalls) {
      calls.push(...m.toolCalls);
    }
  }
  return calls;
}

/** Tool results, each paired with its call's tool name. */
function extractToolResults(messages: ReadonlyArray<Message>): ToolResult[] {
  const names = new Map(extractToolCalls(messages).map((c) => [c.id, c.name]));
  const out: ToolResult[] = [];
  for (const m of messages) {
    if (m.role === "tool") {
      out.push({
        toolCallId: m.toolCallId,
        name: names.get(m.toolCallId) ?? "",
        content: m.content,
        failed: m.isError === true,
      });
    }
  }
  return out;
}

/**
 * Add the per-call usage delta into a running total. Cache fields are
 * optional on each delta; when undefined we leave the running cache
 * total unchanged. We intentionally don't promote the running total's
 * undefined to 0 — observability code can distinguish "no cache info"
 * (undefined) from "cache reported zero" (0).
 */
function sumUsage(
  total: UsageStats,
  delta: {
    inputTokens: number;
    outputTokens: number;
    cacheReadTokens?: number;
    cacheWriteTokens?: number;
  },
): UsageStats {
  return {
    inputTokens: total.inputTokens + delta.inputTokens,
    outputTokens: total.outputTokens + delta.outputTokens,
    cacheReadTokens:
      delta.cacheReadTokens !== undefined
        ? (total.cacheReadTokens ?? 0) + delta.cacheReadTokens
        : total.cacheReadTokens,
    cacheWriteTokens:
      delta.cacheWriteTokens !== undefined
        ? (total.cacheWriteTokens ?? 0) + delta.cacheWriteTokens
        : total.cacheWriteTokens,
  };
}

/**
 * An async stream fed by `push` and ended by `close`. Every iterator reads
 * the whole stream from the start (late subscribers miss nothing) and
 * parks on a promise until the next push or close — no polling.
 */
class PushChannel<T> implements AsyncIterable<T> {
  private readonly items: T[] = [];
  private closed = false;
  private waiters: Array<() => void> = [];

  push(item: T): void {
    if (this.closed) return;
    this.items.push(item);
    this.wake();
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.wake();
  }

  private wake(): void {
    const waiters = this.waiters;
    this.waiters = [];
    for (const w of waiters) w();
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    let i = 0;
    return {
      next: async () => {
        while (i >= this.items.length && !this.closed) {
          await new Promise<void>((resolve) => this.waiters.push(resolve));
        }
        if (i < this.items.length) return { value: this.items[i++]!, done: false };
        return { value: undefined, done: true };
      },
    };
  }
}
