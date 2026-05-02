// ---------------------------------------------------------------------------
// `CursorAgent` — `Agent` implementation that drives the Cursor CLI.
//
// Each call to `invoke` / `stream` / `thread.send` spawns one `agent -p`
// turn via `runCursorSession`. The NDJSON event stream is translated to
// `AgentEvent`s and surfaced on the `AgentRunOutput`.
//
// Parity vs LocalAgent / RemoteAgent:
//   - `invoke` / `stream` — supported
//   - `thread(id).send` / `thread(id).stream` — supported. v1 spawns a
//     fresh Cursor session per send (no `--resume` mapping yet); the
//     thread id is stamped on session metadata for debugging but doesn't
//     persist conversation across calls.
//   - `compact` / `distill` / workingMemory / metadata — throw (Cursor
//     doesn't expose these surfaces)
//   - `withScope` — returns a new instance with the scope baked in;
//     scope flows into the `--workspace` choice when callers wire one.
// ---------------------------------------------------------------------------

import type {
  Agent,
  AgentEvent,
  AgentInvokeOpts,
  AgentRunOutput,
  AgentScope,
  AgentThread,
  FinishReason,
  ListThreadsParams,
  MessageRange,
  Step,
  ThreadOptions,
  ThreadSummary,
  ToolResult,
  UsageStats,
} from "../agent/types.ts";
import type { CompactThreadOptions, DistillThreadOptions } from "../memory/consolidator.ts";
import type { EpisodicRecord } from "../memory/types.ts";
import type { Message, ToolCall } from "../message.ts";
import { runCursorSession } from "./session.ts";
import type { CursorTransport } from "./session.ts";

export interface CursorAgentConfig {
  /** Override the binary name. Default: "agent". */
  readonly command?: string;
  readonly model?: string;
  readonly workspace?: string;
  readonly worktree?: boolean;
  readonly trust?: boolean;
  readonly sandbox?: "enabled" | "disabled";
  /** Extra raw args appended to every turn. */
  readonly extraArgs?: ReadonlyArray<string>;
  /** Env vars merged over `process.env`. `CURSOR_API_KEY` lives here. */
  readonly env?: Readonly<Record<string, string | undefined>>;
  /** Bound scope. Set via `withScope`. */
  readonly scope?: AgentScope;
  /** Test seam — let tests inject a fake transport. */
  readonly transport?: CursorTransport;
}

export class CursorAgent implements Agent {
  constructor(private readonly cfg: CursorAgentConfig = {}) {}

  // --- one-shot ------------------------------------------------------------

  async invoke(input: { task: string }, opts?: AgentInvokeOpts): Promise<AgentRunOutput> {
    const out = this.stream(input, opts);
    // Drain to completion so the resolved promises settle before we
    // hand the output back. invoke is the synchronous-feel surface.
    await out.text;
    return out;
  }

  stream(input: { task: string }, opts?: AgentInvokeOpts): AgentRunOutput {
    return this.runOnce(input.task, opts);
  }

  // --- threads -------------------------------------------------------------

  async thread(threadId: string, opts?: ThreadOptions): Promise<AgentThread> {
    return new CursorAgentThread(this, threadId, opts?.resourceId ?? null);
  }

  async listThreads(_params?: ListThreadsParams): Promise<ThreadSummary[]> {
    // Cursor sessions live server-side; we don't have a list API for them.
    return [];
  }

  async compactThread(_threadId: string, _opts?: CompactThreadOptions): Promise<EpisodicRecord> {
    throw new Error("CursorAgent.compactThread is not supported.");
  }

  async distillThread(_threadId: string, _opts?: DistillThreadOptions): Promise<EpisodicRecord> {
    throw new Error("CursorAgent.distillThread is not supported.");
  }

  withScope(scope: AgentScope): CursorAgent {
    return new CursorAgent({ ...this.cfg, scope: { ...(this.cfg.scope ?? {}), ...scope } });
  }

  // --- internals -----------------------------------------------------------

  /**
   * package-private — used by `CursorAgentThread.send`. The
   * `onSessionId` callback fires once Cursor's `session_id` is captured
   * (from any frame). The thread uses it to record the id for
   * subsequent `--resume` calls.
   */
  runOnce(
    task: string,
    opts?: AgentInvokeOpts,
    resumeSessionId?: string,
    onSessionId?: (sessionId: string) => void,
  ): AgentRunOutput {
    const req = {
      prompt: task,
      ...(this.cfg.command !== undefined && { command: this.cfg.command }),
      ...(this.cfg.model !== undefined && { model: this.cfg.model }),
      ...(this.cfg.workspace !== undefined && { workspace: this.cfg.workspace }),
      ...(this.cfg.worktree !== undefined && { worktree: this.cfg.worktree }),
      ...(this.cfg.trust !== undefined && { trust: this.cfg.trust }),
      ...(this.cfg.sandbox !== undefined && { sandbox: this.cfg.sandbox }),
      ...(this.cfg.extraArgs !== undefined && { extraArgs: this.cfg.extraArgs }),
      ...(this.cfg.env !== undefined && { env: this.cfg.env }),
      ...(resumeSessionId !== undefined && { resumeSessionId }),
      ...(opts?.signal !== undefined && { signal: opts.signal }),
    };
    const { events, result } = runCursorSession(req, this.cfg.transport);
    if (onSessionId) {
      result
        .then((r) => {
          if (r.sessionId) onSessionId(r.sessionId);
        })
        .catch(() => undefined);
    }
    return buildRunOutput(events, result);
  }
}

// ---------------------------------------------------------------------------
// AgentThread — Cursor-backed.
// ---------------------------------------------------------------------------

class CursorAgentThread implements AgentThread {
  readonly id: string;
  readonly resourceId: string | null;
  readonly isNew = true;
  /** Cursor's own `session_id`, captured from the first `system init` frame. */
  private cursorSessionId: string | null = null;

  constructor(
    private readonly agent: CursorAgent,
    threadId: string,
    resourceId: string | null = null,
  ) {
    this.id = threadId;
    this.resourceId = resourceId;
  }

  async send(input: { task: string }, opts?: AgentInvokeOpts): Promise<AgentRunOutput> {
    const out = this.stream(input, opts);
    await out.text;
    return out;
  }

  stream(input: { task: string }, opts?: AgentInvokeOpts): AgentRunOutput {
    // Resume the same Cursor session across sends in this thread, and
    // record the Cursor-assigned session id from the first turn so the
    // next send passes it as `--resume`.
    return this.agent.runOnce(input.task, opts, this.cursorSessionId ?? undefined, (sid) => {
      this.cursorSessionId = sid;
    });
  }

  async messages(_range?: MessageRange): Promise<Message[]> {
    return [];
  }

  async workingMemory(): Promise<string | null> {
    return null;
  }

  async setWorkingMemory(_markdown: string | null): Promise<void> {
    // Cursor has no working-memory surface. No-op so callers don't have
    // to special-case the backend.
  }

  async metadata(): Promise<Readonly<Record<string, unknown>>> {
    return {};
  }

  async setMetadata(_metadata: Readonly<Record<string, unknown>>): Promise<void> {
    // Same rationale as setWorkingMemory.
  }

  async title(): Promise<string | null> {
    return null;
  }

  async setTitle(_title: string | null): Promise<void> {
    // No-op.
  }

  async setArchived(_archivedAt: number | null): Promise<void> {
    // No-op.
  }

  async delete(): Promise<void> {
    // No durable state on this side; Cursor's session expires on its own.
  }

  async compact(_opts?: CompactThreadOptions): Promise<EpisodicRecord | null> {
    throw new Error("CursorAgentThread.compact is not supported.");
  }

  async distill(_opts?: DistillThreadOptions): Promise<EpisodicRecord | null> {
    throw new Error("CursorAgentThread.distill is not supported.");
  }
}

// ---------------------------------------------------------------------------
// Event translation — Cursor session events → AgentRunOutput
// ---------------------------------------------------------------------------

import type { CursorEvent, CursorSessionResult } from "./session.ts";

function buildRunOutput(
  events: AsyncIterable<CursorEvent>,
  resultPromise: Promise<CursorSessionResult>,
): AgentRunOutput {
  // Tee the event stream into two consumers (textStream + fullStream)
  // by relaying through a small ring buffer. Each AgentRunOutput is
  // consumed by at most a few iterators in practice; we keep it simple.
  const buffered: CursorEvent[] = [];
  const waiters: Array<(ev: CursorEvent | null) => void> = [];
  let drained = false;
  let drainError: unknown = null;

  (async () => {
    try {
      for await (const ev of events) {
        if (waiters.length > 0) {
          const w = waiters.shift()!;
          w(ev);
        } else {
          buffered.push(ev);
        }
      }
    } catch (err) {
      drainError = err;
    } finally {
      drained = true;
      while (waiters.length > 0) waiters.shift()!(null);
    }
  })();

  function makeReader(): () => Promise<CursorEvent | null> {
    let cursor = 0;
    return () => {
      if (cursor < buffered.length) {
        const ev = buffered[cursor++];
        return Promise.resolve(ev ?? null);
      }
      if (drained) {
        if (drainError) return Promise.reject(drainError);
        return Promise.resolve(null);
      }
      return new Promise<CursorEvent | null>((resolve) => waiters.push(resolve));
    };
  }

  async function* fullStream(): AsyncGenerator<AgentEvent> {
    const next = makeReader();
    while (true) {
      const ev = await next();
      if (ev === null) return;
      const out = toAgentEvent(ev);
      if (out) yield out;
    }
  }

  async function* textStream(): AsyncGenerator<string> {
    const next = makeReader();
    while (true) {
      const ev = await next();
      if (ev === null) return;
      if (ev.type === "text-delta") yield ev.delta;
    }
  }

  // Resolved-view promises: these all wait for the run to finish. We
  // also need to drain at least one event-stream consumer so the
  // background drainer makes progress — kick a no-op consumer that
  // doesn't observe anything.
  const drainKick = (async () => {
    const next = makeReader();
    while ((await next()) !== null) {
      // Ignore — the buffered/waiters split in the drainer means this
      // loop is what keeps progress moving when no real consumer is
      // attached (e.g. `await out.text` without iterating events).
    }
  })();

  const finalResult: Promise<CursorSessionResult> = drainKick.then(() => resultPromise);

  const text = finalResult.then((r) => r.text);
  const usage: Promise<UsageStats> = Promise.resolve({ inputTokens: 0, outputTokens: 0 });
  const finishReason: Promise<FinishReason> = finalResult.then((r) =>
    r.isError ? "error" : "stop",
  );
  const messages: Promise<Message[]> = finalResult.then((r) => [
    {
      role: "assistant",
      content: r.text,
    } as Message,
  ]);
  const toolCalls: Promise<ToolCall[]> = finalResult.then((r) =>
    r.toolCalls.map((tc) => ({
      id: tc.callId,
      name: extractToolName(tc.started),
      input: extractToolInput(tc.started),
    })),
  );
  const toolResults: Promise<ToolResult[]> = finalResult.then((r) =>
    r.toolCalls
      .filter((tc) => tc.completed !== undefined)
      .map((tc) => ({
        toolCallId: tc.callId,
        name: extractToolName(tc.completed!),
        content: extractToolResultText(tc.completed!),
        failed: extractToolFailed(tc.completed!),
      })),
  );
  const steps: Promise<Step[]> = finalResult.then((r) =>
    r.toolCalls.map((tc, i) => ({
      type: "tool" as const,
      index: i,
      name: extractToolName(tc.started),
      toolCallId: tc.callId,
      input: extractToolInput(tc.started),
      ...(tc.completed !== undefined && {
        output: extractToolResultText(tc.completed),
        failed: extractToolFailed(tc.completed),
      }),
      failed: tc.completed !== undefined ? extractToolFailed(tc.completed) : false,
      durationMs: 0,
    })),
  );

  return {
    textStream: { [Symbol.asyncIterator]: () => textStream() },
    fullStream: { [Symbol.asyncIterator]: () => fullStream() },
    text,
    output: Promise.resolve(undefined),
    toolCalls,
    toolResults,
    steps,
    usage,
    finishReason,
    messages,
    cancel: async () => {
      // The session's transport sees the abort signal already. Nothing
      // to do here for now — the AsyncIterable just ends.
    },
  };
}

function toAgentEvent(ev: CursorEvent): AgentEvent | null {
  switch (ev.type) {
    case "text-delta":
      return { type: "text-delta", delta: ev.delta };
    case "tool-call-start": {
      const call: ToolCall = {
        id: ev.callId,
        name: extractToolName(ev.call),
        input: extractToolInput(ev.call),
      };
      return { type: "tool-call", call };
    }
    case "tool-call-end": {
      const result: ToolResult = {
        toolCallId: ev.callId,
        name: extractToolName(ev.call),
        content: extractToolResultText(ev.call),
        failed: extractToolFailed(ev.call),
      };
      return { type: "tool-result", result };
    }
    case "stderr":
      return { type: "error", error: { message: ev.text } };
    default:
      return null;
  }
}

// Cursor's tool_call frame keys the inner shape by name (e.g.
// `readToolCall`, `writeToolCall`). We extract a (name, args, result)
// tuple from whichever inner key is present, defaulting gracefully so
// an unknown future tool type still maps to something useful.

function extractToolName(call: { tool_call: Readonly<Record<string, unknown>> }): string {
  for (const key of Object.keys(call.tool_call)) {
    if (key.endsWith("ToolCall") || key.endsWith("Call")) {
      return key.replace(/ToolCall$|Call$/, "") || key;
    }
  }
  return Object.keys(call.tool_call)[0] ?? "unknown";
}

function extractToolInput(call: { tool_call: Readonly<Record<string, unknown>> }): unknown {
  const inner = innerToolPayload(call.tool_call);
  if (!inner) return {};
  return (inner as Record<string, unknown>).args ?? inner;
}

function extractToolResultText(call: { tool_call: Readonly<Record<string, unknown>> }): string {
  const inner = innerToolPayload(call.tool_call);
  if (!inner) return "";
  const result = (inner as Record<string, unknown>).result;
  if (result === undefined) return "";
  return typeof result === "string" ? result : JSON.stringify(result);
}

function extractToolFailed(call: { tool_call: Readonly<Record<string, unknown>> }): boolean {
  const inner = innerToolPayload(call.tool_call);
  if (!inner) return false;
  const result = (inner as Record<string, unknown>).result;
  if (!result || typeof result !== "object") return false;
  return (result as Record<string, unknown>).error !== undefined;
}

function innerToolPayload(
  toolCall: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> | null {
  for (const key of Object.keys(toolCall)) {
    const v = toolCall[key];
    if (v && typeof v === "object") return v as Record<string, unknown>;
  }
  return null;
}
