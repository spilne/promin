// ---------------------------------------------------------------------------
// `RemoteAgent` — `Agent` implementation that forwards all calls over HTTP
// to another Zorya deployment.
//
// Wire contract (matches the ZoryaServer agent routes):
//
//   POST /api/agents/:id/invoke                    → InvokeResponse JSON
//   POST /api/agents/:id/stream                    → SSE (text/event-stream)
//   POST /api/agents/:id/threads/:tid              → ThreadInvokeResponse JSON
//   POST /api/agents/:id/threads/:tid/stream       → SSE
//   POST /api/agents/:id/threads/:tid/cancel       → cancel the running turn
//   GET  /api/agents/:id/threads?namespaceId=...   → { threads: [...] }
//   GET  /api/agents/:id/threads/:tid/messages     → { messages: [...] }
//   PATCH /api/agents/:id/threads/:tid             → rename (setTitle)
//   POST /api/agents/:id/threads/:tid/archive      → setArchived
//
// SSE frame format:
//   - text delta       : `data: {"delta":"..."}` (no event name)
//   - thread metadata  : `event: thread\ndata: {"threadId":"...","isNew":bool}`
//   - finish           : `event: finish\ndata: {"text":"...","finishReason":"stop","usage":{...}}`
//   - error            : `event: error\ndata: {"message":"..."}`
//   - suspended        : `event: suspended\ndata: {...}`
//   - approval-request : `event: approval-requested\ndata: {"toolCallId":"...","toolName":"..."}`
//
// v1 scope:
//   - Full chat path: invoke / stream / thread.send / thread.stream
//   - Thread list + message history read
//   - Thread title + archive writes
//   - compactThread / distillThread throw (run on the origin deployment)
//   - workingMemory / metadata write: throw (no direct API today)
//
// `withScope` returns a new proxy that bakes namespaceId + resourceId into
// all request bodies — mirrors LocalAgent.withScope semantics.
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
import type { EpisodicRecord } from "../memory/types.ts";
import type { Message, ToolCall } from "../message.ts";
import type { CompactThreadOptions, DistillThreadOptions } from "../memory/consolidator.ts";
import { SystemWallClock, type WallClock } from "@promin/workflow";

// ---------------------------------------------------------------------------
// State shared across a proxy instance + its `withScope` derivatives.
// ---------------------------------------------------------------------------

/** Default timeout of the proxy's non-turn calls (thread list, messages, title, archive). */
export const DEFAULT_REMOTE_REQUEST_TIMEOUT_MS = 30_000;

export interface RemoteAgentConfig {
  readonly endpoint: string;
  readonly remoteAgentId: string;
  readonly auth?: { readonly kind: "bearer"; readonly token: string };
  /** Timeout of a turn (invoke, send, stream). Default: none — turns can run long. */
  readonly timeoutMs?: number;
  /**
   * Timeout of every other call (thread list, message history, title,
   * archive). Default: `DEFAULT_REMOTE_REQUEST_TIMEOUT_MS`.
   */
  readonly requestTimeoutMs?: number;
  readonly namespaceId?: string;
  readonly resourceId?: string;
  /**
   * Transport for every call. Default: global `fetch`. Hosts calling
   * endpoints they don't control (self-registered deployments) pass
   * `createOutboundFetch({ policy })` so each call is policy-checked and
   * pinned to the address it vetted.
   */
  readonly fetch?: (url: string, init: RequestInit) => Promise<Response>;
  /** Timer source for the timeouts. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

/** An abort signal that fires on the upstream signal or after `timeoutMs`. */
interface Deadline {
  readonly ctrl: AbortController;
  /** Stop the timer and drop the upstream listener. Idempotent. */
  release(): void;
}

function startDeadline(params: {
  readonly clock: WallClock;
  readonly timeoutMs?: number;
  readonly upstream?: AbortSignal;
}): Deadline {
  const ctrl = new AbortController();
  const onAbort = () => ctrl.abort();
  params.upstream?.addEventListener("abort", onAbort);
  if (params.upstream?.aborted) ctrl.abort();
  const timer =
    params.timeoutMs !== undefined
      ? params.clock.setTimeout(
          () => ctrl.abort(new Error(`RemoteAgent: request timed out after ${params.timeoutMs}ms`)),
          params.timeoutMs,
        )
      : undefined;
  timer?.unref?.();
  let released = false;
  return {
    ctrl,
    release() {
      if (released) return;
      released = true;
      timer?.clear();
      params.upstream?.removeEventListener("abort", onAbort);
    },
  };
}

/** HTTP plumbing shared by `RemoteAgent` and its threads. */
class RemoteTransport {
  constructor(private readonly cfg: RemoteAgentConfig) {}

  private get clock(): WallClock {
    return this.cfg.clock ?? SystemWallClock;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = { "content-type": "application/json" };
    if (this.cfg.auth?.kind === "bearer") {
      h.authorization = `Bearer ${this.cfg.auth.token}`;
    }
    return h;
  }

  private send(params: {
    method: string;
    path: string;
    body?: unknown;
    signal: AbortSignal;
  }): Promise<Response> {
    const init: RequestInit = {
      method: params.method,
      headers: this.headers(),
      signal: params.signal,
      ...(params.body !== undefined && { body: JSON.stringify(params.body) }),
    };
    const url = `${this.cfg.endpoint}${params.path}`;
    return this.cfg.fetch ? this.cfg.fetch(url, init) : fetch(url, init);
  }

  /**
   * One request whose whole exchange — headers and body — runs under a
   * deadline: the turn timeout for turns, the request timeout otherwise.
   */
  async call(params: {
    method: string;
    path: string;
    body?: unknown;
    turn?: boolean;
    signal?: AbortSignal;
  }): Promise<{ readonly status: number; readonly ok: boolean; readonly text: string }> {
    const timeoutMs = params.turn
      ? this.cfg.timeoutMs
      : (this.cfg.requestTimeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS);
    const deadline = startDeadline({
      clock: this.clock,
      ...(timeoutMs !== undefined && { timeoutMs }),
      ...(params.signal !== undefined && { upstream: params.signal }),
    });
    try {
      const res = await this.send({ ...params, signal: deadline.ctrl.signal });
      return { status: res.status, ok: res.ok, text: await res.text() };
    } finally {
      deadline.release();
    }
  }

  /**
   * A streamed turn. The caller's `signal` or the turn timeout cancels it
   * (`cancel()`); the deadline runs until the stream ends. `cancelRemote`
   * cancels the run on the server first — for a thread stream, where
   * closing the stream only detaches from the turn.
   */
  stream(params: {
    path: string;
    body: unknown;
    signal?: AbortSignal;
    cancelRemote?: (turnId: string | undefined) => Promise<void>;
  }): AgentRunOutput {
    const ctrl = new AbortController();
    const deadline = startDeadline({
      clock: this.clock,
      ...(this.cfg.timeoutMs !== undefined && { timeoutMs: this.cfg.timeoutMs }),
      ...(params.signal !== undefined && { upstream: params.signal }),
    });
    const pending = this.send({
      method: "POST",
      path: params.path,
      body: params.body,
      signal: ctrl.signal,
    });
    const out = buildSseRunOutput({
      pending,
      ctrl,
      ...(params.cancelRemote !== undefined && { cancelRemote: params.cancelRemote }),
      onSettled: () => deadline.release(),
    });
    const stop = (): void => {
      out.cancel().catch(() => {});
    };
    if (deadline.ctrl.signal.aborted) stop();
    else deadline.ctrl.signal.addEventListener("abort", stop, { once: true });
    return out;
  }

  /** Cancel a thread's running turn on the server; a turn that already ended (404) is fine. */
  async cancelTurn(params: { path: string; body: Record<string, unknown> }): Promise<void> {
    const res = await this.call({ method: "POST", path: params.path, body: params.body });
    if (!res.ok && res.status !== 404) {
      throw new Error(`RemoteAgent cancel failed: ${res.status} ${res.text}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Public class
// ---------------------------------------------------------------------------

export class RemoteAgent implements Agent {
  private readonly http: RemoteTransport;

  constructor(private readonly cfg: RemoteAgentConfig) {
    this.http = new RemoteTransport(cfg);
  }

  // --- one-shot ------------------------------------------------------------

  async invoke(input: { task: string }, opts?: AgentInvokeOpts): Promise<AgentRunOutput> {
    const res = await this.http.call({
      method: "POST",
      path: `/api/agents/${enc(this.cfg.remoteAgentId)}/invoke`,
      body: this.baseBody(input.task),
      turn: true,
      ...(opts?.signal !== undefined && { signal: opts.signal }),
    });
    if (!res.ok) throw new Error(`RemoteAgent.invoke failed: ${res.status} ${res.text}`);
    const data = JSON.parse(res.text) as { text: string; finishReason: string; usage: UsageStats };
    return buildStaticRunOutput(data);
  }

  stream(input: { task: string }, opts?: AgentInvokeOpts): AgentRunOutput {
    return this.http.stream({
      path: `/api/agents/${enc(this.cfg.remoteAgentId)}/stream`,
      body: this.baseBody(input.task),
      ...(opts?.signal !== undefined && { signal: opts.signal }),
    });
  }

  // --- thread --------------------------------------------------------------

  async thread(threadId: string, _opts?: ThreadOptions): Promise<AgentThread> {
    return new RemoteAgentThread({ cfg: this.cfg, http: this.http, id: threadId });
  }

  // --- threads list --------------------------------------------------------

  async listThreads(params?: ListThreadsParams): Promise<ThreadSummary[]> {
    if (!this.cfg.namespaceId) {
      throw new Error("RemoteAgent.listThreads: call .withScope({ namespaceId }) first.");
    }
    const qp = new URLSearchParams({ namespaceId: this.cfg.namespaceId });
    if (this.cfg.resourceId) qp.set("resourceId", this.cfg.resourceId);
    if (params?.q) qp.set("q", params.q);
    if (params?.limit !== undefined) qp.set("limit", String(params.limit));
    const res = await this.http.call({
      method: "GET",
      path: `/api/agents/${enc(this.cfg.remoteAgentId)}/threads?${qp}`,
    });
    if (!res.ok) throw new Error(`RemoteAgent.listThreads failed: ${res.status}`);
    const body = JSON.parse(res.text) as { threads: RemoteThreadSummary[] };
    return body.threads.map(toThreadSummary);
  }

  // --- not supported in v1 -------------------------------------------------

  async compactThread(_threadId: string, _opts?: CompactThreadOptions): Promise<EpisodicRecord> {
    throw new Error(
      "RemoteAgent: compactThread is not proxied — run it directly on the origin deployment.",
    );
  }

  async distillThread(_threadId: string, _opts?: DistillThreadOptions): Promise<EpisodicRecord> {
    throw new Error(
      "RemoteAgent: distillThread is not proxied — run it directly on the origin deployment.",
    );
  }

  // --- scope ----------------------------------------------------------------

  withScope(scope: AgentScope): RemoteAgent {
    return new RemoteAgent({
      ...this.cfg,
      ...(scope.namespaceId !== undefined && { namespaceId: scope.namespaceId }),
      ...(scope.resourceId !== undefined && { resourceId: scope.resourceId }),
    });
  }

  private baseBody(task: string): Record<string, unknown> {
    const body: Record<string, unknown> = { task };
    if (this.cfg.namespaceId) body.namespaceId = this.cfg.namespaceId;
    if (this.cfg.resourceId) body.resourceId = this.cfg.resourceId;
    return body;
  }
}

// ---------------------------------------------------------------------------
// RemoteAgentThread
// ---------------------------------------------------------------------------

class RemoteAgentThread implements AgentThread {
  readonly isNew = false;
  readonly id: string;
  private readonly cfg: RemoteAgentConfig;
  private readonly http: RemoteTransport;

  constructor(params: { cfg: RemoteAgentConfig; http: RemoteTransport; id: string }) {
    this.cfg = params.cfg;
    this.http = params.http;
    this.id = params.id;
  }

  get resourceId(): string | null {
    return this.cfg.resourceId ?? null;
  }

  // --- send / stream -------------------------------------------------------

  async send(input: { task: string }, opts?: AgentInvokeOpts): Promise<AgentRunOutput> {
    const res = await this.http.call({
      method: "POST",
      path: this.threadPath(),
      body: this.scoped({ task: input.task }),
      turn: true,
      ...(opts?.signal !== undefined && { signal: opts.signal }),
    });
    if (!res.ok) throw new Error(`RemoteAgentThread.send failed: ${res.status} ${res.text}`);
    const data = JSON.parse(res.text) as { text: string; finishReason: string; usage: UsageStats };
    return buildStaticRunOutput(data);
  }

  /**
   * Stream a turn. Closing a thread stream only detaches from the turn on
   * the server, so `cancel()` — and the caller's `signal` or `timeoutMs`
   * running out — cancels the turn through the cancel route (by the turn
   * id the server named), then closes. `cancel()` rejects when the server
   * refuses the cancel; a turn that already ended (404) is not an error.
   */
  stream(input: { task: string }, opts?: AgentInvokeOpts): AgentRunOutput {
    return this.http.stream({
      path: `${this.threadPath()}/stream`,
      body: this.scoped({ task: input.task }),
      ...(opts?.signal !== undefined && { signal: opts.signal }),
      cancelRemote: (turnId) =>
        this.http.cancelTurn({
          path: `${this.threadPath()}/cancel`,
          body: this.scoped(turnId !== undefined ? { turnId } : {}),
        }),
    });
  }

  // --- messages ------------------------------------------------------------

  async messages(range?: MessageRange): Promise<Message[]> {
    const qp = new URLSearchParams();
    if (this.cfg.namespaceId) qp.set("namespaceId", this.cfg.namespaceId);
    if (this.cfg.resourceId) qp.set("resourceId", this.cfg.resourceId);
    if (range?.limit !== undefined) qp.set("limit", String(range.limit));
    const res = await this.http.call({
      method: "GET",
      path: `${this.threadPath()}/messages?${qp}`,
    });
    if (!res.ok) return [];
    const body = JSON.parse(res.text) as { messages?: Message[] };
    return body.messages ?? [];
  }

  // --- metadata stubs (no direct API today) --------------------------------

  async workingMemory(): Promise<string | null> {
    return null;
  }

  async setWorkingMemory(_markdown: string | null): Promise<void> {
    throw new Error("RemoteAgent: setWorkingMemory has no remote API in v1.");
  }

  async metadata(): Promise<Readonly<Record<string, unknown>>> {
    return {};
  }

  async setMetadata(_metadata: Readonly<Record<string, unknown>>): Promise<void> {
    throw new Error("RemoteAgent: setMetadata has no remote API in v1.");
  }

  // --- title ---------------------------------------------------------------

  async title(): Promise<string | null> {
    return null;
  }

  async setTitle(title: string | null): Promise<void> {
    const res = await this.http.call({
      method: "PATCH",
      path: this.threadPath(),
      body: this.scoped({ title }),
    });
    if (!res.ok) throw new Error(`RemoteAgentThread.setTitle failed: ${res.status} ${res.text}`);
  }

  // --- archive -------------------------------------------------------------

  async setArchived(archivedAt: number | null): Promise<void> {
    const res = await this.http.call({
      method: "POST",
      path: `${this.threadPath()}/archive`,
      body: this.scoped({ archivedAt }),
    });
    if (!res.ok) {
      throw new Error(`RemoteAgentThread.setArchived failed: ${res.status} ${res.text}`);
    }
  }

  // --- delete --------------------------------------------------------------

  async delete(): Promise<void> {
    throw new Error("RemoteAgent: thread delete has no remote API in v1.");
  }

  // --- helpers -------------------------------------------------------------

  private threadPath(): string {
    return `/api/agents/${enc(this.cfg.remoteAgentId)}/threads/${enc(this.id)}`;
  }

  private scoped(body: Record<string, unknown>): Record<string, unknown> {
    const out = { ...body };
    if (this.cfg.namespaceId) out.namespaceId = this.cfg.namespaceId;
    if (this.cfg.resourceId) out.resourceId = this.cfg.resourceId;
    return out;
  }
}

// ---------------------------------------------------------------------------
// AgentRunOutput builders
// ---------------------------------------------------------------------------

function buildStaticRunOutput(data: {
  text: string;
  finishReason: string;
  usage: UsageStats;
}): AgentRunOutput {
  async function* empty(): AsyncGenerator<never> {}
  return {
    textStream: empty(),
    fullStream: empty(),
    text: Promise.resolve(data.text),
    output: Promise.resolve(undefined),
    toolCalls: Promise.resolve([] as ToolCall[]),
    toolResults: Promise.resolve([] as ToolResult[]),
    steps: Promise.resolve([] as Step[]),
    usage: Promise.resolve(data.usage),
    finishReason: Promise.resolve(data.finishReason as FinishReason),
    messages: Promise.resolve([] as Message[]),
    cancel: async () => {},
  };
}

/**
 * Build a live `AgentRunOutput` from a pending SSE fetch response.
 * Starts a background reader that fans events to:
 *   - two buffered async generators (`textStream`, `fullStream`)
 *   - deferred promises (`text`, `finishReason`, `usage`)
 */
function buildSseRunOutput(params: {
  pending: Promise<Response>;
  ctrl: AbortController;
  /** Cancels the run (turn id from `x-turn-id`) on the server; closing the stream alone only detaches. */
  cancelRemote?: (turnId: string | undefined) => Promise<void>;
  /** Called once the stream has ended, however it ended. */
  onSettled?: () => void;
}): AgentRunOutput {
  const { pending, ctrl, cancelRemote, onSettled } = params;
  const turnId = pending.then(
    (res) => res.headers.get("x-turn-id") ?? undefined,
    () => undefined,
  );
  // Deferred promise handles
  let resolveText!: (t: string) => void;
  let rejectText!: (e: unknown) => void;
  let resolveFinish!: (r: FinishReason) => void;
  let resolveUsage!: (u: UsageStats) => void;

  const textPromise = new Promise<string>((res, rej) => {
    resolveText = res;
    rejectText = rej;
  });
  const finishPromise = new Promise<FinishReason>((res) => {
    resolveFinish = res;
  });
  const usagePromise = new Promise<UsageStats>((res) => {
    resolveUsage = res;
  });

  // Buffered channels for the two async generator consumers.
  const textBuf: string[] = [];
  const fullBuf: AgentEvent[] = [];
  let textNotify: (() => void) | null = null;
  let fullNotify: (() => void) | null = null;
  let eof = false;

  const pushText = (d: string) => {
    textBuf.push(d);
    const fn = textNotify;
    textNotify = null;
    fn?.();
  };
  const pushFull = (e: AgentEvent) => {
    fullBuf.push(e);
    const fn = fullNotify;
    fullNotify = null;
    fn?.();
  };
  const close = () => {
    eof = true;
    textNotify?.();
    fullNotify?.();
  };

  // Background SSE reader.
  void (async () => {
    try {
      const res = await pending;
      if (!res.ok || !res.body) {
        const msg = res.ok ? "no body" : `HTTP ${res.status}`;
        rejectText(new Error(`RemoteAgent stream failed: ${msg}`));
        resolveFinish("error");
        resolveUsage({ inputTokens: 0, outputTokens: 0 });
        return;
      }
      for await (const { event, data } of parseSseFrames(res)) {
        if (event === null) {
          // Unnamed frame → text delta
          try {
            const payload = JSON.parse(data) as { delta?: string };
            if (typeof payload.delta === "string") {
              pushText(payload.delta);
              pushFull({ type: "text-delta", delta: payload.delta });
            }
          } catch {
            /* skip malformed */
          }
        } else if (event === "finish") {
          try {
            const p = JSON.parse(data) as {
              text: string;
              finishReason: FinishReason;
              usage: UsageStats;
            };
            resolveText(p.text);
            resolveFinish(p.finishReason);
            resolveUsage(p.usage);
            pushFull({ type: "finish", reason: p.finishReason, usage: p.usage });
          } catch {
            /* skip malformed */
          }
        } else if (event === "error") {
          try {
            const p = JSON.parse(data) as { message: string };
            rejectText(new Error(p.message));
            resolveFinish("error");
            resolveUsage({ inputTokens: 0, outputTokens: 0 });
            pushFull({ type: "error", error: { message: p.message } });
          } catch {
            /* skip malformed */
          }
        }
        // thread / approval-requested / suspended frames are informational;
        // no AgentRunOutput surface for them in v1.
      }
    } catch (err) {
      if ((err as Error).name !== "AbortError") {
        rejectText(err);
        resolveFinish("error");
        resolveUsage({ inputTokens: 0, outputTokens: 0 });
      }
      // On abort (cancel, upstream signal or timeout) the fetch rejects
      // with AbortError; the finally below settles every deferred promise.
    } finally {
      // Ensure all deferred promises are settled so callers don't hang.
      resolveText?.("");
      resolveFinish?.("stop");
      resolveUsage?.({ inputTokens: 0, outputTokens: 0 });
      onSettled?.();
      close();
    }
  })();

  async function* textStream(): AsyncGenerator<string> {
    let i = 0;
    while (true) {
      while (i < textBuf.length) yield textBuf[i++]!;
      if (eof) return;
      await new Promise<void>((r) => {
        textNotify = r;
      });
    }
  }

  async function* fullStream(): AsyncGenerator<AgentEvent> {
    let i = 0;
    while (true) {
      while (i < fullBuf.length) yield fullBuf[i++]!;
      if (eof) return;
      await new Promise<void>((r) => {
        fullNotify = r;
      });
    }
  }

  return {
    textStream: textStream(),
    fullStream: fullStream(),
    text: textPromise,
    output: Promise.resolve(undefined),
    toolCalls: Promise.resolve([] as ToolCall[]),
    toolResults: Promise.resolve([] as ToolResult[]),
    steps: Promise.resolve([] as Step[]),
    usage: usagePromise,
    finishReason: finishPromise,
    messages: Promise.resolve([] as Message[]),
    cancel: async () => {
      try {
        if (cancelRemote) await cancelRemote(await turnId);
      } finally {
        ctrl.abort();
      }
    },
  };
}

// ---------------------------------------------------------------------------
// SSE frame parser
// ---------------------------------------------------------------------------

async function* parseSseFrames(
  response: Response,
): AsyncGenerator<{ event: string | null; data: string }> {
  if (!response.body) return;
  const reader = response.body.getReader();
  const dec = new TextDecoder();
  let buf = "";
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let sep: number;
      while ((sep = buf.indexOf("\n\n")) !== -1) {
        const frame = buf.slice(0, sep);
        buf = buf.slice(sep + 2);
        let event: string | null = null;
        let data = "";
        for (const line of frame.split("\n")) {
          if (line.startsWith("event: ")) event = line.slice(7).trim();
          else if (line.startsWith("data: ")) data = line.slice(6);
        }
        if (data) yield { event, data };
      }
    }
  } finally {
    reader.releaseLock();
  }
}

// ---------------------------------------------------------------------------
// Thread summary mapping
// ---------------------------------------------------------------------------

interface RemoteThreadSummary {
  id: string;
  resourceId?: string | null;
  title?: string | null;
  metadata?: Record<string, unknown>;
  archivedAt?: number | null;
  messageCount?: number;
  lastActiveAt?: number;
  createdAt?: number;
}

function toThreadSummary(r: RemoteThreadSummary): ThreadSummary {
  return {
    id: r.id,
    resourceId: r.resourceId ?? null,
    title: r.title ?? null,
    metadata: r.metadata ?? {},
    archivedAt: r.archivedAt ?? null,
    messageCount: r.messageCount ?? 0,
    lastActiveAt: r.lastActiveAt ?? 0,
    createdAt: r.createdAt ?? 0,
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function enc(s: string): string {
  return encodeURIComponent(s);
}
