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

// ---------------------------------------------------------------------------
// State shared across a proxy instance + its `withScope` derivatives.
// ---------------------------------------------------------------------------

interface RemoteAgentConfig {
  readonly endpoint: string;
  readonly remoteAgentId: string;
  readonly auth?: { readonly kind: "bearer"; readonly token: string };
  readonly timeoutMs?: number;
  readonly namespaceId?: string;
  readonly resourceId?: string;
}

// ---------------------------------------------------------------------------
// Public class
// ---------------------------------------------------------------------------

export class RemoteAgent implements Agent {
  constructor(private readonly cfg: RemoteAgentConfig) {}

  // --- one-shot ------------------------------------------------------------

  async invoke(input: { task: string }, _opts?: AgentInvokeOpts): Promise<AgentRunOutput> {
    const body = this.baseBody(input.task);
    const ctrl = this.abortCtrl(_opts?.signal);
    const res = await this.post(`/api/agents/${enc(this.cfg.remoteAgentId)}/invoke`, body, ctrl);
    if (!res.ok) {
      throw new Error(`RemoteAgent.invoke failed: ${res.status} ${await res.text()}`);
    }
    const data = (await res.json()) as { text: string; finishReason: string; usage: UsageStats };
    return buildStaticRunOutput(data);
  }

  stream(input: { task: string }, opts?: AgentInvokeOpts): AgentRunOutput {
    const body = this.baseBody(input.task);
    const ctrl = this.abortCtrl(opts?.signal);
    const pending = this.post(`/api/agents/${enc(this.cfg.remoteAgentId)}/stream`, body, ctrl);
    return buildSseRunOutput(pending, ctrl);
  }

  // --- thread --------------------------------------------------------------

  async thread(threadId: string, _opts?: ThreadOptions): Promise<AgentThread> {
    return new RemoteAgentThread(this.cfg, threadId);
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
    const res = await this.get(`/api/agents/${enc(this.cfg.remoteAgentId)}/threads?${qp}`);
    if (!res.ok) throw new Error(`RemoteAgent.listThreads failed: ${res.status}`);
    const body = (await res.json()) as { threads: RemoteThreadSummary[] };
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

  // --- HTTP helpers ---------------------------------------------------------

  private baseBody(task: string): Record<string, unknown> {
    const body: Record<string, unknown> = { task };
    if (this.cfg.namespaceId) body.namespaceId = this.cfg.namespaceId;
    if (this.cfg.resourceId) body.resourceId = this.cfg.resourceId;
    return body;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = { "content-type": "application/json" };
    if (this.cfg.auth?.kind === "bearer") {
      h.authorization = `Bearer ${this.cfg.auth.token}`;
    }
    return h;
  }

  private abortCtrl(upstream?: AbortSignal): AbortController {
    const ctrl = new AbortController();
    if (upstream) upstream.addEventListener("abort", () => ctrl.abort());
    if (this.cfg.timeoutMs !== undefined) {
      setTimeout(() => ctrl.abort(), this.cfg.timeoutMs);
    }
    return ctrl;
  }

  post(path: string, body: unknown, ctrl: AbortController): Promise<Response> {
    return fetch(`${this.cfg.endpoint}${path}`, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });
  }

  get(path: string): Promise<Response> {
    return fetch(`${this.cfg.endpoint}${path}`, {
      method: "GET",
      headers: this.headers(),
    });
  }

  patch(path: string, body: unknown): Promise<Response> {
    return fetch(`${this.cfg.endpoint}${path}`, {
      method: "PATCH",
      headers: this.headers(),
      body: JSON.stringify(body),
    });
  }

  postJson(path: string, body: unknown): Promise<Response> {
    return fetch(`${this.cfg.endpoint}${path}`, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify(body),
    });
  }
}

// ---------------------------------------------------------------------------
// RemoteAgentThread
// ---------------------------------------------------------------------------

class RemoteAgentThread implements AgentThread {
  readonly isNew = false;

  constructor(
    private readonly cfg: RemoteAgentConfig,
    readonly id: string,
  ) {}

  get resourceId(): string | null {
    return this.cfg.resourceId ?? null;
  }

  // --- send / stream -------------------------------------------------------

  async send(input: { task: string }, opts?: AgentInvokeOpts): Promise<AgentRunOutput> {
    const body = this.threadBody(input.task);
    const ctrl = this.abortCtrl(opts?.signal);
    const proxy = new RemoteAgent(this.cfg);
    const res = await proxy.post(this.threadPath(), body, ctrl);
    if (!res.ok) {
      throw new Error(`RemoteAgentThread.send failed: ${res.status} ${await res.text()}`);
    }
    const data = (await res.json()) as {
      text: string;
      finishReason: string;
      usage: UsageStats;
    };
    return buildStaticRunOutput(data);
  }

  stream(input: { task: string }, opts?: AgentInvokeOpts): AgentRunOutput {
    const body = this.threadBody(input.task);
    const ctrl = this.abortCtrl(opts?.signal);
    const proxy = new RemoteAgent(this.cfg);
    const pending = proxy.post(`${this.threadPath()}/stream`, body, ctrl);
    return buildSseRunOutput(pending, ctrl);
  }

  // --- messages ------------------------------------------------------------

  async messages(range?: MessageRange): Promise<Message[]> {
    const qp = new URLSearchParams();
    if (this.cfg.namespaceId) qp.set("namespaceId", this.cfg.namespaceId);
    if (this.cfg.resourceId) qp.set("resourceId", this.cfg.resourceId);
    if (range?.limit !== undefined) qp.set("limit", String(range.limit));
    const proxy = new RemoteAgent(this.cfg);
    const res = await proxy.get(`${this.threadPath()}/messages?${qp}`);
    if (!res.ok) return [];
    const body = (await res.json()) as { messages: Message[] };
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
    const body: Record<string, unknown> = { title };
    if (this.cfg.namespaceId) body.namespaceId = this.cfg.namespaceId;
    if (this.cfg.resourceId) body.resourceId = this.cfg.resourceId;
    const proxy = new RemoteAgent(this.cfg);
    await proxy.patch(this.threadPath(), body);
  }

  // --- archive -------------------------------------------------------------

  async setArchived(archivedAt: number | null): Promise<void> {
    const body: Record<string, unknown> = { archivedAt };
    if (this.cfg.namespaceId) body.namespaceId = this.cfg.namespaceId;
    if (this.cfg.resourceId) body.resourceId = this.cfg.resourceId;
    const proxy = new RemoteAgent(this.cfg);
    await proxy.postJson(`${this.threadPath()}/archive`, body);
  }

  // --- delete --------------------------------------------------------------

  async delete(): Promise<void> {
    throw new Error("RemoteAgent: thread delete has no remote API in v1.");
  }

  // --- helpers -------------------------------------------------------------

  private threadPath(): string {
    return `/api/agents/${enc(this.cfg.remoteAgentId)}/threads/${enc(this.id)}`;
  }

  private threadBody(task: string): Record<string, unknown> {
    const body: Record<string, unknown> = { task };
    if (this.cfg.namespaceId) body.namespaceId = this.cfg.namespaceId;
    if (this.cfg.resourceId) body.resourceId = this.cfg.resourceId;
    return body;
  }

  private abortCtrl(upstream?: AbortSignal): AbortController {
    const ctrl = new AbortController();
    if (upstream) upstream.addEventListener("abort", () => ctrl.abort());
    if (this.cfg.timeoutMs !== undefined) {
      setTimeout(() => ctrl.abort(), this.cfg.timeoutMs);
    }
    return ctrl;
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
function buildSseRunOutput(pending: Promise<Response>, ctrl: AbortController): AgentRunOutput {
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
      // On abort: callers who awaited `text` get their rejection from
      // `ctrl.abort()` triggering `AbortError` inside the fetch — handled
      // by the catch above and the reject path below.
    } finally {
      // Ensure all deferred promises are settled so callers don't hang.
      resolveText?.("");
      resolveFinish?.("stop");
      resolveUsage?.({ inputTokens: 0, outputTokens: 0 });
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
    cancel: async () => ctrl.abort(),
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
