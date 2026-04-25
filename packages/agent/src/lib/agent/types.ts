// ---------------------------------------------------------------------------
// Universal `Agent` interface — the contract every backend implements.
//
// Backends (each will live in its own file):
//   - `LocalAgent`   wraps `@promin/agent` agentLoop / agentAction
//   - `ACPAgent`     wraps any ACP-speaking process (Claude Code, Codex,
//                    OpenCode, OpenClaw)
//   - `MastraAgent`  wraps `@mastra/core` Agent
//   - `HTTPAgent`    proxies a remote `Agent` over HTTP
//
// Three call shapes:
//   - `invoke(input)`  — one-shot, stateless. Awaits completion, returns the
//                        result object with already-resolved promises.
//   - `stream(input)`  — one-shot, streaming. Returns synchronously with live
//                        textStream / fullStream and deferred promises.
//   - `thread(id, opts)` — get-or-create a persistent thread. All subsequent
//                          send / stream calls on the thread are bound to it
//                          and (when wired) persist via `MemoryStore`.
//
// `AgentRunOutput` carries the rich result surface (Mastra-style): both a
// streaming view (`textStream`, `fullStream`) and a "fully resolved"
// view (`text`, `toolCalls`, `steps`, `usage`, `finishReason`) as
// promises that resolve at run completion.
// ---------------------------------------------------------------------------

import type { Message, ToolCall } from "../message.ts";
import type { CompactThreadOptions, DistillThreadOptions } from "../memory/consolidator.ts";
import type { EpisodicRecord } from "../memory/types.ts";

/** Streaming + resolved view of one agent run. */
export interface AgentRunOutput<Output = unknown> {
  /** Text deltas as they stream in. Empty stream when called from `invoke`. */
  readonly textStream: AsyncIterable<string>;
  /** Fully typed event stream — text deltas, tool starts/ends, lifecycle. */
  readonly fullStream: AsyncIterable<AgentEvent>;
  /** Final answer text. Resolves when the run completes. */
  readonly text: Promise<string>;
  /** Structured output when the agent has an `outputSchema`. */
  readonly output: Promise<Output | undefined>;
  /** Tool calls emitted across the run, in order. */
  readonly toolCalls: Promise<ToolCall[]>;
  /** Tool results, paired with the originating call by `toolCallId`. */
  readonly toolResults: Promise<ToolResult[]>;
  /** Per-step records. */
  readonly steps: Promise<Step[]>;
  /** Token + cost accounting. */
  readonly usage: Promise<UsageStats>;
  /** Why the run ended. */
  readonly finishReason: Promise<FinishReason>;
  /** All messages produced or ingested by the run, in order. */
  readonly messages: Promise<Message[]>;
  /** Best-effort cancel. May not interrupt the underlying provider mid-stream. */
  cancel(): Promise<void>;
}

/** Single agent step — one `llm` call, one `tool` invocation, etc. */
export type Step =
  | { readonly type: "llm"; readonly index: number; readonly durationMs: number }
  | {
      readonly type: "tool";
      readonly index: number;
      readonly name: string;
      readonly toolCallId: string;
      readonly input: unknown;
      readonly output?: unknown;
      readonly failed: boolean;
      readonly durationMs: number;
    };

export interface ToolResult {
  readonly toolCallId: string;
  readonly name: string;
  readonly content: string;
  readonly failed: boolean;
}

export interface UsageStats {
  readonly inputTokens: number;
  readonly outputTokens: number;
  /**
   * Tokens served from the provider's prompt cache. Populated when the
   * underlying LLMProvider reports it (the Anthropic adapter does;
   * other adapters fall back to undefined / 0). Useful as observability:
   * a non-zero value means the system + tool prefix from a previous
   * turn was still warm.
   */
  readonly cacheReadTokens?: number;
  /**
   * Tokens written into the provider's prompt cache on this turn — i.e.
   * the cache miss prefix the provider just stored for next time.
   */
  readonly cacheWriteTokens?: number;
}

export type FinishReason = "stop" | "tool_use" | "length" | "max_steps" | "cancelled" | "error";

/**
 * Streamed events. Discriminated union mirroring the runtime's session
 * event bus. Backends MAY emit a subset; consumers MUST tolerate unknown
 * `type` values by ignoring them (forward-compat).
 */
export type AgentEvent =
  | { readonly type: "text-delta"; readonly delta: string }
  | { readonly type: "tool-call"; readonly call: ToolCall }
  | { readonly type: "tool-result"; readonly result: ToolResult }
  | {
      readonly type: "approval-requested";
      readonly toolCallId: string;
      readonly toolName: string;
    }
  | { readonly type: "step-start"; readonly step: Step }
  | { readonly type: "step-end"; readonly step: Step }
  | { readonly type: "finish"; readonly reason: FinishReason; readonly usage: UsageStats }
  | { readonly type: "data"; readonly payload: unknown }
  | { readonly type: "error"; readonly error: { readonly message: string } };

/** Per-call options. Identical for `invoke` / `stream` / thread `send` / `stream`. */
export interface AgentInvokeOpts {
  /** Abort signal — cancels the run when the underlying runtime supports it. */
  readonly signal?: AbortSignal;
  /** Override `resourceId` for this call (rare; usually bound at thread create). */
  readonly resourceId?: string;
  /** Override `namespaceId` for this call (rare; usually bound at agent construct). */
  readonly namespaceId?: string;
  /** Free-form metadata that adapters may surface back on events. */
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface ThreadOptions {
  /** User / persona key — wires to `MemoryStore` resource layer. */
  readonly resourceId?: string;
  /** Tenant key — wires to `MemoryStore` namespace layer. Defaults to the agent's bound namespace. */
  readonly namespaceId?: string;
  /** Default `true`. When `false`, refuses to create a thread that doesn't exist. */
  readonly createIfMissing?: boolean;
  /** Free-form thread metadata persisted with the thread row. */
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface ListThreadsParams {
  readonly resourceId?: string;
  readonly metadataFilter?: Readonly<Record<string, unknown>>;
  /** Case-insensitive substring match on thread id. Pushed to storage. */
  readonly q?: string;
  readonly limit?: number;
  readonly cursor?: string;
  readonly order?: "lastActiveDesc" | "createdAsc" | "createdDesc";
}

export interface ThreadSummary {
  readonly id: string;
  readonly resourceId: string | null;
  readonly metadata: Readonly<Record<string, unknown>>;
  readonly messageCount: number;
  readonly lastActiveAt: number;
  readonly createdAt: number;
}

export interface MessageRange {
  readonly fromSeq?: number;
  readonly toSeq?: number;
  readonly limit?: number;
  readonly order?: "asc" | "desc";
}

/**
 * One agent — invoke, stream, or operate on a persistent thread.
 *
 * `Input` is the model-facing input shape (often `{ task: string }`);
 * `Output` is the structured output type (often `unknown` when no
 * `outputSchema` is configured). Backends without per-call structured
 * output use `Output = unknown`.
 */
export interface Agent<Input = AgentInput, Output = unknown> {
  /** One-shot, stateless. Awaits completion. */
  invoke(input: Input, opts?: AgentInvokeOpts): Promise<AgentRunOutput<Output>>;
  /** One-shot, streaming. Returns immediately. */
  stream(input: Input, opts?: AgentInvokeOpts): AgentRunOutput<Output>;
  /** Get or create a persistent thread. */
  thread(threadId: string, opts?: ThreadOptions): Promise<AgentThread<Input, Output>>;
  /** List threads visible to this agent (optionally filtered by resource). */
  listThreads(params?: ListThreadsParams): Promise<ThreadSummary[]>;
  /**
   * Roll up the oldest portion of a thread into a `ThreadEpisode`,
   * freeing message budget while keeping the gist queryable. Backends
   * without a consolidator should throw a clear error.
   */
  compactThread(threadId: string, opts?: CompactThreadOptions): Promise<EpisodicRecord>;
  /**
   * Distill a thread into a single `ResourceEpisode` so future threads
   * under the same `(namespaceId, resourceId)` pick up the gist via
   * `resolveContext`'s episode-injection budget. Backends without a
   * consolidator should throw a clear error.
   */
  distillThread(threadId: string, opts?: DistillThreadOptions): Promise<EpisodicRecord>;
  /**
   * Return a tenant-bound view of this agent. Multi-tenant gateways call
   * this per request: `agent.withScope({ namespaceId, resourceId }).invoke(...)`.
   * The original agent is not mutated. Backends without persistent state
   * may return `this` unchanged.
   */
  withScope(scope: AgentScope): Agent<Input, Output>;
}

/** Re-exported here so callers that depend on `Agent` get the option types alongside. */
export type {
  CompactThreadOptions,
  DistillThreadOptions,
  Consolidator,
} from "../memory/consolidator.ts";
export type { EpisodicRecord } from "../memory/types.ts";

/** Tenant binding for `Agent.bind()`. */
export interface AgentScope {
  readonly namespaceId?: string;
  readonly resourceId?: string;
}

/** Default model-facing input — most backends use this shape. */
export interface AgentInput {
  readonly task: string;
  /** Pre-existing messages to seed the thread / one-shot run. */
  readonly messages?: ReadonlyArray<Message>;
}

/**
 * A persistent conversation. `send` and `stream` append to the thread;
 * messages are persisted to the underlying `MemoryStore` (when wired).
 */
export interface AgentThread<Input = AgentInput, Output = unknown> {
  readonly id: string;
  /** The resource (user/persona) this thread belongs to, when set. */
  readonly resourceId: string | null;
  /**
   * `true` when this `thread()` call CREATED the thread (no prior state),
   * `false` when it resumed an existing one. Use to branch app logic:
   *
   *     const t = await agent.thread(req.body.threadId, { resourceId });
   *     if (t.isNew) { ...send greeting / onboarding... }
   */
  readonly isNew: boolean;

  /** One conversational turn. Awaits completion. */
  send(input: Input, opts?: AgentInvokeOpts): Promise<AgentRunOutput<Output>>;
  /** One conversational turn, streaming. Returns immediately. */
  stream(input: Input, opts?: AgentInvokeOpts): AgentRunOutput<Output>;

  /** Read the persisted message history (paginated). */
  messages(range?: MessageRange): Promise<Message[]>;

  /** Read the markdown scratchpad at the thread layer. */
  workingMemory(): Promise<string | null>;
  /** Replace the markdown scratchpad. */
  setWorkingMemory(markdown: string | null): Promise<void>;

  /** Patch thread metadata. Merge semantics depend on backend. */
  setMetadata(metadata: Readonly<Record<string, unknown>>): Promise<void>;

  /** Permanently delete the thread and its messages / facts / episodes. */
  delete(): Promise<void>;
}
