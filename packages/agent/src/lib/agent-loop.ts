import {
  workflow,
  completeSignal,
  isActivityJournalStorage,
  isJournaledSuspendStorage,
} from "@promin/workflow";
import type {
  WorkflowRunner,
  JournaledSuspendStorage,
  ActivityJournalStorage,
} from "@promin/workflow";
import { SystemClock } from "@promin/core";
import type { RateLimiter, Clock, TimerHandle } from "@promin/core";
import type { LLMProvider } from "./llm-provider.ts";
import type { AgentTool, AutoApprove, ApprovalDecision } from "./tool.ts";
import { shouldAutoApprove } from "./tool.ts";
import type { ToolRegistry } from "./tool-registry.ts";
import { buildToolDefs } from "./tool-registry.ts";
import type { MemoryIndex, MemoryScope } from "./memory-index.ts";
import type { ProcessorsConfig } from "./processors.ts";
import type { Message, AssistantMessage, ToolResultMessage, ToolCall } from "./message.ts";
import {
  executeToolCall,
  runLlmCall,
  nonSystemMsgs,
  resolveTools,
  searchRelevantMemories,
  surfaceAgentError,
} from "./agent-shared.ts";
import type { SessionLogger, SessionEvent } from "./session-logger.ts";
import { SessionEventBus } from "./session-logger.ts";
import { compact, RECAP_SUMMARY_PROMPT } from "./agent-loop-compaction.ts";
import type { CompactionConfig } from "./agent-loop-compaction.ts";

// ---- hooks ----

export interface HooksTurnParams {
  task: string;
  messages: Message[];
  /**
   * `true` when the agent's workflow body is executing on top of
   * pre-existing journal entries (worker restart / signal-resume).
   * `beforeTurn` runs inside a journaled activity so it fires only on
   * fresh runs of the activity itself — `isReplay: true` means the
   * body has been re-started but THIS activity is past the journal
   * cursor and is firing for the first time.
   */
  isReplay: boolean;
}

export interface HooksAfterTurnParams {
  task: string;
  answer: string;
  messages: Message[];
  usage: { inputTokens: number; outputTokens: number };
  /** True when the turn was cut short by maxStepsPerTurn — the answer is a "(step limit reached)" sentinel. */
  truncated: boolean;
  /**
   * `true` when the workflow body is replaying. Same semantics as
   * `HooksTurnParams.isReplay` — `afterTurn` runs inside a journaled
   * activity, so this is only `true` when the body itself has been
   * re-started but the activity is firing fresh.
   */
  isReplay: boolean;
}

export interface HooksConfig {
  /**
   * Runs before the LLM think loop for each turn.
   * Return an updated message list to inject extra context, or void to leave unchanged.
   *
   * @replay
   * Runs inside a journaled activity. On worker-restart replay the hook is
   * SKIPPED — the journaled return value (the modified messages) is used
   * directly. Hook side effects (metrics, external calls, audit writes)
   * do NOT re-fire on replay. Keep it pure, or move side effects to
   * persistent storage that's read independently.
   */
  beforeTurn?: (params: HooksTurnParams) => Promise<Message[] | void>;
  /**
   * Runs after the answer is emitted for each turn.
   *
   * @replay
   * Runs inside a journaled activity — SKIPPED on replay. Same caveats as
   * `beforeTurn`: pure transformations only; durable side-effects belong
   * outside (memory store, durable approval inbox, etc.).
   */
  afterTurn?: (params: HooksAfterTurnParams) => Promise<void>;
  /**
   * Fires when no send() arrives within idleTimeoutMs after the previous turn ended.
   * Receives the actual elapsed idle time in milliseconds.
   *
   * @replay
   * NOT journaled — driven by a clock timer outside the workflow. Will
   * NOT fire automatically after a worker restart (timer state is lost).
   * If you need durable idle detection, schedule via the workflow scheduler.
   */
  onIdle?: (idleMs: number) => Promise<void>;
  /**
   * How long (ms) the session must be idle before onIdle fires.
   * Required when onIdle is set.
   */
  idleTimeoutMs?: number;
  /**
   * Runs when session.close() is called.
   *
   * @replay
   * NOT journaled and NOT replayed. Suitable for in-process cleanup
   * (closing buffers, flushing logs). Failures here do not affect the
   * workflow state.
   */
  onClose?: () => Promise<void>;
  /**
   * Called when a tool with `requireApproval: true` needs user approval.
   * Return `{ approved: true }` to allow execution, or `{ approved: false, reason? }` to reject.
   *
   * When set, the workflow does NOT suspend — the decision is awaited inline. This is simpler
   * than the `session.approve()` / `session.reject()` signal path; use it when the approval UI
   * lives in the same process (e.g. a terminal REPL). Falls back to the signal-based path when
   * omitted.
   *
   * @replay
   * The decision is awaited inside a journaled activity, so the journaled
   * approve/reject result IS durable. But the CALLBACK ITSELF is skipped
   * on replay — any side effects (audit-log write, Slack notification,
   * rate-limit counter) do NOT re-fire on replay. For durable audit
   * trails of who approved what, use the persistent approval-storage
   * primitive (promin-2nh2 — under construction) which records decisions
   * outside the activity and survives replay correctly.
   */
  onApprovalRequired?: (call: ToolCall) => Promise<{ approved: boolean; reason?: string }>;
}

// ---- config types ----

export interface ContextConfig {
  /**
   * Compact when non-system messages exceed this count.
   * Default: 80.
   */
  maxMessages?: number;
  /**
   * Number of recent messages to keep after compaction.
   * Default: 40.
   */
  keepMessages?: number;
  /**
   * Ask the LLM to summarize dropped messages and inject the summary as context.
   * Default: true.
   */
  summarize?: boolean;
  /**
   * Model context limit in tokens. When set, enables token-based compaction:
   * if the input tokens for a think step exceed `contextLimit * compressAt`,
   * the message history is compacted before the next step.
   */
  contextLimit?: number;
  /**
   * Fraction of `contextLimit` at which token-based compaction fires.
   * Default: 0.70.
   */
  compressAt?: number;
  /**
   * System prompt used when compacting via token-based RECAP.
   * Defaults to RECAP_SUMMARY_PROMPT. Override to customise the summary style.
   */
  recapPrompt?: string;
  /**
   * Strip extended-thinking blocks from assistant messages older than the
   * current turn before they're sent back to the LLM. Anthropic's
   * extended-thinking output is useful in the turn that produced it (the
   * model uses it to plan tool calls), but on the next turn it's dead
   * weight — the thinking helped pick the prior step, not the next one,
   * and replaying it inflates input-token cost without contributing to
   * future reasoning.
   *
   * Defaults to `true`. Set `false` to preserve the full transcript
   * (e.g. for debugging or for non-Anthropic LLMs that ignore the
   * blocks anyway).
   *
   * The redaction targets only `assistantMessage.thinkingBlocks` —
   * `content` and `toolCalls` are never touched.
   */
  redactPriorThinking?: boolean;
}

export interface MemoryConfig {
  store: MemoryIndex;
  /**
   * Scope for all read and write operations on this store.
   * Omit for the global namespace (same as pre-scoping behaviour).
   * Example: { namespaceId: orgId, sessionId: sessionId }
   */
  scope?: MemoryScope;
  /**
   * Save compaction summaries to the memory store so they are retrievable
   * in future sessions. Default: true.
   */
  saveOnCompact?: boolean;
  /**
   * How many memories to retrieve and inject at session start.
   * Set to 0 to disable injection. Default: 5.
   */
  injectLimit?: number;
  /**
   * Query used to retrieve relevant memories at session start.
   * Defaults to the system prompt, or "general context" if no system prompt.
   */
  searchQuery?: string;
}

export interface AgentLoopConfig {
  name: string;
  llm: LLMProvider;
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
  tools?: Record<string, AgentTool<any, any>>;
  toolRegistry?: ToolRegistry;
  autoApprove?: AutoApprove;
  systemPrompt?: string;
  maxTurns?: number;
  maxStepsPerTurn?: number;
  rateLimiter?: RateLimiter;
  context?: ContextConfig;
  memory?: MemoryConfig;
  hooks?: HooksConfig;
  processors?: ProcessorsConfig;
  /**
   * Audit sink for elevated tools. Threaded through to `ctx.toolAuditLogger`
   * on every tool `execute`, so `createElevatedTool` emits a durable
   * record per `ctx.audit()` call. Absent → audit calls are enforced
   * but not persisted.
   */
  toolAuditLogger?: import("./tool-audit/types.ts").ToolAuditLogger;
  /** Time source. Default: SystemClock. Pass FakeClock in tests to drive idle timers. */
  clock?: Clock;
  /**
   * Separate LLM used only for context compaction (summarising dropped messages).
   * Useful for routing summaries to a cheaper or locally-hosted model.
   * Falls back to `llm` when omitted.
   */
  compactionLlm?: LLMProvider;
  /**
   * Optional telemetry sink. When set, every LLM call and tool
   * invocation records counters / histograms. Defaults to no-op when
   * unset — instrumentation cost is zero. See `metrics/types.ts` for
   * the metric catalog.
   */
  metrics?: import("./metrics/types.ts").AgentMetrics;
  /**
   * Per-model USD rates (loaded from config — hardcoded prices go
   * stale fast). When set together with `metrics`, llm.cost.usd is
   * emitted per call.
   */
  costs?: import("./metrics/types.ts").ModelCostRegistry;
  /**
   * Provider id used for metric labels (e.g. "anthropic"). Required
   * when `metrics` is set so metrics aren't unlabeled.
   */
  llmProvider?: string;
  /**
   * Model id used for metric labels (e.g. "claude-sonnet-4-6"). Required
   * when `metrics` is set so metrics aren't unlabeled.
   */
  llmModel?: string;
  /** Extra labels appended to every metric (e.g. agent id, mode). */
  metricLabels?: Readonly<Record<string, string>>;
  /**
   * Idle keepalive interval in ms. While a turn is in flight, if no
   * event fires for `heartbeatMs`, a `heartbeat` event is emitted to
   * the bus. Lets HTTP / SSE consumers tell "still thinking" from
   * "connection died" — long extended-thinking turns can otherwise go
   * silent for 30s+ and trip proxy idle timeouts.
   *
   * Default: 15_000. Set to 0 to disable. Heartbeat is transient (not
   * journaled / replayed); subscribers that don't care can ignore the
   * type.
   */
  heartbeatMs?: number;
  /**
   * Called on every agent lifecycle transition across all sessions created by this loop.
   * Fires from inside a journaled activity — async return values are awaited in the
   * background.
   *
   * @replay
   * The activity is SKIPPED on journal replay (e.g. after server restart),
   * so the callback does NOT re-fire for historical turns.
   *
   * @errors
   * **Errors are logged via console.error and SWALLOWED.** This hook is
   * advisory observability — it MUST NOT be load-bearing for correctness.
   * Failed external state-machine writes will not abort the turn or propagate.
   * If you need correctness-tied lifecycle effects, drive them from the
   * persistent storage transitions (workflow row status, step rows) rather
   * than this callback.
   */
  onLifecycle?: (event: AgentLifecycleEvent) => void | Promise<void>;
  /**
   * Token budget for extended thinking. When set, every LLM call will include a
   * thinking phase up to this many tokens before producing the final response.
   * Requires a model that supports extended thinking (e.g. Claude 3.7+).
   */
  thinkingBudgetTokens?: number;
  /**
   * Structured event log for this session. When provided, the agent emits
   * turn, llm.call, tool, compact, and approval events to this logger.
   * Use InMemorySessionLogger for an in-process ring buffer.
   */
  logger?: SessionLogger;
}

/**
 * Current processing state of the session.
 * - `"idle"` — waiting for the next `send()` / `stream()` call.
 * - `"thinking"` — LLM call or tool execution in progress.
 * - `"waiting_approval"` — suspended mid-turn waiting for `approve()` / `reject()`.
 */
export type AgentStatus = "idle" | "thinking" | "waiting_approval";

/** Mirrors AgentStatus — the set of states the embedded lifecycle machine can be in. */
export type AgentLifecycleState = "idle" | "thinking" | "waiting_approval";

export interface AgentLifecycleEntry {
  from: AgentLifecycleState;
  event: string;
  to: AgentLifecycleState;
  createdAt: Date;
}

export interface AgentLifecycleEvent extends AgentLifecycleEntry {
  sessionId: string;
  /** The context of the destination state (e.g. `{ turn, task }` for "thinking"). */
  context: unknown;
  /**
   * `true` when the workflow body is executing on top of pre-existing
   * journal entries. Lifecycle events fire from inside journaled
   * activities so the hook only runs when the activity is fresh —
   * `isReplay: true` indicates the body has been re-started by a
   * worker restart or signal-resume but this particular transition
   * is past the journal cursor.
   */
  isReplay: boolean;
}

export interface StreamOptions {
  signal?: AbortSignal;
  /**
   * Called with each thinking delta when extended thinking is enabled.
   * Fires before the first text delta for the same step.
   */
  onThinking?: (delta: string) => void;
}

export interface CompactResult {
  /** Non-system messages kept after compaction. */
  kept: number;
  /** Non-system messages dropped. */
  dropped: number;
  /** LLM-generated summary of the dropped messages, or null when summarization was skipped. */
  summary: string | null;
}

export interface AgentSession {
  send(task: string): Promise<string>;
  /**
   * Send a task and receive the answer as a stream of token deltas.
   * Falls back to a single-chunk stream when the LLM adapter has no chatStream.
   *
   * Pass an AbortSignal (or `options.signal`) to cancel mid-flight.
   * Pass `options.onThinking` to receive extended-thinking deltas in real time.
   * The signal is propagated to the underlying fetch — the workflow completes
   * the turn with an empty response so conversation history stays consistent.
   */
  stream(task: string, options?: AbortSignal | StreamOptions): AsyncIterable<string>;
  /**
   * Approve a pending tool call that has `requireApproval: true`.
   * Returns `true` if the signal was delivered and the workflow was resumed,
   * `false` if no approval gate is pending for that `toolCallId`.
   */
  approve(toolCallId: string): Promise<boolean>;
  /**
   * Reject a pending tool call. The agent receives the rejection as a tool
   * result and continues its turn without executing the tool.
   * Returns `true` if the signal was delivered, `false` if nothing was pending.
   */
  reject(toolCallId: string, reason?: string): Promise<boolean>;
  /** Query the current processing state of this session. */
  status(): Promise<AgentStatus>;
  /** Return the full conversation message history as of the last completed turn. */
  messages(): Message[];
  /** Cumulative token usage for this session across all completed turns. */
  usage(): { inputTokens: number; outputTokens: number };
  /** Current lifecycle state. Matches `status()` but is synchronous and sourced from in-memory state. */
  lifecycleState(): { current: AgentLifecycleState; context: unknown };
  /**
   * Full lifecycle transition history for this session.
   * **Ephemeral** — stored in-memory only, resets on server restart. For durable tracking,
   * use `onLifecycle` to drive an external state machine.
   */
  lifecycleHistory(): AgentLifecycleEntry[];
  /**
   * Returns all session events emitted so far (turn, llm.call, tool, compact, approval).
   * Empty when no logger was passed to agentLoop. Ephemeral — resets on server restart.
   */
  eventLog(): import("./session-logger.ts").SessionEvent[];
  /**
   * Subscribe to live session events. The observer fires for every event the
   * agent emits — turn lifecycle, tool start / end, approval requests, and
   * (when streaming) `token.delta` + `tool.progress` events. Returns an
   * unsubscribe function.
   *
   * Subscribers attach at any time (idle, mid-stream, between turns) and
   * receive only events from the moment they subscribe; no replay buffer.
   * For event history, use `eventLog()`.
   *
   * Used by cross-process forwarders (e.g. the WS relay that ships agent
   * events from a Zorya worker to the dashboard). One observer fault is
   * isolated — a throwing subscriber is removed silently and does not
   * affect the agent loop or its peers.
   */
  subscribe(observer: (event: SessionEvent) => void): () => void;
  /**
   * Compact the conversation history right now, between turns.
   * The compacted messages are applied at the start of the next turn via the
   * task signal (so they are journaled and survive replay).
   * Throws if called while a turn is in progress.
   */
  compact(config?: { keepMessages?: number }): Promise<CompactResult>;
  close(): Promise<void>;
}

export interface AgentLoop {
  session(params: { runner: WorkflowRunner; sessionId: string }): Promise<AgentSession>;
}

/**
 * Strip extended-thinking blocks from assistant messages older than the
 * most recent user turn. Thinking is per-turn ephemeral state — useful
 * to the model while it picked the prior step, dead weight on the next
 * turn. Cuts input-token cost for any extended-thinking-enabled agent
 * (typically 10–30% on long sessions) without affecting reasoning
 * quality.
 *
 * Boundary: walk back from the end, find the most recent user message,
 * drop `thinkingBlocks` from every assistant message older than it.
 * The current turn's thinking is preserved so the model can see its
 * own reasoning while it plans the next step within the turn.
 *
 * Pure — returns a new array. Messages without thinkingBlocks are
 * shared by reference (no allocation when nothing to redact).
 */
export function redactPriorThinkingBlocks(messages: ReadonlyArray<Message>): Message[] {
  // Find the index of the most recent user message — anything before it
  // is "prior turn" by definition.
  let mostRecentUserIdx = -1;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i]!.role === "user") {
      mostRecentUserIdx = i;
      break;
    }
  }
  // No user message yet (e.g. system-only) → nothing to redact.
  if (mostRecentUserIdx <= 0) return [...messages];

  return messages.map((m, i) => {
    if (i >= mostRecentUserIdx) return m;
    if (m.role !== "assistant") return m;
    if (!m.thinkingBlocks || m.thinkingBlocks.length === 0) return m;
    const { thinkingBlocks: _, ...rest } = m;
    return rest as Message;
  });
}

// ---- ChunkQueue ----

// Single-consumer async queue for streaming token chunks.
// Synchronous push/close (safe to call from inside ctx.activity) with a pull-based
// AsyncIterable consumer. Replaces the hand-rolled array+notify+null-sentinel pattern.
class ChunkQueue implements AsyncIterable<string> {
  private readonly _buf: string[] = [];
  private _closed = false;
  private _notify: (() => void) | null = null;

  push(chunk: string): void {
    this._buf.push(chunk);
    this._notify?.();
    this._notify = null;
  }

  close(): void {
    this._closed = true;
    this._notify?.();
    this._notify = null;
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<string> {
    while (true) {
      while (this._buf.length > 0) yield this._buf.shift()!;
      if (this._closed) return;
      await new Promise<void>((r) => {
        this._notify = r;
      });
    }
  }
}

// ---- SessionState ----

interface SessionState {
  /** Next turn number to assign on send()/stream(). */
  turn: number;
  closed: boolean;
  inTurn: boolean;
  inDelivery: boolean;
  latestMessages: Message[];
  idleStart: number;
  idleTimer: TimerHandle | null;
  /** Cumulative token usage across all completed turns. */
  totalInputTokens: number;
  totalOutputTokens: number;
  /** Embedded lifecycle state machine. */
  lifecycleState: AgentLifecycleState;
  lifecycleContext: unknown;
  lifecycleHistory: AgentLifecycleEntry[];
}

// ---- agentLoop ----

/**
 * Create a persistent interactive agent loop.
 *
 * Returns an `AgentLoop` whose `session()` creates an `AgentSession` — a long-lived
 * conversational handle that accumulates message history across many `send()` / `stream()`
 * calls. Each session is a durable workflow (journaled, crash-safe) so it can be
 * recreated on server restart by passing the same `sessionId` and storage backend.
 *
 * Key features vs `agentAction`:
 * - **Multi-turn**: one session, many user turns.
 * - **Streaming**: `session.stream(task)` yields token deltas in real time.
 * - **Context management**: auto-compacts when message count exceeds `context.maxMessages`.
 * - **Memory**: retrieves relevant memories at session start; saves compaction summaries.
 * - **Approval gate**: tools with `requireApproval: true` pause the turn until
 *   `session.approve(toolCallId)` or `session.reject(toolCallId)` is called.
 * - **Idle hook**: `hooks.onIdle` fires after the session sits quiet for `idleTimeoutMs`.
 *
 * ## Error contract
 *
 * `session.send()` / `session.stream()` throw a plain `Error` with the
 * underlying cause. Every error path goes through `surfaceAgentError` so
 * Effect FiberFailure wrappers are unwrapped before they leave this
 * module — what you catch is what your tool / hook actually threw.
 * `WorkflowSuspendedError` (the workflow waiting for a signal / sleep)
 * never escapes — it's classified as `suspended` and swallowed.
 * Step-limit truncation is signalled separately via `HooksAfterTurnParams.truncated`
 * + a `(step limit reached)` answer, not by throwing.
 *
 * @example
 * ```ts
 * const loop = agentLoop({
 *   name: "my-agent",
 *   llm: anthropic("claude-sonnet-4-6", { apiKey }),
 *   tools: { search: webSearchTool },
 *   systemPrompt: "You are a helpful assistant.",
 *   memory: { store: memoryIndex },
 * });
 *
 * const session = await loop.session({ runner, sessionId: "s1" });
 * for await (const chunk of session.stream("Hello!")) process.stdout.write(chunk);
 * await session.close();
 * ```
 */
export function agentLoop(config: AgentLoopConfig): AgentLoop {
  const maxTurns = config.maxTurns ?? 1000;
  const maxStepsPerTurn = config.maxStepsPerTurn ?? 20;

  const contextConfig: CompactionConfig & { maxMessages: number } = {
    maxMessages: config.context?.maxMessages ?? 80,
    keepMessages: config.context?.keepMessages ?? 40,
    summarize: config.context?.summarize ?? true,
  };
  const contextLimit = config.context?.contextLimit;
  const compressAt = config.context?.compressAt ?? 0.7;
  const recapPrompt = config.context?.recapPrompt ?? RECAP_SUMMARY_PROMPT;
  const redactPriorThinking = config.context?.redactPriorThinking ?? true;

  return {
    async session({ runner, sessionId }) {
      if (!isActivityJournalStorage(runner.storage) || !isJournaledSuspendStorage(runner.storage)) {
        throw new Error(
          "agentLoop requires storage that implements JournaledSuspendStorage " +
            "(e.g. InMemoryWorkflowStorage or PgWorkflowStorage).",
        );
      }
      const journalStorage = runner.storage as unknown as JournaledSuspendStorage;
      const activityStorage = runner.storage as unknown as ActivityJournalStorage;

      const clock = config.clock ?? SystemClock;
      // Multi-subscriber event bus. The legacy `config.logger` (if provided)
      // is wired in as one subscriber so existing `session.eventLog()` callers
      // keep working unchanged. New subscribers attach via `session.subscribe`
      // — used by cross-process forwarders (the WS relay in promin-o8dj /
      // promin-yxxk) and any other live observer.
      const eventBus = new SessionEventBus();
      const sessionLogger = config.logger ?? null;
      if (sessionLogger) {
        eventBus.subscribe((event) => sessionLogger.emit(event));
      }
      // Turn start times flow through the journal (activity return value) so
      // they survive worker restarts mid-turn. Before, this Map was populated
      // inside `lc-${turn}-message`; on crash recovery the activity replayed
      // from the journal without re-running the Map.set, and the later
      // `emit-${turn}` activity found the Map empty and logged durationMs: 0.
      // Now the start time is returned from the lc activity (journaled) and
      // read from that journaled value inside emit. Map stays for in-process
      // lookup; populated from the activity's return value, not a side effect.
      const turnStarts = new Map<number, number>();
      // Aborted by close() to cancel any in-flight LLM fetch.
      const sessionAc = new AbortController();

      // Heartbeat pump — emits a `heartbeat` event when the bus has been
      // idle for `heartbeatMs` while a turn is in flight. Subscribers
      // (SSE forwarder, tests) get a keepalive so they can distinguish
      // "still thinking" from "connection died". Heartbeats are
      // transient — never journaled, no replay impact.
      const heartbeatMs = config.heartbeatMs ?? 15_000;
      let lastEventAt = clock.currentTimeMs();
      let heartbeatTimer: TimerHandle | null = null;
      let heartbeatTurn: number | null = null;
      eventBus.subscribe((ev) => {
        // Don't reset the idle clock for our OWN heartbeat — otherwise
        // a single heartbeat would gate every subsequent one.
        if (ev.type !== "heartbeat") lastEventAt = clock.currentTimeMs();
      });
      const startHeartbeat = (turn: number): void => {
        if (heartbeatMs <= 0) return;
        if (heartbeatTimer) heartbeatTimer.clear();
        heartbeatTurn = turn;
        lastEventAt = clock.currentTimeMs();
        heartbeatTimer = clock.setInterval(() => {
          if (heartbeatTurn !== turn) return;
          if (clock.currentTimeMs() - lastEventAt >= heartbeatMs) {
            eventBus.emit({ type: "heartbeat", turn });
          }
        }, heartbeatMs);
      };
      const stopHeartbeat = (): void => {
        if (heartbeatTimer) heartbeatTimer.clear();
        heartbeatTimer = null;
        heartbeatTurn = null;
      };
      const pendingResponses = new Map<
        number,
        { resolve: (answer: string) => void; reject: (err: Error) => void }
      >();
      const pendingStreams = new Map<number, ChunkQueue>();
      const pendingThinkingCallbacks = new Map<number, (delta: string) => void>();
      const pendingSignals = new Map<number, AbortSignal>();
      const state: SessionState = {
        turn: 0,
        closed: false,
        inTurn: false,
        inDelivery: false,
        latestMessages: [],
        idleStart: 0,
        idleTimer: null,
        totalInputTokens: 0,
        totalOutputTokens: 0,
        lifecycleState: "idle",
        lifecycleContext: { turns: 0 },
        lifecycleHistory: [],
      };

      function transitionLifecycle(
        event: string,
        to: AgentLifecycleState,
        ctx: unknown,
        isReplay: boolean,
      ) {
        const entry: AgentLifecycleEntry = {
          from: state.lifecycleState,
          event,
          to,
          createdAt: new Date(),
        };
        state.lifecycleHistory.push(entry);
        state.lifecycleState = to;
        state.lifecycleContext = ctx;
        Promise.resolve(
          config.onLifecycle?.({ ...entry, sessionId, context: ctx, isReplay }),
        ).catch((err) => console.error("[agentLoop] onLifecycle error:", err));
      }

      const resetIdleTimer = () => {
        state.idleTimer?.clear();
        state.idleTimer = null;
        const { onIdle, idleTimeoutMs } = config.hooks ?? {};
        if (!onIdle || !idleTimeoutMs) return;
        state.idleStart = clock.currentTimeMs();
        state.idleTimer = clock.setTimeout(() => {
          state.idleTimer = null;
          onIdle(clock.currentTimeMs() - state.idleStart).catch((err) =>
            console.error("[agentLoop] onIdle error:", err),
          );
        }, idleTimeoutMs);
      };

      const sessionWorkflow = workflow<void>({ name: config.name }).journaled(
        "conversation",
        function* (ctx, _input) {
          let messages: Message[] = config.systemPrompt
            ? [{ role: "system" as const, content: config.systemPrompt }]
            : [];

          // Inject relevant memories from previous sessions
          if (config.memory && (config.memory.injectLimit ?? 5) > 0) {
            const memMsgs = yield* ctx.activity("inject-memories", () =>
              searchRelevantMemories(config.memory!, config.systemPrompt ?? "general context"),
            );
            messages = [...messages, ...memMsgs];
          }

          for (let turn = 0; turn < maxTurns; turn++) {
            const { task, compactedMessages } = yield* ctx.signal<{
              task: string;
              compactedMessages?: Message[];
            }>(`task-${turn}`);
            // Apply manually-triggered compaction (delivered via the task signal so it is journaled).
            if (compactedMessages !== undefined) messages = compactedMessages;
            // Journal the turn start time so replay preserves it. The
            // return value is what's durable; the Map.set is an in-process
            // optimization that repopulates from the journaled value on
            // both fresh run and replay.
            const turnStartTime = yield* ctx.activity(`lc-${turn}-message`, async () => {
              transitionLifecycle("message", "thinking", { turns: turn, turn, task }, ctx.isReplay);
              const startedAt = Date.now();
              eventBus.emit({ type: "turn.start", turn, task });
              return startedAt;
            });
            turnStarts.set(turn, turnStartTime);
            // Start the idle keepalive — runs outside any activity so
            // it's a pure side-effect for live observers, never journaled.
            startHeartbeat(turn);
            messages = [...messages, { role: "user", content: task }];

            // beforeTurn hook — can inject additional context into the message list
            if (config.hooks?.beforeTurn) {
              const modified = yield* ctx.activity(`before-turn-${turn}`, () =>
                config.hooks!.beforeTurn!({ task, messages, isReplay: ctx.isReplay }),
              );
              if (modified !== undefined) messages = modified;
            }

            let answer = "";
            let hitStepLimit = true;
            let turnInputTokens = 0;
            let turnOutputTokens = 0;

            for (let step = 0; step < maxStepsPerTurn; step++) {
              // Snapshot tool names each step so new tools hot-loaded via writeTool are visible
              // immediately on the next think step. Only names are journaled — implementations
              // come from the live registry so replay sees the same names but current code.
              const allTools = resolveTools(config);
              const stepToolNames = yield* ctx.activity(`tool-snapshot-${turn}-${step}`, async () =>
                Object.keys(allTools),
              );
              const toolMap = Object.fromEntries(
                stepToolNames.flatMap((name) => (allTools[name] ? [[name, allTools[name]]] : [])),
              );
              const toolDefs = buildToolDefs(toolMap);
              const chunkQueue = pendingStreams.get(turn);
              const thinkingCb = pendingThinkingCallbacks.get(turn);
              const response = yield* ctx.activity(`think-${turn}-${step}`, async () => {
                const llmStart = Date.now();
                // Strip thinking blocks from prior turns before each LLM
                // call. Cheap (single-pass, returns same array when no
                // redaction needed) and saves real tokens on every turn
                // beyond the first when extended-thinking is enabled.
                const llmMessages = redactPriorThinking
                  ? redactPriorThinkingBlocks(messages)
                  : messages;
                const result = await runLlmCall({
                  llm: config.llm,
                  messages: llmMessages,
                  tools: toolDefs.length > 0 ? toolDefs : undefined,
                  rateLimiter: config.rateLimiter,
                  processors: config.processors,
                  processorCtx: {
                    step,
                    turn,
                    workflowId: ctx.workflowId,
                    isReplay: ctx.isReplay,
                  },
                  ...(config.metrics && { metrics: config.metrics }),
                  ...(config.costs && { costs: config.costs }),
                  ...(config.llmProvider && { provider: config.llmProvider }),
                  ...(config.llmModel && { model: config.llmModel }),
                  ...(config.metricLabels && { metricLabels: config.metricLabels }),
                  // Tee the LLM stream into both the chunk queue (for
                  // session.stream()'s AsyncIterable<string>) AND the event
                  // bus as token.delta events (for cross-process forwarders).
                  // The bus marks token.delta as transient by default so the
                  // logger ring buffer doesn't fill up with chat noise.
                  onChunk: (delta) => {
                    if (chunkQueue) chunkQueue.push(delta);
                    eventBus.emit({ type: "token.delta", turn, delta });
                  },
                  onThinking: thinkingCb,
                  thinkingBudgetTokens: config.thinkingBudgetTokens,
                  signal: pendingSignals.get(turn),
                });
                eventBus.emit({
                  type: "llm.call",
                  turn,
                  step,
                  durationMs: Date.now() - llmStart,
                  tokens: result.usage,
                });
                return result;
              });

              if (response.usage) {
                turnInputTokens += response.usage.inputTokens;
                turnOutputTokens += response.usage.outputTokens;

                // Token-based rolling RECAP: compact before the next think step when
                // input tokens approach the model context limit, so we never hit a
                // hard "prompt too long" error mid-session.
                if (
                  contextLimit !== undefined &&
                  response.usage.inputTokens >= contextLimit * compressAt
                ) {
                  const beforeRecap = nonSystemMsgs(messages).length;
                  const recapResult = yield* ctx.activity(`compress-${turn}-${step}`, async () => {
                    const r = await compact(
                      messages,
                      contextConfig,
                      config.compactionLlm ?? config.llm,
                      recapPrompt,
                    );
                    const afterRecap = nonSystemMsgs(r.messages).length;
                    eventBus.emit({
                      type: "compact",
                      turn,
                      reason: "token_limit",
                      kept: afterRecap,
                      dropped: beforeRecap - afterRecap,
                    });
                    return r;
                  });
                  messages = recapResult.messages;
                  if (
                    recapResult.summary &&
                    config.memory?.saveOnCompact !== false &&
                    config.memory?.store
                  ) {
                    yield* ctx.activity(`save-memory-recap-${turn}-${step}`, () =>
                      config.memory!.store.save(
                        {
                          content: recapResult.summary!,
                          metadata: { sessionId, turn, step, type: "recap-summary" },
                        },
                        config.memory!.scope,
                      ),
                    );
                  }
                }
              }

              const assistantMsg: AssistantMessage = {
                role: "assistant",
                content: response.content,
                toolCalls: response.toolCalls,
                thinkingBlocks: response.thinkingBlocks,
              };
              messages = [...messages, assistantMsg];

              if (response.finishReason === "stop" || !response.toolCalls?.length) {
                hitStepLimit = false;
                answer = response.content ?? "";
                break;
              }

              const toolResultMsgs: ToolResultMessage[] = [];
              // biome-ignore lint/suspicious/noExplicitAny: Zod validates input at runtime
              const toExecute: Array<{ call: ToolCall; toolDef: AgentTool<any, any> }> = [];

              // Phase 1: resolve approvals sequentially (each may need a user signal)
              for (const call of response.toolCalls) {
                const toolDef = toolMap[call.name];
                if (!toolDef) {
                  toolResultMsgs.push({
                    role: "tool",
                    toolCallId: call.id,
                    content: `Error: unknown tool "${call.name}"`,
                  });
                  continue;
                }
                if (
                  toolDef.requireApproval &&
                  !shouldAutoApprove(config.autoApprove, call, toolDef)
                ) {
                  const logRequested = () =>
                    eventBus.emit({
                      type: "approval.requested",
                      turn,
                      toolCallId: call.id,
                      toolName: call.name,
                    });
                  const logDecision = (approved: boolean) =>
                    eventBus.emit({
                      type: "approval.decision",
                      turn,
                      toolCallId: call.id,
                      approved,
                    });

                  let decision: ApprovalDecision;
                  if (config.hooks?.onApprovalRequired) {
                    logRequested();
                    decision = yield* ctx.activity(`approval-${call.id}`, () =>
                      config.hooks!.onApprovalRequired!(call),
                    );
                    logDecision(decision.approved);
                  } else {
                    // Activity result is journaled — listPendingApprovals
                    // reads `{ toolName, toolInput }` from the matching
                    // `lc-*-approval-<callId>-start` step to surface the
                    // pending request without re-running anything.
                    yield* ctx.activity(`lc-${turn}-approval-${call.id}-start`, async () => {
                      transitionLifecycle(
                        "approval-required",
                        "waiting_approval",
                        { toolCallId: call.id },
                        ctx.isReplay,
                      );
                      logRequested();
                      return { toolName: call.name, toolInput: call.input };
                    });
                    decision = yield* ctx.signal<ApprovalDecision>(`approve:${call.id}`);
                    yield* ctx.activity(`lc-${turn}-approval-${call.id}-end`, async () => {
                      transitionLifecycle(
                        decision.approved ? "approved" : "rejected",
                        "thinking",
                        { toolCallId: call.id, approved: decision.approved },
                        ctx.isReplay,
                      );
                      logDecision(decision.approved);
                    });
                  }
                  if (!decision.approved) {
                    toolResultMsgs.push({
                      role: "tool",
                      toolCallId: call.id,
                      content: `Rejected: ${decision.reason ?? "user rejected"}`,
                    });
                    continue;
                  }
                }
                toExecute.push({ call, toolDef });
              }

              // Phase 2: run all approved tools in parallel, each individually journaled
              if (toExecute.length > 0) {
                const results = yield* ctx.parallel(
                  toExecute.map(({ call, toolDef }) =>
                    ctx.activity(`tool-${call.name}-${turn}-${step}-${call.id}`, async () => {
                      eventBus.emit({
                        type: "tool.start",
                        turn,
                        step,
                        name: call.name,
                        input: call.input,
                      });
                      const toolStart = Date.now();
                      // Bridge tool.write(payload) into a tool.progress
                      // event labeled with the current turn/step/callId.
                      // Tools that don't write progress get no events;
                      // the writer is opt-in on the tool author's side.
                      const result = await executeToolCall(call, toolDef, undefined, (payload) =>
                        eventBus.emit({
                          type: "tool.progress",
                          turn,
                          step,
                          toolCallId: call.id,
                          name: call.name,
                          payload,
                        }),
                      );
                      const durationMs = Date.now() - toolStart;
                      const isParseError = result.content.startsWith("Invalid input");
                      const failed =
                        isParseError || result.content.startsWith("Tool execution failed");
                      if (isParseError) {
                        eventBus.emit({
                          type: "tool.parse_error",
                          turn,
                          step,
                          name: call.name,
                          error: result.content,
                        });
                      }
                      eventBus.emit({
                        type: "tool.end",
                        turn,
                        step,
                        name: call.name,
                        durationMs,
                        failed,
                      });
                      return result;
                    }),
                  ),
                );
                toolResultMsgs.push(...results);
              }

              messages = [...messages, ...toolResultMsgs];
            }

            if (hitStepLimit) {
              console.warn(
                `[agentLoop] turn ${turn}: maxStepsPerTurn (${maxStepsPerTurn}) reached without a final answer.`,
              );
              // Synthesize a closing assistant message so history doesn't end on tool
              // results. Without this, the next turn's LLM call receives malformed context
              // (consecutive user-role messages in Anthropic's format).
              answer = "(step limit reached)";
              messages = [...messages, { role: "assistant", content: answer }];
              eventBus.emit({ type: "step_limit.hit", turn, maxSteps: maxStepsPerTurn });
            }

            yield* ctx.activity(`emit-${turn}`, async () => {
              // Stop the idle heartbeat — turn is settling. Done outside
              // a journaled write since it's a pure live-side-effect.
              stopHeartbeat();
              // Update messages snapshot here so messages() is consistent immediately
              // after send() / stream() returns, not only after after-turn-N completes.
              state.latestMessages = messages;
              // Capture abort state before deleting the signal from the map.
              const wasAborted = pendingSignals.get(turn)?.aborted ?? false;
              pendingResponses.get(turn)?.resolve(answer);
              pendingResponses.delete(turn);
              pendingStreams.get(turn)?.close();
              pendingThinkingCallbacks.delete(turn);
              pendingSignals.delete(turn);
              const durationMs = turnStarts.has(turn) ? Date.now() - turnStarts.get(turn)! : 0;
              turnStarts.delete(turn);
              if (wasAborted) {
                eventBus.emit({
                  type: "turn.aborted",
                  turn,
                  reason: sessionAc.signal.aborted ? "close" : "signal",
                });
              } else {
                eventBus.emit({
                  type: "turn.end",
                  turn,
                  answer,
                  durationMs,
                  tokens: { inputTokens: turnInputTokens, outputTokens: turnOutputTokens },
                });
              }
              return answer;
            });

            // afterTurn hook — user hook for logging, memory writes, analytics.
            yield* ctx.activity(`after-turn-${turn}`, async () => {
              state.totalInputTokens += turnInputTokens;
              state.totalOutputTokens += turnOutputTokens;
              await config.hooks?.afterTurn?.({
                task,
                answer,
                messages,
                usage: { inputTokens: turnInputTokens, outputTokens: turnOutputTokens },
                truncated: hitStepLimit,
                isReplay: ctx.isReplay,
              });
            });
            yield* ctx.activity(`lc-${turn}-done`, async () => {
              transitionLifecycle("done", "idle", { turns: turn + 1 }, ctx.isReplay);
            });

            // Compact if non-system messages exceed the threshold
            const nonSystemCount = nonSystemMsgs(messages).length;
            if (nonSystemCount > contextConfig.maxMessages) {
              const beforeCompact = nonSystemCount;
              const result = yield* ctx.activity(`compact-${turn}`, async () => {
                const r = await compact(
                  messages,
                  contextConfig,
                  config.compactionLlm ?? config.llm,
                );
                const afterCompact = nonSystemMsgs(r.messages).length;
                eventBus.emit({
                  type: "compact",
                  turn,
                  reason: "message_count",
                  kept: afterCompact,
                  dropped: beforeCompact - afterCompact,
                });
                return r;
              });
              messages = result.messages;

              if (
                result.summary &&
                config.memory?.saveOnCompact !== false &&
                config.memory?.store
              ) {
                yield* ctx.activity(`save-memory-${turn}`, () =>
                  config.memory!.store.save(
                    {
                      content: result.summary!,
                      metadata: { sessionId, turn, type: "compaction-summary" },
                    },
                    config.memory!.scope,
                  ),
                );
              }
            }
          }
        },
      );

      const builtWorkflow = sessionWorkflow.build();

      await runner.start({
        workflow: builtWorkflow,
        workflowId: sessionId,
        input: undefined,
      });

      // Restore turn counter from the journal so that recreating the session
      // object (e.g. server restart with persistent storage) doesn't re-deliver
      // task-0. Count completed emit-N activities — each represents one done turn.
      const pastEntries = await activityStorage.loadJournal(sessionId, "conversation");
      const reconstructedTurn = pastEntries.reduce((max, e) => {
        const m = e.activityName.match(/^emit-(\d+)$/);
        return m && e.exit?.tag === "Success" ? Math.max(max, Number(m[1]) + 1) : max;
      }, 0);

      // Sanity check: the reconstructed counter must be at least as high
      // as the highest task-N signal that was DELIVERED (we count those by
      // looking at activity names matching lc-N-message — the very first
      // activity each turn). If the journal is missing emit entries that
      // logically must exist (because lc-N-message is present), we'd
      // re-deliver task-N and clobber prior turns. This usually only fires
      // on a custom storage backend that didn't round-trip exit.tag, or
      // on a corrupted journal.
      const maxStartedTurn = pastEntries.reduce((max, e) => {
        const m = e.activityName.match(/^lc-(\d+)-message$/);
        return m && e.exit?.tag === "Success" ? Math.max(max, Number(m[1]) + 1) : max;
      }, 0);
      if (reconstructedTurn < maxStartedTurn) {
        throw new Error(
          `Turn counter reconstruction mismatch on session "${sessionId}": ` +
            `journal reports ${reconstructedTurn} completed turn(s) but at least ` +
            `${maxStartedTurn} turn(s) were started (lc-${maxStartedTurn - 1}-message ` +
            `is present without a matching emit-${maxStartedTurn - 1}). This implies ` +
            `the storage backend lost emit-N exit metadata, or the journal is corrupt. ` +
            `Refusing to proceed to avoid duplicate turn delivery.`,
        );
      }
      state.turn = reconstructedTurn;

      // Holds compacted messages to inject on the next deliverAndRun call.
      let _pendingCompact: Message[] | null = null;

      async function deliverAndRun(task: string, turn: number): Promise<void> {
        const value: { task: string; compactedMessages?: Message[] } = { task };
        if (_pendingCompact !== null) {
          value.compactedMessages = _pendingCompact;
          _pendingCompact = null;
        }
        await completeSignal({
          storage: journalStorage,
          workflowId: sessionId,
          stepName: "conversation",
          signalName: `task-${turn}`,
          value,
        });
        // Re-run: replays journal, consumes signal, suspends at next signal.
        const { error } = await runner.runSafe({
          workflow: builtWorkflow,
          workflowId: sessionId,
          input: undefined,
        });
        if (error) {
          const surfaced = surfaceAgentError(error);
          if (surfaced.kind !== "suspended") throw surfaced.error;
        }
      }

      // Race guard: if approve/reject is called before the workflow has reached ctx.signal
      // (e.g. UI sends the decision before the suspension is registered), retry up to
      // 5 times with 50ms gaps to let the workflow reach its suspension point.
      async function deliverApprovalSignal(
        toolCallId: string,
        value: ApprovalDecision,
      ): Promise<boolean> {
        for (let attempt = 0; attempt < 5; attempt++) {
          const delivered = await completeSignal({
            storage: journalStorage,
            workflowId: sessionId,
            stepName: "conversation",
            signalName: `approve:${toolCallId}`,
            value,
          });
          if (delivered) {
            const { error } = await runner.runSafe({
              workflow: builtWorkflow,
              workflowId: sessionId,
              input: undefined,
            });
            if (error) {
              const surfaced = surfaceAgentError(error);
              if (surfaced.kind !== "suspended") throw surfaced.error;
            }
            return true;
          }
          if (attempt < 4) await new Promise<void>((r) => clock.setTimeout(r, 50));
        }
        return false;
      }

      return {
        async send(task: string): Promise<string> {
          if (state.closed) throw new Error("Session is closed");
          if (state.inTurn) throw new Error("Session is busy — only one turn at a time");
          const turn = state.turn++;
          state.inTurn = true;
          state.inDelivery = true;
          pendingSignals.set(turn, sessionAc.signal);
          const promise = new Promise<string>((resolve, reject) =>
            pendingResponses.set(turn, { resolve, reject }),
          );
          try {
            await deliverAndRun(task, turn);
            state.inDelivery = false;
            return await promise;
          } catch (err) {
            pendingResponses.delete(turn);
            pendingStreams.delete(turn);
            pendingThinkingCallbacks.delete(turn);
            pendingSignals.delete(turn);
            throw err;
          } finally {
            state.inTurn = false;
            state.inDelivery = false;
            resetIdleTimer();
          }
        },

        async *stream(task: string, options?: AbortSignal | StreamOptions): AsyncIterable<string> {
          if (state.closed) throw new Error("Session is closed");
          if (state.inTurn) throw new Error("Session is busy — only one turn at a time");
          const turn = state.turn++;
          state.inTurn = true;

          const opts: StreamOptions =
            options instanceof AbortSignal ? { signal: options } : (options ?? {});

          // Combine caller's abort signal with the session-level close signal.
          const combinedSignal = opts.signal
            ? AbortSignal.any([opts.signal, sessionAc.signal])
            : sessionAc.signal;
          pendingSignals.set(turn, combinedSignal);

          const chunkQueue = new ChunkQueue();
          pendingStreams.set(turn, chunkQueue);
          if (opts.onThinking) pendingThinkingCallbacks.set(turn, opts.onThinking);

          const answerPromise = new Promise<string>((resolve, reject) =>
            pendingResponses.set(turn, { resolve, reject }),
          );
          // Pre-attach a no-op catch so that if close() rejects this promise
          // while state.closed has already flipped (and we therefore skip
          // awaiting it below), the rejection isn't reported as an
          // unhandled rejection. Adding .catch() to a Promise returns a
          // new derived promise — `await answerPromise` later still sees
          // the original rejection.
          answerPromise.catch(() => {});

          try {
            state.inDelivery = true;
            try {
              await completeSignal({
                storage: journalStorage,
                workflowId: sessionId,
                stepName: "conversation",
                signalName: `task-${turn}`,
                value: { task },
              });
            } finally {
              // inDelivery = false while runSafe is still in flight. status()
              // returns "thinking" via the runner.getStatus() fallback (inTurn=true),
              // which is correct — the agent is actively processing, just past the
              // signal-delivery phase.
              state.inDelivery = false;
            }

            // Start workflow run concurrently — chunks arrive via chunkQueue during this call.
            // On failure, close the queue to unblock the for-await below.
            let runError: unknown = undefined;
            const runPromise = runner
              .runSafe({
                workflow: builtWorkflow,
                workflowId: sessionId,
                input: undefined,
              })
              .then(({ error }) => {
                if (error) {
                  const surfaced = surfaceAgentError(error);
                  if (surfaced.kind !== "suspended") {
                    runError = surfaced.error;
                    chunkQueue.close();
                  }
                }
              });

            try {
              for await (const chunk of chunkQueue) yield chunk;
              if (runError) throw runError;
            } finally {
              pendingStreams.delete(turn);
              pendingThinkingCallbacks.delete(turn);
              pendingResponses.delete(turn);
              pendingSignals.delete(turn);
              await runPromise;
              // Invariant: if the run succeeded, emit-N has resolved answerPromise.
              // Skip if the session was closed (close() already rejected the promise and
              // the caller should not see a "Session closed" error propagated here).
              if (!runError && !state.closed) await answerPromise;
              resetIdleTimer();
            }
          } finally {
            state.inTurn = false;
          }
        },

        async approve(toolCallId: string): Promise<boolean> {
          return deliverApprovalSignal(toolCallId, { approved: true });
        },

        async reject(toolCallId: string, reason?: string): Promise<boolean> {
          return deliverApprovalSignal(toolCallId, { approved: false, reason });
        },

        async status(): Promise<AgentStatus> {
          if (state.closed || !state.inTurn) return "idle";
          if (state.inDelivery) return "thinking";
          const info = await runner.getStatus(sessionId, {});
          if (!info || info.state === "completed" || info.state === "failed") return "idle";
          if (info.state === "suspended") return "waiting_approval";
          return "thinking";
        },

        messages(): Message[] {
          return state.latestMessages;
        },

        usage(): { inputTokens: number; outputTokens: number } {
          return { inputTokens: state.totalInputTokens, outputTokens: state.totalOutputTokens };
        },

        lifecycleState() {
          return { current: state.lifecycleState, context: state.lifecycleContext };
        },

        lifecycleHistory() {
          return [...state.lifecycleHistory];
        },

        eventLog() {
          return sessionLogger?.events() ?? [];
        },

        subscribe(observer: (event: SessionEvent) => void): () => void {
          return eventBus.subscribe(observer);
        },

        async compact(cfg?: { keepMessages?: number }): Promise<CompactResult> {
          if (state.closed) throw new Error("Session is closed");
          if (state.inTurn) throw new Error("Cannot compact while a turn is in progress");
          const msgs = state.latestMessages;
          const before = nonSystemMsgs(msgs).length;
          if (before === 0) return { kept: 0, dropped: 0, summary: null };
          const compactCfg: CompactionConfig = {
            keepMessages: cfg?.keepMessages ?? contextConfig.keepMessages,
            summarize: contextConfig.summarize,
          };
          const result = await compact(msgs, compactCfg, config.compactionLlm ?? config.llm);
          const after = nonSystemMsgs(result.messages).length;
          _pendingCompact = result.messages;
          state.latestMessages = result.messages;
          if (result.summary && config.memory?.saveOnCompact !== false && config.memory?.store) {
            await config.memory.store.save(
              { content: result.summary, metadata: { sessionId, type: "compaction-summary" } },
              config.memory.scope,
            );
          }
          return { kept: after, dropped: before - after, summary: result.summary };
        },

        async close() {
          state.closed = true;
          state.idleTimer?.clear();
          state.idleTimer = null;
          stopHeartbeat();
          // Cancel any in-flight LLM fetch so runSafe returns promptly.
          sessionAc.abort();
          // Unblock callers awaiting send() or the answerPromise inside stream().
          const closeErr = new Error("Session closed");
          pendingResponses.forEach(({ reject }) => reject(closeErr));
          pendingResponses.clear();
          // Unblock any for-await loops on chunk streams.
          pendingStreams.forEach((q) => q.close());
          pendingStreams.clear();
          pendingThinkingCallbacks.clear();
          pendingSignals.clear();
          await config.hooks?.onClose?.();
        },
      };
    },
  };
}
