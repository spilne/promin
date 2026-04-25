// ---------------------------------------------------------------------------
// `LocalAgent` — `Agent` implementation backed by `@promin/agent`'s
// `agentAction`. Both one-shot (`invoke` / `stream`) AND conversational
// (`thread.send` / `thread.stream`) paths run as fresh `agentAction`
// workflows; thread continuity comes from seeding each turn with the
// persisted message history from `MemoryStore`.
//
//   invoke(input)    → fresh agentAction, awaited, returns resolved AgentRunOutput
//   stream(input)    → fresh agentAction with bus, returns live AgentRunOutput
//   thread(id, opts) → LocalAgentThread; each .send/.stream is a fresh
//                       agentAction seeded with stored messages
//
// Why stateless thread turns: multi-tenant gateways resolve a fresh
// `LocalAgent` per request. A long-lived `agentLoop` session keyed on
// threadId would conflict ("workflow already running") because the
// session's underlying workflow id is reserved across requests. Stateless
// turns sidestep this entirely — the thread's "memory" lives in the
// `MemoryStore`, not the in-process session map.
//
// `MemoryStore` integration:
//   - thread.send / thread.stream load history → run agentAction → persist
//     the new tail (user task + assistant + any tool messages)
//   - thread.workingMemory / setWorkingMemory / setMetadata delegate to the
//     store directly
//   - thread.delete cascades through MemoryStore.deleteThread
//
// `MemoryStore` is OPTIONAL. When omitted, threads keep history in an
// in-memory array on the LocalAgentThread instance — useful for tests
// and single-process REPLs but not multi-tenant gateways.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import type { WorkflowRunner } from "@promin/workflow";
import type { z } from "zod";
import { agentAction, type AgentActionConfig, type AgentResult } from "../agent-action.ts";
import { agentLoop, type AgentLoopConfig } from "../agent-loop.ts";
import type { Message, ToolCall } from "../message.ts";
import { SessionEventBus, type SessionEvent } from "../session-logger.ts";
import type {
  EpisodicRecord,
  MemoryStore,
  ResolvedContext,
  ThreadKey,
  ThreadRow,
  TokenBudget,
} from "../memory/types.ts";
import {
  DefaultConsolidator,
  type CompactThreadOptions,
  type Consolidator,
  type DistillThreadOptions,
} from "../memory/consolidator.ts";
import { createLayeredMemoryTool } from "../tools/layered-memory-tools.ts";
import type {
  Agent,
  AgentEvent,
  AgentInput,
  AgentInvokeOpts,
  AgentRunOutput,
  AgentThread,
  FinishReason,
  ListThreadsParams,
  MessageRange,
  Step,
  ThreadOptions,
  ThreadSummary,
  ToolResult,
  UsageStats,
} from "./types.ts";

// biome-ignore lint/suspicious/noExplicitAny: outputSchema generic carries through
export interface LocalAgentConfig<TOutput = any> {
  /** Underlying agent config — same shape used by `agentAction` / `agentLoop`. */
  readonly agent: AgentLoopConfig & {
    readonly outputSchema?: z.ZodType<TOutput>;
  };
  /** Workflow runner used to drive both one-shot and conversational paths. */
  readonly runner: WorkflowRunner;
  /** Optional layered memory. When set, thread state persists across processes. */
  readonly memory?: MemoryStore;
  /**
   * Default tenant key. Optional — multi-tenant gateways leave this unset and
   * pass `namespaceId` per call (`agent.thread(id, { namespaceId })`,
   * `agent.invoke(input, { namespaceId })`) or via `agent.withScope({ namespaceId })`.
   * When `memory` is configured, a `namespaceId` MUST resolve at call time
   * (either from this default, from `withScope()`, or from per-call opts).
   */
  readonly namespaceId?: string;
  /** Default user / persona key. Per-thread override via `ThreadOptions`. */
  readonly resourceId?: string;
  /**
   * ID prefix used for one-shot workflow IDs (`<prefix>-<uuid>`). Default
   * is the agent name. Threads use `threadId` directly as workflow ID, so
   * this prefix only affects `invoke` / `stream` calls.
   */
  readonly invokeIdPrefix?: string;
  /**
   * When `memory` is provided, auto-register a `memory` tool on every
   * thread so the model can write facts and update working memory itself.
   * Default: `true`. Set `false` to opt out (e.g. you're wiring a custom
   * memory tool, or want a stricter agent that can't self-edit).
   *
   * One-shot `invoke` / `stream` paths never auto-register the tool —
   * they're stateless by design.
   */
  readonly autoMemoryTool?: boolean;
  /**
   * Memory consolidator backing `compactThread` / `distillThread`.
   * Optional — when omitted AND `memory` is provided, LocalAgent auto-
   * builds a `DefaultConsolidator` over this agent's `memory` and the
   * LLM picked from `consolidatorLlm ?? agent.llm`. Pass your own to
   * override the prompt / salience / embeddings, or to plug in a
   * non-LLM implementation.
   */
  readonly consolidator?: Consolidator;
  /**
   * LLM used by the auto-built `DefaultConsolidator` for distillation
   * and compaction. Distillation is summarisation work — usually fine
   * to use a cheaper / faster model than the chat path. Common pattern:
   * `agent.llm = anthropic("claude-sonnet-4-6")` for chat,
   * `consolidatorLlm = anthropic("claude-haiku-4-5-20251001")` for
   * distillation. Ignored when `consolidator` is supplied directly.
   */
  readonly consolidatorLlm?: import("../llm-provider.ts").LLMProvider;
  /**
   * Auto-fire `compactThread` after each thread turn whose persisted
   * message count crosses `threshold`. Writes a `ThreadEpisode` covering
   * the trimmed range; the raw messages stay on disk for replay.
   *
   * Defaults to `false` (off). Set `{ threshold: 30 }` for a sensible
   * starting point — start compacting once a thread has ~30 messages.
   *
   * The episodes auto-compact writes are consumed by `resolveContext`
   * on subsequent turns — see `contextBudget` below.
   */
  readonly autoCompact?: AutoCompactConfig | false;
  /**
   * Auto-fire `distillThread` after each thread turn whose configured
   * rule trips. Writes a `ResourceEpisode` (and dedup'd resource-scope
   * facts) so future threads under the same `(namespace, resource)`
   * see this thread's gist via `resolveContext`'s episode-injection
   * budget.
   *
   * Default: `false` (off). Distillation is the cross-thread side of
   * memory consolidation — different from auto-compact (in-thread
   * rollup). Common shapes:
   *
   *     // distill once when a thread reaches a sensible "done" length
   *     autoDistill: { messageThreshold: 6 }
   *
   *     // distill every 10 turns, replacing the previous summary
   *     autoDistill: {
   *       when: ({ turnsSinceLastDistill }) => turnsSinceLastDistill >= 10,
   *       force: true,
   *     }
   *
   * Idle-based and cron-based triggers (e.g. "distill when this thread
   * has been silent for 10 minutes") aren't covered here — they need
   * scheduling outside the agent process. This is the per-turn hook.
   */
  readonly autoDistill?: AutoDistillConfig | false;
  /**
   * Token budget governing how `MemoryStore.resolveContext` assembles
   * each turn's prompt + message tail. When unset, defaults to a
   * permissive budget (16k message tokens, 0 episode tokens — episodes
   * stay disk-only unless explicitly enabled).
   *
   * Set `maxEpisodeTokens > 0` to consume rollups written by
   * `compactThread` / `distillThread`. Set `maxMessageTokens` lower
   * than the model's context window minus output budget to leave room
   * for the assistant's reply.
   *
   * Ignored when `memory` is unset — in-memory threads use the raw
   * message buffer with no cascade.
   */
  readonly contextBudget?: TokenBudget;
}

export interface AutoCompactConfig {
  /**
   * Built-in count gate: fire when the count of UNCOMPACTED messages —
   * those with seq higher than the most recent compaction episode's
   * toSeq — exceeds this number. Tracks "since last compact" so the
   * trigger doesn't re-fire every turn once the thread is past the
   * threshold.
   */
  readonly messageThreshold?: number;
  /**
   * Built-in token gate: fire when uncompacted-message tokens exceed
   * this number. Tokens are estimated as `chars/4` over `content`,
   * matching the `resolveContext` default estimator. More accurate
   * than `messageThreshold` when message lengths vary widely (one
   * giant tool result can blow context even at low message counts).
   *
   * Often more ergonomic to express via `contextLimit` + `compressAt`
   * below — that way you don't have to recompute the threshold when
   * you swap chat models.
   */
  readonly tokenThreshold?: number;
  /**
   * Model context window in tokens. When combined with `compressAt`,
   * the effective token gate becomes `contextLimit * compressAt`.
   * Mirrors the same-named field on `agentLoop`'s `ContextConfig` so
   * operators familiar with the loop pattern recognise the knob.
   *
   * Examples: 200_000 for current Anthropic models. Wins over
   * `tokenThreshold` when both are set.
   */
  readonly contextLimit?: number;
  /**
   * Fraction of `contextLimit` at which to compact. Default 0.70 —
   * leaves the model 30% headroom for the assistant's reply.
   * Same default and meaning as `agent-loop.ContextConfig.compressAt`.
   * Ignored when `contextLimit` isn't set.
   */
  readonly compressAt?: number;
  /**
   * Custom predicate. Mirrors `RetryPolicy.when` from `@promin/core`:
   * receives a signals envelope and returns `true` to fire on this
   * turn. When set, REPLACES both built-in thresholds. Use this for
   * compound rules.
   *
   * Examples:
   *
   *     // count + cooldown — don't compact more than once a minute
   *     when: ({ uncompactedCount, lastCompactedAt }) =>
   *       uncompactedCount > 30 &&
   *       Date.now() - lastCompactedAt > 60_000
   *
   *     // token gate with thread-id allowlist
   *     when: ({ uncompactedTokens, threadKey }) =>
   *       uncompactedTokens > 8_000 &&
   *       threadKey.threadId.startsWith("lr-")
   */
  readonly when?: (signals: AutoCompactSignals) => boolean;
  /** Newest messages to leave unsummarised. Default 10. */
  readonly keepRecent?: number;
  /**
   * Execution mode:
   *   - `"background"` (default) — fire-and-forget AFTER the turn
   *     resolves. The caller never waits for the compact LLM call.
   *     Errors are logged but not surfaced.
   *   - `"blocking"` — await the compact before the turn resolves.
   *     Caller pays the compactionLlm round-trip on every triggered
   *     turn. Use only when a downstream caller expects the episode
   *     to exist before moving on.
   */
  readonly mode?: "background" | "blocking";
}

/** State the auto-compact predicate sees. */
export interface AutoCompactSignals {
  /** Total persisted message count for this thread. */
  readonly totalCount: number;
  /** Count of messages with seq > most recent compact episode's toSeq. */
  readonly uncompactedCount: number;
  /** Estimated token count over all persisted messages (chars/4). */
  readonly totalTokens: number;
  /** Estimated token count over uncompacted messages (chars/4). */
  readonly uncompactedTokens: number;
  /** `createdAt` of the most recent compact episode (0 if none). */
  readonly lastCompactedAt: number;
  readonly threadKey: ThreadKey;
}

export interface AutoDistillConfig {
  /**
   * Built-in count gate: fire when the thread's persisted message
   * count reaches this number. Combined with `force: false` (the
   * default), this means "distill once when the thread reaches N
   * messages" — the consolidator's idempotency dedupes subsequent
   * triggers on the same thread.
   *
   * Common values: 6 (after a couple of turns, the conversation has
   * enough substance to be worth a cross-thread summary), 20 (only
   * substantial threads).
   */
  readonly messageThreshold?: number;
  /**
   * Custom predicate. Mirrors `AutoCompactConfig.when`. When set,
   * REPLACES `messageThreshold`. Use for compound rules like "every
   * N turns, re-distill" (pair with `force: true`):
   *
   *     when: ({ turnsSinceLastDistill }) => turnsSinceLastDistill >= 10,
   *     force: true,
   *
   * Or "the user said something goodbye-y":
   *
   *     when: ({ lastUserMessage }) =>
   *       /^(thanks|bye|goodbye|see ya)\b/i.test(lastUserMessage ?? ""),
   */
  readonly when?: (signals: AutoDistillSignals) => boolean;
  /**
   * Re-distill even when an episode for this thread already exists.
   * Default `false` (idempotent — first qualifying turn writes one
   * episode; subsequent turns are no-ops). Set `true` for "every N
   * turns" patterns where you want each pass to capture the latest
   * additions.
   */
  readonly force?: boolean;
  /**
   * Execution mode (same semantics as `AutoCompactConfig.mode`):
   *   - `"background"` (default) — fire-and-forget after the turn resolves
   *   - `"blocking"` — await before resolving the turn
   */
  readonly mode?: "background" | "blocking";
}

/** State the auto-distill predicate sees. */
export interface AutoDistillSignals {
  /** Total persisted message count for this thread. */
  readonly totalCount: number;
  /**
   * Number of NEW messages since the most recent
   * resource-scope episode whose `sourceThreadId` is this thread.
   * Equals `totalCount` when never distilled.
   */
  readonly turnsSinceLastDistill: number;
  /**
   * `createdAt` of the most recent distill episode for this thread
   * (0 if none).
   */
  readonly lastDistilledAt: number;
  /**
   * Trimmed string content of the thread's most recent user message,
   * or `undefined` when the latest message isn't a user turn.
   * Useful for goodbye-detection heuristics.
   */
  readonly lastUserMessage?: string;
  readonly threadKey: ThreadKey;
}

export class LocalAgent<TOutput = unknown> implements Agent<AgentInput, TOutput> {
  private readonly config: LocalAgentConfig<TOutput>;

  constructor(config: LocalAgentConfig<TOutput>) {
    this.config = config;
    // Note: agentLoop is built fresh per-thread so the auto-injected
    // `memory` tool can bind the right `threadId`. One-shot paths build
    // agentAction inline.
  }

  // --- One-shot ---------------------------------------------------------

  async invoke(input: AgentInput, opts?: AgentInvokeOpts): Promise<AgentRunOutput<TOutput>> {
    const { promise, capture } = this.runOnce(input, opts);
    const result = await promise;
    return resolvedOutput<TOutput>(result, capture.events);
  }

  stream(input: AgentInput, opts?: AgentInvokeOpts): AgentRunOutput<TOutput> {
    const bus = new SessionEventBus();
    const { promise, capture } = this.runOnce(input, opts, bus);
    return liveOutput<TOutput>(promise, bus, capture);
  }

  /**
   * Return a tenant-bound view of this agent. Use in multi-tenant gateways:
   *
   *     const supportBot = new LocalAgent({ agent, runner, memory });  // template
   *     // per request
   *     const scoped = supportBot.withScope({ namespaceId: tenant.id, resourceId: user.id });
   *     const t = await scoped.thread(threadId);
   *
   * `withScope()` is cheap — just constructs a new `LocalAgent` carrying the same
   * underlying config plus the new defaults.
   */
  withScope(scope: { namespaceId?: string; resourceId?: string }): LocalAgent<TOutput> {
    return new LocalAgent<TOutput>({
      ...this.config,
      namespaceId: scope.namespaceId ?? this.config.namespaceId,
      resourceId: scope.resourceId ?? this.config.resourceId,
    });
  }

  // --- Conversational ---------------------------------------------------

  async thread(
    threadId: string,
    opts: ThreadOptions = {},
  ): Promise<AgentThread<AgentInput, TOutput>> {
    const namespaceId = opts.namespaceId ?? this.config.namespaceId;
    const resourceId = opts.resourceId ?? this.config.resourceId;
    if (!namespaceId) {
      throw new Error(
        "LocalAgent.thread: no namespaceId resolved. Pass it on construction, " +
          "via .withScope({ namespaceId }), or in ThreadOptions.",
      );
    }
    const key: ThreadKey = { namespaceId, resourceId, threadId };

    let created = false;
    if (this.config.memory) {
      const existing = await this.config.memory.getThread(key);
      if (!existing && opts.createIfMissing === false) {
        throw new Error(`Thread not found: ${namespaceId}/${threadId}`);
      }
      if (!existing) {
        await this.config.memory.createThread(key, {
          resourceId,
          metadata: opts.metadata,
        });
        created = true;
      }
    } else {
      // No memory store — every thread() call is "new" from the agent's POV.
      created = true;
    }

    // Build a per-thread agentLoop config (with the auto-injected `memory`
    // tool bound to this threadId). Each conversational turn is run as a
    // FRESH agentAction workflow seeded with the persisted message history,
    // so multi-tenant gateways can resolve a new LocalAgent per request
    // without the "workflow already running" conflict that long-lived
    // agentLoop sessions cause. The `loop` field is kept for the
    // structural shape but isn't actually used for thread send/stream.
    const loopConfig: AgentLoopConfig = {
      ...this.config.agent,
      tools: this.buildTools(key),
    };
    const loop = agentLoop(loopConfig);

    return new LocalAgentThread<TOutput>({
      id: threadId,
      key,
      loop,
      loopConfig,
      runner: this.config.runner,
      memory: this.config.memory,
      created,
      autoCompact: this.config.autoCompact === false ? undefined : this.config.autoCompact,
      autoDistill: this.config.autoDistill === false ? undefined : this.config.autoDistill,
      consolidator: () => this.resolveConsolidator(),
      contextBudget: this.config.contextBudget,
    });
  }

  /**
   * Build the per-thread tools map. Auto-injects `memory` when a store is
   * configured and `autoMemoryTool` is not explicitly disabled. User-supplied
   * tools win on key collision (so callers can override with their own).
   */
  private buildTools(key: ThreadKey): AgentLoopConfig["tools"] {
    const userTools = this.config.agent.tools;
    if (!this.config.memory) return userTools;
    if (this.config.autoMemoryTool === false) return userTools;
    if (userTools && "memory" in userTools) return userTools;
    const memoryTool = createLayeredMemoryTool({
      store: this.config.memory,
      namespaceId: key.namespaceId,
      resourceId: key.resourceId,
      threadId: key.threadId,
    });
    return { ...(userTools ?? {}), memory: memoryTool };
  }

  /**
   * Resolve the consolidator: caller-supplied wins; fall back to a
   * lazily-built DefaultConsolidator over this agent's memory + LLM.
   * Memoised so subsequent calls reuse the same instance (so an
   * implementation that holds an internal cache stays warm).
   */
  private _consolidator?: Consolidator;
  private resolveConsolidator(): Consolidator {
    if (this._consolidator) return this._consolidator;
    if (this.config.consolidator) {
      this._consolidator = this.config.consolidator;
      return this._consolidator;
    }
    if (!this.config.memory) {
      throw new Error(
        "LocalAgent.{compact,distill}Thread: no MemoryStore configured. Pass `memory` (and optionally `consolidator`) in LocalAgentConfig.",
      );
    }
    this._consolidator = new DefaultConsolidator({
      store: this.config.memory,
      // Prefer the explicit distill model when set — distillation is
      // summarisation work, often cheaper to run with a faster/smaller
      // model than the chat path.
      llm: this.config.consolidatorLlm ?? this.config.agent.llm,
    });
    return this._consolidator;
  }

  async compactThread(threadId: string, opts?: CompactThreadOptions): Promise<EpisodicRecord> {
    const key = this.threadKeyFor(threadId);
    return this.resolveConsolidator().compactThread(key, opts);
  }

  async distillThread(threadId: string, opts?: DistillThreadOptions): Promise<EpisodicRecord> {
    const key = this.threadKeyFor(threadId);
    return this.resolveConsolidator().distillThread(key, opts);
  }

  private threadKeyFor(threadId: string): ThreadKey {
    const namespaceId = this.config.namespaceId;
    if (!namespaceId) {
      throw new Error(
        "LocalAgent.{compact,distill}Thread: no namespaceId resolved. Call .withScope({ namespaceId, resourceId? }) first.",
      );
    }
    return {
      namespaceId,
      resourceId: this.config.resourceId,
      threadId,
    };
  }

  async listThreads(params?: ListThreadsParams): Promise<ThreadSummary[]> {
    if (!this.config.memory) return [];
    const namespaceId = this.config.namespaceId;
    if (!namespaceId) {
      throw new Error(
        "LocalAgent.listThreads: no namespaceId resolved. Call .withScope({ namespaceId }) first.",
      );
    }
    const summaries = await this.config.memory.listThreads({
      namespaceId,
      resourceId: params?.resourceId ?? this.config.resourceId,
      metadataFilter: params?.metadataFilter,
      q: params?.q,
      limit: params?.limit,
      cursor: params?.cursor,
      order: params?.order,
    });
    return summaries.map((s) => ({
      id: s.threadId,
      resourceId: s.resourceId,
      metadata: s.metadata,
      messageCount: s.messageCount,
      lastActiveAt: s.lastActiveAt,
      createdAt: s.createdAt,
    }));
  }

  // --- internal: one-shot runner ----------------------------------------

  private runOnce(
    input: AgentInput,
    opts?: AgentInvokeOpts,
    bus?: SessionEventBus,
  ): {
    promise: Promise<AgentResult>;
    capture: RunCapture;
  } {
    const capture = makeCapture();
    if (bus) bus.subscribe((e) => recordEvent(capture, e));

    // Re-build agentAction with an injected bus when streaming. We can't
    // mutate the AgentLoopConfig that was used to build `this.loop`, so
    // one-shot uses an inline action.
    const { agent } = this.config;
    // Pass everything except agentLoop-specific knobs into agentAction.
    const actionConfig = toActionConfig(agent, bus);
    const wf = (agentAction as (cfg: AgentActionConfig<TOutput>) => ReturnType<typeof agentAction>)(
      actionConfig as AgentActionConfig<TOutput>,
    );
    const workflowId = `${this.config.invokeIdPrefix ?? agent.name}-${randomUUID().slice(0, 8)}`;

    // Best-effort cancel: when caller's signal aborts, the underlying
    // runner.run currently has no abort hook, so we just record and
    // surface "cancelled" if it fires before completion.
    if (opts?.signal) {
      opts.signal.addEventListener("abort", () => {
        capture.cancelled = true;
      });
    }

    const promise = this.config.runner
      .run({
        workflow: wf,
        workflowId,
        input: { task: input.task, messages: [...(input.messages ?? [])] },
      })
      .then((r) => r as AgentResult);
    return { promise, capture };
  }
}

// ---------------------------------------------------------------------------
// Thread
// ---------------------------------------------------------------------------

interface LocalAgentThreadDeps {
  readonly id: string;
  readonly key: ThreadKey;
  readonly loop: ReturnType<typeof agentLoop>;
  readonly loopConfig: AgentLoopConfig;
  readonly runner: WorkflowRunner;
  readonly memory?: MemoryStore;
  readonly created: boolean;
  /** Auto-compaction config inherited from `LocalAgentConfig.autoCompact`. */
  readonly autoCompact?: AutoCompactConfig;
  /** Auto-distillation config inherited from `LocalAgentConfig.autoDistill`. */
  readonly autoDistill?: AutoDistillConfig;
  /** Lazy accessor — same Consolidator the agent uses for manual calls. */
  readonly consolidator?: () => Consolidator;
  /** Token budget for resolveContext. Default DEFAULT_CONTEXT_BUDGET. */
  readonly contextBudget?: TokenBudget;
}

/**
 * Default budget when LocalAgentConfig.contextBudget is unset:
 *
 *   - `maxMessageTokens: 16_000` — fits comfortably under any modern
 *     chat model's context window even after the system prompt + tools.
 *     Long threads get trimmed from the oldest end.
 *   - `maxEpisodeTokens: 0` — episodes stay disk-only by default. Set
 *     a positive value (e.g. 2_000) to inject resource-scope rollups
 *     into the system prompt so future turns see compacted gist.
 */
const DEFAULT_CONTEXT_BUDGET: TokenBudget = {
  maxMessageTokens: 16_000,
  maxEpisodeTokens: 0,
};

class LocalAgentThread<TOutput = unknown> implements AgentThread<AgentInput, TOutput> {
  readonly id: string;
  readonly resourceId: string | null;
  readonly isNew: boolean;
  private readonly deps: LocalAgentThreadDeps;
  /** In-memory transcript used when no MemoryStore is configured. */
  private inMemoryMessages: Message[] = [];

  constructor(deps: LocalAgentThreadDeps) {
    this.deps = deps;
    this.id = deps.id;
    this.resourceId = deps.key.resourceId ?? null;
    this.isNew = deps.created;
  }

  async send(input: AgentInput, opts?: AgentInvokeOpts): Promise<AgentRunOutput<TOutput>> {
    const { promise, capture } = await this.runTurn(input, opts);
    const result = await promise;
    return resolvedOutput<TOutput>(result, capture.events);
  }

  stream(input: AgentInput, opts?: AgentInvokeOpts): AgentRunOutput<TOutput> {
    const capture = makeCapture();
    const promise = this.runTurn(input, opts, capture).then(({ promise }) => promise);
    return liveOutput<TOutput>(promise, undefined, capture);
  }

  /**
   * Run one conversational turn as a fresh `agentAction` workflow seeded
   * with the thread's persisted message history. Stateless per call —
   * lets the gateway resolve a new `LocalAgent` per request without the
   * "workflow already running" conflict that `agentLoop.session` causes.
   */
  private async runTurn(
    input: AgentInput,
    opts?: AgentInvokeOpts,
    captureIn?: RunCapture,
  ): Promise<{ promise: Promise<AgentResult>; capture: RunCapture }> {
    const capture = captureIn ?? makeCapture();
    const bus = new SessionEventBus();
    bus.subscribe((e) => recordEvent(capture, e));

    if (opts?.signal) {
      opts.signal.addEventListener("abort", () => {
        capture.cancelled = true;
      });
    }

    // resolveContext-driven prompt assembly. The persona (agent's static
    // prompt) goes through agentAction's config.systemPrompt — that puts
    // it at messages[0]. The cascade (cascade-resolved memory) goes in
    // FRONT of the seed as a second system message. Two distinct system
    // blocks → two cache breakpoints at the Anthropic adapter, so the
    // persona prefix stays cached when the cascade changes mid-session.
    const { persona, cascade, messages: history } = await this.loadContext();
    const seed: Message[] = [];
    if (cascade) seed.push({ role: "system", content: cascade });
    seed.push(...history);
    if (input.messages) seed.push(...input.messages);

    const actionConfig = toActionConfig({ ...this.deps.loopConfig, systemPrompt: persona }, bus);
    const wf = (agentAction as (cfg: AgentActionConfig<TOutput>) => ReturnType<typeof agentAction>)(
      actionConfig as AgentActionConfig<TOutput>,
    );
    const workflowId = `${this.deps.key.threadId}-${randomUUID().slice(0, 8)}`;

    const seedLen = seed.length;
    const promise = this.deps.runner
      .run({
        workflow: wf,
        workflowId,
        input: { task: input.task, messages: seed },
      })
      .then(async (raw) => {
        const r = raw as AgentResult;
        await this.persistTurn(seedLen, !!persona, r);
        return r;
      });

    return { promise, capture };
  }

  /**
   * Load the thread's prompt-ready view via `MemoryStore.resolveContext`.
   * Returns the agent's static persona prompt and the cascade-resolved
   * prompt as SEPARATE strings — keeps them in distinct system blocks
   * downstream so each gets its own prompt-cache breakpoint at the
   * adapter layer.
   *
   * Why two blocks instead of one merged string:
   *   - the persona is always stable across a session
   *   - the cascade changes when memory.set / distillThread / facts edit
   *     mid-conversation
   *   - a single merged block invalidates the entire cached prefix on
   *     any cascade change — you pay the full input-token price to
   *     re-cache the persona too. Two blocks let Anthropic's caching
   *     hit on the persona prefix even when the cascade changed.
   *
   * In-memory fallback (no `MemoryStore`) returns the raw message
   * buffer with no cascade.
   */
  private async loadContext(): Promise<{
    persona: string | undefined;
    cascade: string | undefined;
    messages: Message[];
  }> {
    const persona = this.deps.loopConfig.systemPrompt?.trim() || undefined;

    if (!this.deps.memory) {
      return { persona, cascade: undefined, messages: [...this.inMemoryMessages] };
    }

    const budget = this.deps.contextBudget ?? DEFAULT_CONTEXT_BUDGET;
    let resolved: ResolvedContext;
    try {
      resolved = await this.deps.memory.resolveContext(this.deps.key, budget);
    } catch {
      // Thread row may not exist yet on a fresh send — fall back to
      // raw messages so the first turn still runs cleanly.
      const stored = await this.deps.memory.getMessages(this.deps.key).catch(() => []);
      return {
        persona,
        cascade: undefined,
        messages: stored.map((m) => stripStorageMeta(m)),
      };
    }

    // resolveContext always emits the cache-boundary marker, even when
    // every layer is empty. Treat "boundary marker only" as no cascade
    // — there's no point emitting a system block that contains only
    // the marker, and skipping it lets the persona stand alone.
    const cascadeRaw = resolved.systemPrompt.trim();
    const cascade =
      cascadeRaw.length > 0 && cascadeRaw !== "<!-- promin:cache-boundary -->"
        ? cascadeRaw
        : undefined;
    return {
      persona,
      cascade,
      messages: resolved.messages.map((m) => stripStorageMeta(m)),
    };
  }

  /**
   * Persist the new turn — the slice of `result.messages` AFTER the seeded
   * history. `agentAction` returns:
   *
   *     [systemPrompt?, ...seed, userTask, assistant, ...maybeTools, finalAssistant]
   *
   * so the new tail starts at `seedLen + (hasSystemPrompt ? 1 : 0)` and
   * already includes the user message + assistant response (and any tool
   * messages in between).
   */
  private async persistTurn(
    seedLen: number,
    turnHadSystemPrompt: boolean,
    result: AgentResult,
  ): Promise<void> {
    // Use the per-turn flag, not the static loopConfig.systemPrompt:
    // resolveContext can produce a non-empty cascade even when the
    // agent's static prompt is unset. Slicing on the static field
    // would mis-attribute the cascade as the first user message.
    const skip = seedLen + (turnHadSystemPrompt ? 1 : 0);
    const newTail = result.messages.slice(skip).filter((m) => m.role !== "system");

    if (newTail.length === 0) return;

    if (this.deps.memory) {
      if (!(await this.deps.memory.getThread(this.deps.key))) {
        await this.deps.memory.createThread(this.deps.key);
      }
      await this.deps.memory.appendMessages(this.deps.key, newTail);
      await this.maybeAutoCompact();
      await this.maybeAutoDistill();
    } else {
      this.inMemoryMessages.push(...newTail);
    }
  }

  /**
   * Auto-compaction trigger. Reads the persisted state, computes signals
   * (uncompacted-since-last-compact counts in messages and tokens),
   * checks whether the configured rule fires, and dispatches the
   * compactThread call in either background or blocking mode.
   *
   * Phase 1 only writes the episode; resolveContext consumption of
   * those episodes lands in promin-37qn.
   */
  private async maybeAutoCompact(): Promise<void> {
    const cfg = this.deps.autoCompact;
    if (!cfg || !this.deps.memory || !this.deps.consolidator) return;
    if (
      cfg.messageThreshold === undefined &&
      cfg.tokenThreshold === undefined &&
      cfg.contextLimit === undefined &&
      !cfg.when
    ) {
      return; // nothing to gate on
    }

    const messages = await this.deps.memory.getMessages(this.deps.key, { order: "asc" });
    const episodes = await this.deps.memory
      .listThreadEpisodes(this.deps.key, { order: "createdDesc" })
      .catch(() => []);
    const lastCompact = episodes.find(
      (e) => (e.metadata as { kind?: unknown } | null)?.kind === "compact",
    );
    const lastCompactedSeq = lastCompact?.sourceMessageRange?.toSeq ?? 0;
    const lastCompactedAt = lastCompact?.createdAt ?? 0;

    const uncompacted = messages.filter((m) => m.seq > lastCompactedSeq);
    const totalTokens = messages.reduce((s, m) => s + estimateTokens(m), 0);
    const uncompactedTokens = uncompacted.reduce((s, m) => s + estimateTokens(m), 0);

    const signals: AutoCompactSignals = {
      totalCount: messages.length,
      uncompactedCount: uncompacted.length,
      totalTokens,
      uncompactedTokens,
      lastCompactedAt,
      threadKey: this.deps.key,
    };

    // Effective token gate: contextLimit * compressAt wins when set,
    // else tokenThreshold. Matches agent-loop's `compressAt` convention.
    const effectiveTokenThreshold =
      cfg.contextLimit !== undefined
        ? cfg.contextLimit * (cfg.compressAt ?? 0.7)
        : cfg.tokenThreshold;

    const fire = cfg.when
      ? cfg.when(signals)
      : (cfg.messageThreshold !== undefined && uncompacted.length > cfg.messageThreshold) ||
        (effectiveTokenThreshold !== undefined && uncompactedTokens > effectiveTokenThreshold);
    if (!fire) return;

    const consolidator = this.deps.consolidator();
    const opts = { keepRecent: cfg.keepRecent ?? 10 };
    const run = consolidator.compactThread(this.deps.key, opts).catch((err) => {
      // Don't propagate — auto-compact is best-effort. Log so operators
      // can spot a misconfigured consolidator (missing key, malformed
      // LLM output, etc.) without breaking the user-visible turn.
      console.warn(
        `[LocalAgent] autoCompact failed for thread ${this.deps.key.threadId}:`,
        err instanceof Error ? err.message : err,
      );
    });

    if ((cfg.mode ?? "background") === "blocking") {
      await run;
    }
    // else: fire-and-forget; the run's promise carries no value the
    // turn cares about, errors are already logged above.
  }

  /**
   * Auto-distillation trigger. Symmetric with maybeAutoCompact:
   * computes signals (totalCount, turnsSinceLastDistill, lastDistilledAt,
   * lastUserMessage), runs the configured rule, dispatches to the
   * Consolidator. Idempotency is on the consolidator side — a second
   * fire on the same thread without `force: true` returns the existing
   * episode without re-running the LLM.
   */
  private async maybeAutoDistill(): Promise<void> {
    const cfg = this.deps.autoDistill;
    if (!cfg || !this.deps.memory || !this.deps.consolidator) return;
    if (cfg.messageThreshold === undefined && !cfg.when) return;
    // Distillation writes a ResourceEpisode — only meaningful when a
    // resourceId is bound. Without one, silently skip.
    if (!this.deps.key.resourceId) return;

    const messages = await this.deps.memory.getMessages(this.deps.key, { order: "asc" });
    const resourceKey = {
      namespaceId: this.deps.key.namespaceId,
      resourceId: this.deps.key.resourceId,
    };
    const resourceEpisodes = await this.deps.memory
      .listResourceEpisodes(resourceKey, { order: "createdDesc" })
      .catch(() => []);
    const lastDistill = resourceEpisodes.find(
      (e) =>
        e.sourceThreadId === this.deps.key.threadId &&
        (e.metadata as { kind?: unknown } | null)?.kind === "distill",
    );
    const lastDistilledSeq = lastDistill?.sourceMessageRange?.toSeq ?? 0;
    const lastDistilledAt = lastDistill?.createdAt ?? 0;
    const turnsSinceLastDistill = messages.filter((m) => m.seq > lastDistilledSeq).length;

    // Walk back from the end to find the most recent USER message —
    // after persistTurn the assistant turn (and any tool messages)
    // are at the tail; the user's input sits before them.
    let lastUserMessage: string | undefined;
    for (let i = messages.length - 1; i >= 0; i--) {
      const m = messages[i]!;
      if (m.role === "user" && typeof m.content === "string") {
        lastUserMessage = m.content.trim();
        break;
      }
    }

    const signals: AutoDistillSignals = {
      totalCount: messages.length,
      turnsSinceLastDistill,
      lastDistilledAt,
      lastUserMessage,
      threadKey: this.deps.key,
    };

    const fire = cfg.when
      ? cfg.when(signals)
      : cfg.messageThreshold !== undefined && messages.length >= cfg.messageThreshold;
    if (!fire) return;

    const consolidator = this.deps.consolidator();
    const run = consolidator
      .distillThread(this.deps.key, { force: cfg.force ?? false })
      .catch((err) => {
        console.warn(
          `[LocalAgent] autoDistill failed for thread ${this.deps.key.threadId}:`,
          err instanceof Error ? err.message : err,
        );
      });

    if ((cfg.mode ?? "background") === "blocking") {
      await run;
    }
  }

  async messages(range?: MessageRange): Promise<Message[]> {
    if (this.deps.memory) {
      const stored = await this.deps.memory.getMessages(this.deps.key, range);
      return stored.map((m) => stripStorageMeta(m));
    }
    return applyRange(this.inMemoryMessages, range);
  }

  async workingMemory(): Promise<string | null> {
    if (!this.deps.memory) return null;
    const row = await this.deps.memory.getThread(this.deps.key);
    return row?.workingMemory ?? null;
  }

  async setWorkingMemory(markdown: string | null): Promise<void> {
    if (!this.deps.memory) return;
    if (!(await this.deps.memory.getThread(this.deps.key))) {
      await this.deps.memory.createThread(this.deps.key);
    }
    await this.deps.memory.setThreadWorking(this.deps.key, markdown);
  }

  async setMetadata(metadata: Readonly<Record<string, unknown>>): Promise<void> {
    if (!this.deps.memory) return;
    if (!(await this.deps.memory.getThread(this.deps.key))) {
      await this.deps.memory.createThread(this.deps.key);
    }
    await this.deps.memory.setThreadMetadata(this.deps.key, metadata);
  }

  async delete(): Promise<void> {
    if (this.deps.memory) {
      await this.deps.memory.deleteThread(this.deps.key);
    }
    this.inMemoryMessages = [];
  }
}

// ---------------------------------------------------------------------------
// Helpers — capture, conversion, output assembly
// ---------------------------------------------------------------------------

interface RunCapture {
  readonly queue: ChunkQueue;
  readonly events: AgentEvent[];
  readonly toolCalls: ToolCall[];
  readonly toolResults: ToolResult[];
  readonly steps: Step[];
  cancelled: boolean;
  finishReason: FinishReason | null;
  usage: UsageStats;
  llmStartByStep: Map<number, number>;
  toolStartByCallId: Map<string, { name: string; ts: number; step: number }>;
}

function makeCapture(): RunCapture {
  return {
    queue: new ChunkQueue(),
    events: [],
    toolCalls: [],
    toolResults: [],
    steps: [],
    cancelled: false,
    finishReason: null,
    usage: { inputTokens: 0, outputTokens: 0 },
    llmStartByStep: new Map(),
    toolStartByCallId: new Map(),
  };
}

function recordEvent(capture: RunCapture, e: SessionEvent): void {
  switch (e.type) {
    case "token.delta":
      capture.queue.push(e.delta);
      capture.events.push({ type: "text-delta", delta: e.delta });
      break;
    case "tool.start": {
      capture.toolStartByCallId.set(callKeyForStart(e), {
        name: e.name,
        ts: e.ts,
        step: e.step,
      });
      break;
    }
    case "tool.end": {
      // We don't have the tool call id on this event in the existing bus
      // shape — best-effort: record a step entry without input/output.
      const step: Step = {
        type: "tool",
        index: capture.steps.length,
        name: e.name,
        toolCallId: "",
        input: undefined,
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
      });
      break;
    case "step_limit.hit":
      capture.finishReason = "max_steps";
      break;
    case "turn.end":
      capture.usage = sumUsage(capture.usage, e.tokens);
      if (capture.finishReason === null) capture.finishReason = "stop";
      capture.events.push({ type: "finish", reason: capture.finishReason, usage: capture.usage });
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

function callKeyForStart(e: SessionEvent & { type: "tool.start" }): string {
  // No call id on tool.start either; key by (turn, step) instead.
  return `${e.turn}:${e.step}`;
}

function deriveFinishReason(capture: RunCapture): FinishReason {
  if (capture.cancelled) return "cancelled";
  return capture.finishReason ?? "stop";
}

/** Build an AgentRunOutput where every promise is already resolved. */
function resolvedOutput<TOutput>(
  result: AgentResult,
  events: AgentEvent[],
): AgentRunOutput<TOutput> {
  const toolCalls = extractToolCalls(result.messages);
  const toolResults = extractToolResults(result.messages);
  const usage: UsageStats = {
    inputTokens: result.usage?.inputTokens ?? 0,
    outputTokens: result.usage?.outputTokens ?? 0,
    cacheReadTokens: result.usage?.cacheReadTokens,
    cacheWriteTokens: result.usage?.cacheWriteTokens,
  };
  return {
    textStream: emptyAsyncIterable<string>(),
    fullStream: arrayAsAsyncIterable(events),
    text: Promise.resolve(result.answer),
    output: Promise.resolve(result.output as TOutput | undefined),
    toolCalls: Promise.resolve(toolCalls),
    toolResults: Promise.resolve(toolResults),
    steps: Promise.resolve([]),
    usage: Promise.resolve(usage),
    finishReason: Promise.resolve("stop"),
    messages: Promise.resolve([...result.messages]),
    cancel: async () => {},
  };
}

function liveOutput<TOutput>(
  promise: Promise<AgentResult>,
  _bus: SessionEventBus | undefined,
  capture: RunCapture,
): AgentRunOutput<TOutput> {
  // Close the queue when the underlying run completes.
  const settled = promise.finally(() => capture.queue.close());

  return {
    textStream: capture.queue,
    fullStream: makeFullStream(capture),
    text: settled.then((r) => r.answer),
    output: settled.then((r) => r.output as TOutput | undefined),
    toolCalls: settled.then((r) => extractToolCalls(r.messages)),
    toolResults: settled.then((r) => extractToolResults(r.messages)),
    steps: settled.then(() => capture.steps.slice()),
    usage: settled.then((r) => ({
      inputTokens: r.usage?.inputTokens ?? 0,
      outputTokens: r.usage?.outputTokens ?? 0,
      cacheReadTokens: r.usage?.cacheReadTokens,
      cacheWriteTokens: r.usage?.cacheWriteTokens,
    })),
    finishReason: settled.then(() => deriveFinishReason(capture)),
    messages: settled.then((r) => [...r.messages]),
    cancel: async () => {
      capture.cancelled = true;
    },
  };
}

/** Build an agentAction config from an agentLoop config. */
function toActionConfig(loopConfig: AgentLoopConfig, bus?: SessionEventBus): AgentActionConfig {
  return {
    name: loopConfig.name,
    llm: loopConfig.llm,
    tools: loopConfig.tools,
    toolRegistry: loopConfig.toolRegistry,
    autoApprove: loopConfig.autoApprove,
    systemPrompt: loopConfig.systemPrompt,
    maxSteps: loopConfig.maxStepsPerTurn,
    rateLimiter: loopConfig.rateLimiter,
    clock: loopConfig.clock,
    processors: loopConfig.processors,
    bus,
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

function extractToolResults(messages: ReadonlyArray<Message>): ToolResult[] {
  const out: ToolResult[] = [];
  for (const m of messages) {
    if (m.role === "tool") {
      out.push({
        toolCallId: m.toolCallId,
        name: "",
        content: m.content,
        failed: false,
      });
    }
  }
  return out;
}

function applyRange(messages: ReadonlyArray<Message>, range?: MessageRange): Message[] {
  // Without storage seq numbers, range filtering on in-memory mode is positional.
  if (!range) return [...messages];
  let out = [...messages];
  if (range.fromSeq !== undefined) out = out.slice(range.fromSeq);
  if (range.toSeq !== undefined) out = out.slice(0, range.toSeq + 1);
  if (range.order === "desc") out = out.reverse();
  if (range.limit !== undefined) out = out.slice(0, range.limit);
  return out;
}

/**
 * chars/4 token estimator — same heuristic resolveContext's default
 * uses. Matches the worst-case "ASCII text" approximation that's good
 * enough for compaction-trigger decisions; an over-estimate is fine
 * (we'd compact slightly early).
 */
function estimateTokens(m: { content?: string | null }): number {
  const len = typeof m.content === "string" ? m.content.length : 0;
  return Math.ceil(len / 4);
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

function stripStorageMeta(m: Message & { seq?: number; createdAt?: number }): Message {
  const { seq: _seq, createdAt: _createdAt, ...rest } = m as Record<string, unknown> & Message;
  return rest as Message;
}

// ---- ChunkQueue (single-consumer async queue, ported from agent-loop) ----

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

  [Symbol.asyncIterator](): AsyncIterator<string> {
    return {
      next: async () => {
        while (this._buf.length === 0 && !this._closed) {
          await new Promise<void>((resolve) => {
            this._notify = resolve;
          });
        }
        if (this._buf.length > 0) {
          return { value: this._buf.shift()!, done: false };
        }
        return { value: undefined as never, done: true };
      },
    };
  }
}

async function* emptyAsyncIterable<T>(): AsyncIterable<T> {
  // intentionally empty
}

async function* arrayAsAsyncIterable<T>(arr: ReadonlyArray<T>): AsyncIterable<T> {
  for (const x of arr) yield x;
}

function makeFullStream(capture: RunCapture): AsyncIterable<AgentEvent> {
  // Snapshot events as they accumulate. Single-consumer: re-iterating yields
  // the events captured up to that point (no push-based hooks needed for the
  // first cut — keeps the surface honest about what we actually capture).
  return {
    [Symbol.asyncIterator]() {
      let i = 0;
      return {
        next: async () => {
          while (i >= capture.events.length && !capture.queue["_closed"]) {
            await new Promise<void>((r) => setTimeout(r, 5));
          }
          if (i < capture.events.length) {
            return { value: capture.events[i++]!, done: false };
          }
          return { value: undefined as never, done: true };
        },
      };
    },
  };
}

// Re-export `ThreadRow` so callers can introspect downcast types if needed.
export type { ThreadRow };
