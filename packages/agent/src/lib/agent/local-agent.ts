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
// `LocalAgent` per request. Each turn is its own workflow, so no session
// workflow id is reserved across requests — the thread's "memory" lives in
// the `MemoryStore`, not in an in-process session map.
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
import {
  agentAction,
  type AgentActionConfig,
  type AgentEngineConfig,
  type AgentResult,
} from "../agent-action.ts";
import type { ToolRegistry } from "../tool-registry.ts";
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
import { createCallAgentTool, createFindAgentTool } from "../network/runtime.ts";
import { createRetrieverTool, formatRetrievedChunks } from "../rag/retriever-tool.ts";
import {
  fenceReferenceData,
  isReferenceDataMessage,
  referenceDataMessage,
} from "../reference-data.ts";
import { retrieverToolName } from "../rag/retriever-tool-name.ts";
import type { Retriever, RetrieverFilter } from "../rag/types.ts";
import type { AgentKnowledgeMode } from "../registry/types.ts";
import { frameTask } from "./frame-task.ts";
import { SystemWallClock, completeSignal, hasCapability } from "@promin/workflow";
import { ThreadNotFoundError } from "./errors.ts";
import { THREAD_AGENT_METADATA_KEY } from "./types.ts";
import type { AgentTool, ApprovalDecision } from "../tool.ts";
import { filterToolsByCapability } from "../tool.ts";
import { resolveLocalAgent } from "../registry/resolve-local-agent.ts";
import type { ResolveLocalAgentDeps } from "../registry/resolve-local-agent.ts";
import type { AgentRegistry } from "../registry/types.ts";
import type {
  Agent,
  AgentEvent,
  AgentInput,
  AgentInputSource,
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
} from "./types.ts";

export interface LocalAgentConfig<TOutput = any> {
  /**
   * Engine settings every turn runs with — the `agentAction` config minus
   * the per-run wiring (`bus`, `scope`, `signal`), which LocalAgent
   * supplies. Every field is honored on every path (invoke, stream,
   * thread send / stream / resume).
   */
  readonly agent: AgentEngineConfig<TOutput> & {
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
   * Owner the `resourceId` belongs to, when the caller resolved it from an
   * agent instance. Set by the authorizing gateway through
   * `withScope({ ownerId })`; `callAgent` uses it to run a peer under the
   * same owner's instance of that peer. Never derived from `resourceId`.
   */
  readonly ownerId?: string;
  /**
   * Agent capabilities, from the recipe's `metadata.capabilities`.
   * Gates elevated tools: an elevated tool whose `requires` capability
   * is not listed here is filtered out of the LLM's tool list (see
   * `filterToolsByCapability`). Default `[]` — no capabilities granted,
   * so no elevated tools are exposed.
   */
  readonly capabilities?: ReadonlyArray<string>;
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
   * override the prompt / salience, or to plug in a
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
  /**
   * Optional agents-network wiring. When set together with a recipe-side
   * `backend.network` declaration, `findAgent` and `callAgent` are
   * auto-attached to every turn so the agent can discover + delegate to
   * peers. Without it, the recipe's `network` field is ignored.
   *
   * The shape mirrors `NetworkRuntimeDeps` from
   * `packages/agent/src/lib/network/runtime.ts` plus this agent's
   * recipe (used for the policy check) — the resolver fills both in.
   */
  readonly network?: {
    readonly deps: import("../network/runtime.ts").NetworkRuntimeDeps;
    readonly recipe: import("../registry/types.ts").RegisteredAgent;
  };
  /**
   * RAG bindings, keyed by binding id. A bare `Retriever` is exposed as a
   * search tool named `retrieverToolName(id)`; an `AgentRetrieverBinding`
   * can instead (or also) inject retrieved context before every turn —
   * see `AgentRetrieverBinding.mode`. A tool never shadows a same-named
   * tool already on the agent.
   */
  readonly retrievers?: Readonly<Record<string, Retriever | AgentRetrieverBinding>>;
  /**
   * Recipe id surfaced as `ctx.scope.agentId` to scope-aware tools
   * (the durable scheduler stamps this in schedule metadata so
   * `dispatchAgentSchedule` can look the recipe up later). Decoupled
   * from `agent.name` because hosts often want a recipe-id namespace
   * ("writer-v3") that's more stable than the workflow name. Defaults
   * to `agent.name`.
   */
  readonly agentId?: string;
}

/** A retriever plus how it is exposed to the model. */
export interface AgentRetrieverBinding {
  readonly retriever: Retriever;
  /** LLM-facing tool name. Default `retrieverToolName(id)`. */
  readonly name?: string;
  /** LLM-facing tool description. */
  readonly description?: string;
  /** Results per search. Default: the retriever's own default. */
  readonly topK?: number;
  /** Include source metadata in tool output. Default true. */
  readonly includeSources?: boolean;
  /** Include relevance scores in the model-visible tool output. Default false. */
  readonly includeScores?: boolean;
  /** Truncate each returned chunk to this many characters. */
  readonly maxChunkCharacters?: number;
  /** Filter applied to every search (tool calls and context injection). */
  readonly filter?: RetrieverFilter;
  /**
   * `tool` (default) exposes a search tool; `context` searches with the
   * turn's task before every turn and injects the hits as a fenced
   * reference-data message (user role, never system — the chunks are
   * untrusted); `tool-and-context` does both.
   */
  readonly mode?: AgentKnowledgeMode;
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
   *
   * Examples: 200_000 for current Anthropic models. Wins over
   * `tokenThreshold` when both are set.
   */
  readonly contextLimit?: number;
  /**
   * Fraction of `contextLimit` at which to compact. Default 0.70 —
   * leaves the model 30% headroom for the assistant's reply.
   * Ignored when `contextLimit` isn't set.
   */
  readonly compressAt?: number;
  /**
   * Custom predicate. Mirrors `RetryPolicy.when` from `@promin/workflow`:
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
   * Built-in message-count gate: fire when the thread's persisted
   * message count reaches this number. Cheap to compute — best for
   * simple "distill once after N messages" patterns.
   *
   * Common values: 6 (after a couple of turns, conversation has
   * enough substance), 20 (only substantial threads).
   *
   * Multiple gates compose with OR semantics — fires when ANY of
   * `messageThreshold`, `tokenThreshold`, `intervalMs` trips.
   */
  readonly messageThreshold?: number;
  /**
   * Built-in token-count gate: fire when the SUM of estimated tokens
   * across NEW messages (since last distill) exceeds this value.
   * Tokens estimated as `chars/4`, matching autoCompact's heuristic.
   *
   * More cost-aware than `messageThreshold` — three short messages
   * vs three giant ones cost very different amounts to distill.
   * Common values: 4_000 (roughly one Haiku context's worth of new
   * material), 16_000 (substantial accumulated content).
   */
  readonly tokenThreshold?: number;
  /**
   * Built-in time-since-last-distill gate (milliseconds): fire when
   * the elapsed time since the last distill episode exceeds this.
   * Distinct from message/token gates — useful for low-volume
   * threads where 6 messages might span a week, OR for periodic
   * re-distillation patterns regardless of message volume.
   *
   * Combine with `force: true` to re-distill on every interval tick.
   */
  readonly intervalMs?: number;
  /**
   * Custom predicate. When set, REPLACES the built-in gates entirely
   * — the predicate has full control. Use for compound rules:
   *
   *     when: ({ turnsSinceLastDistill, lastUserMessage }) =>
   *       turnsSinceLastDistill >= 10 ||
   *       /^(thanks|bye|goodbye|see ya)\b/i.test(lastUserMessage ?? ""),
   */
  readonly when?: (signals: AutoDistillSignals) => boolean;
  /**
   * Re-distill even when an episode for this thread already exists.
   * Default `false` (idempotent — first qualifying turn writes one
   * episode; subsequent turns are no-ops). Set `true` for periodic
   * re-distillation patterns (works well with `intervalMs` or
   * `tokenThreshold` past initial fire).
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
   * Estimated token count (chars/4) summed across the NEW messages
   * since the most recent distill episode. Equals total-thread-tokens
   * when never distilled.
   */
  readonly tokensSinceLastDistill: number;
  /**
   * Milliseconds elapsed since the most recent distill episode.
   * `Infinity` when never distilled (so `intervalMs` always trips).
   */
  readonly msSinceLastDistill: number;
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
  }

  /**
   * Materialize a `LocalAgent` straight from an `AgentRegistry` — fetch
   * the recipe by id, then resolve it against `deps`. Collapses the
   * `registry.get(id)` + `resolveLocalAgent(recipe, deps)` two-step that
   * operators otherwise have to remember into a single call.
   *
   * Throws if no agent is registered under `id` (or `id` + `version`).
   *
   * @example
   * const agent = await LocalAgent.fromRegistry(registry, "support-bot", deps);
   * await agent.withScope({ namespaceId, resourceId }).invoke({ task });
   */
  static async fromRegistry(
    registry: AgentRegistry,
    id: string,
    deps: ResolveLocalAgentDeps,
    opts?: { version?: string },
  ): Promise<LocalAgent> {
    const recipe = await registry.get(id, opts?.version);
    if (!recipe) {
      const v = opts?.version ? ` version "${opts.version}"` : "";
      throw new Error(`LocalAgent.fromRegistry: no agent "${id}"${v} in the registry.`);
    }
    return resolveLocalAgent(recipe, deps);
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
  withScope(scope: AgentScope): LocalAgent<TOutput> {
    const resourceId = scope.resourceId ?? this.config.resourceId;
    // The owner travels with the resource it was resolved for: a new
    // resource without an owner drops the old owner.
    const ownerId =
      scope.ownerId ?? (scope.resourceId === undefined ? this.config.ownerId : undefined);
    const { ownerId: _previousOwner, ...rest } = this.config;
    return new LocalAgent<TOutput>({
      ...rest,
      namespaceId: scope.namespaceId ?? this.config.namespaceId,
      resourceId,
      ...(ownerId !== undefined && { ownerId }),
    });
  }

  /** Recipe id this agent runs as (`config.agentId`, else the loop name). */
  private agentId(): string {
    return this.config.agentId ?? this.config.agent.name;
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
        throw new ThreadNotFoundError({ namespaceId, threadId });
      }
      // A thread belongs to the resource that created it. A caller scoped
      // to a different resource must not read or extend it: treat the
      // thread as absent for them (same error as a missing thread, so ids
      // of other resources' threads can't be probed). An unscoped caller
      // (no resourceId) keeps namespace-wide access.
      if (existing && resourceId !== undefined && existing.resourceId !== resourceId) {
        throw new ThreadNotFoundError({ namespaceId, threadId });
      }
      if (!existing) {
        // Stamp the owning agent so a gateway that scopes threads by agent
        // reaches this one however it was created.
        await this.config.memory.createThread(key, {
          resourceId,
          metadata: { ...opts.metadata, [THREAD_AGENT_METADATA_KEY]: this.agentId() },
        });
        created = true;
      }
    } else {
      // No memory store — every thread() call is "new" from the agent's POV.
      created = true;
    }

    // Per-thread engine config (with the auto-injected `memory` tool bound
    // to this threadId). Each conversational turn runs as a FRESH
    // agentAction workflow seeded with the persisted message history, so
    // multi-tenant gateways can resolve a new LocalAgent per request.
    const engine: AgentEngineConfig<TOutput> = {
      ...this.config.agent,
      ...this.buildTools({
        namespaceId: key.namespaceId,
        resourceId: key.resourceId,
        threadId: key.threadId,
      }),
    };

    return new LocalAgentThread<TOutput>({
      id: threadId,
      key,
      engine,
      runner: this.config.runner,
      memory: this.config.memory,
      created,
      agentId: this.agentId(),
      autoCompact: this.config.autoCompact === false ? undefined : this.config.autoCompact,
      autoDistill: this.config.autoDistill === false ? undefined : this.config.autoDistill,
      consolidator: () => this.resolveConsolidator(),
      contextBudget: this.config.contextBudget,
      ...(this.config.retrievers !== undefined && { retrievers: this.config.retrievers }),
    });
  }

  /**
   * Build the per-turn tool source. Used by both the conversational
   * (`thread.send`) path with a full ThreadKey AND the one-shot
   * (`invoke` / `stream`) path with just the scope. The memory tool
   * attaches only when threadId is known; the network tools attach
   * whenever the recipe opted in and a namespaceId is resolved. Host
   * tools win on key collision.
   *
   * An agent configured with a `toolRegistry` keeps a live registry: the
   * auto-attached tools are merged into every `getTools()` read, so
   * hot-reloaded tools still show up on the next step.
   */
  private buildTools(scope: { namespaceId?: string; resourceId?: string; threadId?: string }): {
    tools?: Record<string, AgentTool<any, any>>;
    toolRegistry?: ToolRegistry;
  } {
    const registry = this.config.agent.toolRegistry;
    if (registry) {
      return {
        tools: undefined,
        toolRegistry: {
          getTools: () => this.withAutoTools({ scope, userTools: registry.getTools() }),
          close: () => registry.close(),
        },
      };
    }
    return { tools: this.withAutoTools({ scope, userTools: this.config.agent.tools ?? {} }) };
  }

  private withAutoTools(params: {
    scope: { namespaceId?: string; resourceId?: string; threadId?: string };
    userTools: Record<string, AgentTool<any, any>>;
  }): Record<string, AgentTool<any, any>> {
    const { scope, userTools } = params;
    let tools: Record<string, AgentTool<any, any>> = userTools;

    // Auto-attached memory tool — needs a threadId, only the thread path
    // has one. One-shot invoke is stateless by design.
    if (
      scope.threadId &&
      scope.namespaceId &&
      this.config.memory &&
      this.config.autoMemoryTool !== false &&
      !("memory" in userTools)
    ) {
      const memoryTool = createLayeredMemoryTool({
        store: this.config.memory,
        namespaceId: scope.namespaceId,
        resourceId: scope.resourceId,
        threadId: scope.threadId,
        agentId: this.agentId(),
      });
      tools = { ...tools, memory: memoryTool };
    }

    // Auto-attached network tools — recipe opt-in + host deps + a
    // resolved namespace. Built per-turn because they close over the
    // current scope.
    if (this.config.network && scope.namespaceId) {
      const recipe = this.config.network.recipe;
      const networkRecipe = recipe.backend.type === "local" ? recipe.backend.network : undefined;
      if (networkRecipe) {
        // The owner is the one the gateway bound with `withScope`, and only
        // while the turn runs at the resource it was bound for.
        const ownerId =
          this.config.ownerId !== undefined && scope.resourceId === this.config.resourceId
            ? this.config.ownerId
            : undefined;
        const callerScope: import("../network/runtime.ts").NetworkCallerScope = {
          namespaceId: scope.namespaceId,
          ...(scope.resourceId !== undefined && { resourceId: scope.resourceId }),
          ...(ownerId !== undefined && { ownerId }),
        };
        const findAgent = createFindAgentTool({
          deps: this.config.network.deps,
          scope: callerScope,
          callerRecipe: recipe,
          network: networkRecipe,
        });
        const callAgent = createCallAgentTool({
          deps: this.config.network.deps,
          scope: callerScope,
          callerRecipe: recipe,
          network: networkRecipe,
        });
        // Don't shadow user-supplied tools of the same name — host wins.
        const merged = { ...tools };
        if (!("findAgent" in merged)) merged.findAgent = findAgent;
        if (!("callAgent" in merged)) merged.callAgent = callAgent;
        tools = merged;
      }
    }

    // Retriever tools — one search tool per binding whose mode includes
    // `tool`. Host / recipe tools of the same name win.
    if (this.config.retrievers) {
      const merged = { ...tools };
      for (const binding of retrieverBindings(this.config.retrievers)) {
        if (binding.mode === "context") continue;
        if (binding.toolName in merged) continue;
        merged[binding.toolName] = createRetrieverTool({
          retriever: binding.retriever,
          name: binding.toolName,
          ...(binding.description !== undefined && { description: binding.description }),
          ...(binding.topK !== undefined && { topK: binding.topK }),
          ...(binding.includeSources !== undefined && { includeSources: binding.includeSources }),
          ...(binding.includeScores !== undefined && { includeScores: binding.includeScores }),
          ...(binding.maxChunkCharacters !== undefined && {
            maxChunkCharacters: binding.maxChunkCharacters,
          }),
          ...(binding.filter !== undefined && { filter: binding.filter }),
        });
      }
      tools = merged;
    }

    // Capability gate — an elevated tool is exposed only when the
    // recipe's `capabilities` grant its `requires`. Applied last so the
    // auto-attached memory / network tools are gated too (they aren't
    // elevated today, so they pass through unchanged).
    return filterToolsByCapability(tools, this.config.capabilities ?? []);
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
      archived: params?.archived,
    });
    return summaries.map((s) => ({
      id: s.threadId,
      resourceId: s.resourceId,
      title: s.title,
      metadata: s.metadata,
      archivedAt: s.archivedAt,
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
    const capture = makeCapture(opts?.signal);
    const runBus = bus ?? new SessionEventBus();
    runBus.subscribe((e) => recordEvent(capture, e));

    // Tools come from buildTools so the network tools auto-attach for
    // one-shot invoke too. No threadId here (one-shot is stateless), so
    // the layered memory tool stays off for invoke.
    const namespaceId = opts?.namespaceId ?? this.config.namespaceId;
    const resourceId = this.config.resourceId;
    const engine: AgentEngineConfig<TOutput> = {
      ...this.config.agent,
      ...this.buildTools({
        ...(namespaceId !== undefined && { namespaceId }),
        ...(resourceId !== undefined && { resourceId }),
      }),
    };
    const wf = buildTurnWorkflow<TOutput>({
      engine,
      bus: runBus,
      scope: {
        ...(namespaceId !== undefined && { namespaceId }),
        ...(resourceId !== undefined && { resourceId }),
        agentId: this.agentId(),
      },
      signal: capture.abort.signal,
    });
    // Honor caller-provided `runId` so a schedule tick (or any deterministic
    // caller) lands on the same workflow row across retries. Falls back to
    // a fresh random id for plain interactive invocations.
    const workflowId =
      opts?.runId ?? `${this.config.invokeIdPrefix ?? engine.name}-${randomUUID().slice(0, 8)}`;

    // Context-mode retrievers search with the task before the run starts;
    // the hits ride in the workflow input as a fenced reference-data
    // message right before the task, so a replay sees the same context
    // without searching again.
    const promise = (async () => {
      const knowledge = await buildRetrieverContext({
        retrievers: this.config.retrievers,
        query: input.task,
      });
      const messages: Message[] = [...(input.messages ?? [])];
      if (knowledge) messages.push(referenceDataMessage(knowledge));
      const r = await this.config.runner.run({
        workflow: wf,
        workflowId,
        input: { task: frameTask(input), messages },
      });
      return r as AgentResult;
    })();
    return { promise, capture };
  }
}

// ---------------------------------------------------------------------------
// Thread
// ---------------------------------------------------------------------------

interface LocalAgentThreadDeps<TOutput> {
  readonly id: string;
  readonly key: ThreadKey;
  /** Engine config for this thread's turns, with the thread's tools. */
  readonly engine: AgentEngineConfig<TOutput>;
  readonly runner: WorkflowRunner;
  readonly memory?: MemoryStore;
  readonly created: boolean;
  /** Recipe id surfaced as `ctx.scope.agentId` to scope-aware tools. */
  readonly agentId: string;
  /** Auto-compaction config inherited from `LocalAgentConfig.autoCompact`. */
  readonly autoCompact?: AutoCompactConfig;
  /** Auto-distillation config inherited from `LocalAgentConfig.autoDistill`. */
  readonly autoDistill?: AutoDistillConfig;
  /** Lazy accessor — same Consolidator the agent uses for manual calls. */
  readonly consolidator?: () => Consolidator;
  /** Token budget for resolveContext. Default DEFAULT_CONTEXT_BUDGET. */
  readonly contextBudget?: TokenBudget;
  /** Retriever bindings; context-mode ones are searched before every turn. */
  readonly retrievers?: LocalAgentConfig["retrievers"];
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
  private readonly deps: LocalAgentThreadDeps<TOutput>;
  /** In-memory transcript used when no MemoryStore is configured. */
  private inMemoryMessages: Message[] = [];

  constructor(deps: LocalAgentThreadDeps<TOutput>) {
    this.deps = deps;
    this.id = deps.id;
    this.resourceId = deps.key.resourceId ?? null;
    this.isNew = deps.created;
  }

  /** ToolScope surfaced via `ctx.scope` to scope-aware tools per call. */
  private toolScope(): import("../tool.ts").ToolScope {
    return {
      namespaceId: this.deps.key.namespaceId,
      ...(this.deps.key.resourceId !== undefined && { resourceId: this.deps.key.resourceId }),
      threadId: this.deps.key.threadId,
      agentId: this.deps.agentId,
    };
  }

  async send(input: AgentInput, opts?: AgentInvokeOpts): Promise<AgentRunOutput<TOutput>> {
    const { promise, capture } = await this.runTurn(input, opts);
    const result = await promise;
    return resolvedOutput<TOutput>(result, capture.events);
  }

  stream(input: AgentInput, opts?: AgentInvokeOpts): AgentRunOutput<TOutput> {
    const capture = makeCapture(opts?.signal);
    const promise = this.runTurn(input, opts, capture).then(({ promise }) => promise);
    return liveOutput<TOutput>(promise, undefined, capture);
  }

  async resume(
    callId: string,
    decision: ApprovalDecision,
    opts?: AgentInvokeOpts,
  ): Promise<AgentRunOutput<TOutput>> {
    const { promise, capture } = await this.resumeTurn(callId, decision, opts);
    const result = await promise;
    return resolvedOutput<TOutput>(result, capture.events);
  }

  resumeStream(
    callId: string,
    decision: ApprovalDecision,
    opts?: AgentInvokeOpts,
  ): AgentRunOutput<TOutput> {
    const capture = makeCapture(opts?.signal);
    const promise = this.resumeTurn(callId, decision, opts, capture).then(({ promise }) => promise);
    return liveOutput<TOutput>(promise, undefined, capture);
  }

  async hasPendingApproval(callId: string): Promise<boolean> {
    return (await this.findSuspendedApproval(`approve:${callId}`)) !== null;
  }

  /**
   * Find the suspended workflow for this thread waiting on `signalName`.
   * Each chat turn creates its own workflow id prefixed by threadId, so
   * the search is scoped to this thread alone.
   */
  private async findSuspendedApproval(signalName: string) {
    const suspended = await this.deps.runner.storage.listWorkflows({
      status: "suspended",
      limit: 100,
    });
    return (
      suspended.find(
        (w) =>
          w.workflowId.startsWith(`${this.deps.key.threadId}-`) &&
          Object.values(w.steps).some(
            (s) => s.status === "waiting_for_signal" && s.signalName === signalName,
          ),
      ) ?? null
    );
  }

  /**
   * Continue a turn that suspended on `approve:<callId>`. Finds the
   * suspended workflow for this thread, delivers the approval decision
   * to its journal, then re-runs the workflow definition under the
   * SAME workflowId — the runner replays from the journal, consumes
   * the now-completed signal, and continues to completion (or the
   * next suspension).
   *
   * The rebuilt workflow definition must be replay-equivalent to the
   * original turn's. agentAction's control flow reads only journaled
   * values (tool-set snapshots included), so a recipe edited mid-turn
   * changes later steps only.
   */
  private async resumeTurn(
    callId: string,
    decision: ApprovalDecision,
    opts?: AgentInvokeOpts,
    captureIn?: RunCapture,
  ): Promise<{ promise: Promise<AgentResult>; capture: RunCapture }> {
    const storage = this.deps.runner.storage;
    if (!hasCapability(storage, "journal")) {
      throw new Error(
        "LocalAgentThread.resume: storage doesn't support journaled signal delivery. " +
          "Approvals require a journaled storage backend (in-memory, sqlite, postgres).",
      );
    }

    const signalName = `approve:${callId}`;
    const wf = await this.findSuspendedApproval(signalName);
    if (!wf) {
      throw new Error(
        `No suspended approval "${signalName}" found for thread "${this.deps.key.threadId}".`,
      );
    }
    const stepName = Object.values(wf.steps).find(
      (s) => s.status === "waiting_for_signal" && s.signalName === signalName,
    )!.stepName;

    const capture = captureIn ?? makeCapture(opts?.signal);
    const bus = new SessionEventBus();
    bus.subscribe((e) => recordEvent(capture, e));

    // Rebuild the workflow definition. The journal drives replay so we
    // don't need to recompute the seed messages — the runner reads input
    // from storage when resuming an existing workflow row.
    const { persona } = await this.loadContext();
    const builtWorkflow = buildTurnWorkflow<TOutput>({
      engine: { ...this.deps.engine, systemPrompt: persona },
      bus,
      scope: this.toolScope(),
      signal: capture.abort.signal,
    });

    // Deliver the signal — completes the journal entry the suspended step
    // is waiting on. False means the entry was already completed (race
    // with another resume call) — surface it loudly so the gateway can
    // tell the caller their decision lost the race.
    const delivered = await completeSignal({
      storage,
      workflowId: wf.workflowId,
      stepName,
      signalName,
      value: decision,
    });
    if (!delivered) {
      throw new Error(`Approval "${signalName}" already resolved on workflow "${wf.workflowId}".`);
    }

    // Re-run the workflow. The runner uses the `input` we pass directly
    // (it doesn't fall back to `state.input`), so on resume we feed the
    // ORIGINAL persisted input back in. Replay reads activities from the
    // journal — the function-body itself is deterministic on input.
    const persistedInput = wf.input;
    const promise = this.deps.runner
      .run({
        workflow: builtWorkflow,
        workflowId: wf.workflowId,
        input: persistedInput,
      })
      .then(async (raw) => {
        const r = raw as AgentResult;
        await this.persistTurn({ result: r });
        return r;
      });

    return { promise, capture };
  }

  /**
   * Run one conversational turn as a fresh `agentAction` workflow seeded
   * with the thread's persisted message history. Stateless per call —
   * lets the gateway resolve a new `LocalAgent` per request.
   */
  private async runTurn(
    input: AgentInput,
    opts?: AgentInvokeOpts,
    captureIn?: RunCapture,
  ): Promise<{ promise: Promise<AgentResult>; capture: RunCapture }> {
    const capture = captureIn ?? makeCapture(opts?.signal);
    const bus = new SessionEventBus();
    bus.subscribe((e) => recordEvent(capture, e));

    // resolveContext-driven prompt assembly, ordered stable → volatile so
    // the prompt-cache prefix survives as far as possible:
    //
    //   system: persona            agentAction's config.systemPrompt (messages[0])
    //   system: instructions       operator-authored memory rules — a second
    //                              system block, so the persona prefix stays
    //                              cached when the rules change
    //   user:   memory reference   facts / working memory / episodes, fenced
    //   ...history
    //   user:   knowledge          context-mode retriever hits, fenced; changes
    //                              every turn, so it sits right before the task
    //   user:   task               appended by agentAction
    //
    // Memory and knowledge are untrusted (written by users, tools, the
    // model), so they are reference-data messages, never system ones.
    // Neither is persisted — persistTurn stores only the turn's own
    // messages (`turnMessages`).
    const { persona, instructions, reference, messages: history } = await this.loadContext();
    const knowledge = await buildRetrieverContext({
      retrievers: this.deps.retrievers,
      query: input.task,
    });
    const seed: Message[] = [];
    if (instructions) seed.push({ role: "system", content: instructions });
    if (reference) seed.push(referenceDataMessage(reference));
    seed.push(...history);
    if (input.messages) seed.push(...input.messages);
    if (knowledge) seed.push(referenceDataMessage(knowledge));

    const wf = buildTurnWorkflow<TOutput>({
      engine: { ...this.deps.engine, systemPrompt: persona },
      bus,
      scope: this.toolScope(),
      signal: capture.abort.signal,
    });
    // Honor caller-provided `runId` so a deterministic caller (e.g. a
    // schedule tick using `scheduleTickRunId`) lands on the same workflow
    // row across retries. Falls back to a thread-prefixed random id so
    // `resumeTurn`'s thread-scoped suspended-workflow search still works.
    const workflowId = opts?.runId ?? `${this.deps.key.threadId}-${randomUUID().slice(0, 8)}`;

    const source = input.source;
    const promise = this.deps.runner
      .run({
        workflow: wf,
        workflowId,
        input: { task: frameTask(input), messages: seed },
      })
      .then(async (raw) => {
        const r = raw as AgentResult;
        await this.persistTurn({ result: r, ...(source !== undefined && { source }) });
        return r;
      });

    return { promise, capture };
  }

  /**
   * Load the thread's prompt-ready view via `MemoryStore.resolveContext`.
   * Returns the agent's static persona prompt, the operator-authored
   * memory rules (`instructions`) and the fenced memory reference block
   * as SEPARATE strings. The persona and rules become distinct system
   * blocks, so each gets its own prompt-cache breakpoint at the adapter
   * layer and a rules edit doesn't invalidate the cached persona; the
   * reference block becomes a user-role data message.
   *
   * In-memory fallback (no `MemoryStore`) returns the raw message
   * buffer with no memory blocks.
   */
  private async loadContext(): Promise<{
    persona: string | undefined;
    instructions: string | undefined;
    reference: string | undefined;
    messages: Message[];
  }> {
    const persona = this.deps.engine.systemPrompt?.trim() || undefined;

    if (!this.deps.memory) {
      return {
        persona,
        instructions: undefined,
        reference: undefined,
        messages: [...this.inMemoryMessages],
      };
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
        instructions: undefined,
        reference: undefined,
        messages: stored.map((m) => stripStorageMeta(m)),
      };
    }

    return {
      persona,
      instructions: resolved.instructions.trim() || undefined,
      reference: resolved.reference.trim() || undefined,
      messages: resolved.messages.map((m) => stripStorageMeta(m)),
    };
  }

  /**
   * Persist the new turn: the engine's `turnMessages` (user task, then the
   * assistant and tool messages the turn produced). It excludes the seed
   * and anything a `beforeTurn` hook did to it, so a hook that trims or
   * injects context can't shift what gets stored. Used after a fresh turn
   * and after a resumed one alike — a suspended turn never reached here.
   */
  private async persistTurn(params: {
    result: AgentResult;
    source?: AgentInputSource;
  }): Promise<void> {
    const { result, source } = params;
    let newTail = result.turnMessages.filter(isTranscriptMessage);

    if (newTail.length === 0) return;

    // Stamp non-interactive sources onto the first user message so the UI
    // can render scheduled/webhook/agent triggers differently from live chat.
    if (source && source.kind !== "user") {
      const firstUserIdx = newTail.findIndex((m) => m.role === "user");
      const firstUser = firstUserIdx !== -1 ? newTail[firstUserIdx] : undefined;
      if (firstUser?.role === "user") {
        newTail = [
          ...newTail.slice(0, firstUserIdx),
          { ...firstUser, metadata: { source: serializeSource(source) } },
          ...newTail.slice(firstUserIdx + 1),
        ];
      }
    }

    if (this.deps.memory) {
      await this.ensureThreadRow();
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
   * Writes a thread episode only; `resolveContext` does not render
   * per-thread episodes into the prompt (the memory tool's `recall`
   * searches them on demand).
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
    const hasGate =
      cfg.messageThreshold !== undefined ||
      cfg.tokenThreshold !== undefined ||
      cfg.intervalMs !== undefined ||
      cfg.when !== undefined;
    if (!hasGate) return;
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
    const newMessages = messages.filter((m) => m.seq > lastDistilledSeq);
    const turnsSinceLastDistill = newMessages.length;
    const tokensSinceLastDistill = newMessages.reduce((s, m) => s + estimateTokens(m), 0);
    const now = (this.deps.engine.clock ?? SystemWallClock).currentTimeMs();
    const msSinceLastDistill = lastDistilledAt ? now - lastDistilledAt : Number.POSITIVE_INFINITY;

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
      tokensSinceLastDistill,
      msSinceLastDistill,
      lastDistilledAt,
      lastUserMessage,
      threadKey: this.deps.key,
    };

    // Custom predicate REPLACES built-in gates entirely. Otherwise OR
    // across whichever built-in gates are configured: any one tripping
    // is enough to fire.
    const fire = cfg.when
      ? cfg.when(signals)
      : (cfg.messageThreshold !== undefined && messages.length >= cfg.messageThreshold) ||
        (cfg.tokenThreshold !== undefined && tokensSinceLastDistill >= cfg.tokenThreshold) ||
        (cfg.intervalMs !== undefined && msSinceLastDistill >= cfg.intervalMs);
    if (!fire) return;

    const consolidator = this.deps.consolidator();
    const run = consolidator
      .distillThread(this.deps.key, { force: cfg.force ?? false })
      .catch((err) => {
        // Rate-limit hits are by design (host-configured cost cap),
        // not failures worth a warn. Swallow silently.
        if (
          err &&
          typeof err === "object" &&
          (err as { name?: string }).name === "ConsolidatorRateLimitError"
        ) {
          return;
        }
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
    await this.ensureThreadRow();
    await this.deps.memory.setThreadWorking(this.deps.key, markdown);
  }

  async metadata(): Promise<Readonly<Record<string, unknown>>> {
    if (!this.deps.memory) return {};
    const row = await this.deps.memory.getThread(this.deps.key);
    return row?.metadata ?? {};
  }

  /** Replace the metadata bag; the thread's owner stamp is kept. */
  async setMetadata(metadata: Readonly<Record<string, unknown>>): Promise<void> {
    if (!this.deps.memory) return;
    const row = await this.ensureThreadRow();
    await this.deps.memory.setThreadMetadata(this.deps.key, {
      ...metadata,
      [THREAD_AGENT_METADATA_KEY]: row.metadata[THREAD_AGENT_METADATA_KEY] ?? this.deps.agentId,
    });
  }

  async title(): Promise<string | null> {
    if (!this.deps.memory) return null;
    const row = await this.deps.memory.getThread(this.deps.key);
    return row?.title ?? null;
  }

  async setTitle(title: string | null): Promise<void> {
    if (!this.deps.memory) return;
    await this.ensureThreadRow();
    await this.deps.memory.setThreadTitle(this.deps.key, title);
  }

  async setArchived(archivedAt: number | null): Promise<void> {
    if (!this.deps.memory) return;
    await this.ensureThreadRow();
    await this.deps.memory.setThreadArchived(this.deps.key, archivedAt);
  }

  /**
   * The thread's row, created (stamped with the owning agent) when it is
   * missing — e.g. deleted out from under a live handle.
   */
  private async ensureThreadRow(): Promise<ThreadRow> {
    const memory = this.deps.memory!;
    const existing = await memory.getThread(this.deps.key);
    if (existing) return existing;
    return await memory.createThread(this.deps.key, {
      ...(this.deps.key.resourceId !== undefined && { resourceId: this.deps.key.resourceId }),
      metadata: { [THREAD_AGENT_METADATA_KEY]: this.deps.agentId },
    });
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
  /** Aborts the run: `cancel()` and the caller's `signal` both feed it. */
  readonly abort: AbortController;
  finishReason: FinishReason | null;
  usage: UsageStats;
  toolStartByCallId: Map<string, { input: unknown }>;
}

function makeCapture(signal?: AbortSignal): RunCapture {
  const abort = new AbortController();
  if (signal) {
    if (signal.aborted) abort.abort(signal.reason);
    else signal.addEventListener("abort", () => abort.abort(signal.reason), { once: true });
  }
  return {
    queue: new ChunkQueue(),
    events: [],
    toolCalls: [],
    toolResults: [],
    steps: [],
    abort,
    finishReason: null,
    usage: { inputTokens: 0, outputTokens: 0 },
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
      capture.toolStartByCallId.set(e.toolCallId, { input: e.input });
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
    finishReason: Promise.resolve(result.finishReason),
    messages: Promise.resolve([...result.messages]),
    // Already finished: nothing left to cancel.
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
    finishReason: settled.then((r) => r.finishReason),
    messages: settled.then((r) => [...r.messages]),
    cancel: async () => {
      capture.abort.abort();
    },
  };
}

/**
 * Build one turn's workflow: the agent's engine config plus this run's
 * bus, tool scope and cancellation signal. Nothing in `engine` is dropped.
 */
function buildTurnWorkflow<TOutput>(params: {
  engine: AgentEngineConfig<TOutput>;
  bus: SessionEventBus;
  scope: import("../tool.ts").ToolScope;
  signal: AbortSignal;
}): ReturnType<typeof agentAction> {
  const config: AgentActionConfig<TOutput> = {
    ...params.engine,
    bus: params.bus,
    scope: params.scope,
    signal: params.signal,
  };
  // The overloads split on `outputSchema`'s presence; this config may or
  // may not carry one.
  return (agentAction as (cfg: AgentActionConfig<TOutput>) => ReturnType<typeof agentAction>)(
    config,
  );
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

function serializeSource(
  source: Exclude<AgentInputSource, { kind: "user" }>,
): Record<string, unknown> {
  switch (source.kind) {
    case "scheduled":
      return {
        kind: "scheduled",
        firedAt: source.firedAt.toISOString(),
        ...(source.scheduleId !== undefined && { scheduleId: source.scheduleId }),
      };
    case "webhook":
      return {
        kind: "webhook",
        receivedAt: source.receivedAt.toISOString(),
        ...(source.origin !== undefined && { origin: source.origin }),
      };
    case "agent":
      return { kind: "agent", callerAgentId: source.callerAgentId };
  }
}

interface NormalizedRetrieverBinding extends AgentRetrieverBinding {
  readonly id: string;
  readonly toolName: string;
  readonly mode: AgentKnowledgeMode;
}

function retrieverBindings(
  retrievers: NonNullable<LocalAgentConfig["retrievers"]>,
): NormalizedRetrieverBinding[] {
  return Object.entries(retrievers).map(([id, bindingLike]) => {
    const binding: AgentRetrieverBinding =
      "retrieve" in bindingLike ? { retriever: bindingLike } : bindingLike;
    return {
      ...binding,
      id,
      toolName: binding.name ?? retrieverToolName(id),
      mode: binding.mode ?? "tool",
    };
  });
}

/**
 * Whether a turn message belongs in the persisted transcript: system
 * blocks and injected reference data are rebuilt every turn.
 */
function isTranscriptMessage(m: Message): boolean {
  return m.role !== "system" && !isReferenceDataMessage(m);
}

/**
 * Search every context-mode binding with `query` and render the hits as
 * one fenced reference-data block (one section per binding). Returns
 * `undefined` when no binding is in context mode or nothing matched.
 */
async function buildRetrieverContext(params: {
  retrievers: LocalAgentConfig["retrievers"];
  query: string;
}): Promise<string | undefined> {
  if (!params.retrievers) return undefined;
  const contextBindings = retrieverBindings(params.retrievers).filter((b) => b.mode !== "tool");
  if (contextBindings.length === 0) return undefined;
  const batches = await Promise.all(
    contextBindings.map(async (binding) => ({
      binding,
      results: await binding.retriever.retrieve({
        query: params.query,
        ...(binding.topK !== undefined && { topK: binding.topK }),
        ...(binding.filter !== undefined && { filter: binding.filter }),
        ...(binding.maxChunkCharacters !== undefined && {
          maxChunkCharacters: binding.maxChunkCharacters,
        }),
      }),
    })),
  );
  return fenceReferenceData({
    source: "knowledge",
    sections: batches
      .filter(({ results }) => results.length > 0)
      .map(({ binding, results }) => ({
        title: `Knowledge results from ${binding.id}`,
        body: formatRetrievedChunks({
          results: results.map((r) => ({
            text: r.chunk.text,
            score: r.score,
            ...(binding.includeSources !== false && { source: r.chunk.source }),
          })),
          ...(binding.includeScores !== undefined && { includeScores: binding.includeScores }),
        }),
      })),
  });
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
