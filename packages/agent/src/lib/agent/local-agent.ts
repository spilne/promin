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
// A thread turn is one durable unit:
//   - its workflow id is deterministic — `opts.runId`, else a digest of
//     (namespace, thread, `opts.turnKey`) — so a retried send with the same
//     turn key reaches the same workflow instead of running the model and
//     tools again;
//   - the user message is recorded by the workflow's `transcript-open`
//     step and the produced messages by its `transcript-commit` step, each
//     an idempotent append keyed by the workflow id, so a crash, a retry or
//     a recovery on another worker never loses or duplicates history; a
//     failed or suspended turn has still recorded its user message;
//   - an approval the turn suspends on is recorded in the pending-approval
//     index (`PendingApprovalStore`), which `resume` reads by exact key;
//   - a new turn is refused while an earlier one waits for an approval
//     (`ThreadAwaitingApprovalError`), so a resumed turn's output always
//     lands right after its own user message.
//
// `MemoryStore` integration:
//   - thread.send / thread.stream load history → run agentAction, which
//     appends the turn's messages through the transcript steps
//   - thread.workingMemory / setWorkingMemory / setMetadata delegate to the
//     store directly
//   - thread.delete cascades through MemoryStore.deleteThread
//
// `MemoryStore` is OPTIONAL. When omitted, threads keep history in an
// in-memory array on the LocalAgentThread instance — useful for tests
// and single-process REPLs but not multi-tenant gateways.
// ---------------------------------------------------------------------------

import { createHash, randomUUID } from "node:crypto";
import type { WorkflowRunner, WorkflowState, WorkflowStatus } from "@promin/workflow";
import type { z } from "zod";
import {
  agentAction,
  type AgentActionConfig,
  type AgentEngineConfig,
  type AgentResult,
  type TurnApprovalIndex,
  type TurnTranscript,
} from "../agent-action.ts";
import { composeApprovalSignal } from "../approvals/approve-signal.ts";
import { InMemoryPendingApprovalStore } from "../approvals/in-memory-pending-approval-store.ts";
import {
  ApprovalAlreadyDecidedError,
  ApprovalExpiredError,
  ApprovalNotFoundError,
  ApprovalNotReadyError,
  ThreadAwaitingApprovalError,
  type PendingApproval,
  type PendingApprovalKey,
  type PendingApprovalStore,
} from "../approvals/pending-approval-store.ts";
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
import {
  InMemoryWorkflowStorage,
  SystemWallClock,
  completeSignal,
  hasCapability,
} from "@promin/workflow";
import { ThreadNotFoundError, TurnKeyConflictError } from "./errors.ts";
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
   * Index of the tool approvals thread turns are suspended on. Approve /
   * reject (`thread.resume`) looks the suspended turn up here by exact
   * (namespace, thread, tool call) key. Default: an
   * `InMemoryPendingApprovalStore` shared by every LocalAgent over the same
   * `runner` in this process. It is lost on restart, so a host whose
   * workflow storage is durable passes a durable store
   * (`SqlitePendingApprovalStore`, `PostgresPendingApprovalStore`) shared
   * by all its processes.
   */
  readonly approvals?: PendingApprovalStore;
  /**
   * Expire a pending approval this long after it is requested: an expired
   * approval can only be rejected. Default: approvals never expire.
   */
  readonly approvalTtlMs?: number;
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
    this.config = {
      ...config,
      approvals: config.approvals ?? defaultApprovalStore(config),
    };
  }

  /** The pending-approval index (configured, or the in-memory default). */
  private approvalStore(): PendingApprovalStore {
    return this.config.approvals!;
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
    const capture = makeCapture(opts?.signal);
    const result = await this.runOnce({ input, ...(opts && { opts }), capture });
    return resolvedOutput<TOutput>({ result, capture });
  }

  stream(input: AgentInput, opts?: AgentInvokeOpts): AgentRunOutput<TOutput> {
    const capture = makeCapture(opts?.signal);
    return liveOutput<TOutput>({
      promise: this.runOnce({ input, ...(opts && { opts }), capture }),
      capture,
    });
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
      approvals: this.approvalStore(),
      ...(this.config.approvalTtlMs !== undefined && { approvalTtlMs: this.config.approvalTtlMs }),
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

  private async runOnce(params: {
    input: AgentInput;
    opts?: AgentInvokeOpts;
    capture: RunCapture;
  }): Promise<AgentResult> {
    const { input, opts, capture } = params;
    const bus = new SessionEventBus();
    bus.subscribe((e) => recordEvent(capture, e));

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
      bus,
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

    // A retried run keeps its first input; replay reads the journal.
    const existing = await this.config.runner.storage.loadWorkflow(workflowId);
    if (existing) {
      assertSameTask({ workflowId, input: existing.input, task: frameTask(input) });
      return (await this.config.runner.run({
        workflow: wf,
        workflowId,
        input: existing.input,
      })) as AgentResult;
    }

    // Context-mode retrievers search with the task before the run starts;
    // the hits ride in the workflow input as a fenced reference-data
    // message right before the task, so a replay sees the same context
    // without searching again.
    const knowledge = await buildRetrieverContext({
      retrievers: this.config.retrievers,
      query: input.task,
    });
    const messages: Message[] = [...(input.messages ?? [])];
    if (knowledge) messages.push(referenceDataMessage(knowledge));
    return (await this.config.runner.run({
      workflow: wf,
      workflowId,
      input: { task: frameTask(input), messages },
    })) as AgentResult;
  }
}

/** Default pending-approval index per runner, shared by agents over it. */
const DEFAULT_APPROVAL_STORES = new WeakMap<WorkflowRunner, PendingApprovalStore>();

function defaultApprovalStore(config: LocalAgentConfig<any>): PendingApprovalStore {
  let store = DEFAULT_APPROVAL_STORES.get(config.runner);
  if (!store) {
    if (!(config.runner.storage instanceof InMemoryWorkflowStorage)) {
      // Durable turns with an index that dies with the process: a restart
      // loses every pending approval (the turns stay suspended).
      console.warn(
        "[LocalAgent] no `approvals` store configured: pending approvals are kept in memory " +
          "while the workflow storage is durable. Pass a durable PendingApprovalStore " +
          "(SqlitePendingApprovalStore, PostgresPendingApprovalStore) shared by every process.",
      );
    }
    store = new InMemoryPendingApprovalStore({
      ...(config.agent.clock !== undefined && { clock: config.agent.clock }),
    });
    DEFAULT_APPROVAL_STORES.set(config.runner, store);
  }
  return store;
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
  /** Pending-approval index the thread's turns record into and resume from. */
  readonly approvals: PendingApprovalStore;
  /** Expiry of a pending approval; omitted: never. */
  readonly approvalTtlMs?: number;
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
  /** Idempotency keys of the in-memory transcript's appends. */
  private readonly inMemoryAppends = new Set<string>();

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
    const capture = makeCapture(opts?.signal);
    const result = await this.runTurn({ input, ...(opts && { opts }), capture });
    return resolvedOutput<TOutput>({ result, capture });
  }

  stream(input: AgentInput, opts?: AgentInvokeOpts): AgentRunOutput<TOutput> {
    const capture = makeCapture(opts?.signal);
    return liveOutput<TOutput>({
      promise: this.runTurn({ input, ...(opts && { opts }), capture }),
      capture,
    });
  }

  async resume(
    callId: string,
    decision: ApprovalDecision,
    opts?: AgentInvokeOpts,
  ): Promise<AgentRunOutput<TOutput>> {
    const capture = makeCapture(opts?.signal);
    const result = await this.resumeTurn({ callId, decision, capture });
    return resolvedOutput<TOutput>({ result, capture });
  }

  resumeStream(
    callId: string,
    decision: ApprovalDecision,
    opts?: AgentInvokeOpts,
  ): AgentRunOutput<TOutput> {
    const capture = makeCapture(opts?.signal);
    return liveOutput<TOutput>({
      promise: this.resumeTurn({ callId, decision, capture }),
      capture,
    });
  }

  /**
   * Open approvals of this thread's turns, from the pending-approval index
   * (exact namespace + thread match), oldest first: approvals waiting for
   * a decision, and decided ones whose decision has not reached the turn
   * yet (a resume interrupted between the two — resume again to finish it).
   */
  async pendingApprovals(): Promise<PendingApproval[]> {
    return this.deps.approvals.list({
      namespaceId: this.deps.key.namespaceId,
      threadId: this.deps.key.threadId,
      open: true,
    });
  }

  /**
   * Make the thread ready for a new turn, or throw
   * `ThreadAwaitingApprovalError`. Turns run one at a time per thread, in
   * order: a new turn is refused while an earlier one waits for an
   * approval (or for a recorded decision to reach it), so that turn's
   * output lands in the transcript before the next turn's user message.
   *
   * On the way it settles what it can:
   *   - an approval whose turn has ended (completed / failed out of band)
   *     or is gone is closed, as rejected when it was still pending;
   *   - an expired approval is closed as `expired` and its turn finished
   *     with the rejection, so the thread moves on.
   * A turn that is still running — including one between recording its
   * approval and suspending on it — always blocks.
   */
  async prepareTurn(): Promise<void> {
    const blocking: PendingApproval[] = [];
    for (const row of await this.pendingApprovals()) {
      const key = {
        namespaceId: row.namespaceId,
        threadId: row.threadId,
        toolCallId: row.toolCallId,
      };
      const wf = await this.deps.runner.storage.loadWorkflow(row.workflowId);
      if (!wf || isTerminal(wf.status)) {
        await this.closeEndedApproval(row);
        continue;
      }
      let current = row;
      if (current.status === "pending") {
        const expired = await this.deps.approvals.decide({
          ...key,
          approved: false,
          decidedBy: SYSTEM_DECIDER,
          reason: "approval expired",
          onlyIfExpired: true,
        });
        if (expired.outcome !== "decided") {
          blocking.push(current);
          continue;
        }
        current = expired.approval;
      }
      if (current.status === "expired") {
        // Finish the turn with the rejection before the new turn starts.
        await this.resumeTurn({
          callId: current.toolCallId,
          decision: {
            approved: false,
            by: current.decidedBy ?? SYSTEM_DECIDER,
            reason: "approval expired",
          },
          capture: makeCapture(),
        }).catch((err: unknown) => {
          // Not yet suspended on it: the turn still blocks this send (below).
          if (err instanceof ApprovalNotReadyError) return;
          if (!isSuspension(err)) throw err;
        });
        const after = await this.deps.approvals.get(key);
        if (after && after.deliveredAt === null) blocking.push(after);
        continue;
      }
      blocking.push(current);
    }
    if (blocking.length > 0) throw new ThreadAwaitingApprovalError(blocking);
  }

  /** Close an approval whose turn ended or vanished without its decision. */
  private async closeEndedApproval(row: PendingApproval): Promise<void> {
    const key = {
      namespaceId: row.namespaceId,
      threadId: row.threadId,
      toolCallId: row.toolCallId,
    };
    if (row.status === "pending") {
      await this.deps.approvals.decide({
        ...key,
        approved: false,
        decidedBy: SYSTEM_DECIDER,
        reason: "the turn ended without this approval",
      });
    }
    await this.deps.approvals.markDelivered(key);
  }

  /**
   * Continue a turn that suspended on `approve:<callId>`. The pending-
   * approval index names the turn's workflow exactly; the decision is
   * recorded there first (atomically — of concurrent deciders one wins),
   * then delivered to the workflow's journal, and the workflow re-runs
   * under the SAME workflowId: the runner replays from the journal,
   * consumes the signal and continues to completion (or the next
   * suspension). The turn's output is committed by the workflow's own
   * `transcript-commit` step; the row is closed once the run settles.
   *
   * A decision that arrives while the turn is still on its way to the
   * suspension (the approval is recorded just before it) waits for the
   * suspension, up to `RESUME_WAIT_MS`, then throws
   * `ApprovalNotReadyError`. A resume interrupted after recording the
   * decision is finished by resuming again with the same decision.
   *
   * The rebuilt workflow definition must be replay-equivalent to the
   * original turn's. agentAction's control flow reads only journaled
   * values (tool-set snapshots included), so a recipe edited mid-turn
   * changes later steps only.
   */
  private async resumeTurn(params: {
    callId: string;
    decision: ApprovalDecision;
    capture: RunCapture;
  }): Promise<AgentResult> {
    const { callId, decision, capture } = params;
    const storage = this.deps.runner.storage;
    if (!hasCapability(storage, "journal")) {
      throw new Error(
        "LocalAgentThread.resume: storage doesn't support journaled signal delivery. " +
          "Approvals require a journaled storage backend (in-memory, sqlite, postgres).",
      );
    }

    const key = {
      namespaceId: this.deps.key.namespaceId,
      threadId: this.deps.key.threadId,
      toolCallId: callId,
    };
    let entry = await this.deps.approvals.get(key);
    if (!entry) throw new ApprovalNotFoundError(key);
    // The decision reached the turn already; the turn has moved on.
    if (entry.deliveredAt !== null) throw new ApprovalAlreadyDecidedError(entry);
    const signalName = composeApprovalSignal(callId);

    const wf = await this.awaitSignalWait({
      workflowId: entry.workflowId,
      signalName,
      key,
      pending: entry.status === "pending",
    });
    if (!wf) {
      await this.closeEndedApproval(entry);
      throw new ApprovalNotFoundError(key);
    }

    if (entry.status === "pending") {
      const decided = await this.deps.approvals.decide({
        ...key,
        approved: decision.approved,
        ...(decision.by !== undefined && { decidedBy: decision.by }),
        ...(decision.reason !== undefined && { reason: decision.reason }),
      });
      if (decided.outcome === "not_found") throw new ApprovalNotFoundError(key);
      if (decided.outcome === "expired") throw new ApprovalExpiredError(decided.approval);
      if (decided.outcome !== "decided") {
        throw new ApprovalAlreadyDecidedError(decided.approval);
      }
      entry = decided.approval;
    } else {
      // Decided, not delivered: finish only the decision that was recorded.
      const recordedApproval = entry.status === "approved";
      if (recordedApproval !== decision.approved) {
        if (entry.status === "expired" && decision.approved) {
          throw new ApprovalExpiredError(entry);
        }
        throw new ApprovalAlreadyDecidedError(entry);
      }
    }

    // Deliver the recorded decision when the turn is still waiting on it.
    // A completed entry (an interrupted resume being finished) needs none:
    // the journal holds it either way.
    const recorded: ApprovalDecision = {
      approved: entry.status === "approved",
      ...(entry.reason !== null && { reason: entry.reason }),
      ...(entry.decidedBy !== null && { by: entry.decidedBy }),
    };
    const waiting = waitingStep({ wf, signalName });
    if (waiting) {
      await completeSignal({
        storage,
        workflowId: entry.workflowId,
        stepName: waiting.stepName,
        signalName,
        value: recorded,
      });
    }

    // Rebuild the workflow definition and re-run it on its persisted
    // input; replay reads every recorded activity from the journal.
    const { persona } = await this.loadContext();
    let result: AgentResult;
    try {
      result = await this.runWorkflow({
        workflowId: entry.workflowId,
        input: wf.input as AgentRunInput,
        persona,
        capture,
      });
    } catch (err) {
      // Another process is running the turn: the decision is recorded and
      // delivered, that process carries it on.
      if (errorTag(err) === "WorkflowLockError") throw new ApprovalNotReadyError(key);
      // The run may have failed before the turn consumed the decision (a
      // transient storage error, say): then the turn is still suspended on
      // this signal and the row stays open, so the next resume or send
      // delivers it again instead of orphaning the turn.
      const after = await storage.loadWorkflow(entry.workflowId).catch(() => undefined);
      if (!after || isTerminal(after.status) || !waitingStep({ wf: after, signalName })) {
        await this.deps.approvals.markDelivered(key);
      }
      throw err;
    }
    await this.deps.approvals.markDelivered(key);
    await this.afterTurn();
    return result;
  }

  /**
   * The turn's workflow when a decision can be delivered to it. For a
   * pending approval that is once the turn is suspended on `signalName`:
   * while the turn is still on its way there (the approval is recorded just
   * before it suspends), polls on the engine clock up to `RESUME_WAIT_MS`,
   * then throws `ApprovalNotReadyError`. A decided approval only needs the
   * turn not to have ended. Null when the turn ended or is gone.
   */
  private async awaitSignalWait(params: {
    workflowId: string;
    signalName: string;
    key: PendingApprovalKey;
    pending: boolean;
  }): Promise<WorkflowState | null> {
    const { workflowId, signalName, key, pending } = params;
    const clock = this.deps.engine.clock ?? SystemWallClock;
    const deadline = clock.currentTimeMs() + RESUME_WAIT_MS;
    for (;;) {
      const wf = await this.deps.runner.storage.loadWorkflow(workflowId);
      if (!wf || isTerminal(wf.status)) return null;
      if (!pending) return wf;
      if (wf.status === "suspended") return waitingStep({ wf, signalName }) ? wf : null;
      if (clock.currentTimeMs() >= deadline) throw new ApprovalNotReadyError(key);
      await new Promise<void>((resolve) => clock.setTimeout(resolve, RESUME_POLL_MS));
    }
  }

  /**
   * Run one conversational turn as an `agentAction` workflow seeded with
   * the thread's persisted message history. Stateless per call — lets the
   * gateway resolve a new `LocalAgent` per request.
   *
   * The workflow id is deterministic: `opts.runId` when given, else derived
   * from (namespace, thread, `opts.turnKey`). A retried send with the same
   * turn key lands on the same workflow, which the runner answers from the
   * journal (a finished turn returns its stored result) instead of running
   * the model and tools again.
   */
  private async runTurn(params: {
    input: AgentInput;
    opts?: AgentInvokeOpts;
    capture: RunCapture;
  }): Promise<AgentResult> {
    const { input, opts, capture } = params;
    const workflowId =
      opts?.runId ??
      threadTurnWorkflowId({
        namespaceId: this.deps.key.namespaceId,
        threadId: this.deps.key.threadId,
        turnKey: opts?.turnKey ?? randomUUID(),
      });

    const existing = await this.deps.runner.storage.loadWorkflow(workflowId);
    if (existing) {
      // A retry of a turn that already started: its input is fixed, and
      // must be for the same task.
      assertSameTask({ workflowId, input: existing.input, task: frameTask(input) });
      const { persona } = await this.loadContext();
      const result = await this.runWorkflow({
        workflowId,
        input: existing.input as AgentRunInput,
        persona,
        capture,
        ...(input.source !== undefined && { source: input.source }),
      });
      await this.afterTurn();
      return result;
    }

    await this.prepareTurn();

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
    // Neither is persisted — the transcript records only the turn's own
    // messages.
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

    const result = await this.runWorkflow({
      workflowId,
      input: { task: frameTask(input), messages: seed },
      persona,
      capture,
      ...(input.source !== undefined && { source: input.source }),
    });
    await this.afterTurn();
    return result;
  }

  /** Build this thread's turn workflow and run it under `workflowId`. */
  private async runWorkflow(params: {
    workflowId: string;
    input: AgentRunInput;
    persona: string | undefined;
    capture: RunCapture;
    source?: AgentInputSource;
  }): Promise<AgentResult> {
    const { workflowId, input, persona, capture, source } = params;
    const bus = new SessionEventBus();
    bus.subscribe((e) => recordEvent(capture, e));
    const wf = buildTurnWorkflow<TOutput>({
      engine: { ...this.deps.engine, systemPrompt: persona },
      bus,
      scope: this.toolScope(),
      signal: capture.abort.signal,
      transcript: this.transcript(source),
      approvalIndex: this.approvalIndex(),
    });
    return (await this.deps.runner.run({ workflow: wf, workflowId, input })) as AgentResult;
  }

  /**
   * The turn's durable transcript: the user message at turn start, the
   * produced messages once the turn has its answer. Each append carries an
   * idempotency key of the turn's workflow id, so a step re-run after a
   * crash (or on another worker) never duplicates history.
   */
  private transcript(source: AgentInputSource | undefined): TurnTranscript {
    return {
      open: async ({ workflowId, message }) => {
        // Stamp non-interactive sources onto the user message so the UI can
        // render scheduled / webhook / agent triggers differently from chat.
        const stamped: Message =
          source && source.kind !== "user" && message.role === "user"
            ? { ...message, metadata: { source: serializeSource(source) } }
            : message;
        await this.appendTranscript({ workflowId, phase: "open", messages: [stamped] });
      },
      commit: async ({ workflowId, messages }) => {
        await this.appendTranscript({
          workflowId,
          phase: "commit",
          messages: messages.filter(isTranscriptMessage),
        });
      },
    };
  }

  private async appendTranscript(params: {
    workflowId: string;
    phase: "open" | "commit";
    messages: Message[];
  }): Promise<void> {
    const { workflowId, phase, messages } = params;
    if (messages.length === 0) return;
    const idempotencyKey = `${workflowId}:${phase}`;
    if (this.deps.memory) {
      await this.ensureThreadRow();
      await this.deps.memory.appendMessages(this.deps.key, messages, { idempotencyKey });
      return;
    }
    if (this.inMemoryAppends.has(idempotencyKey)) return;
    this.inMemoryAppends.add(idempotencyKey);
    this.inMemoryMessages.push(...messages);
  }

  /** Records each approval a turn suspends on in the pending-approval index. */
  private approvalIndex(): TurnApprovalIndex {
    return {
      requested: async ({ workflowId, call }) => {
        await this.deps.approvals.record({
          namespaceId: this.deps.key.namespaceId,
          threadId: this.deps.key.threadId,
          toolCallId: call.id,
          ...(this.deps.key.resourceId !== undefined && { resourceId: this.deps.key.resourceId }),
          agentId: this.deps.agentId,
          workflowId,
          toolName: call.name,
          input: call.input,
          ...(this.deps.approvalTtlMs !== undefined && { ttlMs: this.deps.approvalTtlMs }),
        });
      },
    };
  }

  /** Best-effort memory consolidation once a turn has finished. */
  private async afterTurn(): Promise<void> {
    if (!this.deps.memory) return;
    await this.maybeAutoCompact();
    await this.maybeAutoDistill();
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
    // after the turn's commit the assistant turn (and any tool messages)
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
    await this.deps.approvals.deleteThread({
      namespaceId: this.deps.key.namespaceId,
      threadId: this.deps.key.threadId,
    });
    this.inMemoryMessages = [];
    this.inMemoryAppends.clear();
  }
}

// ---------------------------------------------------------------------------
// Helpers — capture, conversion, output assembly
// ---------------------------------------------------------------------------

/** What one turn's workflow runs on: the (framed) task and its seed. */
type AgentRunInput = { task: string; messages: Message[] };

/** `decidedBy` of an approval the agent closed itself (expired, or its turn ended). */
const SYSTEM_DECIDER = "system";

/** How long a resume waits for a turn to reach its approval suspension. */
const RESUME_WAIT_MS = 5_000;
const RESUME_POLL_MS = 25;

function isTerminal(status: WorkflowStatus): boolean {
  return status === "completed" || status === "failed" || status === "tripwire";
}

/** The step of `wf` suspended on `signalName`, if any. */
function waitingStep(params: { wf: WorkflowState; signalName: string }) {
  return Object.values(params.wf.steps).find(
    (s) => s.status === "waiting_for_signal" && s.signalName === params.signalName,
  );
}

/** A run id / turn key names one task: refuse a retry that carries another. */
function assertSameTask(params: { workflowId: string; input: unknown; task: string }): void {
  const stored = (params.input as { task?: unknown } | null)?.task;
  if (stored !== params.task) throw new TurnKeyConflictError({ workflowId: params.workflowId });
}

function errorTag(err: unknown): unknown {
  return (err as { _tag?: unknown } | null)?._tag;
}

function isSuspension(err: unknown): boolean {
  return errorTag(err) === "WorkflowSuspendedError";
}

/**
 * Workflow id of a thread turn: a digest of (namespace, thread, turn key),
 * so the same send retried with the same key reaches the same workflow,
 * and no thread or namespace can collide with another by prefix.
 */
export function threadTurnWorkflowId(params: {
  readonly namespaceId: string;
  readonly threadId: string;
  readonly turnKey: string;
}): string {
  const digest = createHash("sha256")
    .update(JSON.stringify([params.namespaceId, params.threadId, params.turnKey]))
    .digest("hex");
  return `thread-turn-${digest.slice(0, 40)}`;
}

interface RunCapture {
  readonly text: PushChannel<string>;
  readonly events: PushChannel<AgentEvent>;
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
    text: new PushChannel<string>(),
    events: new PushChannel<AgentEvent>(),
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

/** Build an AgentRunOutput where every promise is already resolved. */
function resolvedOutput<TOutput>(params: {
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

function liveOutput<TOutput>(params: {
  promise: Promise<AgentResult>;
  capture: RunCapture;
}): AgentRunOutput<TOutput> {
  const { promise, capture } = params;
  // Both streams end when the run settles, however it ends.
  const settled = promise.finally(() => {
    capture.text.close();
    capture.events.close();
  });
  return {
    textStream: capture.text,
    fullStream: capture.events,
    text: settled.then((r) => r.answer),
    output: settled.then((r) => r.output as TOutput | undefined),
    toolCalls: settled.then((r) => extractToolCalls(r.turnMessages)),
    toolResults: settled.then((r) => extractToolResults(r.turnMessages)),
    steps: settled.then(() => capture.steps.slice()),
    usage: settled.then(usageOf),
    finishReason: settled.then((r) => r.finishReason),
    messages: settled.then((r) => [...r.messages]),
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

/**
 * Build one turn's workflow: the agent's engine config plus this run's
 * bus, tool scope, cancellation signal and (for thread turns) durable
 * transcript and approval index. Nothing in `engine` is dropped.
 */
function buildTurnWorkflow<TOutput>(params: {
  engine: AgentEngineConfig<TOutput>;
  bus: SessionEventBus;
  scope: import("../tool.ts").ToolScope;
  signal: AbortSignal;
  transcript?: TurnTranscript;
  approvalIndex?: TurnApprovalIndex;
}): ReturnType<typeof agentAction> {
  const config: AgentActionConfig<TOutput> = {
    ...params.engine,
    bus: params.bus,
    scope: params.scope,
    signal: params.signal,
    ...(params.transcript && { transcript: params.transcript }),
    ...(params.approvalIndex && { approvalIndex: params.approvalIndex }),
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

// ---- PushChannel: buffered, push-based async stream ----

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

// Re-export `ThreadRow` so callers can introspect downcast types if needed.
export type { ThreadRow };
