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
  type AgentInput as TurnInput,
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
import { childToolCallId } from "../delegation/child-wiring.ts";
import type { DelegableAgent, PreparedTurn } from "../delegation/types.ts";
import type { ToolRegistry } from "../tool-registry.ts";
import type { Message, ToolCall } from "../message.ts";
import { SessionEventBus } from "../session-logger.ts";
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
import { isReferenceDataMessage, referenceDataMessage } from "../reference-data.ts";
import {
  maybeAutoCompact,
  maybeAutoDistill,
  type AutoCompactConfig,
  type AutoDistillConfig,
} from "./auto-consolidation.ts";
import { withAutoTools, type AutoToolScope } from "./auto-tools.ts";
import { frameTask } from "./frame-task.ts";
import { buildRetrieverContext, type AgentRetrievers } from "./retriever-bindings.ts";
import {
  liveOutput,
  makeCapture,
  recordEvent,
  resolvedOutput,
  type RunCapture,
} from "./run-output.ts";
import {
  InMemoryWorkflowStorage,
  SystemWallClock,
  childOfEndedSignal,
  completeSignal,
  hasCapability,
} from "@promin/workflow";
import { ThreadNotFoundError, TurnKeyConflictError } from "./errors.ts";
import { THREAD_AGENT_METADATA_KEY } from "./types.ts";
import type { AgentTool, ApprovalDecision } from "../tool.ts";
import { resolveLocalAgent } from "../registry/resolve-local-agent.ts";
import type { ResolveLocalAgentDeps } from "../registry/resolve-local-agent.ts";
import type { AgentRegistry } from "../registry/types.ts";
import type {
  Agent,
  AgentInput,
  AgentInputSource,
  AgentInvokeOpts,
  AgentRunOutput,
  AgentScope,
  AgentThread,
  ListThreadsParams,
  MessageRange,
  ThreadOptions,
  ThreadSummary,
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
  readonly retrievers?: AgentRetrievers;
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

export class LocalAgent<TOutput = unknown> implements Agent<AgentInput, TOutput>, DelegableAgent {
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
    const recipe = await registry.get({
      id,
      ...(opts?.version !== undefined && { version: opts.version }),
    });
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
    scope: AutoToolScope;
    userTools: Record<string, AgentTool<any, any>>;
  }): Record<string, AgentTool<any, any>> {
    const config = this.config;
    return withAutoTools({
      sources: {
        agentId: this.agentId(),
        ...(config.memory && { memory: config.memory }),
        ...(config.autoMemoryTool !== undefined && { autoMemoryTool: config.autoMemoryTool }),
        ...(config.network && { network: config.network }),
        ...(config.resourceId !== undefined && { boundResourceId: config.resourceId }),
        ...(config.ownerId !== undefined && { ownerId: config.ownerId }),
        ...(config.retrievers && { retrievers: config.retrievers }),
        ...(config.capabilities && { capabilities: config.capabilities }),
      },
      scope: params.scope,
      userTools: params.userTools,
    });
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

  // --- Delegation (DelegableAgent) ---------------------------------------

  /**
   * Prepare a turn of this agent for a delegating caller: its agent id and
   * the turn input (framed task plus retrieved context). The caller
   * journals the result, so the retrievers run once per delegated call.
   */
  async prepareTurn(input: AgentInput): Promise<PreparedTurn> {
    return { agentId: this.agentId(), input: await this.turnInput(input) };
  }

  /**
   * This agent's one-shot turn as a workflow, bound to its scope and
   * stopped by `signal`. A delegating turn runs it as its child workflow,
   * wiring its approvals and events to the root turn (`bus`,
   * `approvalIndex`).
   */
  turnWorkflow(params: {
    readonly signal: AbortSignal;
    readonly bus?: SessionEventBus;
    readonly approvalIndex?: TurnApprovalIndex;
    readonly onApprovalRequired?: (call: ToolCall) => Promise<ApprovalDecision>;
  }): ReturnType<typeof agentAction> {
    return this.oneShotWorkflow({
      namespaceId: this.config.namespaceId,
      bus: params.bus ?? new SessionEventBus(),
      signal: params.signal,
      ...(params.approvalIndex && { approvalIndex: params.approvalIndex }),
      ...(params.onApprovalRequired && { onApprovalRequired: params.onApprovalRequired }),
    });
  }

  // --- internal: one-shot runner ----------------------------------------

  private oneShotWorkflow(params: {
    namespaceId: string | undefined;
    bus: SessionEventBus;
    signal: AbortSignal;
    approvalIndex?: TurnApprovalIndex;
    onApprovalRequired?: (call: ToolCall) => Promise<ApprovalDecision>;
  }): ReturnType<typeof agentAction> {
    // Tools come from buildTools so the network tools auto-attach for
    // one-shot invoke too. No threadId here (one-shot is stateless), so
    // the layered memory tool stays off for invoke.
    const { namespaceId } = params;
    const resourceId = this.config.resourceId;
    const engine: AgentEngineConfig<TOutput> = {
      ...this.config.agent,
      ...this.buildTools({
        ...(namespaceId !== undefined && { namespaceId }),
        ...(resourceId !== undefined && { resourceId }),
      }),
      ...(params.onApprovalRequired && { onApprovalRequired: params.onApprovalRequired }),
    };
    return buildTurnWorkflow<TOutput>({
      engine,
      bus: params.bus,
      scope: {
        ...(namespaceId !== undefined && { namespaceId }),
        ...(resourceId !== undefined && { resourceId }),
        agentId: this.agentId(),
      },
      signal: params.signal,
      ...(params.approvalIndex && { approvalIndex: params.approvalIndex }),
    });
  }

  /**
   * The engine input of a one-shot turn. Context-mode retrievers search
   * with the task before the run starts; the hits ride in the workflow
   * input as a fenced reference-data message right before the task, so a
   * replay sees the same context without searching again.
   */
  private async turnInput(input: AgentInput): Promise<TurnInput> {
    const knowledge = await buildRetrieverContext({
      retrievers: this.config.retrievers,
      query: input.task,
    });
    const messages: Message[] = [...(input.messages ?? [])];
    if (knowledge) messages.push(referenceDataMessage(knowledge));
    return { task: frameTask(input), messages };
  }

  private async runOnce(params: {
    input: AgentInput;
    opts?: AgentInvokeOpts;
    capture: RunCapture;
  }): Promise<AgentResult> {
    const { input, opts, capture } = params;
    const bus = new SessionEventBus();
    bus.subscribe((e) => recordEvent(capture, e));

    const wf = this.oneShotWorkflow({
      namespaceId: opts?.namespaceId ?? this.config.namespaceId,
      bus,
      signal: capture.abort.signal,
    });
    // Honor caller-provided `runId` so a schedule tick (or any deterministic
    // caller) lands on the same workflow row across retries. Falls back to
    // a fresh random id for plain interactive invocations.
    const workflowId =
      opts?.runId ?? `${this.config.invokeIdPrefix ?? wf.name}-${randomUUID().slice(0, 8)}`;

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
    return (await this.config.runner.run({
      workflow: wf,
      workflowId,
      input: await this.turnInput(input),
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
  readonly retrievers?: AgentRetrievers;
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
      // Judged by the ROOT turn: an approval carried up from a sub-agent
      // names the child's run, which may end while the root goes on.
      const { run, root } = await this.rowRuns(row);
      if (!root || isTerminal(root.status)) {
        await this.closeEndedApproval({ row, run });
        continue;
      }
      if (run && isTerminal(run.status)) {
        // The child ended under a live root. A root parked on that child is
        // finished first; a root someone is running blocks this send.
        if (root.status === "suspended") {
          const after = await this.rerunRoot(root);
          if (!after || isTerminal(after.status)) {
            await this.closeEndedApproval({ row, run });
            continue;
          }
        }
        blocking.push(row);
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

  /**
   * The run an approval row names and the root turn above it (through
   * `parentWorkflowId`) — the same run for a turn's own approval. Null
   * when the row's run is gone.
   */
  private async rowRuns(row: PendingApproval): Promise<{
    run: WorkflowState | null;
    root: WorkflowState | null;
  }> {
    const storage = this.deps.runner.storage;
    const run = await storage.loadWorkflow(row.workflowId);
    if (!run) return { run: null, root: null };
    return { run, root: await rootRun({ storage, run }) };
  }

  /**
   * Re-run a root turn parked on a sub-agent that ended, so it reads the
   * child's outcome and goes on; its state afterwards. A root that
   * suspends again or is driven elsewhere stays as it is.
   */
  private async rerunRoot(root: WorkflowState): Promise<WorkflowState | null> {
    const { persona } = await this.loadContext();
    try {
      await this.runWorkflow({
        workflowId: root.workflowId,
        input: root.input as AgentRunInput,
        persona,
        capture: makeCapture(),
      });
      await this.afterTurn();
    } catch {
      // Suspended again, driven elsewhere, or failed: its state says which.
    }
    return this.deps.runner.storage.loadWorkflow(root.workflowId);
  }

  /** Close every open approval of the ended root turn `rootId` (siblings included). */
  private async closeRowsOfRoot(rootId: string): Promise<void> {
    for (const row of await this.pendingApprovals()) {
      const { run, root } = await this.rowRuns(row);
      if (root && root.workflowId !== rootId) continue;
      if (root && !isTerminal(root.status)) continue;
      await this.closeEndedApproval({ row, run });
    }
  }

  /**
   * Close an approval whose turn ended or vanished without its decision
   * reaching it (rejected, when it was still pending), and cancel the
   * sub-agent runs it was waiting in, when the root turn ended without
   * them: nothing would ever drive them again.
   */
  private async closeEndedApproval(params: {
    row: PendingApproval;
    run?: WorkflowState | null;
  }): Promise<void> {
    const { row } = params;
    const run = params.run === undefined ? null : params.run;
    if (run && !isTerminal(run.status) && run.parentWorkflowId !== undefined) {
      const top = await topLiveDescendant({ storage: this.deps.runner.storage, run });
      await this.deps.runner.storage.cancelWorkflow({ workflowId: top.workflowId, cascade: true });
    }
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
    // An approval carried up from a sub-agent names the child's run and a
    // call id scoped to it; the child waits on its own call id.
    const signalName = composeApprovalSignal(
      childToolCallId({ approvalId: callId, workflowId: entry.workflowId }),
    );
    // The root turn ended (cancelled, marked failed or done out of band)
    // while this approval waited: nothing will consume a decision.
    const { run: before, root: rootBefore } = await this.rowRuns(entry);
    if (!rootBefore || isTerminal(rootBefore.status)) {
      await this.closeEndedApproval({ row: entry, run: before });
      throw new ApprovalNotFoundError(key);
    }

    const wf = await this.awaitSignalWait({
      workflowId: entry.workflowId,
      signalName,
      key,
      pending: entry.status === "pending",
    });
    if (!wf) {
      // The sub-agent run ended under a live root: let the root read its
      // outcome (or block while someone else runs it), then close the row.
      const root = await storage.loadWorkflow(rootBefore.workflowId);
      if (root && root.status === "suspended") await this.rerunRoot(root);
      else if (root && !isTerminal(root.status)) throw new ApprovalNotReadyError(key);
      await this.closeEndedApproval({ row: entry, run: before });
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

    // Rebuild the turn's workflow definition and re-run it on its persisted
    // input; replay reads every recorded activity from the journal. For an
    // approval of a sub-agent that is the ROOT turn: its replay re-attaches
    // to the child (and the child to its own children), which consumes the
    // decision, so the whole chain finishes and the root commits once.
    const turn = await rootRun({ storage, run: wf });
    const { persona } = await this.loadContext();
    let result: AgentResult;
    try {
      result = await this.runRootPastSiblings({
        key,
        run: {
          workflowId: turn.workflowId,
          input: turn.input as AgentRunInput,
          persona,
          capture,
        },
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
      const rootAfter = await storage.loadWorkflow(turn.workflowId).catch(() => undefined);
      if (rootAfter && isTerminal(rootAfter.status)) {
        await this.closeRowsOfRoot(turn.workflowId);
      } else if (!after || isTerminal(after.status) || !waitingStep({ wf: after, signalName })) {
        await this.deps.approvals.markDelivered(key);
      }
      throw err;
    }
    await this.deps.approvals.markDelivered(key);
    // Sibling approvals of the same root (parallel sub-agents, a concurrent
    // resume that lost the race) are settled with it.
    await this.closeRowsOfRoot(turn.workflowId);
    await this.afterTurn();
    return result;
  }

  /**
   * Run a resumed root turn. A sibling sub-agent an earlier resume set
   * going may still be finishing its run in the background: the root then
   * parks on that running child instead of reaching its end. Wait (engine
   * clock, up to `RESUME_WAIT_MS`) for such a child to settle and run the
   * root again; past the wait, `ApprovalNotReadyError`. A root that parks
   * on a child waiting for its own approval is suspended for real.
   */
  private async runRootPastSiblings(params: {
    key: PendingApprovalKey;
    run: {
      workflowId: string;
      input: AgentRunInput;
      persona: string | undefined;
      capture: RunCapture;
    };
  }): Promise<AgentResult> {
    const storage = this.deps.runner.storage;
    const clock = this.deps.engine.clock ?? SystemWallClock;
    const deadline = clock.currentTimeMs() + RESUME_WAIT_MS;
    for (;;) {
      try {
        return await this.runWorkflow(params.run);
      } catch (err) {
        if (!isSuspension(err)) throw err;
        const root = await storage.loadWorkflow(params.run.workflowId);
        const running = root ? await runningChildAwaited({ storage, root }) : undefined;
        if (!running) throw err;
        for (;;) {
          const child = await storage.loadWorkflow(running);
          if (!child || !(await childInFlight({ storage, child }))) break;
          if (clock.currentTimeMs() >= deadline) throw new ApprovalNotReadyError(params.key);
          await new Promise<void>((resolve) => clock.setTimeout(resolve, RESUME_POLL_MS));
        }
      }
    }
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

  /**
   * Records each approval a turn — or a sub-agent it delegated to — suspends
   * on in the pending-approval index, under this thread.
   */
  private approvalIndex(): TurnApprovalIndex {
    return {
      thread: {
        namespaceId: this.deps.key.namespaceId,
        threadId: this.deps.key.threadId,
        agentId: this.deps.agentId,
      },
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
    const { memory, consolidator, key } = this.deps;
    if (!memory || !consolidator) return;
    if (this.deps.autoCompact) {
      await maybeAutoCompact({ config: this.deps.autoCompact, memory, key, consolidator });
    }
    if (this.deps.autoDistill) {
      await maybeAutoDistill({
        config: this.deps.autoDistill,
        memory,
        key,
        consolidator,
        clock: this.deps.engine.clock ?? SystemWallClock,
      });
    }
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

/**
 * The highest run above `run` (itself included) still in flight below its
 * ended root — cancelling it with cascade stops every orphaned sub-agent
 * of the chain.
 */
async function topLiveDescendant(params: {
  storage: WorkflowRunner["storage"];
  run: WorkflowState;
}): Promise<WorkflowState> {
  let current = params.run;
  const seen = new Set<string>([current.workflowId]);
  while (current.parentWorkflowId !== undefined && !seen.has(current.parentWorkflowId)) {
    const parent = await params.storage.loadWorkflow(current.parentWorkflowId);
    if (!parent || isTerminal(parent.status)) break;
    seen.add(parent.workflowId);
    current = parent;
  }
  return current;
}

/**
 * The top of `run`'s delegation chain: the run itself, or the turn that
 * (through its sub-agents) started it, found through `parentWorkflowId`.
 */
async function rootRun(params: {
  storage: WorkflowRunner["storage"];
  run: WorkflowState;
}): Promise<WorkflowState> {
  let current = params.run;
  const seen = new Set<string>([current.workflowId]);
  while (current.parentWorkflowId !== undefined && !seen.has(current.parentWorkflowId)) {
    const parent = await params.storage.loadWorkflow(current.parentWorkflowId);
    if (!parent) break;
    seen.add(parent.workflowId);
    current = parent;
  }
  return current;
}

/** The step of `wf` suspended on `signalName`, if any. */
/** A child `root` is parked on whose run is still going (see `childInFlight`). */
async function runningChildAwaited(params: {
  storage: WorkflowRunner["storage"];
  root: WorkflowState;
}): Promise<string | undefined> {
  if (params.root.status !== "suspended") return undefined;
  for (const step of Object.values(params.root.steps)) {
    if (step.status !== "waiting_for_signal" || !step.signalName) continue;
    const childId = childOfEndedSignal(step.signalName);
    if (!childId) continue;
    const child = await params.storage.loadWorkflow(childId);
    if (child && (await childInFlight({ storage: params.storage, child }))) return childId;
  }
  return undefined;
}

/**
 * Whether `child`'s run is going on somewhere: it has not ended and a
 * driver holds its run lock. Probed with a 1ms lease, released at once.
 */
async function childInFlight(params: {
  storage: WorkflowRunner["storage"];
  child: WorkflowState;
}): Promise<boolean> {
  const { storage, child } = params;
  if (isTerminal(child.status)) return false;
  const probe = await storage.tryLock({ workflowId: child.workflowId, lockDurationMs: 1 });
  if (!probe.acquired) return true;
  await storage
    .releaseLock({
      workflowId: child.workflowId,
      ...(probe.token !== undefined && { guard: { fenceToken: probe.token } }),
    })
    .catch(() => undefined);
  return false;
}

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

/**
 * Whether a turn message belongs in the persisted transcript: system
 * blocks and injected reference data are rebuilt every turn.
 */
function isTranscriptMessage(m: Message): boolean {
  return m.role !== "system" && !isReferenceDataMessage(m);
}

function stripStorageMeta(m: Message & { seq?: number; createdAt?: number }): Message {
  const { seq: _seq, createdAt: _createdAt, ...rest } = m as Record<string, unknown> & Message;
  return rest as Message;
}

// Re-export `ThreadRow` so callers can introspect downcast types if needed.
export type { ThreadRow };
