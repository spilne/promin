// ---------------------------------------------------------------------------
// ZoryaAgents — owns agent registry + resolver + memory + instances + scan
// loop + agent schedule dispatch. The single object the server config
// expects when agent functionality is enabled.
//
// Implements AgentScheduleDispatcher so ZoryaScheduler can route
// agent-targeted ticks here without the server needing to special-case it.
// ---------------------------------------------------------------------------

import type {
  Agent,
  AgentBudgets,
  AgentInstanceRegistry,
  AgentRegistry,
  AgentToolCatalog,
  FragmentRegistry,
  MemoryStore,
  ModelCatalog,
  RegisteredAgent,
  RetrieverRegistry,
  RoleRegistry,
  ScanLoopHandle,
  ScanLoopTick,
  ToolHistoryStore,
} from "@promin/agent";
import {
  FILE_SOURCE,
  listAllVersioned,
  AgentTurnGate,
  InMemoryLeaseStore,
  InMemoryMemoryStore,
  LocalAgent,
  startAgentSchedule,
  startAgentScanLoop,
} from "@promin/agent";
import type { DurableScheduleConfig, ScheduleTick } from "@promin/workflow/scheduler";
import type { ScopeAuthorizer } from "../../routes/agent-gateway-kit.ts";
import type { TurnStreamLimits } from "../../routes/agents/agent-sse.ts";
import type { AgentScheduleDispatcher, AgentScheduleStart } from "../scheduler/index.ts";

export interface ZoryaAgentsScanConfig {
  /** Filesystem root to scan for agent recipe modules. */
  root: string;
  /** Poll interval in ms. Default 5000. */
  intervalMs?: number;
  /** Remove scanner-written entries whose file is gone (never operator-authored ones). Default false. */
  sync?: boolean;
  /** Fired after each scan tick. */
  onTick?: (tick: ScanLoopTick) => void;
  /** A failed scan tick lands here. Default: console. */
  onError?: (err: unknown) => void;
}

export interface ZoryaAgentsConfig {
  registry: AgentRegistry;
  /**
   * Materialize a `LocalAgent` from a recipe. Optional `scope` carries
   * the per-request namespace + resource so the host can resolve
   * recipe-level credentialRefs (BYOK) at this boundary; hosts that
   * don't care can ignore it.
   */
  resolve: (
    recipe: RegisteredAgent,
    scope?: { readonly namespaceId?: string; readonly resourceId?: string },
  ) => Agent | Promise<Agent>;
  memory?: MemoryStore;
  instances?: AgentInstanceRegistry;
  /**
   * Catalog of available LLM models, keyed by `(provider, id)`. When set,
   * the server exposes `GET /api/agents/_catalog/models` so the designer UI
   * can populate its model dropdown. The same catalog typically backs the
   * `resolve` callback's LLM lookup so a recipe pointing at
   * `(provider, id)` resolves to the registered runtime instance.
   */
  models?: ModelCatalog;
  /**
   * Optional tool catalog. When set, the server exposes
   * `GET /api/agents/_catalog/tools` so the Designer's tool multi-
   * select can populate options from in-process / file / MCP sources.
   * Without it, the route is not mounted.
   */
  toolCatalog?: AgentToolCatalog;
  /**
   * Optional retriever registry. When set, `GET /api/agents/_catalog/retrievers`
   * lists its rows (id, description, tags, metadata — never the retriever)
   * for the agent editor's knowledge picker. Pass the same registry to
   * `resolveLocalAgent({ retrievers })` in `resolve` so the ids recipes
   * bind actually resolve. Without it, the catalog answers an empty list.
   */
  retrievers?: RetrieverRegistry;
  /**
   * Optional fragment registry. When set, the server exposes
   * `GET /api/agents/_catalog/fragments` so the agent editor's
   * layered-prompt editor can populate its layer picker (and so the
   * resolver itself, if wired separately, has the same registry).
   * Mirrors the `toolCatalog` opt-in.
   */
  fragments?: FragmentRegistry;
  /**
   * Optional role registry. When set, the server mounts `/api/roles/*`
   * CRUD and `GET /api/agents/_catalog/roles` (the agent editor's role
   * picker), and `extract-role` can lift an inline role into it. The same
   * registry should back the `resolve` callback so `ref`-bound agents
   * load their role at materialize time.
   */
  roles?: RoleRegistry;
  /**
   * Optional tool-history store. When set, the server exposes
   * `GET /api/agents/_catalog/tools/history` so a compliance / forensic
   * query can read which tools the host has exposed over time. Pair it
   * with an `AgentToolCatalogHistory` snapshot loop to populate it.
   * Without it, the route is not mounted.
   */
  toolHistory?: ToolHistoryStore;
  /** Filesystem hot-reload scan loop. Omit to disable. */
  scan?: ZoryaAgentsScanConfig;
  /**
   * Per-thread turn gate: the gateway serializes thread-bound routes
   * (POST /threads/:threadId, /stream, /approve), so two turns never run
   * on one conversation at once — they would interleave its history.
   *
   * On by default, with an in-process lease store: enough for a single
   * process. Several processes sharing a durable memory store must pass a
   * gate over a shared lease store (`PostgresLeaseStore`). `false` turns
   * the gate off (only for hosts that serialize turns themselves).
   */
  turnGate?: AgentTurnGate | false;
  /**
   * Identifier for THIS process — used as the lease ownerId so 409
   * responses can name which replica holds the conversation. Defaults
   * to a random hex string.
   */
  workerId?: string;
  /**
   * Binds the `(namespaceId, resourceId | ownerId)` a request names to its
   * authenticated caller; `false` answers 403. It is also asked for
   * `{ kind: "operator" }` before global secrets are stored, listed or
   * deleted (directly or by a clone), which only operators should get.
   * Without it, any caller the server's auth admits may act in any scope
   * it names (thread ownership by resource and agent is enforced either
   * way).
   */
  authorizeScope?: ScopeAuthorizer;
  /**
   * Bounds of the agent routes' event streams: per-turn buffer bytes,
   * retention for reattach, heartbeat interval, kept turns and the
   * per-response queue. Defaults: see `TurnStreamLimits`.
   */
  streaming?: TurnStreamLimits;
  /**
   * Thread and namespace budgets for local agents: a thread's cumulative
   * budget and a namespace's rolling-window budget. Applied to every agent
   * `resolve` returns, so gateway and scheduled thread turns and one-shot
   * invokes are admitted against them — refused once their thread or
   * namespace has used its budget up (429 `budget_exceeded`), else capped
   * by what is left — and record their spend, committed or failed. A DAG
   * run is admitted against its namespace's budget at the DAG route and
   * records its nodes' spend under the run id (`ZoryaDags`); its node
   * turns, like sub-agent turns, record nothing themselves. Spend goes to
   * `budgets.ledger` — pass a durable one
   * (`SqliteUsageLedger`, `PostgresUsageLedger`) shared by every process.
   * Spend is shown on `GET /api/agents/:id/threads/:threadId/usage` and
   * `GET /api/agents/_usage`. Remote-backend agents are not budgeted here.
   */
  budgets?: AgentBudgets;
}

export class ZoryaAgents implements AgentScheduleDispatcher {
  readonly registry: AgentRegistry;
  readonly resolve: (
    recipe: RegisteredAgent,
    scope?: { readonly namespaceId?: string; readonly resourceId?: string },
  ) => Agent | Promise<Agent>;
  readonly memory?: MemoryStore;
  readonly instances?: AgentInstanceRegistry;
  readonly models?: ModelCatalog;
  readonly toolCatalog?: AgentToolCatalog;
  readonly retrievers?: RetrieverRegistry;
  readonly fragments?: FragmentRegistry;
  readonly roles?: RoleRegistry;
  readonly toolHistory?: ToolHistoryStore;
  /** The gate thread-bound routes and scheduled thread turns hold; unset only with `turnGate: false`. */
  readonly turnGate?: AgentTurnGate;
  /**
   * True when `turnGate` is the default one over an in-process lease store:
   * it serializes turns in this process only. A clustered server refuses it.
   */
  readonly turnGateInProcess: boolean;
  readonly workerId?: string;
  readonly authorizeScope?: ScopeAuthorizer;
  readonly streaming?: TurnStreamLimits;
  readonly budgets?: AgentBudgets;
  /**
   * Whether a new, never evaluated version of an agent may become active
   * on a path without a request to refuse (the file scan, remote
   * deployment re-syncs); refused versions wait as drafts. `ZoryaEvals`
   * installs its promotion gates here. Default: always.
   */
  canAutoPromote?: (params: { readonly id: string }) => boolean | Promise<boolean>;
  private readonly scanConfig?: ZoryaAgentsScanConfig;
  private scanHandle?: ScanLoopHandle<ScanLoopTick>;

  constructor(config: ZoryaAgentsConfig) {
    this.registry = config.registry;
    const budgets = config.budgets;
    if (budgets) {
      this.budgets = budgets;
      const resolve = config.resolve;
      this.resolve = async (recipe, scope) => {
        const agent = await resolve(recipe, scope);
        return agent instanceof LocalAgent ? agent.withBudgets(budgets) : agent;
      };
    } else {
      this.resolve = config.resolve;
    }
    if (config.memory) this.memory = config.memory;
    if (config.instances) this.instances = config.instances;
    if (config.models) this.models = config.models;
    if (config.toolCatalog) this.toolCatalog = config.toolCatalog;
    if (config.retrievers) this.retrievers = config.retrievers;
    if (config.fragments) this.fragments = config.fragments;
    if (config.roles) this.roles = config.roles;
    if (config.toolHistory) this.toolHistory = config.toolHistory;
    if (config.scan) this.scanConfig = config.scan;
    this.turnGateInProcess = config.turnGate === undefined;
    if (config.turnGate !== false) {
      this.turnGate =
        config.turnGate ?? new AgentTurnGate({ leaseStore: new InMemoryLeaseStore() });
    }
    if (
      this.turnGateInProcess &&
      config.memory &&
      !(config.memory instanceof InMemoryMemoryStore)
    ) {
      // Durable memory usually means several processes can reach a thread;
      // the default gate only serializes turns inside this one.
      console.warn(
        "[zorya] ZoryaAgents: durable memory store with the default in-process turn gate. " +
          "If more than one process serves agents, pass `turnGate: new AgentTurnGate({ " +
          "leaseStore: new PostgresLeaseStore({ db }) })` so turns on a thread never interleave.",
      );
    }
    if (config.workerId) this.workerId = config.workerId;
    if (config.authorizeScope) this.authorizeScope = config.authorizeScope;
    if (config.streaming) this.streaming = config.streaming;
  }

  /**
   * Called by ZoryaScheduler when isAgentSchedule(schedule) is true. Starts
   * the turn through `startAgentSchedule` and returns once it is under way;
   * the turn's outcome arrives in `done`. A missing recipe rejects, so the
   * tick is delivered again (after backoff) instead of being dropped.
   */
  async dispatchSchedule(
    tick: ScheduleTick,
    schedule: DurableScheduleConfig,
  ): Promise<AgentScheduleStart> {
    const started = await startAgentSchedule(tick, schedule, {
      registry: this.registry,
      resolve: this.resolve,
      // A scheduled turn on a thread takes the thread's turn lease like a
      // gateway send, so it never interleaves with a live conversation.
      ...(this.turnGate !== undefined && { turnGate: this.turnGate }),
      ...(this.workerId !== undefined && { ownerId: this.workerId }),
    });
    if ("done" in started) return { done: started.done };
    if (started.skipped === "recipe_not_found" || started.skipped === "not_agent_schedule") {
      throw new Error(
        `agent schedule "${tick.scheduleId}" was not started: ${started.error ?? started.skipped}`,
      );
    }
    return { done: Promise.resolve(started) };
  }

  /**
   * Ids whose active version a file scan wrote. Read from the registry, so
   * every replica agrees from boot.
   */
  async fileManagedIds(): Promise<string[]> {
    const managed = await listAllVersioned({ registry: this.registry, source: FILE_SOURCE });
    return managed.map((e) => e.id);
  }

  async start(): Promise<void> {
    if (this.scanHandle || !this.scanConfig) return;
    const userOnTick = this.scanConfig.onTick;
    this.scanHandle = startAgentScanLoop({
      registry: this.registry,
      root: this.scanConfig.root,
      ...(this.scanConfig.intervalMs !== undefined && {
        intervalMs: this.scanConfig.intervalMs,
      }),
      ...(this.scanConfig.sync !== undefined && { sync: this.scanConfig.sync }),
      // Read per call: the evals service installs its gates after construction.
      canAutoPromote: (p) => this.canAutoPromote?.(p) ?? true,
      ...(this.scanConfig.onError !== undefined && { onError: this.scanConfig.onError }),
      ...(userOnTick !== undefined && { onTick: userOnTick }),
    });
    // One immediate tick so file recipes are registered at boot instead of
    // after the first interval (which is 5s in the demo).
    await this.scanHandle.tick();
  }

  async stop(): Promise<void> {
    if (this.scanHandle) {
      this.scanHandle.stop();
      this.scanHandle = undefined;
    }
  }
}
