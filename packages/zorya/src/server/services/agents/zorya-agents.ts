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
  AgentInstanceRegistry,
  AgentRegistry,
  AgentToolCatalog,
  AgentTurnGate,
  FragmentRegistry,
  MemoryStore,
  ModelCatalog,
  RegisteredAgent,
  RoleRegistry,
  ToolHistoryStore,
} from "@promin/agent";
import { dispatchAgentSchedule } from "@promin/agent";
import type { DurableScheduleConfig, ScheduleTick } from "@promin/workflow";
import {
  startAgentsScanLoop,
  type AgentScanFolderResult as _ScanResult,
} from "../../agent-registry.ts";
import type { AgentScheduleDispatcher } from "../scheduler/index.ts";

export interface ZoryaAgentsScanConfig {
  /** Filesystem root to scan for agent recipe modules. */
  root: string;
  /** Poll interval in ms. Default 5000. */
  intervalMs?: number;
  /** Sweep removed files (delete from registry). Default false. */
  sync?: boolean;
  /** Fired after each scan tick. */
  onTick?: (tick: {
    added: string[];
    upserted: string[];
    deleted: string[];
    warnings: string[];
    durationMs: number;
  }) => void;
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
   * Per-thread turn gate. When set, the gateway serializes thread-bound
   * routes (POST /threads/:threadId, /stream, /approve) so two replicas
   * cannot run a turn on the same conversation concurrently. Required
   * for multi-replica deployments sharing a Postgres memory store; safe
   * to omit for single-process deployments.
   */
  turnGate?: AgentTurnGate;
  /**
   * Identifier for THIS process — used as the lease ownerId so 409
   * responses can name which replica holds the conversation. Defaults
   * to a random hex string.
   */
  workerId?: string;
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
  readonly fragments?: FragmentRegistry;
  readonly roles?: RoleRegistry;
  readonly toolHistory?: ToolHistoryStore;
  readonly turnGate?: AgentTurnGate;
  readonly workerId?: string;
  private readonly scanConfig?: ZoryaAgentsScanConfig;
  private scanHandle?: { stop(): void; tick(): Promise<unknown> };
  // Agent ids currently backed by a recipe file on disk (rebuilt each scan
  // tick). The editor uses this to mark a recipe read-only — without it, an
  // in-place edit gets silently overwritten by the next scan. Empty when no
  // scanner is configured. Sibling of ZoryaSkills.fileManagedIds().
  private _fileManaged: ReadonlySet<string> = new Set();

  constructor(config: ZoryaAgentsConfig) {
    this.registry = config.registry;
    this.resolve = config.resolve;
    if (config.memory) this.memory = config.memory;
    if (config.instances) this.instances = config.instances;
    if (config.models) this.models = config.models;
    if (config.toolCatalog) this.toolCatalog = config.toolCatalog;
    if (config.fragments) this.fragments = config.fragments;
    if (config.roles) this.roles = config.roles;
    if (config.toolHistory) this.toolHistory = config.toolHistory;
    if (config.scan) this.scanConfig = config.scan;
    if (config.turnGate) this.turnGate = config.turnGate;
    if (config.workerId) this.workerId = config.workerId;
  }

  /**
   * Called by ZoryaScheduler when isAgentSchedule(schedule) is true. Thin
   * wrapper around dispatchAgentSchedule that hides the deps the helper
   * needs (registry + resolve) so the scheduler interface stays minimal.
   */
  async dispatchSchedule(tick: ScheduleTick, schedule: DurableScheduleConfig): Promise<void> {
    await dispatchAgentSchedule(tick, schedule, {
      registry: this.registry,
      resolve: this.resolve,
    });
  }

  /** Agent ids currently managed by a recipe file on disk. */
  fileManagedIds(): string[] {
    return [...this._fileManaged];
  }

  async start(): Promise<void> {
    if (this.scanHandle || !this.scanConfig) return;
    const userOnTick = this.scanConfig.onTick;
    this.scanHandle = startAgentsScanLoop({
      registry: this.registry,
      root: this.scanConfig.root,
      ...(this.scanConfig.intervalMs !== undefined && {
        intervalMs: this.scanConfig.intervalMs,
      }),
      ...(this.scanConfig.sync !== undefined && { sync: this.scanConfig.sync }),
      // Rebuild the file-managed set from each tick's discovered ids
      // (`upserted` = every recipe the scan applied, so a deleted file
      // drops out next tick), then fan out to the user hook. Mirrors
      // ZoryaSkills.start().
      onTick: (tick) => {
        this._fileManaged = new Set(tick.upserted);
        userOnTick?.(tick);
      },
    });
    // One immediate tick so the file-managed flag is available at boot
    // instead of after the first interval (which is 5s in the demo).
    await this.scanHandle.tick();
  }

  async stop(): Promise<void> {
    if (this.scanHandle) {
      this.scanHandle.stop();
      this.scanHandle = undefined;
    }
  }
}
