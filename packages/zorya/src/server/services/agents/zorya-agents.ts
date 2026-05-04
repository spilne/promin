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
  MemoryStore,
  ModelCatalog,
  RegisteredAgent,
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
  resolve: (recipe: RegisteredAgent) => Agent;
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
  /** Filesystem hot-reload scan loop. Omit to disable. */
  scan?: ZoryaAgentsScanConfig;
}

export class ZoryaAgents implements AgentScheduleDispatcher {
  readonly registry: AgentRegistry;
  readonly resolve: (recipe: RegisteredAgent) => Agent;
  readonly memory?: MemoryStore;
  readonly instances?: AgentInstanceRegistry;
  readonly models?: ModelCatalog;
  private readonly scanConfig?: ZoryaAgentsScanConfig;
  private scanHandle?: { stop(): void };

  constructor(config: ZoryaAgentsConfig) {
    this.registry = config.registry;
    this.resolve = config.resolve;
    if (config.memory) this.memory = config.memory;
    if (config.instances) this.instances = config.instances;
    if (config.models) this.models = config.models;
    if (config.scan) this.scanConfig = config.scan;
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

  async start(): Promise<void> {
    if (this.scanHandle || !this.scanConfig) return;
    this.scanHandle = startAgentsScanLoop({
      registry: this.registry,
      root: this.scanConfig.root,
      ...(this.scanConfig.intervalMs !== undefined && {
        intervalMs: this.scanConfig.intervalMs,
      }),
      ...(this.scanConfig.sync !== undefined && { sync: this.scanConfig.sync }),
      ...(this.scanConfig.onTick !== undefined && { onTick: this.scanConfig.onTick }),
    });
  }

  async stop(): Promise<void> {
    if (this.scanHandle) {
      this.scanHandle.stop();
      this.scanHandle = undefined;
    }
  }
}
