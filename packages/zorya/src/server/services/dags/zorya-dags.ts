// ---------------------------------------------------------------------------
// ZoryaDags — owns DagRegistry + agent resolver wiring for the DAG
// gateway. Composes with ZoryaAgents: each DAG node references an
// agentId, and the DAG executor resolves it via this service's
// resolveAgent — typically delegating to ZoryaAgents.resolve under
// the hood.
// ---------------------------------------------------------------------------

import type { Agent, AgentResolver, DagRegistry } from "@promin/agent";
import type { WorkflowRunner } from "@promin/workflow";

export interface ZoryaDagsConfig {
  registry: DagRegistry;
  /**
   * Workflow runner used to execute the durable DAG workflow. Typically
   * the same runner the rest of the host shares (so DAG runs show up
   * in the dashboard's runs list alongside everything else).
   */
  runner: WorkflowRunner;
  /**
   * Resolve a node's `agentId` (and optional version) to an Agent
   * instance. Hosts typically build this by closing over
   * `ZoryaAgents.registry.get(id, version)` + `ZoryaAgents.resolve(recipe)`
   * so the DAG layer doesn't need to know about agent recipe shapes.
   */
  resolveAgent: (agentId: string, version?: string) => Promise<Agent>;
}

export class ZoryaDags {
  readonly registry: DagRegistry;
  readonly runner: WorkflowRunner;
  readonly resolver: AgentResolver;

  constructor(config: ZoryaDagsConfig) {
    this.registry = config.registry;
    this.runner = config.runner;
    this.resolver = config.resolveAgent;
  }
}
