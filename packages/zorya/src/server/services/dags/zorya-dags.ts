// ---------------------------------------------------------------------------
// ZoryaDags — owns DagRegistry + agent resolver wiring for the DAG
// gateway. Composes with ZoryaAgents: each DAG node references an
// agentId, resolved through the agents service's registry and `resolve`
// (refusing disabled recipes), so DAG nodes run with the same
// credentials, tools and memory as direct invocations — and under the
// same scope authorization.
// ---------------------------------------------------------------------------

import { createRegistryAgentResolver, type AgentResolver, type DagRegistry } from "@promin/agent";
import type { WorkflowRunner } from "@promin/workflow";
import type { ZoryaAgents } from "../agents/zorya-agents.ts";

export interface ZoryaDagsConfig {
  registry: DagRegistry;
  /**
   * Workflow runner used to execute the durable DAG workflow. Typically
   * the same runner the rest of the host shares (so DAG runs show up
   * in the dashboard's runs list alongside everything else).
   */
  runner: WorkflowRunner;
  /**
   * The agents service DAG nodes run through: its registry and `resolve`
   * materialize each node, its instance registry places owner-scoped
   * runs, and its `authorizeScope` decides who may start a run.
   */
  agents: ZoryaAgents;
}

export class ZoryaDags {
  readonly registry: DagRegistry;
  readonly runner: WorkflowRunner;
  readonly agents: ZoryaAgents;
  readonly resolver: AgentResolver;

  constructor(config: ZoryaDagsConfig) {
    this.registry = config.registry;
    this.runner = config.runner;
    this.agents = config.agents;
    this.resolver = createRegistryAgentResolver({
      registry: config.agents.registry,
      resolve: config.agents.resolve,
    });
  }
}
