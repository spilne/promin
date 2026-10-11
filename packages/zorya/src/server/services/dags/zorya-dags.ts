// ---------------------------------------------------------------------------
// ZoryaDags — owns DagRegistry + agent resolver wiring for the DAG
// gateway. Composes with ZoryaAgents: each DAG node references an
// agentId, resolved through the agents service's registry and `resolve`
// (refusing disabled recipes), so DAG nodes run with the same
// credentials, tools and memory as direct invocations — and under the
// same scope authorization.
//
// The DAG workflow is defined once, here, on the host's in-process
// workflows under a fixed name: runs start through it, show up in the
// runs list (nodes as their child runs), and recovery resumes them by
// name after a restart.
// ---------------------------------------------------------------------------

import {
  createDagWorkflow,
  createRegistryAgentResolver,
  DAG_WORKFLOW_NAME,
  type AgentResolver,
  type DagRegistry,
  type DurableDagInput,
  type DurableDagOutput,
} from "@promin/agent";
import type { Workflow } from "@promin/workflow";
import type { ZoryaAgents } from "../agents/zorya-agents.ts";
import type { LocalWorkflows } from "../workflows/local-workflows.ts";

export interface ZoryaDagsConfig {
  registry: DagRegistry;
  /**
   * In-process workflows the DAG workflow is defined on (`define`). Runs
   * start, recover and show in the runs list through them — use the same
   * instance the server mounts.
   */
  workflows: LocalWorkflows;
  /**
   * The agents service DAG nodes run through: its registry and `resolve`
   * materialize each node, its instance registry places owner-scoped
   * runs, and its `authorizeScope` decides who may start a run.
   */
  agents: ZoryaAgents;
}

export class ZoryaDags {
  readonly registry: DagRegistry;
  readonly workflows: LocalWorkflows;
  readonly agents: ZoryaAgents;
  readonly resolver: AgentResolver;
  /** The DAG workflow, defined on `workflows` under `DAG_WORKFLOW_NAME`. */
  readonly workflow: Workflow<DurableDagInput, DurableDagOutput>;

  constructor(config: ZoryaDagsConfig) {
    this.registry = config.registry;
    this.workflows = config.workflows;
    this.agents = config.agents;
    this.resolver = createRegistryAgentResolver({
      registry: config.agents.registry,
      resolve: config.agents.resolve,
    });
    this.workflow = createDagWorkflow({
      registry: config.registry,
      resolver: this.resolver,
      ...(config.agents.instances !== undefined && { instances: config.agents.instances }),
      name: DAG_WORKFLOW_NAME,
    });
    // Internal: runs start through the DAG route only, after its scope checks.
    config.workflows.define(this.workflow as Workflow<unknown, unknown>, { internal: true });
  }
}
