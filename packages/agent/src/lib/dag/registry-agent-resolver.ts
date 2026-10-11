// ---------------------------------------------------------------------------
// Registry-backed `AgentResolver` for DAG nodes: look the node's agent up in
// an `AgentRegistry`, refuse unknown and disabled (kill-switched) recipes,
// then materialize the recipe for the node's scope. The check runs when the
// node runs, so disabling an agent stops the DAG nodes that would call it
// next, even inside a run that already started.
// ---------------------------------------------------------------------------

import type { Agent, AgentScope } from "../agent/types.ts";
import type { AgentRegistry, RegisteredAgent } from "../registry/types.ts";
import { AgentRefusedError, type AgentResolver } from "./types.ts";

export function createRegistryAgentResolver(params: {
  readonly registry: AgentRegistry;
  /** Materialize a recipe; the same function the host uses for direct invocations. */
  readonly resolve: (recipe: RegisteredAgent, scope: AgentScope) => Agent | Promise<Agent>;
}): AgentResolver {
  const { registry, resolve } = params;
  return async ({ agentId, scope }) => {
    const recipe = await registry.get(agentId);
    if (!recipe) {
      throw new AgentRefusedError("agent_not_found", `no agent registered as "${agentId}"`);
    }
    if (recipe.metadata.enabled === false) {
      throw new AgentRefusedError("agent_disabled", `"${agentId}" is disabled`);
    }
    return resolve(recipe, scope);
  };
}
