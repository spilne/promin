// ---------------------------------------------------------------------------
// Tools a `LocalAgent` attaches to every turn on top of its own: the
// layered `memory` tool (thread turns), the agents-network `findAgent` /
// `callAgent` tools (recipes that opt in) and the retriever search tools.
// Host tools win on a name collision; the capability gate applies last.
// ---------------------------------------------------------------------------

import type { MemoryStore } from "../memory/types.ts";
import {
  createCallAgentTool,
  createFindAgentTool,
  type NetworkCallerScope,
  type NetworkRuntimeDeps,
} from "../network/runtime.ts";
import type { RegisteredAgent } from "../registry/types.ts";
import { filterToolsByCapability, type AgentTool } from "../tool.ts";
import { createLayeredMemoryTool } from "../tools/layered-memory-tools.ts";
import { withRetrieverTools, type AgentRetrievers } from "./retriever-bindings.ts";

/** Where a turn runs; the memory tool needs a thread, the network tools a namespace. */
export interface AutoToolScope {
  readonly namespaceId?: string;
  readonly resourceId?: string;
  readonly threadId?: string;
}

export interface AutoToolSources {
  readonly agentId: string;
  readonly memory?: MemoryStore;
  /** `false` keeps the memory tool off. */
  readonly autoMemoryTool?: boolean;
  readonly network?: {
    readonly deps: NetworkRuntimeDeps;
    readonly recipe: RegisteredAgent;
  };
  /** The resource the agent's owner was bound for, and that owner. */
  readonly boundResourceId?: string;
  readonly ownerId?: string;
  readonly retrievers?: AgentRetrievers;
  readonly capabilities?: ReadonlyArray<string>;
}

/** `userTools` plus the auto-attached tools for a turn in `scope`. */
export function withAutoTools(params: {
  readonly sources: AutoToolSources;
  readonly scope: AutoToolScope;
  readonly userTools: Record<string, AgentTool<any, any>>;
}): Record<string, AgentTool<any, any>> {
  const { sources, scope, userTools } = params;
  let tools: Record<string, AgentTool<any, any>> = userTools;

  // Auto-attached memory tool — needs a threadId, only the thread path
  // has one. One-shot invoke is stateless by design.
  if (
    scope.threadId &&
    scope.namespaceId &&
    sources.memory &&
    sources.autoMemoryTool !== false &&
    !("memory" in userTools)
  ) {
    const memoryTool = createLayeredMemoryTool({
      store: sources.memory,
      namespaceId: scope.namespaceId,
      resourceId: scope.resourceId,
      threadId: scope.threadId,
      agentId: sources.agentId,
    });
    tools = { ...tools, memory: memoryTool };
  }

  // Auto-attached network tools — recipe opt-in + host deps + a
  // resolved namespace. Built per-turn because they close over the
  // current scope.
  if (sources.network && scope.namespaceId) {
    const recipe = sources.network.recipe;
    const networkRecipe = recipe.backend.type === "local" ? recipe.backend.network : undefined;
    if (networkRecipe) {
      // The owner is the one the gateway bound with `withScope`, and only
      // while the turn runs at the resource it was bound for.
      const ownerId =
        sources.ownerId !== undefined && scope.resourceId === sources.boundResourceId
          ? sources.ownerId
          : undefined;
      const callerScope: NetworkCallerScope = {
        namespaceId: scope.namespaceId,
        ...(scope.resourceId !== undefined && { resourceId: scope.resourceId }),
        ...(ownerId !== undefined && { ownerId }),
      };
      const networkParams = {
        deps: sources.network.deps,
        scope: callerScope,
        callerRecipe: recipe,
        network: networkRecipe,
      };
      // Don't shadow user-supplied tools of the same name — host wins.
      const merged = { ...tools };
      if (!("findAgent" in merged)) merged.findAgent = createFindAgentTool(networkParams);
      if (!("callAgent" in merged)) merged.callAgent = createCallAgentTool(networkParams);
      tools = merged;
    }
  }

  // Retriever tools — one search tool per binding whose mode includes
  // `tool`. Host / recipe tools of the same name win.
  if (sources.retrievers) {
    tools = withRetrieverTools({ retrievers: sources.retrievers, tools });
  }

  // Capability gate — an elevated tool is exposed only when the
  // recipe's `capabilities` grant its `requires`. Applied last so the
  // auto-attached memory / network tools are gated too (they aren't
  // elevated today, so they pass through unchanged).
  return filterToolsByCapability(tools, sources.capabilities ?? []);
}
