// ---------------------------------------------------------------------------
// Runtime dependencies + tool factories that the LocalAgent resolver
// uses to materialize `findAgent` and `callAgent` for a recipe that
// declared `backend.network`. `callAgent` is a `subAgentTool`: each call
// runs the peer's turn as a journaled child workflow of the caller's turn.
//
// Why factories and not pre-built tools
// -------------------------------------
// Both tools need to see the caller's scope (`namespaceId`, `ownerId`)
// and the caller's recipe (for the policy check). The scope is only
// known at `withScope()` time, not when the LocalAgent is resolved.
// So `LocalAgent.buildTools(key)` calls these factories per turn,
// closing over the current key.
// ---------------------------------------------------------------------------

import { z } from "zod";
import { tool, type AgentTool } from "../tool.ts";
import type { Agent, AgentScope } from "../agent/types.ts";
import { agentTurnWorkflow, subAgentTool } from "../delegation/sub-agent-tool.ts";
import { DEFAULT_MAX_DEPTH, DelegationCycleError, isDelegableAgent } from "../delegation/types.ts";
import type { AgentInstanceRegistry } from "../instance/types.ts";
import type { AgentRegistry, LocalAgentBackend, RegisteredAgent } from "../registry/types.ts";
import { inlineRoleDefinition } from "../role/resolve-role.ts";
import type { NetworkRecipe, NetworkScope, NetworkScopeObject, PeerView } from "./types.ts";
import {
  NetworkPermissionError,
  matchesNetworkScope,
  networksOverlap,
  peerVisible,
} from "./types.ts";

/**
 * Resolver-supplied wiring that lets `findAgent` / `callAgent` reach
 * the registry and the recursive resolver. The host builds this once
 * and hands the same value to every resolveLocalAgent call.
 */
export interface NetworkRuntimeDeps {
  readonly registry: AgentRegistry;
  /**
   * Materialize a callee `Agent` from its recipe for the scope it will run
   * in — the host's resolver for direct invocations, which may load roles
   * and credentials (BYOK) for that scope. Recursive: the callee's own
   * network tools come from the same resolver. A `LocalAgent` callee runs
   * its turn as a child workflow of the caller's; any other agent is
   * invoked inside the child.
   */
  readonly resolve: (recipe: RegisteredAgent, scope: AgentScope) => Promise<Agent>;
  /**
   * Optional. When set, `callAgent` propagates the caller's `ownerId`
   * into the callee's instance via
   * `instanceRegistry.resolveOrCreate({ registeredAgentId: callee.id,
   * namespaceId, ownerId })` and uses `instance.id` as the callee's
   * resourceId. Without it, the callee runs at the same raw resourceId
   * the caller had — fine for tests, less clean for multi-tenant
   * production.
   */
  readonly instanceRegistry?: AgentInstanceRegistry;
  /**
   * Optional host check run before every delegation, after the recipe
   * policy passed. Return `false` to refuse the call. Use it to bind
   * delegation to the same rules the host applies to direct invocations
   * (e.g. which agents a tenant may run).
   */
  readonly authorizeCall?: NetworkCallAuthorizer;
}

/** Host hook deciding whether one agent may delegate to another in a scope. */
export type NetworkCallAuthorizer = (params: {
  readonly callerAgentId: string;
  readonly calleeAgentId: string;
  /** The scope the callee will run in — the caller's own. */
  readonly scope: NetworkCallerScope;
  /** Agents waiting on this call, oldest first; ends with the caller. */
  readonly chain: ReadonlyArray<string>;
}) => boolean | Promise<boolean>;

/** Per-turn scope that the tools close over. Built by LocalAgent.buildTools. */
export interface NetworkCallerScope {
  readonly namespaceId: string;
  readonly resourceId?: string;
  /**
   * Owner the caller's `resourceId` was resolved for, as bound by the
   * authorizing gateway (`withScope({ ownerId })`). Used to run callees
   * under the same owner's instance. Without it the callee runs at the
   * caller's raw resourceId.
   */
  readonly ownerId?: string;
}

/**
 * Project a registered recipe into the minimal `PeerView` the policy
 * checks need. Recipes don't currently store a namespaceId on the
 * registry (one registry per tenant in the current design), so we use
 * the caller's namespaceId for the visibility check — every peer in
 * the registry the caller can see is in the caller's tenant by
 * construction.
 */
function asPeerView(recipe: RegisteredAgent, callerNamespaceId: string): PeerView {
  const network = (recipe.backend as LocalAgentBackend).network;
  return {
    id: recipe.id,
    namespaceId: callerNamespaceId,
    networks: network?.networks ?? [],
    capabilities: recipe.metadata.capabilities,
    tags: recipe.metadata.tags,
  };
}

// ---------------------------------------------------------------------------
// findAgent
// ---------------------------------------------------------------------------

interface FindAgentInput {
  capability?: string;
  tag?: string;
  query?: string;
  limit?: number;
}

interface FindAgentHit {
  id: string;
  description: string | null;
  capabilities: string[];
  tags: string[];
  tools: string[];
}

interface FindAgentOutput {
  hits: FindAgentHit[];
  total: number;
}

export function createFindAgentTool(args: {
  readonly deps: NetworkRuntimeDeps;
  readonly scope: NetworkCallerScope;
  readonly callerRecipe: RegisteredAgent;
  readonly network: NetworkRecipe;
}): AgentTool<FindAgentInput, FindAgentOutput> {
  const { deps, scope, callerRecipe, network } = args;

  return tool({
    name: "findAgent",
    description:
      "Search the agent network for peer agents available to you. Returns matching recipes with " +
      "id, description, capabilities, tools, and tags. Filtered to your namespace + the networks " +
      "you participate in + the policy your recipe declares.",
    parameters: z.object({
      capability: z
        .string()
        .optional()
        .describe("Match agents that expose this capability (exact, not substring)."),
      tag: z.string().optional().describe("Match agents tagged with this label."),
      query: z
        .string()
        .optional()
        .describe("Free-text substring match against agent id and description."),
      limit: z.number().int().positive().max(100).optional().describe("Max hits. Default 25."),
    }),
    execute: async (input): Promise<FindAgentOutput> => {
      const all = await deps.registry.list({
        ...(input.capability !== undefined ? { capability: input.capability } : {}),
        ...(input.tag !== undefined ? { tag: input.tag } : {}),
        limit: 1000,
      });

      const visible = all.filter((p) => {
        if (p.id === callerRecipe.id) return false; // never list self
        // A disabled (kill-switched) agent can't be called, so don't offer it.
        if (p.metadata.enabled === false) return false;
        return peerVisible({
          callerNamespaceId: scope.namespaceId,
          callerNetworks: network.networks,
          callerScope: network.canDiscover,
          peer: asPeerView(p, scope.namespaceId),
        });
      });

      const q = input.query?.toLowerCase().trim();
      const matchesQuery = q
        ? (p: RegisteredAgent) =>
            p.id.toLowerCase().includes(q) ||
            (p.metadata.description?.toLowerCase().includes(q) ?? false)
        : () => true;

      const filtered = visible.filter(matchesQuery);
      const limit = input.limit ?? 25;
      const hits: FindAgentHit[] = filtered.slice(0, limit).map((p) => ({
        id: p.id,
        description: p.metadata.description ?? null,
        capabilities: [...p.metadata.capabilities],
        tags: [...p.metadata.tags],
        tools:
          p.backend.type === "local"
            ? [...(inlineRoleDefinition(p.backend.role)?.tools ?? [])]
            : [],
      }));
      return { hits, total: filtered.length };
    },
  });
}

// ---------------------------------------------------------------------------
// callAgent
// ---------------------------------------------------------------------------

interface CallAgentInput {
  id: string;
  prompt: string;
}

/** Where a routed `callAgent` call runs the callee; journaled with the route. */
interface CallAgentRouteData {
  readonly scope: AgentScope;
  /** The recipe version the call was routed to. */
  readonly version: string;
  /** How the peer runs: its own turn as the child, or invoked inside it. */
  readonly kind: "turn" | "invoke";
}

/**
 * The agents network's sub-agent tool: `callAgent({ id, prompt })` runs
 * one turn of the peer `id` as a child workflow of the caller's turn (see
 * `subAgentTool`). The call is refused — as a tool error — when the peer
 * is unknown, disabled, already waiting on the call chain, deeper than the
 * recipe's `maxDepth`, outside the caller's `canCall` policy, or refused by
 * the host's `authorizeCall`. The peer runs in the caller's namespace,
 * under the caller's owner's instance of the peer when the caller has an
 * owner, with its own tools, prompt and model.
 */
export function createCallAgentTool(args: {
  readonly deps: NetworkRuntimeDeps;
  readonly scope: NetworkCallerScope;
  readonly callerRecipe: RegisteredAgent;
  readonly network: NetworkRecipe;
}): AgentTool<CallAgentInput, string> {
  const { deps, scope, callerRecipe, network } = args;

  /** The scope a callee runs in: the caller's, at the owner's instance when there is one. */
  async function calleeScope(callee: RegisteredAgent): Promise<AgentScope> {
    if (deps.instanceRegistry && scope.ownerId !== undefined) {
      const instance = await deps.instanceRegistry.resolveOrCreate({
        registeredAgentId: callee.id,
        namespaceId: scope.namespaceId,
        ownerId: scope.ownerId,
      });
      return { namespaceId: scope.namespaceId, resourceId: instance.id, ownerId: scope.ownerId };
    }
    return {
      namespaceId: scope.namespaceId,
      ...(scope.resourceId !== undefined && { resourceId: scope.resourceId }),
      ...(scope.ownerId !== undefined && { ownerId: scope.ownerId }),
    };
  }

  /** The callee's recipe, if it may still run. */
  async function enabledRecipe(id: string): Promise<RegisteredAgent | string> {
    const recipe = await deps.registry.get(id);
    if (!recipe) return `agent_not_found: no recipe registered as "${id}"`;
    if (recipe.metadata.enabled === false) return `agent_disabled: "${recipe.id}" is disabled`;
    return recipe;
  }

  return subAgentTool({
    name: "callAgent",
    description:
      "Delegate a task to a peer agent. The peer runs one full turn with its own tool kit + " +
      "system prompt + model, and its answer is returned. Your owner identity is " +
      "propagated so the peer's working memory is scoped to the same end-user. The peer cannot " +
      "see your conversation history, only the prompt you pass here.",
    parameters: z.object({
      id: z.string().min(1).describe("Recipe id of the peer to call. Use findAgent to discover."),
      prompt: z.string().min(1).describe("The task / question to send to the peer."),
    }),
    maxDepth: network.maxDepth ?? DEFAULT_MAX_DEPTH,
    route: async ({ input, frame }) => {
      // The frame is the one the peer would run in; its chain ends with
      // the caller, so a peer already on it (the caller included) is a cycle.
      if (frame.chain.includes(input.id)) {
        return { ok: false, error: new DelegationCycleError(input.id, frame.chain).message };
      }
      const recipe = await enabledRecipe(input.id);
      if (typeof recipe === "string") return { ok: false, error: recipe };

      const peerView = asPeerView(recipe, scope.namespaceId);
      const allowed = peerVisible({
        callerNamespaceId: scope.namespaceId,
        callerNetworks: network.networks,
        callerScope: network.canCall,
        peer: peerView,
      });
      if (!allowed) {
        const reason = explainDenial({ network, peer: peerView });
        const err = new NetworkPermissionError(callerRecipe.id, recipe.id, reason);
        return { ok: false, error: err.message };
      }
      if (deps.authorizeCall) {
        const authorized = await deps.authorizeCall({
          callerAgentId: callerRecipe.id,
          calleeAgentId: recipe.id,
          scope,
          chain: frame.chain,
        });
        if (!authorized) {
          const err = new NetworkPermissionError(callerRecipe.id, recipe.id, "refused by the host");
          return { ok: false, error: err.message };
        }
      }

      const runScope = await calleeScope(recipe);
      const agent = (await deps.resolve(recipe, runScope)).withScope(runScope);
      const input_ = {
        task: input.prompt,
        source: { kind: "agent" as const, callerAgentId: callerRecipe.id },
      };
      const turnInput = isDelegableAgent(agent)
        ? (await agent.prepareTurn(input_)).input
        : { task: input.prompt };
      const data: CallAgentRouteData = {
        scope: runScope,
        version: recipe.version,
        kind: isDelegableAgent(agent) ? "turn" : "invoke",
      };
      return { ok: true, route: { agentId: recipe.id, input: turnInput, data: { ...data } } };
    },
    load: async ({ route, signal, wiring }) => {
      // Checked again when the peer (re)starts: a peer disabled since the
      // call was routed does not run.
      const latest = await enabledRecipe(route.agentId);
      if (typeof latest === "string") throw new Error(latest);
      // The peer runs as the version the call was routed to: a child run
      // re-attached after a crash is never driven by another definition.
      const data = route.data as unknown as CallAgentRouteData;
      const recipe = await deps.registry.get(route.agentId, data.version);
      if (!recipe) {
        throw new Error(
          `agent_version_gone: "${route.agentId}" version ${data.version} is no longer registered`,
        );
      }
      const agent = (await deps.resolve(recipe, data.scope)).withScope(data.scope);
      const kind = isDelegableAgent(agent) ? "turn" : "invoke";
      if (kind !== data.kind) {
        throw new Error(
          `agent_backend_changed: "${route.agentId}" was routed as ${data.kind}, now resolves as ${kind}`,
        );
      }
      return agentTurnWorkflow({ agent, agentId: recipe.id, signal, wiring });
    },
  });
}

function explainDenial(params: { network: NetworkRecipe; peer: PeerView }): string {
  const { network, peer } = params;
  if (network.canCall === undefined || network.canCall === false) {
    return "your recipe does not declare canCall";
  }
  if (!networksOverlap(network.networks, peer.networks)) {
    return "no shared network membership";
  }
  if (!matchesNetworkScope(network.canCall, peer)) {
    return `peer does not match canCall policy (${describeScope(network.canCall)})`;
  }
  return "denied";
}

function describeScope(scope: NetworkScope): string {
  if (scope === true) return "true";
  if (scope === false) return "false";
  if (Array.isArray(scope)) return `[${scope.join(", ")}]`;
  const obj = scope as NetworkScopeObject;
  const parts: string[] = [];
  if (obj.ids?.length) parts.push(`ids=${obj.ids.join(",")}`);
  if (obj.capabilities?.length) parts.push(`caps=${obj.capabilities.join(",")}`);
  if (obj.tags?.length) parts.push(`tags=${obj.tags.join(",")}`);
  if (obj.excludeIds?.length) parts.push(`exclude=${obj.excludeIds.join(",")}`);
  return parts.length > 0 ? `{ ${parts.join("; ")} }` : "(empty)";
}
