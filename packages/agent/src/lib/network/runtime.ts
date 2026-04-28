// ---------------------------------------------------------------------------
// Runtime dependencies + tool factories that the LocalAgent resolver
// uses to materialize `findAgent` and `callAgent` for a recipe that
// declared `backend.network`.
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
import type { Agent } from "../agent/types.ts";
import type { AgentInstanceRegistry } from "../instance/types.ts";
import type { AgentRegistry, LocalAgentBackend, RegisteredAgent } from "../registry/types.ts";
import type { NetworkRecipe, NetworkScope, NetworkScopeObject, PeerView } from "./types.ts";
import {
  DEFAULT_MAX_DEPTH,
  NetworkMaxDepthError,
  NetworkPermissionError,
  matchesNetworkScope,
  networksOverlap,
  peerVisible,
} from "./types.ts";
import { currentCallContext, nextCallFrame, runInCallContext } from "./depth.ts";

/**
 * Resolver-supplied wiring that lets `findAgent` / `callAgent` reach
 * the registry and the recursive resolver. The host builds this once
 * and hands the same value to every resolveLocalAgent call.
 */
export interface NetworkRuntimeDeps {
  readonly registry: AgentRegistry;
  /**
   * Materialize a callee `Agent` from its recipe. Recursive — typically
   * the same factory the host wraps with `resolveLocalAgent(...)`.
   * Forward-declared via a function reference so the resolver and the
   * tool can reach back into the resolver without a circular module
   * import.
   */
  readonly resolve: (recipe: RegisteredAgent) => Agent;
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
}

/** Per-turn scope that the tools close over. Built by LocalAgent.buildTools. */
export interface NetworkCallerScope {
  readonly namespaceId: string;
  readonly resourceId?: string;
  /**
   * Owner inferred from `resourceId`. Used to propagate ownership to
   * callees via the instance registry. When the resourceId isn't a
   * composed instance id, owner stays undefined and the callee runs at
   * the same raw resourceId.
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
        tools: p.backend.type === "local" ? [...p.backend.tools] : [],
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

interface CallAgentOutput {
  ok: boolean;
  text?: string;
  finishReason?: string;
  usage?: {
    inputTokens: number;
    outputTokens: number;
    cacheReadTokens?: number;
    cacheWriteTokens?: number;
  };
  resolvedResourceId?: string;
  error?: string;
}

export function createCallAgentTool(args: {
  readonly deps: NetworkRuntimeDeps;
  readonly scope: NetworkCallerScope;
  readonly callerRecipe: RegisteredAgent;
  readonly network: NetworkRecipe;
}): AgentTool<CallAgentInput, CallAgentOutput> {
  const { deps, scope, callerRecipe, network } = args;
  const maxDepth = network.maxDepth ?? DEFAULT_MAX_DEPTH;

  return tool({
    name: "callAgent",
    description:
      "Delegate a task to a peer agent. The peer runs one full turn with its own tool kit + " +
      "system prompt + model, and the assistant text is returned. Your owner identity is " +
      "propagated so the peer's working memory is scoped to the same end-user. The peer cannot " +
      "see your conversation history, only the prompt you pass here.",
    parameters: z.object({
      id: z.string().min(1).describe("Recipe id of the peer to call. Use findAgent to discover."),
      prompt: z.string().min(1).describe("The task / question to send to the peer."),
    }),
    execute: async (input): Promise<CallAgentOutput> => {
      // Cycle / depth guard. nextCallFrame reads the current ALS frame
      // and returns what the next one would be — which is what we open
      // around the callee's invoke().
      const nextFrame = nextCallFrame(callerRecipe.id);
      if (nextFrame.depth > maxDepth) {
        const err = new NetworkMaxDepthError(maxDepth, nextFrame.depth, nextFrame.chain);
        return { ok: false, error: err.message };
      }

      const calleeRecipe = await deps.registry.get(input.id);
      if (!calleeRecipe) {
        return { ok: false, error: `agent_not_found: no recipe registered as "${input.id}"` };
      }

      const peerView = asPeerView(calleeRecipe, scope.namespaceId);
      const allowed = peerVisible({
        callerNamespaceId: scope.namespaceId,
        callerNetworks: network.networks,
        callerScope: network.canCall,
        peer: peerView,
      });
      if (!allowed) {
        const reason = explainDenial(network, peerView, currentCallContext());
        const err = new NetworkPermissionError(callerRecipe.id, calleeRecipe.id, reason);
        return { ok: false, error: err.message };
      }

      // Owner propagation: if we have an instance registry AND the caller
      // has an inferred owner, resolve the callee's instance under the
      // caller's owner. Otherwise, the callee runs at the same raw
      // resourceId the caller had (fine for tests / non-instance flows).
      let calleeResourceId: string | undefined = scope.resourceId;
      if (deps.instanceRegistry && scope.ownerId !== undefined) {
        const instance = await deps.instanceRegistry.resolveOrCreate({
          registeredAgentId: calleeRecipe.id,
          namespaceId: scope.namespaceId,
          ownerId: scope.ownerId,
        });
        calleeResourceId = instance.id;
      }

      // Materialize + scope the callee, then invoke inside the next
      // frame. ALS propagates through the await automatically — nested
      // callAgent invocations during this callee's turn see depth+1.
      const calleeAgent = deps.resolve(calleeRecipe);
      const scoped = calleeAgent.withScope({
        namespaceId: scope.namespaceId,
        resourceId: calleeResourceId,
      });

      try {
        return await runInCallContext(nextFrame, async () => {
          const out = await scoped.invoke({ task: input.prompt });
          const text = await out.text;
          const finishReason = await out.finishReason;
          const usage = await out.usage;
          return {
            ok: true,
            text,
            finishReason,
            usage,
            ...(calleeResourceId !== undefined ? { resolvedResourceId: calleeResourceId } : {}),
          };
        });
      } catch (err) {
        return {
          ok: false,
          error: err instanceof Error ? err.message : String(err),
        };
      }
    },
  });
}

function explainDenial(
  network: NetworkRecipe,
  peer: PeerView,
  ctx: { chain: ReadonlyArray<string> } | undefined,
): string {
  if (network.canCall === undefined || network.canCall === false) {
    return "your recipe does not declare canCall";
  }
  if (!networksOverlap(network.networks, peer.networks)) {
    return "no shared network membership";
  }
  if (!matchesNetworkScope(network.canCall, peer)) {
    return `peer does not match canCall policy (${describeScope(network.canCall)})`;
  }
  if (ctx && ctx.chain.includes(peer.id)) {
    return "would create a cycle";
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
