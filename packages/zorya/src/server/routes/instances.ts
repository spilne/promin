// ---------------------------------------------------------------------------
// Agent instance routes — long-lived per-(agent, namespace, owner) records.
//
// Routes:
//   GET    /api/agents/:id/instances                       — list instances for this agent
//   POST   /api/agents/:id/instances                       — resolve-or-create
//   GET    /api/agents/:id/instances/:instanceId           — get one
//   PATCH  /api/agents/:id/instances/:instanceId           — update displayName / metadata
//   DELETE /api/agents/:id/instances/:instanceId           — wipe (cascades to memory)
//   GET    /api/instances?namespaceId=&ownerId=            — cross-agent list
//
// `ownerId` is the entity an instance belongs to — a user, team, project,
// device, or any other addressable principal. Treated as opaque.
//
// Instance is metadata + a deterministic id; the actual chat state
// (working memory, facts, episodes, threads, messages) lives in the
// memory store under `resourceId = instance.id`. DELETE is the only
// operation that crosses the boundary — it cascades through both stores
// via `wipeAgentInstance`.
//
// Every `/api/agents/:id/instances/:instanceId` route answers 404 for an
// instance that belongs to a different agent, so the path's agent id can
// never be used to read, edit or wipe another agent's instance.
// ---------------------------------------------------------------------------

import {
  wipeAgentInstance,
  type AgentInstance,
  type AgentInstanceRegistry,
  type AgentRegistry,
  type MemoryStore,
} from "@promin/agent";
import type { NamespaceRegistry } from "@promin/workflow";
import { json, jsonError } from "../router.ts";
import { requireActiveNamespace } from "../namespace-guard.ts";
import { internalError, type ScopeAuthorizer } from "./agent-gateway-kit.ts";

export interface InstanceDeps {
  readonly registry: AgentInstanceRegistry;
  readonly memory: MemoryStore;
  /** Agent recipes — creating an instance of an unknown agent is a 404. */
  readonly agents: AgentRegistry;
  /** Binds the requested (namespace, owner) to the caller. See `ScopeAuthorizer`. */
  readonly authorizeScope?: ScopeAuthorizer;
  /**
   * When set, creating an instance needs a registered, active namespace
   * (404 `namespace_not_found`, 409 `namespace_archived`).
   */
  readonly namespaces?: NamespaceRegistry;
}

interface ResolveBody {
  readonly namespaceId?: unknown;
  readonly ownerId?: unknown;
  readonly displayName?: unknown;
  readonly metadata?: unknown;
}

interface UpdateBody {
  readonly displayName?: unknown;
  readonly metadata?: unknown;
}

type InstanceScope = Pick<AgentInstance, "namespaceId" | "ownerId" | "registeredAgentId">;

async function authorize(params: {
  deps: InstanceDeps;
  req: Request;
  instance: InstanceScope;
}): Promise<Response | null> {
  const { deps, req, instance } = params;
  if (!deps.authorizeScope) return null;
  const allowed = await deps.authorizeScope({
    req,
    scope: {
      namespaceId: instance.namespaceId,
      ownerId: instance.ownerId,
      agentId: instance.registeredAgentId,
    },
  });
  return allowed ? null : jsonError(403, "scope_forbidden");
}

/** The path's instance, when it belongs to the path's agent and the caller may see it. */
async function loadOwnedInstance(params: {
  deps: InstanceDeps;
  req: Request;
  routeParams: Record<string, string>;
}): Promise<AgentInstance | Response> {
  const { deps, req, routeParams } = params;
  const agentId = routeParams.id;
  const instanceId = routeParams.instanceId;
  if (!agentId) return jsonError(400, "missing_agent_id");
  if (!instanceId) return jsonError(400, "missing_instanceId");
  const instance = await deps.registry.get(instanceId);
  if (!instance || instance.registeredAgentId !== agentId) return jsonError(404, "not_found");
  const denied = await authorize({ deps, req, instance });
  return denied ?? instance;
}

export function listAgentInstances(deps: InstanceDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const registeredAgentId = params.id;
    if (!registeredAgentId) return jsonError(400, "missing_agent_id");
    const url = new URL(req.url);
    const namespaceId = url.searchParams.get("namespaceId") ?? undefined;
    const ownerId = url.searchParams.get("ownerId") ?? undefined;
    const limit = parseLimit(url.searchParams.get("limit"));
    const list = await deps.registry.list({
      registeredAgentId,
      ...(namespaceId !== undefined ? { namespaceId } : {}),
      ...(ownerId !== undefined ? { ownerId } : {}),
      ...(limit !== undefined ? { limit } : {}),
    });
    return json(200, { instances: list.map(serialize) });
  };
}

export function resolveAgentInstance(deps: InstanceDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const registeredAgentId = params.id;
    if (!registeredAgentId) return jsonError(400, "missing_agent_id");
    const body = (await req.json().catch(() => null)) as ResolveBody | null;
    if (!body || typeof body !== "object") return jsonError(400, "invalid_body");
    if (typeof body.namespaceId !== "string" || body.namespaceId.length === 0) {
      return jsonError(400, "missing_namespaceId");
    }
    if (typeof body.ownerId !== "string" || body.ownerId.length === 0) {
      return jsonError(400, "missing_ownerId");
    }
    if (body.displayName !== undefined && body.displayName !== null) {
      if (typeof body.displayName !== "string") return jsonError(400, "invalid_displayName");
    }
    if (
      body.metadata !== undefined &&
      (typeof body.metadata !== "object" || body.metadata === null || Array.isArray(body.metadata))
    ) {
      return jsonError(400, "invalid_metadata");
    }
    if (!(await deps.agents.get(registeredAgentId))) {
      return jsonError(404, "agent_not_found", `Agent "${registeredAgentId}" is not registered.`);
    }
    const denied = await authorize({
      deps,
      req,
      instance: { namespaceId: body.namespaceId, ownerId: body.ownerId, registeredAgentId },
    });
    if (denied) return denied;
    await requireActiveNamespace({ registry: deps.namespaces, namespace: body.namespaceId });
    const instance = await deps.registry.resolveOrCreate({
      registeredAgentId,
      namespaceId: body.namespaceId,
      ownerId: body.ownerId,
      displayName: typeof body.displayName === "string" ? body.displayName : null,
      metadata: body.metadata as Record<string, unknown> | undefined,
    });
    return json(200, { instance: serialize(instance) });
  };
}

export function getAgentInstance(deps: InstanceDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const instance = await loadOwnedInstance({ deps, req, routeParams: params });
    if (instance instanceof Response) return instance;
    return json(200, { instance: serialize(instance) });
  };
}

export function updateAgentInstance(deps: InstanceDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = (await req.json().catch(() => null)) as UpdateBody | null;
    if (!body || typeof body !== "object") return jsonError(400, "invalid_body");

    const patch: { displayName?: string | null; metadata?: Record<string, unknown> } = {};
    if ("displayName" in body) {
      if (body.displayName !== null && typeof body.displayName !== "string") {
        return jsonError(400, "invalid_displayName");
      }
      patch.displayName = body.displayName ?? null;
    }
    if ("metadata" in body) {
      if (
        typeof body.metadata !== "object" ||
        body.metadata === null ||
        Array.isArray(body.metadata)
      ) {
        return jsonError(400, "invalid_metadata");
      }
      patch.metadata = body.metadata as Record<string, unknown>;
    }
    if (Object.keys(patch).length === 0) return jsonError(400, "empty_patch");

    const owned = await loadOwnedInstance({ deps, req, routeParams: params });
    if (owned instanceof Response) return owned;
    try {
      const updated = await deps.registry.update(owned.id, patch);
      return json(200, { instance: serialize(updated) });
    } catch (err) {
      // Deleted between the ownership check and the write.
      if (!(await deps.registry.get(owned.id))) return jsonError(404, "not_found");
      return internalError({ code: "update_failed", err });
    }
  };
}

export function deleteAgentInstance(deps: InstanceDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const owned = await loadOwnedInstance({ deps, req, routeParams: params });
    if (owned instanceof Response) return owned;
    const result = await wipeAgentInstance({
      registry: deps.registry,
      memory: deps.memory,
      instanceId: owned.id,
    });
    return json(200, result);
  };
}

export function listInstancesAcrossAgents(deps: InstanceDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const namespaceId = url.searchParams.get("namespaceId") ?? undefined;
    const ownerId = url.searchParams.get("ownerId") ?? undefined;
    const limit = parseLimit(url.searchParams.get("limit"));
    const list = await deps.registry.list({
      ...(namespaceId !== undefined ? { namespaceId } : {}),
      ...(ownerId !== undefined ? { ownerId } : {}),
      ...(limit !== undefined ? { limit } : {}),
    });
    return json(200, { instances: list.map(serialize) });
  };
}

function parseLimit(raw: string | null): number | undefined {
  if (raw === null) return undefined;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n) || n <= 0) return undefined;
  return Math.min(n, 1_000);
}

// Stable serialization — keeps the wire shape independent of any extra
// fields the registry impl might attach internally. Also allows future
// implementations to project lazy fields without breaking clients.
function serialize(instance: AgentInstance): {
  id: string;
  registeredAgentId: string;
  namespaceId: string;
  ownerId: string;
  displayName: string | null;
  metadata: Record<string, unknown>;
  createdAt: number;
} {
  return {
    id: instance.id,
    registeredAgentId: instance.registeredAgentId,
    namespaceId: instance.namespaceId,
    ownerId: instance.ownerId,
    displayName: instance.displayName,
    metadata: { ...instance.metadata },
    createdAt: instance.createdAt,
  };
}
