// ---------------------------------------------------------------------------
// Agent identity routes — long-lived per-(agent, namespace, owner) records.
//
// Routes:
//   GET    /api/agents/:id/identities                       — list identities for this agent
//   POST   /api/agents/:id/identities                       — resolve-or-create
//   GET    /api/agents/:id/identities/:identityId           — get one
//   PATCH  /api/agents/:id/identities/:identityId           — update displayName / metadata
//   DELETE /api/agents/:id/identities/:identityId           — wipe (cascades to memory)
//   GET    /api/identities?namespaceId=&ownerId=            — cross-agent list
//
// `ownerId` is the entity an identity belongs to — a user, team, project,
// device, or any other addressable principal. Treated as opaque.
//
// Identity is metadata + a deterministic id; the actual chat state
// (working memory, facts, episodes, threads, messages) lives in the
// memory store under `resourceId = identity.id`. DELETE is the only
// operation that crosses the boundary — it cascades through both stores
// via `wipeAgentIdentity`.
// ---------------------------------------------------------------------------

import {
  wipeAgentIdentity,
  type AgentIdentity,
  type AgentIdentityRegistry,
  type MemoryStore,
} from "@promin/agent";
import { json, jsonError } from "../router.ts";

export interface IdentityDeps {
  readonly registry: AgentIdentityRegistry;
  readonly memory: MemoryStore;
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

export function listAgentIdentities(deps: IdentityDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const registeredAgentId = params.id;
    if (!registeredAgentId) return jsonError(400, "missing_agent_id");
    const url = new URL(req.url);
    const namespaceId = url.searchParams.get("namespaceId") ?? undefined;
    const ownerId = url.searchParams.get("ownerId") ?? undefined;
    const limit = parseLimit(url.searchParams.get("limit"));
    try {
      const list = await deps.registry.list({
        registeredAgentId,
        ...(namespaceId !== undefined ? { namespaceId } : {}),
        ...(ownerId !== undefined ? { ownerId } : {}),
        ...(limit !== undefined ? { limit } : {}),
      });
      return json(200, { identities: list.map(serialize) });
    } catch (err) {
      return jsonError(500, "list_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function resolveAgentIdentity(deps: IdentityDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const registeredAgentId = params.id;
    if (!registeredAgentId) return jsonError(400, "missing_agent_id");
    const body = (await req.json().catch(() => null)) as ResolveBody | null;
    if (!body) return jsonError(400, "invalid_body");
    if (typeof body.namespaceId !== "string" || body.namespaceId.length === 0) {
      return jsonError(400, "missing_namespaceId");
    }
    if (typeof body.ownerId !== "string" || body.ownerId.length === 0) {
      return jsonError(400, "missing_ownerId");
    }
    try {
      const identity = await deps.registry.resolveOrCreate({
        registeredAgentId,
        namespaceId: body.namespaceId,
        ownerId: body.ownerId,
        displayName: typeof body.displayName === "string" ? body.displayName : null,
        metadata:
          body.metadata !== undefined && typeof body.metadata === "object" && body.metadata !== null
            ? (body.metadata as Record<string, unknown>)
            : undefined,
      });
      return json(200, { identity: serialize(identity) });
    } catch (err) {
      return jsonError(500, "resolve_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function getAgentIdentity(deps: IdentityDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const identityId = params.identityId;
    if (!identityId) return jsonError(400, "missing_identityId");
    try {
      const identity = await deps.registry.get(identityId);
      if (!identity) return jsonError(404, "not_found");
      // Defense-in-depth: callers hitting this through /api/agents/:id/identities/:identityId
      // shouldn't see an identity from a different agent. Block the leak.
      const expectedAgent = params.id;
      if (expectedAgent && identity.registeredAgentId !== expectedAgent) {
        return jsonError(404, "not_found");
      }
      return json(200, { identity: serialize(identity) });
    } catch (err) {
      return jsonError(500, "get_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function updateAgentIdentity(deps: IdentityDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const identityId = params.identityId;
    if (!identityId) return jsonError(400, "missing_identityId");
    const body = (await req.json().catch(() => null)) as UpdateBody | null;
    if (!body) return jsonError(400, "invalid_body");

    const patch: { displayName?: string | null; metadata?: Record<string, unknown> } = {};
    if ("displayName" in body) {
      if (body.displayName !== null && typeof body.displayName !== "string") {
        return jsonError(400, "invalid_displayName");
      }
      patch.displayName = body.displayName ?? null;
    }
    if ("metadata" in body) {
      if (typeof body.metadata !== "object" || body.metadata === null) {
        return jsonError(400, "invalid_metadata");
      }
      patch.metadata = body.metadata as Record<string, unknown>;
    }
    if (Object.keys(patch).length === 0) return jsonError(400, "empty_patch");

    try {
      const updated = await deps.registry.update(identityId, patch);
      return json(200, { identity: serialize(updated) });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      // The InMemory impl throws "AgentIdentity not found: ..." — surface
      // that as a 404 rather than a 500.
      if (msg.toLowerCase().includes("not found")) return jsonError(404, "not_found");
      return jsonError(500, "update_failed", msg);
    }
  };
}

export function deleteAgentIdentity(deps: IdentityDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const identityId = params.identityId;
    if (!identityId) return jsonError(400, "missing_identityId");
    try {
      const result = await wipeAgentIdentity({
        registry: deps.registry,
        memory: deps.memory,
        identityId,
      });
      return json(200, result);
    } catch (err) {
      return jsonError(500, "delete_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function listIdentitiesAcrossAgents(deps: IdentityDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const namespaceId = url.searchParams.get("namespaceId") ?? undefined;
    const ownerId = url.searchParams.get("ownerId") ?? undefined;
    const limit = parseLimit(url.searchParams.get("limit"));
    try {
      const list = await deps.registry.list({
        ...(namespaceId !== undefined ? { namespaceId } : {}),
        ...(ownerId !== undefined ? { ownerId } : {}),
        ...(limit !== undefined ? { limit } : {}),
      });
      return json(200, { identities: list.map(serialize) });
    } catch (err) {
      return jsonError(500, "list_failed", err instanceof Error ? err.message : String(err));
    }
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
function serialize(identity: AgentIdentity): {
  id: string;
  registeredAgentId: string;
  namespaceId: string;
  ownerId: string;
  displayName: string | null;
  metadata: Record<string, unknown>;
  createdAt: number;
  lastActiveAt: number;
} {
  return {
    id: identity.id,
    registeredAgentId: identity.registeredAgentId,
    namespaceId: identity.namespaceId,
    ownerId: identity.ownerId,
    displayName: identity.displayName,
    metadata: { ...identity.metadata },
    createdAt: identity.createdAt,
    lastActiveAt: identity.lastActiveAt,
  };
}
