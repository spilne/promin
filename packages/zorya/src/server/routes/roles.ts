// ---------------------------------------------------------------------------
// Role registry routes — author / edit / browse roles over HTTP. A role is
// the behavioral half of an agent: persona prompt (plain or layered over
// fragments) + tools + skills + capabilities. Agents bind a role by ref
// (shared, live) or inline (one-off). See ROLE_AGENT_MODEL.
//
// Routes:
//   GET    /api/roles              — list (filter by tag / capability)
//   GET    /api/roles/:id          — get one (latest version)
//   GET    /api/roles/:id/versions — all versions of an id
//   POST   /api/roles              — create a role
//   PATCH  /api/roles/:id          — update a role (republishes the row)
//   DELETE /api/roles/:id          — delete (all versions, or ?version=)
//
// Like skills, a role is pure data — there is no resolver here. An agent's
// resolver loads + expands the bound role at materialize time.
// ---------------------------------------------------------------------------

import type {
  RegisteredRole,
  RegisterRoleInput,
  RoleDefinition,
  RoleRegistry,
} from "@promin/agent";
import { json, jsonError, readJson } from "../router.ts";

// Re-export so the UI bundle types responses without importing @promin/agent
// directly (matches agents.ts / skills.ts).
export type { RegisteredRole, RoleDefinition, RoleMetadata } from "@promin/agent";

export interface RolesListResponse {
  roles: RegisteredRole[];
}

export interface RoleVersionsResponse {
  versions: RegisteredRole[];
}

export interface RoleGatewayDeps {
  readonly registry: RoleRegistry;
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function parseIntParam(v: string | null): number | undefined {
  if (v === null) return undefined;
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : undefined;
}

// ---------------------------------------------------------------------------
// List + get
// ---------------------------------------------------------------------------

export function listRoles(deps: RoleGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const capability = url.searchParams.get("capability") ?? undefined;
    const tag = url.searchParams.get("tag") ?? undefined;
    const limit = parseIntParam(url.searchParams.get("limit"));
    const cursor = url.searchParams.get("cursor") ?? undefined;
    const roles = await deps.registry.list({ capability, tag, limit, cursor });
    return json(200, { roles } satisfies RolesListResponse);
  };
}

export function getRole(deps: RoleGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    const role = await deps.registry.get(id, version);
    if (!role) return jsonError(404, "role_not_found", `Role "${id}" is not registered.`);
    return json(200, role);
  };
}

export function listRoleVersions(deps: RoleGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const versions = await deps.registry.versions(id);
    if (versions.length === 0) {
      return jsonError(404, "role_not_found", `Role "${id}" is not registered.`);
    }
    return json(200, { versions } satisfies RoleVersionsResponse);
  };
}

// ---------------------------------------------------------------------------
// Create / update / delete
// ---------------------------------------------------------------------------

interface CreateRoleRequest {
  readonly id?: unknown;
  readonly version?: unknown;
  readonly definition?: unknown;
  readonly metadata?: unknown;
}

/**
 * Validate + coerce a create/update body into a RegisterRoleInput. A role's
 * definition must carry a `systemPrompt` (string, layered object, or null)
 * and a `tools` array; `skills` / `capabilities` are optional.
 */
function parseRoleBody(
  body: CreateRoleRequest,
  id: string,
): { input: RegisterRoleInput } | { error: string; message: string } {
  if (typeof body.definition !== "object" || body.definition === null) {
    return { error: "missing_definition", message: "A role needs a `definition` object." };
  }
  const def = body.definition as Record<string, unknown>;
  // systemPrompt must be present (string | { base, layers? } | null).
  const sp = def.systemPrompt;
  const spOk =
    sp === null ||
    typeof sp === "string" ||
    (typeof sp === "object" && sp !== null && typeof (sp as { base?: unknown }).base === "string");
  if (!spOk) {
    return {
      error: "invalid_systemPrompt",
      message: "definition.systemPrompt must be a string, { base, layers? }, or null.",
    };
  }
  if (!Array.isArray(def.tools)) {
    return { error: "invalid_tools", message: "definition.tools must be an array of tool names." };
  }
  const version =
    typeof body.version === "string" && body.version.length > 0 ? body.version : undefined;
  const metadata =
    typeof body.metadata === "object" && body.metadata !== null
      ? (body.metadata as RegisterRoleInput["metadata"])
      : undefined;
  return {
    input: {
      id,
      definition: body.definition as RoleDefinition,
      ...(version !== undefined && { version }),
      ...(metadata !== undefined && { metadata }),
    },
  };
}

export function createRole(deps: RoleGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateRoleRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) return jsonError(400, "missing_id");
    if (body.id.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Role ids starting with `_` are reserved.");
    }
    const parsed = parseRoleBody(body, body.id);
    if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
    try {
      const role = await deps.registry.register(parsed.input);
      return json(201, role);
    } catch (err) {
      return jsonError(500, "create_failed", asMessage(err));
    }
  };
}

export function updateRole(deps: RoleGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<CreateRoleRequest>(req);
    if (!body) return jsonError(400, "missing_body");

    const version =
      typeof body.version === "string" && body.version.length > 0 ? body.version : undefined;
    const existing = await deps.registry.get(id, version);
    if (!existing) return jsonError(404, "role_not_found", `Role "${id}" is not registered.`);

    // Field-merge against the existing row so a partial PATCH (e.g. just
    // editing tags) doesn't blank out the definition.
    const merged: CreateRoleRequest = {
      version: existing.version,
      definition:
        typeof body.definition === "object" && body.definition !== null
          ? body.definition
          : existing.definition,
      metadata:
        typeof body.metadata === "object" && body.metadata !== null
          ? body.metadata
          : existing.metadata,
    };
    const parsed = parseRoleBody(merged, id);
    if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
    try {
      const updated = await deps.registry.register(parsed.input);
      return json(200, updated);
    } catch (err) {
      return jsonError(500, "update_failed", asMessage(err));
    }
  };
}

export function deleteRole(deps: RoleGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    try {
      await deps.registry.unregister(id, version);
      return new Response(null, { status: 204 });
    } catch (err) {
      return jsonError(500, "delete_failed", asMessage(err));
    }
  };
}
