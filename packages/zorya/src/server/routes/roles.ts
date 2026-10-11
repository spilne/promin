// ---------------------------------------------------------------------------
// Role registry routes — author / edit / browse roles over HTTP. A role is
// the behavioral half of an agent: persona prompt (plain or layered over
// fragments) + tools + skills + capabilities. Agents bind a role by ref
// (shared, live) or inline (one-off). See ROLE_AGENT_MODEL.
//
// Routes:
//   GET    /api/roles              — list (filter by tag / capability)
//   GET    /api/roles/:id          — get one (active version)
//   GET    /api/roles/:id/versions — all versions of an id
//   POST   /api/roles              — create a role (409 when (id, version) exists)
//   PATCH  /api/roles/:id          — edit the active version (metadata merges);
//                                    `If-Match` (the GET `etag`) → 412 on mismatch
//   DELETE /api/roles/:id          — delete (all versions, or ?version=);
//                                    409 while agents bind it, unless ?force=true
//
// Like skills, a role is pure data — there is no resolver here. An agent's
// resolver loads + expands the bound role at materialize time.
// ---------------------------------------------------------------------------

import type {
  AgentRegistry,
  RegisteredRole,
  RegisterRoleInput,
  RoleDefinition,
  RoleMetadata,
  RoleRegistry,
} from "@promin/agent";
import { DEFAULT_ROLE_VERSION } from "@promin/agent";
import { z } from "zod";
import { json, jsonError, parseIntParam, readJson } from "../router.ts";
import { assertIfMatch, contentEtag, withEtag } from "../preconditions.ts";

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
  /** Agent recipes — a role still bound by an agent is not deleted silently. */
  readonly agents?: AgentRegistry;
}

/** Limit-style query parameter: an integer clamped to [1, 1000]. */
function parseLimitParam(raw: string | null): number | undefined {
  return parseIntParam({ raw, min: 1, max: 1_000 });
}

// ---------------------------------------------------------------------------
// List + get
// ---------------------------------------------------------------------------

export function listRoles(deps: RoleGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const capability = url.searchParams.get("capability") ?? undefined;
    const tag = url.searchParams.get("tag") ?? undefined;
    const limit = parseLimitParam(url.searchParams.get("limit"));
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
    if (!role) return roleNotFound(id);
    return roleResponse(200, role);
  };
}

export function listRoleVersions(deps: RoleGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const versions = await deps.registry.versions(id);
    if (versions.length === 0) return roleNotFound(id);
    return json(200, { versions } satisfies RoleVersionsResponse);
  };
}

/** A role row with its entity tag (`If-Match` on PATCH compares against it). */
function roleResponse(status: number, role: RegisteredRole): Response {
  return withEtag(json(status, role), contentEtag(role));
}

function roleNotFound(id: string): Response {
  return jsonError(404, "role_not_found", `Role "${id}" is not registered.`);
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

const RoleMetadataPatchSchema = z
  .object({
    description: z.string().nullable().optional(),
    tags: z.array(z.string()).optional(),
    suggestedSecrets: z.array(z.string()).optional(),
  })
  .strict();

/**
 * Validate a role definition. It must carry a `systemPrompt` (string,
 * layered object, or null) and a `tools` array; `skills` / `capabilities`
 * are optional.
 */
function parseDefinition(
  raw: unknown,
): { definition: RoleDefinition } | { error: string; message: string } {
  if (typeof raw !== "object" || raw === null) {
    return { error: "missing_definition", message: "A role needs a `definition` object." };
  }
  const def = raw as Record<string, unknown>;
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
  if (!Array.isArray(def.tools) || def.tools.some((t) => typeof t !== "string")) {
    return { error: "invalid_tools", message: "definition.tools must be an array of tool names." };
  }
  return { definition: raw as RoleDefinition };
}

function parseMetadata(
  raw: unknown,
): { metadata: Partial<RoleMetadata> } | { error: string; message: string } {
  const parsed = RoleMetadataPatchSchema.safeParse(raw);
  if (!parsed.success) {
    return {
      error: "invalid_metadata",
      message: "metadata accepts description, tags and suggestedSecrets.",
    };
  }
  return { metadata: parsed.data };
}

function optionalString(raw: unknown): string | undefined {
  return typeof raw === "string" && raw.length > 0 ? raw : undefined;
}

export function createRole(deps: RoleGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateRoleRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) return jsonError(400, "missing_id");
    if (body.id.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Role ids starting with `_` are reserved.");
    }
    const def = parseDefinition(body.definition);
    if ("error" in def) return jsonError(400, def.error, def.message);
    let metadata: Partial<RoleMetadata> | undefined;
    if (body.metadata !== undefined && body.metadata !== null) {
      const parsed = parseMetadata(body.metadata);
      if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
      metadata = parsed.metadata;
    }
    const version = optionalString(body.version) ?? DEFAULT_ROLE_VERSION;
    if (await deps.registry.get(body.id, version)) {
      return jsonError(409, "role_exists", `Role "${body.id}@${version}" already exists.`);
    }
    const input: RegisterRoleInput = {
      id: body.id,
      version,
      definition: def.definition,
      ...(metadata !== undefined && { metadata }),
    };
    const registered = await deps.registry.register(input);
    // Answer the row as the store reads it back, so the `etag` matches the next GET.
    return roleResponse(
      201,
      (await deps.registry.get(registered.id, registered.version)) ?? registered,
    );
  };
}

export function updateRole(deps: RoleGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<CreateRoleRequest>(req);
    if (!body) return jsonError(400, "missing_body");

    const active = await deps.registry.get(id);
    if (!active) return roleNotFound(id);
    const version = optionalString(body.version);
    if (version !== undefined && version !== active.version) {
      if (!(await deps.registry.get(id, version))) return roleNotFound(id);
      return jsonError(
        409,
        "version_not_active",
        `Only the active version (${active.version}) can be edited; publish a new version instead.`,
      );
    }
    assertIfMatch({ req, etag: contentEtag(active), what: "role" });

    // Field-merge against the active row so a partial PATCH (e.g. just
    // editing tags) keeps everything it doesn't mention.
    let definition = active.definition;
    if (body.definition !== undefined && body.definition !== null) {
      const def = parseDefinition(body.definition);
      if ("error" in def) return jsonError(400, def.error, def.message);
      definition = def.definition;
    }
    let metadata: RoleMetadata = active.metadata;
    if (body.metadata !== undefined && body.metadata !== null) {
      const parsed = parseMetadata(body.metadata);
      if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
      metadata = { ...active.metadata, ...parsed.metadata };
    }
    const updated = await deps.registry.register({
      id,
      version: active.version,
      definition,
      metadata,
    });
    return roleResponse(200, (await deps.registry.get(id, updated.version)) ?? updated);
  };
}

export function deleteRole(deps: RoleGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    const force = url.searchParams.get("force") === "true";
    if (!(await deps.registry.get(id, version))) return roleNotFound(id);
    if (!force && deps.agents) {
      const bound = await agentsBindingRole({ agents: deps.agents, roleId: id, version });
      if (bound.length > 0) {
        return json(409, {
          error: "role_in_use",
          message: "Agents still bind this role; rebind them or pass ?force=true.",
          agents: bound,
        });
      }
    }
    await deps.registry.unregister(id, version);
    return new Response(null, { status: 204 });
  };
}

/** Ids of agents (any version) bound by ref to `roleId` (at `version`, when given). */
async function agentsBindingRole(params: {
  agents: AgentRegistry;
  roleId: string;
  version?: string;
}): Promise<string[]> {
  const ids = new Set<string>();
  for (const recipe of await params.agents.list({})) {
    if (recipe.backend.type !== "local" || !("ref" in recipe.backend.role)) continue;
    const ref = recipe.backend.role.ref;
    if (ref.id !== params.roleId) continue;
    if (
      params.version !== undefined &&
      ref.version !== undefined &&
      ref.version !== params.version
    ) {
      continue;
    }
    ids.add(recipe.id);
  }
  return [...ids].sort();
}
