// ---------------------------------------------------------------------------
// Role registry routes — author / edit / browse roles over HTTP. A role is
// the behavioral half of an agent: persona prompt (plain or layered over
// fragments) + tools + skills + capabilities. Agents bind a role by ref
// (shared, live) or inline (one-off). See ROLE_AGENT_MODEL.
//
// Routes (versioning per `versioned-resource.ts`):
//   GET    /api/roles              — active version per role (tag / capability
//                                    filter, `limit` + `cursor` paging)
//   GET    /api/roles/:id          — get one (active, or ?version=)
//   GET    /api/roles/:id/versions — all versions of an id
//   POST   /api/roles              — create a role (409 when the id exists)
//   PATCH  /api/roles/:id          — new version from the active one (metadata
//                                    merges) and/or the kill switch (`enabled`);
//                                    `If-Match` required
//   POST   /api/roles/:id/promote  — make a version active
//   DELETE /api/roles/:id          — delete (all versions, or ?version=);
//                                    409 while an agent binds it, unless ?force=true
//
// Like skills, a role is pure data — there is no resolver here. An agent's
// resolver loads + expands the bound role at materialize time.
// ---------------------------------------------------------------------------

import {
  listAllVersioned,
  type AgentRegistry,
  type RegisteredRole,
  type RoleDefinition,
  type RoleMetadata,
  type RoleRegistry,
} from "@promin/agent";
import { z } from "zod";
import { json, jsonError, parseIntParam, readJson } from "../router.ts";
import {
  matchActiveVersion,
  createOnly,
  deleteRoute,
  entryResponse,
  ifMatchVersions,
  listVersionsRoute,
  optionalVersion,
  promoteRoute,
  registryCall,
} from "./versioned-resource.ts";

// Re-export so the UI bundle types responses without importing @promin/agent
// directly (matches agents.ts / skills.ts).
export type { RegisteredRole, RoleDefinition, RoleMetadata } from "@promin/agent";

export interface RolesListResponse {
  roles: RegisteredRole[];
  /** Pass as `cursor` for the next page; `null` on the last one. */
  nextCursor: string | null;
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

function roleNotFound(id: string): Response {
  return jsonError(404, "role_not_found", `Role "${id}" is not registered.`);
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
    const page = await deps.registry.list({
      filter: {
        ...(capability !== undefined && { capability }),
        ...(tag !== undefined && { tag }),
      },
      ...(limit !== undefined && { limit }),
      ...(cursor !== undefined && { cursor }),
    });
    return json(200, {
      roles: page.items,
      nextCursor: page.nextCursor,
    } satisfies RolesListResponse);
  };
}

export function getRole(deps: RoleGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const version = new URL(req.url).searchParams.get("version") ?? undefined;
    const role = await deps.registry.get({ id, ...(version !== undefined && { version }) });
    if (!role) return roleNotFound(id);
    return entryResponse({ status: 200, entry: role });
  };
}

export function listRoleVersions(deps: RoleGatewayDeps) {
  return listVersionsRoute({ registry: deps.registry, notFound: roleNotFound });
}

export function promoteRole(deps: RoleGatewayDeps) {
  return promoteRoute({ registry: deps.registry, notFound: roleNotFound });
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

interface UpdateRoleRequest {
  /** Label of the new version. Default: the next `v<n>`. */
  readonly version?: unknown;
  readonly definition?: unknown;
  readonly metadata?: unknown;
  /** Make the new version active. Default `true`. */
  readonly publish?: unknown;
  /** The role's kill switch: agents bound to a disabled role fail to resolve. */
  readonly enabled?: unknown;
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

export function createRole(deps: RoleGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateRoleRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) return jsonError(400, "missing_id");
    if (body.id.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Role ids starting with `_` are reserved.");
    }
    const id = body.id;
    const def = parseDefinition(body.definition);
    if ("error" in def) return jsonError(400, def.error, def.message);
    let metadata: Partial<RoleMetadata> | undefined;
    if (body.metadata !== undefined && body.metadata !== null) {
      const parsed = parseMetadata(body.metadata);
      if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
      metadata = parsed.metadata;
    }
    const version = optionalVersion(body.version);
    const created = await createOnly({
      register: () =>
        deps.registry.register({
          id,
          ...(version !== undefined && { version }),
          definition: def.definition,
          ...(metadata !== undefined && { metadata }),
          expectedActive: null,
        }),
      onExists: () => jsonError(409, "role_exists", `Role "${id}" already exists.`),
    });
    if (created instanceof Response) return created;
    return entryResponse({ status: 201, entry: created });
  };
}

export function updateRole(deps: RoleGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<UpdateRoleRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    const ifMatch = ifMatchVersions({ req, required: true });
    if (body.publish !== undefined && typeof body.publish !== "boolean") {
      return jsonError(400, "invalid_publish");
    }
    if (body.enabled !== undefined && typeof body.enabled !== "boolean") {
      return jsonError(400, "invalid_enabled");
    }
    const enabled = typeof body.enabled === "boolean" ? body.enabled : undefined;
    const editsContent =
      (body.definition !== undefined && body.definition !== null) ||
      (body.metadata !== undefined && body.metadata !== null);

    const active = await deps.registry.get({ id });
    if (!active) return roleNotFound(id);
    matchActiveVersion({ tags: ifMatch, active: active.version });

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
    const version = optionalVersion(body.version);
    // One write: the new version and the kill switch land together or not at all.
    const updated =
      !editsContent && enabled !== undefined
        ? await registryCall(() =>
            deps.registry.setEnabled({ id, enabled, expectedActive: active.version }),
          )
        : await registryCall(() =>
            deps.registry.register({
              id,
              ...(version !== undefined && { version }),
              definition,
              metadata,
              promote: body.publish !== false,
              expectedActive: active.version,
              ...(enabled !== undefined && { enabled }),
            }),
          );
    return entryResponse({ status: 200, entry: updated });
  };
}

export function deleteRole(deps: RoleGatewayDeps) {
  return deleteRoute({
    registry: deps.registry,
    notFound: roleNotFound,
    beforeDelete: async ({ req, id, version }) => {
      const force = new URL(req.url).searchParams.get("force") === "true";
      if (force || !deps.agents) return undefined;
      const bound = await agentsBindingRole({
        agents: deps.agents,
        roleId: id,
        ...(version !== undefined && { version }),
      });
      if (bound.length === 0) return undefined;
      return json(409, {
        error: "role_in_use",
        message: "Agents still bind this role; rebind them or pass ?force=true.",
        agents: bound,
      });
    },
  });
}

/**
 * Ids of agents whose active version binds `roleId` by ref (at `version`,
 * when given — a ref without a version follows the active role version).
 */
async function agentsBindingRole(params: {
  agents: AgentRegistry;
  roleId: string;
  version?: string;
}): Promise<string[]> {
  const ids = new Set<string>();
  for (const recipe of await listAllVersioned({ registry: params.agents })) {
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
