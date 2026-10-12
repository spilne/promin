// ---------------------------------------------------------------------------
// Skill registry routes — author / edit / browse skills over HTTP. The
// dashboard's skills manager calls these; the agent editor's skill picker
// reads the catalog via GET /api/agents/_catalog/skills (see agent-catalog.ts).
//
// Routes (versioning per `versioned-resource.ts`):
//   GET    /api/skills              — active version per skill (tag / capability
//                                     filter, `limit` + `cursor` paging)
//   GET    /api/skills/:id          — get one (active, or ?version=; includes body)
//   GET    /api/skills/:id/versions — all versions of an id
//   POST   /api/skills              — create a custom skill (409 when it exists)
//   PATCH  /api/skills/:id          — `If-Match` required. Content / metadata
//                                     changes create a new version (refused for
//                                     a file-managed skill); `enabled` flips the
//                                     kill switch; `trust` records a review of
//                                     `version` (default: active)
//   POST   /api/skills/:id/promote  — make a version active
//   DELETE /api/skills/:id          — delete (all versions, or ?version=); 409 while referenced
//
// A skill is instruction-only data (id, description, whenToUse, body,
// tags/capabilities) — there is no resolver here, unlike agents.
// ---------------------------------------------------------------------------

import {
  listAllVersioned,
  type AgentRegistry,
  type RegisteredSkill,
  type RoleDefinition,
  type RoleRegistry,
  type SkillMetadata,
  type SkillRegistry,
} from "@promin/agent";
import { z } from "zod";
import { json, jsonError, parseIntParam, readJson } from "../router.ts";
import {
  isFileManagedEntry,
  listFileManagedRoute,
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

// Re-export the underlying type so the UI bundle types responses without
// importing @promin/agent directly (matches agents.ts).
export type { RegisteredSkill } from "@promin/agent";

export interface SkillsListResponse {
  skills: RegisteredSkill[];
  /** Pass as `cursor` for the next page; `null` on the last one. */
  nextCursor: string | null;
}

export interface SkillVersionsResponse {
  versions: RegisteredSkill[];
}

export interface SkillGatewayDeps {
  readonly registry: SkillRegistry;
  /** Agent recipes — inline roles that list the skill block its delete. */
  readonly agents?: AgentRegistry;
  /** Role registry — roles that list the skill block its delete. */
  readonly roles?: RoleRegistry;
}

export interface SkillSourcesResponse {
  /** Skill ids currently backed by a file on disk — read-only in the UI. */
  fileManaged: string[];
}

/**
 * Report which skills are file-managed (their active version was written by
 * a scan) vs operator-authored. The manager UI marks file-managed skills
 * read-only. Stored, so every replica answers the same from boot.
 */
export function listSkillSources(deps: { readonly registry: SkillRegistry }) {
  return listFileManagedRoute({ registry: deps.registry });
}

/** Limit-style query parameter: an integer clamped to [1, 1000]. */
function parseLimitParam(raw: string | null): number | undefined {
  return parseIntParam({ raw, min: 1, max: 1_000 });
}

function skillNotFound(id: string): Response {
  return jsonError(404, "skill_not_found", `Skill "${id}" is not registered.`);
}

function fileManaged409(id: string): Response {
  return jsonError(
    409,
    "file_managed",
    `Skill "${id}" is managed by a file; edit the file instead.`,
  );
}

// ---------------------------------------------------------------------------
// List + get
// ---------------------------------------------------------------------------

export function listSkills(deps: SkillGatewayDeps) {
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
      skills: page.items,
      nextCursor: page.nextCursor,
    } satisfies SkillsListResponse);
  };
}

export function getSkill(deps: SkillGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const version = new URL(req.url).searchParams.get("version") ?? undefined;
    const skill = await deps.registry.get({ id, ...(version !== undefined && { version }) });
    if (!skill) return skillNotFound(id);
    return entryResponse({ status: 200, entry: skill });
  };
}

export function listSkillVersions(deps: SkillGatewayDeps) {
  return listVersionsRoute({ registry: deps.registry, notFound: skillNotFound });
}

export function promoteSkill(deps: SkillGatewayDeps) {
  return promoteRoute({
    registry: deps.registry,
    notFound: skillNotFound,
  });
}

// ---------------------------------------------------------------------------
// Create / update / delete
// ---------------------------------------------------------------------------

interface CreateSkillRequest {
  readonly id?: unknown;
  readonly version?: unknown;
  readonly description?: unknown;
  readonly whenToUse?: unknown;
  readonly body?: unknown;
  readonly metadata?: unknown;
}

interface UpdateSkillRequest extends Omit<CreateSkillRequest, "id"> {
  /** Make a new content version active. Default `true`. */
  readonly publish?: unknown;
  /** The skill's kill switch. */
  readonly enabled?: unknown;
  /** Review state recorded on `version` (default: the active one). */
  readonly trust?: unknown;
}

const SkillMetadataPatchSchema = z
  .object({
    capabilities: z.array(z.string()).optional(),
    tags: z.array(z.string()).optional(),
  })
  .strict();

const TrustSchema = z.enum(["trusted", "needs-review"]);

function parseMetadata(
  raw: unknown,
): { metadata: Partial<SkillMetadata> } | { error: string; message: string } {
  const parsed = SkillMetadataPatchSchema.safeParse(raw);
  if (!parsed.success) {
    return { error: "invalid_metadata", message: "metadata accepts capabilities and tags." };
  }
  return { metadata: parsed.data };
}

export function createSkill(deps: SkillGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateSkillRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) return jsonError(400, "missing_id");
    if (body.id.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Skill ids starting with `_` are reserved.");
    }
    const id = body.id;
    if (typeof body.description !== "string" || body.description.length === 0) {
      return jsonError(400, "missing_description", "A skill needs a non-empty description.");
    }
    if (typeof body.body !== "string" || body.body.length === 0) {
      return jsonError(400, "missing_body", "A skill needs a non-empty markdown body.");
    }
    let metadata: Partial<SkillMetadata> | undefined;
    if (body.metadata !== undefined && body.metadata !== null) {
      const parsed = parseMetadata(body.metadata);
      if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
      metadata = parsed.metadata;
    }
    const version = optionalVersion(body.version);
    const whenToUse = optionalVersion(body.whenToUse);
    const description = body.description;
    const content = body.body;
    const created = await createOnly({
      register: () =>
        deps.registry.register({
          id,
          ...(version !== undefined && { version }),
          description,
          body: content,
          ...(whenToUse !== undefined && { whenToUse }),
          ...(metadata !== undefined && { metadata }),
          expectedActive: null,
        }),
      onExists: () => jsonError(409, "skill_exists", `Skill "${id}" already exists.`),
    });
    if (created instanceof Response) return created;
    return entryResponse({ status: 201, entry: created });
  };
}

export function updateSkill(deps: SkillGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<UpdateSkillRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    const ifMatch = ifMatchVersions({ req, required: true });
    for (const field of ["description", "body"] as const) {
      const v = body[field];
      if (v !== undefined && (typeof v !== "string" || v.length === 0)) {
        return jsonError(400, `invalid_${field}`, `${field} must be a non-empty string.`);
      }
    }
    if (body.whenToUse !== undefined && typeof body.whenToUse !== "string") {
      return jsonError(400, "invalid_whenToUse");
    }
    if (body.enabled !== undefined && typeof body.enabled !== "boolean") {
      return jsonError(400, "invalid_enabled");
    }
    if (body.publish !== undefined && typeof body.publish !== "boolean") {
      return jsonError(400, "invalid_publish");
    }
    const trust = body.trust === undefined ? undefined : TrustSchema.safeParse(body.trust);
    if (trust !== undefined && !trust.success) return jsonError(400, "invalid_trust");

    const active = await deps.registry.get({ id });
    if (!active) return skillNotFound(id);
    matchActiveVersion({ tags: ifMatch, active: active.version });

    let metadata = active.metadata;
    if (body.metadata !== undefined && body.metadata !== null) {
      const parsed = parseMetadata(body.metadata);
      if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
      metadata = { ...active.metadata, ...parsed.metadata };
    }
    const contentChanged =
      body.description !== undefined ||
      body.whenToUse !== undefined ||
      body.body !== undefined ||
      body.metadata !== undefined;
    // A file owns a scanned skill's text (the next scan would undo an edit);
    // reviewing it and its kill switch are the operator's.
    if (contentChanged && isFileManagedEntry(active)) return fileManaged409(id);

    // The compare-and-set write (a new version and/or the kill switch) goes
    // first and alone, so a 412 leaves nothing half-written; a review only
    // follows a write that landed.
    const enabled = typeof body.enabled === "boolean" ? body.enabled : undefined;
    let current: RegisteredSkill = active;
    if (contentChanged) {
      const version = optionalVersion(body.version);
      current = await registryCall(() =>
        deps.registry.register({
          id,
          ...(version !== undefined && { version }),
          description: typeof body.description === "string" ? body.description : active.description,
          whenToUse: typeof body.whenToUse === "string" ? body.whenToUse : active.whenToUse,
          body: typeof body.body === "string" ? body.body : active.body,
          metadata,
          promote: body.publish !== false,
          expectedActive: active.version,
          ...(enabled !== undefined && { enabled }),
        }),
      );
    } else if (enabled !== undefined) {
      current = await registryCall(() =>
        deps.registry.setEnabled({ id, enabled, expectedActive: active.version }),
      );
    }
    if (trust !== undefined) {
      // A review names the version it vouches for; default: what PATCH shows.
      const reviewed = contentChanged
        ? current.version
        : (optionalVersion(body.version) ?? current.version);
      current = await registryCall(() =>
        deps.registry.setTrust({ id, version: reviewed, trust: trust.data }),
      );
    }
    return entryResponse({ status: 200, entry: current });
  };
}

export function deleteSkill(deps: SkillGatewayDeps) {
  return deleteRoute({
    registry: deps.registry,
    notFound: skillNotFound,
    refuseFileManaged: true,
    beforeDelete: async ({ req, id }) => {
      if (new URL(req.url).searchParams.get("force") === "true") return undefined;
      const users = await skillReferences({ deps, skillId: id });
      if (users.agents.length === 0 && users.roles.length === 0) return undefined;
      return json(409, {
        error: "skill_in_use",
        message: "Roles or agents still reference this skill; remove it there or pass ?force=true.",
        ...users,
      });
    },
  });
}

function definitionUsesSkill(def: RoleDefinition, skillId: string): boolean {
  return def.skills?.some((s) => s.id === skillId) ?? false;
}

/** Agents (inline roles) and roles whose active definition lists `skillId`. */
async function skillReferences(params: {
  deps: SkillGatewayDeps;
  skillId: string;
}): Promise<{ agents: string[]; roles: string[] }> {
  const { deps, skillId } = params;
  const agents = new Set<string>();
  const roles = new Set<string>();
  if (deps.agents) {
    for (const recipe of await listAllVersioned({ registry: deps.agents })) {
      if (recipe.backend.type !== "local" || !("inline" in recipe.backend.role)) continue;
      if (definitionUsesSkill(recipe.backend.role.inline, skillId)) agents.add(recipe.id);
    }
  }
  if (deps.roles) {
    for (const role of await listAllVersioned({ registry: deps.roles })) {
      if (definitionUsesSkill(role.definition, skillId)) roles.add(role.id);
    }
  }
  return { agents: [...agents].sort(), roles: [...roles].sort() };
}
