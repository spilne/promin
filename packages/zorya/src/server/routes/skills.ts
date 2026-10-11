// ---------------------------------------------------------------------------
// Skill registry routes — author / edit / browse skills over HTTP. The
// dashboard's skills manager calls these; the agent editor's skill picker
// reads the catalog via GET /api/agents/_catalog/skills (see agent-catalog.ts).
//
// Routes:
//   GET    /api/skills              — list (filter by tag / capability)
//   GET    /api/skills/:id          — get one (latest version, includes body)
//   GET    /api/skills/:id/versions — all versions of an id
//   POST   /api/skills              — create a custom skill (409 when it exists)
//   PATCH  /api/skills/:id          — edit the active version (metadata merges)
//   DELETE /api/skills/:id          — delete (all versions, or ?version=); 409 while referenced
//
// A skill is instruction-only data (id, description, whenToUse, body,
// tags/capabilities/enabled) — there is no resolver here, unlike agents.
// ---------------------------------------------------------------------------

import type {
  AgentRegistry,
  RegisteredSkill,
  RegisterSkillInput,
  RoleDefinition,
  RoleRegistry,
  SkillRegistry,
} from "@promin/agent";
import { DEFAULT_SKILL_VERSION } from "@promin/agent";
import { z } from "zod";
import { json, jsonError, parseIntParam, readJson } from "../router.ts";

// Re-export the underlying type so the UI bundle types responses without
// importing @promin/agent directly (matches agents.ts).
export type { RegisteredSkill } from "@promin/agent";

export interface SkillsListResponse {
  skills: RegisteredSkill[];
}

export interface SkillVersionsResponse {
  versions: RegisteredSkill[];
}

export interface SkillGatewayDeps {
  readonly registry: SkillRegistry;
  /** Skill ids backed by a file on disk — PATCH / DELETE refuse them. */
  readonly fileManagedIds?: () => ReadonlyArray<string>;
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
 * Report which skills are file-managed (scanned from disk) vs operator-
 * authored. The manager UI marks file-managed skills read-only so an edit
 * isn't silently overwritten by the next scan tick.
 */
export function listSkillSources(deps: { readonly fileManagedIds: () => string[] }) {
  return async (): Promise<Response> => {
    return json(200, { fileManaged: deps.fileManagedIds() } satisfies SkillSourcesResponse);
  };
}

/** Limit-style query parameter: an integer clamped to [1, 1000]. */
function parseLimitParam(raw: string | null): number | undefined {
  return parseIntParam({ raw, min: 1, max: 1_000 });
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
    const skills = await deps.registry.list({ capability, tag, limit, cursor });
    return json(200, { skills } satisfies SkillsListResponse);
  };
}

export function getSkill(deps: SkillGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    const skill = await deps.registry.get(id, version);
    if (!skill) return jsonError(404, "skill_not_found", `Skill "${id}" is not registered.`);
    return json(200, skill);
  };
}

export function listSkillVersions(deps: SkillGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const versions = await deps.registry.versions(id);
    if (versions.length === 0) {
      return jsonError(404, "skill_not_found", `Skill "${id}" is not registered.`);
    }
    return json(200, { versions } satisfies SkillVersionsResponse);
  };
}

// ---------------------------------------------------------------------------
// Create / update / delete
//
//   POST   — create-only: 409 when (id, version) exists.
//   PATCH  — edits the active version; metadata merges field by field, so
//            `trust` and `enabled` change only when the body names them.
//   DELETE — 409 while a role or agent references the skill (unless
//            ?force=true). File-managed skills refuse PATCH / DELETE.
// ---------------------------------------------------------------------------

interface CreateSkillRequest {
  readonly id?: unknown;
  readonly version?: unknown;
  readonly description?: unknown;
  readonly whenToUse?: unknown;
  readonly body?: unknown;
  readonly metadata?: unknown;
}

const SkillMetadataPatchSchema = z
  .object({
    capabilities: z.array(z.string()).optional(),
    tags: z.array(z.string()).optional(),
    enabled: z.boolean().optional(),
    trust: z.enum(["trusted", "needs-review"]).optional(),
  })
  .strict();

type SkillMetadataPatch = z.infer<typeof SkillMetadataPatchSchema>;

function parseMetadata(
  raw: unknown,
): { metadata: SkillMetadataPatch } | { error: string; message: string } {
  const parsed = SkillMetadataPatchSchema.safeParse(raw);
  if (!parsed.success) {
    return {
      error: "invalid_metadata",
      message: "metadata accepts capabilities, tags, enabled and trust.",
    };
  }
  return { metadata: parsed.data };
}

function optionalString(raw: unknown): string | undefined {
  return typeof raw === "string" && raw.length > 0 ? raw : undefined;
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

export function createSkill(deps: SkillGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateSkillRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) return jsonError(400, "missing_id");
    if (body.id.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Skill ids starting with `_` are reserved.");
    }
    if (typeof body.description !== "string" || body.description.length === 0) {
      return jsonError(400, "missing_description", "A skill needs a non-empty description.");
    }
    if (typeof body.body !== "string" || body.body.length === 0) {
      return jsonError(400, "missing_body", "A skill needs a non-empty markdown body.");
    }
    let metadata: SkillMetadataPatch | undefined;
    if (body.metadata !== undefined && body.metadata !== null) {
      const parsed = parseMetadata(body.metadata);
      if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
      metadata = parsed.metadata;
    }
    const version = optionalString(body.version) ?? DEFAULT_SKILL_VERSION;
    if (await deps.registry.get(body.id, version)) {
      return jsonError(409, "skill_exists", `Skill "${body.id}@${version}" already exists.`);
    }
    const whenToUse = optionalString(body.whenToUse);
    const input: RegisterSkillInput = {
      id: body.id,
      version,
      description: body.description,
      body: body.body,
      ...(whenToUse !== undefined && { whenToUse }),
      ...(metadata !== undefined && { metadata }),
    };
    return json(201, await deps.registry.register(input));
  };
}

export function updateSkill(deps: SkillGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    if (deps.fileManagedIds?.().includes(id)) return fileManaged409(id);
    const body = await readJson<CreateSkillRequest>(req);
    if (!body) return jsonError(400, "missing_body");

    const active = await deps.registry.get(id);
    if (!active) return skillNotFound(id);
    const version = optionalString(body.version);
    if (version !== undefined && version !== active.version) {
      if (!(await deps.registry.get(id, version))) return skillNotFound(id);
      return jsonError(
        409,
        "version_not_active",
        `Only the active version (${active.version}) can be edited; publish a new version instead.`,
      );
    }
    for (const field of ["description", "body"] as const) {
      const v = body[field];
      if (v !== undefined && (typeof v !== "string" || v.length === 0)) {
        return jsonError(400, `invalid_${field}`, `${field} must be a non-empty string.`);
      }
    }
    if (body.whenToUse !== undefined && typeof body.whenToUse !== "string") {
      return jsonError(400, "invalid_whenToUse");
    }
    let metadata = active.metadata;
    if (body.metadata !== undefined && body.metadata !== null) {
      const parsed = parseMetadata(body.metadata);
      if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
      metadata = { ...active.metadata, ...parsed.metadata };
    }
    const updated = await deps.registry.register({
      id,
      version: active.version,
      description: typeof body.description === "string" ? body.description : active.description,
      whenToUse: typeof body.whenToUse === "string" ? body.whenToUse : active.whenToUse,
      body: typeof body.body === "string" ? body.body : active.body,
      metadata,
    });
    return json(200, updated);
  };
}

export function deleteSkill(deps: SkillGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    if (deps.fileManagedIds?.().includes(id)) return fileManaged409(id);
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    const force = url.searchParams.get("force") === "true";
    if (!(await deps.registry.get(id, version))) return skillNotFound(id);
    if (!force) {
      const users = await skillReferences({ deps, skillId: id });
      if (users.agents.length > 0 || users.roles.length > 0) {
        return json(409, {
          error: "skill_in_use",
          message:
            "Roles or agents still reference this skill; remove it there or pass ?force=true.",
          ...users,
        });
      }
    }
    await deps.registry.unregister(id, version);
    return new Response(null, { status: 204 });
  };
}

function definitionUsesSkill(def: RoleDefinition, skillId: string): boolean {
  return def.skills?.some((s) => s.id === skillId) ?? false;
}

/** Agents (inline roles) and roles whose definition lists `skillId`. */
async function skillReferences(params: {
  deps: SkillGatewayDeps;
  skillId: string;
}): Promise<{ agents: string[]; roles: string[] }> {
  const { deps, skillId } = params;
  const agents = new Set<string>();
  const roles = new Set<string>();
  if (deps.agents) {
    for (const recipe of await deps.agents.list({})) {
      if (recipe.backend.type !== "local" || !("inline" in recipe.backend.role)) continue;
      if (definitionUsesSkill(recipe.backend.role.inline, skillId)) agents.add(recipe.id);
    }
  }
  if (deps.roles) {
    for (const role of await deps.roles.list({})) {
      if (definitionUsesSkill(role.definition, skillId)) roles.add(role.id);
    }
  }
  return { agents: [...agents].sort(), roles: [...roles].sort() };
}
