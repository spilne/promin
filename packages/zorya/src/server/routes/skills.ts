// ---------------------------------------------------------------------------
// Skill registry routes — author / edit / browse skills over HTTP. The
// dashboard's skills manager calls these; the agent editor's skill picker
// reads the catalog via GET /api/agents/_catalog/skills (see agent-catalog.ts).
//
// Routes:
//   GET    /api/skills              — list (filter by tag / capability)
//   GET    /api/skills/:id          — get one (latest version, includes body)
//   GET    /api/skills/:id/versions — all versions of an id
//   POST   /api/skills              — create a custom skill
//   PATCH  /api/skills/:id          — update a skill (republishes the row)
//   DELETE /api/skills/:id          — delete (all versions, or ?version=)
//
// A skill is instruction-only data (id, description, whenToUse, body,
// tags/capabilities/enabled) — there is no resolver here, unlike agents.
// ---------------------------------------------------------------------------

import type { RegisteredSkill, RegisterSkillInput, SkillRegistry } from "@promin/agent";
import { json, jsonError, readJson } from "../router.ts";

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

export function listSkills(deps: SkillGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const capability = url.searchParams.get("capability") ?? undefined;
    const tag = url.searchParams.get("tag") ?? undefined;
    const limit = parseIntParam(url.searchParams.get("limit"));
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
// ---------------------------------------------------------------------------

interface CreateSkillRequest {
  readonly id?: unknown;
  readonly version?: unknown;
  readonly description?: unknown;
  readonly whenToUse?: unknown;
  readonly body?: unknown;
  readonly metadata?: unknown;
}

/** Validate + coerce a create/update body into a RegisterSkillInput. */
function parseSkillBody(
  body: CreateSkillRequest,
  id: string,
): { input: RegisterSkillInput } | { error: string; message: string } {
  if (typeof body.description !== "string" || body.description.length === 0) {
    return { error: "missing_description", message: "A skill needs a non-empty description." };
  }
  if (typeof body.body !== "string" || body.body.length === 0) {
    return { error: "missing_body", message: "A skill needs a non-empty markdown body." };
  }
  const version =
    typeof body.version === "string" && body.version.length > 0 ? body.version : undefined;
  const whenToUse =
    typeof body.whenToUse === "string" && body.whenToUse.length > 0 ? body.whenToUse : undefined;
  const metadata =
    typeof body.metadata === "object" && body.metadata !== null
      ? (body.metadata as RegisterSkillInput["metadata"])
      : undefined;
  return {
    input: {
      id,
      description: body.description,
      body: body.body,
      ...(version !== undefined && { version }),
      ...(whenToUse !== undefined && { whenToUse }),
      ...(metadata !== undefined && { metadata }),
    },
  };
}

export function createSkill(deps: SkillGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateSkillRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) return jsonError(400, "missing_id");
    if (body.id.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Skill ids starting with `_` are reserved.");
    }
    const parsed = parseSkillBody(body, body.id);
    if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
    try {
      const skill = await deps.registry.register(parsed.input);
      return json(201, skill);
    } catch (err) {
      return jsonError(500, "create_failed", asMessage(err));
    }
  };
}

export function updateSkill(deps: SkillGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<CreateSkillRequest>(req);
    if (!body) return jsonError(400, "missing_body");

    const version =
      typeof body.version === "string" && body.version.length > 0 ? body.version : undefined;
    const existing = await deps.registry.get(id, version);
    if (!existing) return jsonError(404, "skill_not_found", `Skill "${id}" is not registered.`);

    // Field-merge against the existing row so a partial PATCH (e.g. just
    // toggling enabled) doesn't blank out description/body.
    const merged: CreateSkillRequest = {
      version: existing.version,
      description: typeof body.description === "string" ? body.description : existing.description,
      whenToUse: typeof body.whenToUse === "string" ? body.whenToUse : existing.whenToUse,
      body: typeof body.body === "string" ? body.body : existing.body,
      metadata:
        typeof body.metadata === "object" && body.metadata !== null
          ? body.metadata
          : existing.metadata,
    };
    const parsed = parseSkillBody(merged, id);
    if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
    try {
      const updated = await deps.registry.register(parsed.input);
      return json(200, updated);
    } catch (err) {
      return jsonError(500, "update_failed", asMessage(err));
    }
  };
}

export function deleteSkill(deps: SkillGatewayDeps) {
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
