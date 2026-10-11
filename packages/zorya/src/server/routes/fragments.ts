// ---------------------------------------------------------------------------
// Fragment registry routes — author / edit / browse prompt fragments. The
// dashboard's fragments manager calls these; the agent editor's layered-
// prompt picker reads /api/agents/_catalog/fragments (a parallel surface
// over the same registry).
//
// Routes:
//   GET    /api/fragments              — list all
//   GET    /api/fragments/:key         — get one
//   POST   /api/fragments              — create a custom fragment
//   PATCH  /api/fragments/:key         — update content; `If-Match` (the GET
//                                         `etag`) → 412 on mismatch
//   DELETE /api/fragments/:key         — delete; 409 while a role or an
//                                         agent's inline role layers it,
//                                         unless ?force=true
//   GET    /api/fragments/_sources     — { fileManaged: string[] }
// ---------------------------------------------------------------------------

import type { AgentRegistry, FragmentRegistry, RoleDefinition, RoleRegistry } from "@promin/agent";
import { json, jsonError, readJson } from "../router.ts";
import { assertIfMatch, contentEtag, withEtag } from "../preconditions.ts";

export interface FragmentDto {
  readonly key: string;
  readonly content: string;
}

export interface FragmentsListResponse {
  fragments: FragmentDto[];
}

export interface FragmentSourcesResponse {
  /** Fragment keys currently backed by a file on disk — read-only in the UI. */
  fileManaged: string[];
}

export interface FragmentGatewayDeps {
  /** Sync read surface for list/get; resolveSystemPrompt also reads from this. */
  readonly registry: FragmentRegistry;
  /**
   * Write-through mutators. The ZoryaFragments service supplies these
   * (they update the registry AND persist to the optional durable store).
   * Scanned fragments do NOT come through this path.
   */
  setFragment(key: string, content: string): Promise<void>;
  deleteFragment(key: string): Promise<void>;
  /** Agent recipes — an inline role that layers the fragment blocks its delete. */
  readonly agents?: AgentRegistry;
  /** Role registry — a role that layers the fragment blocks its delete. */
  readonly roles?: RoleRegistry;
}

/** A fragment with its entity tag (`If-Match` on PATCH compares against it). */
function fragmentResponse(status: number, fragment: FragmentDto): Response {
  return withEtag(json(status, fragment), contentEtag(fragment));
}

function fragmentNotFound(key: string): Response {
  return jsonError(404, "fragment_not_found", `Fragment "${key}" is not registered.`);
}

function asMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// Same `_`-prefixed reserved namespace as agents/skills — protects `_sources`.
const KEY_PATTERN = /^[A-Za-z][A-Za-z0-9_\-./]{0,127}$/;
function isValidKey(key: string): boolean {
  return KEY_PATTERN.test(key);
}

// ---------------------------------------------------------------------------
// List + get + sources
// ---------------------------------------------------------------------------

export function listFragments(deps: FragmentGatewayDeps) {
  return async (): Promise<Response> => {
    const fragments = deps.registry.list().map((f) => ({ key: f.key, content: f.content }));
    return json(200, { fragments } satisfies FragmentsListResponse);
  };
}

export function getFragment(deps: FragmentGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const key = params.key;
    if (!key) return jsonError(400, "missing_key");
    const content = deps.registry.get(key);
    if (content === undefined) return fragmentNotFound(key);
    return fragmentResponse(200, { key, content });
  };
}

export function listFragmentSources(deps: { readonly fileManagedIds: () => string[] }) {
  return async (): Promise<Response> => {
    return json(200, { fileManaged: deps.fileManagedIds() } satisfies FragmentSourcesResponse);
  };
}

// ---------------------------------------------------------------------------
// Create / update / delete
// ---------------------------------------------------------------------------

interface FragmentBody {
  readonly key?: unknown;
  readonly content?: unknown;
}

export function createFragment(deps: FragmentGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<FragmentBody>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.key !== "string" || body.key.length === 0) {
      return jsonError(400, "missing_key");
    }
    if (body.key.startsWith("_")) {
      return jsonError(400, "reserved_key_prefix", "Fragment keys starting with `_` are reserved.");
    }
    if (!isValidKey(body.key)) {
      return jsonError(
        400,
        "invalid_key",
        "Fragment keys must start with a letter and contain only letters, digits, `_`, `-`, `.`, `/`.",
      );
    }
    if (typeof body.content !== "string" || body.content.length === 0) {
      return jsonError(400, "missing_content");
    }
    if (deps.registry.get(body.key) !== undefined) {
      return jsonError(
        409,
        "fragment_exists",
        `Fragment "${body.key}" already exists — use PATCH to update.`,
      );
    }
    try {
      await deps.setFragment(body.key, body.content);
      return fragmentResponse(201, { key: body.key, content: body.content });
    } catch (err) {
      return jsonError(500, "create_failed", asMessage(err));
    }
  };
}

export function updateFragment(deps: FragmentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const key = params.key;
    if (!key) return jsonError(400, "missing_key");
    const body = await readJson<FragmentBody>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.content !== "string" || body.content.length === 0) {
      return jsonError(400, "missing_content");
    }
    const current = deps.registry.get(key);
    if (current === undefined) return fragmentNotFound(key);
    assertIfMatch({ req, etag: contentEtag({ key, content: current }), what: "fragment" });
    try {
      await deps.setFragment(key, body.content);
      return fragmentResponse(200, { key, content: body.content });
    } catch (err) {
      return jsonError(500, "update_failed", asMessage(err));
    }
  };
}

export function deleteFragment(deps: FragmentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const key = params.key;
    if (!key) return jsonError(400, "missing_key");
    if (deps.registry.get(key) === undefined) return fragmentNotFound(key);
    const force = new URL(req.url).searchParams.get("force") === "true";
    if (!force) {
      const users = await fragmentReferences({ deps, key });
      if (users.agents.length > 0 || users.roles.length > 0) {
        return json(409, {
          error: "fragment_in_use",
          message:
            "Roles or agents still layer this fragment; remove it there or pass ?force=true.",
          ...users,
        });
      }
    }
    try {
      await deps.deleteFragment(key);
      return new Response(null, { status: 204 });
    } catch (err) {
      return jsonError(500, "delete_failed", asMessage(err));
    }
  };
}

function definitionLayersFragment(def: RoleDefinition, key: string): boolean {
  const sp = def.systemPrompt;
  return typeof sp === "object" && sp !== null && (sp.layers?.includes(key) ?? false);
}

/** Agents (inline roles) and roles whose system prompt layers fragment `key`. */
async function fragmentReferences(params: {
  deps: FragmentGatewayDeps;
  key: string;
}): Promise<{ agents: string[]; roles: string[] }> {
  const { deps, key } = params;
  const agents = new Set<string>();
  const roles = new Set<string>();
  if (deps.agents) {
    for (const recipe of await deps.agents.list({})) {
      if (recipe.backend.type !== "local" || !("inline" in recipe.backend.role)) continue;
      if (definitionLayersFragment(recipe.backend.role.inline, key)) agents.add(recipe.id);
    }
  }
  if (deps.roles) {
    for (const role of await deps.roles.list({})) {
      if (definitionLayersFragment(role.definition, key)) roles.add(role.id);
    }
  }
  return { agents: [...agents].sort(), roles: [...roles].sort() };
}
