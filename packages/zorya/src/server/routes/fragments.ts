// ---------------------------------------------------------------------------
// Fragment registry routes — author / edit / browse prompt fragments. The
// dashboard's fragments manager calls these; the agent editor's layered-
// prompt picker reads /api/agents/_catalog/fragments (a parallel surface
// over the same registry).
//
// Routes (versioning per `versioned-resource.ts`):
//   GET    /api/fragments              — active version per fragment
//                                        (`limit` + `cursor` paging)
//   GET    /api/fragments/:id          — get one (active, or ?version=)
//   GET    /api/fragments/:id/versions — all versions of an id
//   POST   /api/fragments              — create a fragment (409 when it exists)
//   PATCH  /api/fragments/:id          — new version (`content`) and/or kill
//                                        switch (`enabled`); `If-Match` required
//   POST   /api/fragments/:id/promote  — make a version active
//   DELETE /api/fragments/:id          — delete (all versions, or ?version=);
//                                        409 while a role or an agent's inline
//                                        role layers it, unless ?force=true
//   GET    /api/fragments/_sources     — { fileManaged: string[] }
//
// A role layers a fragment by id (follows the active version) or by
// `"<id>@<version>"` (pinned).
// ---------------------------------------------------------------------------

import {
  listAllVersioned,
  parseLayerRef,
  type AgentRegistry,
  type FragmentRegistry,
  type RegisteredFragment,
  type RoleDefinition,
  type RoleRegistry,
} from "@promin/agent";
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

export type { RegisteredFragment } from "@promin/agent";

export interface FragmentsListResponse {
  fragments: RegisteredFragment[];
  /** Pass as `cursor` for the next page; `null` on the last one. */
  nextCursor: string | null;
}

export interface FragmentVersionsResponse {
  versions: RegisteredFragment[];
}

export interface FragmentSourcesResponse {
  /** Fragment ids currently backed by a file on disk — read-only in the UI. */
  fileManaged: string[];
}

export interface FragmentGatewayDeps {
  readonly registry: FragmentRegistry;
  /** Agent recipes — an inline role that layers the fragment blocks its delete. */
  readonly agents?: AgentRegistry;
  /** Role registry — a role that layers the fragment blocks its delete. */
  readonly roles?: RoleRegistry;
}

function fragmentNotFound(id: string): Response {
  return jsonError(404, "fragment_not_found", `Fragment "${id}" is not registered.`);
}

function fileManaged409(id: string): Response {
  return jsonError(
    409,
    "file_managed",
    `Fragment "${id}" is managed by a file; edit the file instead.`,
  );
}

// Same `_`-prefixed reserved namespace as agents/skills — protects `_sources`.
// No `@`: it pins a version in role layers.
const ID_PATTERN = /^[A-Za-z][A-Za-z0-9_\-./]{0,127}$/;

// ---------------------------------------------------------------------------
// List + get + sources
// ---------------------------------------------------------------------------

export function listFragments(deps: FragmentGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const limit = parseIntParam({ raw: url.searchParams.get("limit"), min: 1, max: 1_000 });
    const cursor = url.searchParams.get("cursor") ?? undefined;
    const page = await deps.registry.list({
      ...(limit !== undefined && { limit }),
      ...(cursor !== undefined && { cursor }),
    });
    return json(200, {
      fragments: page.items,
      nextCursor: page.nextCursor,
    } satisfies FragmentsListResponse);
  };
}

export function getFragment(deps: FragmentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const version = new URL(req.url).searchParams.get("version") ?? undefined;
    const fragment = await deps.registry.get({ id, ...(version !== undefined && { version }) });
    if (!fragment) return fragmentNotFound(id);
    return entryResponse({ status: 200, entry: fragment });
  };
}

/** File-managed fragment ids (their active version was written by a scan); stored. */
export function listFragmentSources(deps: { readonly registry: FragmentRegistry }) {
  return listFileManagedRoute({ registry: deps.registry });
}

export function listFragmentVersions(deps: FragmentGatewayDeps) {
  return listVersionsRoute({ registry: deps.registry, notFound: fragmentNotFound });
}

export function promoteFragment(deps: FragmentGatewayDeps) {
  return promoteRoute({
    registry: deps.registry,
    notFound: fragmentNotFound,
  });
}

// ---------------------------------------------------------------------------
// Create / update / delete
// ---------------------------------------------------------------------------

interface CreateFragmentRequest {
  readonly id?: unknown;
  readonly version?: unknown;
  readonly content?: unknown;
}

interface UpdateFragmentRequest {
  /** Label of the new version. Default: the next `v<n>`. */
  readonly version?: unknown;
  readonly content?: unknown;
  /** Make the new version active. Default `true`. */
  readonly publish?: unknown;
  /** The fragment's kill switch. */
  readonly enabled?: unknown;
}

export function createFragment(deps: FragmentGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateFragmentRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) {
      return jsonError(400, "missing_id");
    }
    if (body.id.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Fragment ids starting with `_` are reserved.");
    }
    if (!ID_PATTERN.test(body.id)) {
      return jsonError(
        400,
        "invalid_id",
        "Fragment ids must start with a letter and contain only letters, digits, `_`, `-`, `.`, `/`.",
      );
    }
    if (typeof body.content !== "string" || body.content.length === 0) {
      return jsonError(400, "missing_content");
    }
    const id = body.id;
    const content = body.content;
    const version = optionalVersion(body.version);
    const created = await createOnly({
      register: () =>
        deps.registry.register({
          id,
          ...(version !== undefined && { version }),
          content,
          expectedActive: null,
        }),
      onExists: () =>
        jsonError(
          409,
          "fragment_exists",
          `Fragment "${id}" already exists — use PATCH to edit it.`,
        ),
    });
    if (created instanceof Response) return created;
    return entryResponse({ status: 201, entry: created });
  };
}

export function updateFragment(deps: FragmentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<UpdateFragmentRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    const ifMatch = ifMatchVersions({ req, required: true });
    if (
      body.content !== undefined &&
      (typeof body.content !== "string" || body.content.length === 0)
    ) {
      return jsonError(400, "missing_content");
    }
    if (body.enabled !== undefined && typeof body.enabled !== "boolean") {
      return jsonError(400, "invalid_enabled");
    }
    if (body.publish !== undefined && typeof body.publish !== "boolean") {
      return jsonError(400, "invalid_publish");
    }
    const active = await deps.registry.get({ id });
    if (!active) return fragmentNotFound(id);
    // A file owns a scanned fragment's text; its kill switch is the operator's.
    if (body.content !== undefined && isFileManagedEntry(active)) return fileManaged409(id);
    matchActiveVersion({ tags: ifMatch, active: active.version });

    // One write: the new version and the kill switch land together or not at all.
    const enabled = typeof body.enabled === "boolean" ? body.enabled : undefined;
    let current: RegisteredFragment = active;
    if (typeof body.content === "string") {
      const version = optionalVersion(body.version);
      const content = body.content;
      current = await registryCall(() =>
        deps.registry.register({
          id,
          ...(version !== undefined && { version }),
          content,
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
    return entryResponse({ status: 200, entry: current });
  };
}

export function deleteFragment(deps: FragmentGatewayDeps) {
  return deleteRoute({
    registry: deps.registry,
    notFound: fragmentNotFound,
    refuseFileManaged: true,
    beforeDelete: async ({ req, id }) => {
      if (new URL(req.url).searchParams.get("force") === "true") return undefined;
      const users = await fragmentReferences({ deps, id });
      if (users.agents.length === 0 && users.roles.length === 0) return undefined;
      return json(409, {
        error: "fragment_in_use",
        message: "Roles or agents still layer this fragment; remove it there or pass ?force=true.",
        ...users,
      });
    },
  });
}

function definitionLayersFragment(def: RoleDefinition, id: string): boolean {
  const sp = def.systemPrompt;
  return (
    typeof sp === "object" &&
    sp !== null &&
    (sp.layers?.some((layer) => parseLayerRef(layer).id === id) ?? false)
  );
}

/** Agents (inline roles) and roles whose active system prompt layers fragment `id`. */
async function fragmentReferences(params: {
  deps: FragmentGatewayDeps;
  id: string;
}): Promise<{ agents: string[]; roles: string[] }> {
  const { deps, id } = params;
  const agents = new Set<string>();
  const roles = new Set<string>();
  if (deps.agents) {
    for (const recipe of await listAllVersioned({ registry: deps.agents })) {
      if (recipe.backend.type !== "local" || !("inline" in recipe.backend.role)) continue;
      if (definitionLayersFragment(recipe.backend.role.inline, id)) agents.add(recipe.id);
    }
  }
  if (deps.roles) {
    for (const role of await listAllVersioned({ registry: deps.roles })) {
      if (definitionLayersFragment(role.definition, id)) roles.add(role.id);
    }
  }
  return { agents: [...agents].sort(), roles: [...roles].sort() };
}
