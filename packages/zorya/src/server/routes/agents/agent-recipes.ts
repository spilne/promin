// ---------------------------------------------------------------------------
// Agent recipes over HTTP: discovery (list, get, sources), CRUD, versions,
// clone and extract-role.
// ---------------------------------------------------------------------------

import type { RegisteredAgent } from "@promin/agent";
import { SecretScope } from "@promin/agent";
import { json, jsonError, readJson } from "../../router.ts";
import { requireActiveNamespace, secretScopeNamespace } from "../../namespace-guard.ts";
import { AgentListQuerySchema } from "../../api-types.ts";
import { assertIfMatch, contentEtag, withEtag } from "../../preconditions.ts";
import { parseQuery } from "../../request-validation.ts";
import {
  mergeAgentMetadata,
  parseBackend,
  parseMetadataPatch,
  redactRecipe,
  restoreRedactedSecrets,
} from "../agent-gateway-kit.ts";
import { isDraftId } from "../agent-drafts.ts";
import { SECRET_KEY_PATTERN, SECRET_MAX_VALUE_BYTES } from "../secrets.ts";
import type {
  AgentGatewayDeps,
  AgentSourcesResponse,
  AgentsListResponse,
  AgentVersionsResponse,
} from "./agent-gateway-deps.ts";
import { agentNotFound } from "./agent-scope.ts";

// ---------------------------------------------------------------------------
// Discovery — list + get
// ---------------------------------------------------------------------------

/**
 * Report which agent recipes are file-managed (scanned from disk) vs
 * operator-authored. The editor uses this to disable in-place Save on a
 * file-scanned recipe; the server refuses such edits as well.
 */
export function listAgentSources(deps: { readonly fileManagedIds: () => string[] }) {
  return async (): Promise<Response> => {
    return json(200, { fileManaged: deps.fileManagedIds() } satisfies AgentSourcesResponse);
  };
}

/**
 * GET /api/agents — one row per agent: its active version (the one `GET
 * /api/agents/:id` answers) plus how many versions it has, sorted by id.
 * `capability` / `tag` filter on the active version; Designer drafts are
 * hidden. Filtering happens before `limit` / `offset`, so `total` counts
 * every matching agent and every page is full. All versions of one agent
 * are under `GET /api/agents/:id/versions`.
 */
export function listAgents(deps: AgentGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const { capability, tag, limit, offset } = parseQuery({ req, schema: AgentListQuerySchema });
    const versionCounts = new Map<string, number>();
    for (const row of await deps.registry.list({})) {
      if (isDraftId(row.id)) continue;
      versionCounts.set(row.id, (versionCounts.get(row.id) ?? 0) + 1);
    }
    const ids = [...versionCounts.keys()].sort();
    const active = await Promise.all(ids.map((id) => deps.registry.get(id)));
    const matching = active.filter(
      (recipe): recipe is RegisteredAgent =>
        recipe !== null &&
        (capability === undefined || recipe.metadata.capabilities.includes(capability)) &&
        (tag === undefined || recipe.metadata.tags.includes(tag)),
    );
    const agents = matching.slice(offset, offset + limit).map((recipe) => ({
      ...redactRecipe(recipe),
      versionCount: versionCounts.get(recipe.id) ?? 1,
    }));
    return json(200, { agents, total: matching.length } satisfies AgentsListResponse);
  };
}

/** Entity tag of a recipe row (secrets redacted before hashing). */
function recipeEtag(recipe: RegisteredAgent): string {
  return contentEtag(redactRecipe(recipe));
}

function recipeResponse(params: { status: number; recipe: RegisteredAgent }): Response {
  const { status, recipe } = params;
  return withEtag(json(status, redactRecipe(recipe)), recipeEtag(recipe));
}

export function getAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    const recipe = await deps.registry.get(id, version);
    if (!recipe) return agentNotFound(id);
    return recipeResponse({ status: 200, recipe });
  };
}

// ---------------------------------------------------------------------------
// Recipe CRUD — author / edit / clone agents through HTTP.
//
// Semantics:
//   - POST /api/agents is create-only: 409 when (id, version) exists.
//     Publishing a new version of an existing id is a POST with a new
//     `version`; that version becomes the active one.
//   - PATCH edits the active version in place. Editing an older version
//     is refused (409 `version_not_active`) so an edit can never change
//     which version is live. Metadata merges field by field; `If-Match`
//     (the GET `etag`) makes the edit conditional (412 on mismatch).
//   - Clone is create-only on the target id.
// ---------------------------------------------------------------------------

interface CreateAgentRequest {
  readonly id?: unknown;
  readonly version?: unknown;
  readonly backend?: unknown;
  readonly metadata?: unknown;
}

interface UpdateAgentRequest {
  readonly version?: unknown; // version to update; must be the active one
  readonly backend?: unknown; // replaced wholesale when present
  readonly metadata?: unknown; // merged field by field when present
}

interface CloneAgentRequest {
  readonly targetId?: unknown;
  readonly targetVersion?: unknown;
  /**
   * Optional secrets to provision alongside the clone, stored in
   * SecretsStorage at `secretsScope`. Keys follow the `/api/secrets`
   * rules. Requires a configured SecretsStorage (else 400).
   */
  readonly secrets?: unknown;
  /**
   * Where to store the supplied `secrets`. Three shapes match
   * SecretScope:
   *   { kind: 'global' }                            — host-wide
   *   { kind: 'namespace', namespaceId }            — tenant-scope
   *   { kind: 'resource', namespaceId, resourceId } — per-user
   * Default: global.
   */
  readonly secretsScope?: unknown;
}

function isFileManaged(params: { deps: AgentGatewayDeps; id: string }): boolean {
  return params.deps.fileManagedIds?.().includes(params.id) ?? false;
}

function fileManaged409(id: string): Response {
  return jsonError(
    409,
    "file_managed",
    `Agent "${id}" is managed by a recipe file; edit the file instead.`,
  );
}

function optionalString(raw: unknown): string | undefined {
  return typeof raw === "string" && raw.length > 0 ? raw : undefined;
}

export function createAgent(deps: AgentGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateAgentRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string" || body.id.length === 0) {
      return jsonError(400, "missing_id");
    }
    if (body.id.startsWith("_")) {
      // The `_catalog/*` URL prefix is reserved; an agent named `_x`
      // would shadow it once the route has the recipe in registry.
      return jsonError(400, "reserved_id_prefix", "Agent ids starting with `_` are reserved.");
    }
    if (isFileManaged({ deps, id: body.id })) return fileManaged409(body.id);
    if (body.backend === undefined || body.backend === null) {
      return jsonError(400, "missing_backend");
    }
    const backend = parseBackend(body.backend);
    if ("error" in backend) return jsonError(400, backend.error, backend.message);
    const restored = restoreRedactedSecrets({ next: backend.backend });
    if ("error" in restored) return jsonError(400, restored.error);
    let metadata: ReturnType<typeof mergeAgentMetadata> | undefined;
    if (body.metadata !== undefined && body.metadata !== null) {
      const patch = parseMetadataPatch(body.metadata);
      if ("error" in patch) return jsonError(400, patch.error, patch.message);
      metadata = mergeAgentMetadata({
        existing: { description: null, capabilities: [], tags: [] },
        patch: patch.metadata,
      });
    }
    const version = optionalString(body.version) ?? "v1";
    if (await deps.registry.get(body.id, version)) {
      return jsonError(409, "agent_exists", `Agent "${body.id}@${version}" already exists.`);
    }
    const recipe = await deps.registry.register({
      id: body.id,
      version,
      backend: restored,
      ...(metadata !== undefined && { metadata }),
    });
    return recipeResponse({
      status: 201,
      recipe: (await deps.registry.get(recipe.id, recipe.version)) ?? recipe,
    });
  };
}

export function updateAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    if (isFileManaged({ deps, id: id })) return fileManaged409(id);
    const body = await readJson<UpdateAgentRequest>(req);
    if (!body) return jsonError(400, "missing_body");

    const active = await deps.registry.get(id);
    if (!active) return agentNotFound(id);
    const version = optionalString(body.version);
    if (version !== undefined && version !== active.version) {
      if (!(await deps.registry.get(id, version))) return agentNotFound(id);
      return jsonError(
        409,
        "version_not_active",
        `Only the active version (${active.version}) can be edited; publish a new version instead.`,
      );
    }
    assertIfMatch({ req, etag: recipeEtag(active), what: "recipe" });

    let nextBackend = active.backend;
    if (body.backend !== undefined && body.backend !== null) {
      const parsed = parseBackend(body.backend);
      if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
      const restored = restoreRedactedSecrets({ next: parsed.backend, existing: active.backend });
      if ("error" in restored) return jsonError(400, restored.error);
      nextBackend = restored;
    }
    let nextMetadata = active.metadata;
    if (body.metadata !== undefined && body.metadata !== null) {
      const patch = parseMetadataPatch(body.metadata);
      if ("error" in patch) return jsonError(400, patch.error, patch.message);
      nextMetadata = mergeAgentMetadata({ existing: active.metadata, patch: patch.metadata });
    }
    const updated = await deps.registry.register({
      id,
      version: active.version,
      backend: nextBackend,
      metadata: nextMetadata,
    });
    return recipeResponse({
      status: 200,
      recipe: (await deps.registry.get(id, updated.version)) ?? updated,
    });
  };
}

export function deleteAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    if (isFileManaged({ deps, id: id })) return fileManaged409(id);
    const url = new URL(req.url);
    const version = url.searchParams.get("version") ?? undefined;
    if (!(await deps.registry.get(id, version))) return agentNotFound(id);
    await deps.registry.unregister(id, version);
    return new Response(null, { status: 204 });
  };
}

export function listAgentVersions(deps: AgentGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const versions = await deps.registry.versions(id);
    if (versions.length === 0) return agentNotFound(id);
    return json(200, { versions: versions.map(redactRecipe) } satisfies AgentVersionsResponse);
  };
}

export function cloneAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const sourceId = params.id;
    if (!sourceId) return jsonError(400, "missing_id");
    const body = await readJson<CloneAgentRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.targetId !== "string" || body.targetId.length === 0) {
      return jsonError(400, "missing_targetId");
    }
    if (body.targetId.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Agent ids starting with `_` are reserved.");
    }

    const url = new URL(req.url);
    const sourceVersion = url.searchParams.get("version") ?? undefined;
    const source = await deps.registry.get(sourceId, sourceVersion);
    if (!source) return agentNotFound(sourceId);

    const secretsScope = parseCloneSecretsScope(body.secretsScope);
    if ("error" in secretsScope) return jsonError(400, secretsScope.error);
    const secrets = parseCloneSecrets(body.secrets);
    if ("error" in secrets) return jsonError(400, secrets.error, secrets.message);
    const provided = Object.entries(secrets.secrets);
    if (provided.length > 0 && !deps.secrets) {
      return jsonError(400, "secrets_unsupported", "This server has no secrets storage wired.");
    }
    if (provided.length > 0) {
      await requireActiveNamespace({
        registry: deps.namespaces,
        namespace: secretScopeNamespace(secretsScope),
      });
    }

    if (await deps.registry.get(body.targetId)) {
      return jsonError(409, "agent_exists", `Agent "${body.targetId}" already exists.`);
    }

    const targetVersion = optionalString(body.targetVersion);
    const clone = await deps.registry.register({
      id: body.targetId,
      ...(targetVersion !== undefined && { version: targetVersion }),
      backend: source.backend,
      metadata: {
        description: source.metadata.description,
        capabilities: [...source.metadata.capabilities],
        tags: [...source.metadata.tags],
      },
    });
    for (const [key, value] of provided) {
      await deps.secrets!.set({ scope: secretsScope, key, value });
    }
    // `acceptedSecrets` echoes back the stored key names; values are never echoed.
    return json(201, {
      recipe: redactRecipe(clone),
      acceptedSecrets: provided.map(([key]) => key),
    });
  };
}

/**
 * Validate the clone `secrets` map with the same rules as `/api/secrets`:
 * an object of `key → non-empty string`, keys matching the key pattern,
 * values within the size cap.
 */
function parseCloneSecrets(
  raw: unknown,
): { secrets: Record<string, string> } | { error: string; message: string } {
  if (raw === undefined || raw === null) return { secrets: {} };
  if (typeof raw !== "object" || Array.isArray(raw)) {
    return { error: "invalid_secrets", message: "secrets must be an object of key → value." };
  }
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(raw)) {
    if (!SECRET_KEY_PATTERN.test(key)) {
      return {
        error: "invalid_key",
        message: "Secret keys must match [A-Za-z][A-Za-z0-9_\\-.]{0,127}.",
      };
    }
    if (typeof value !== "string" || value.length === 0) {
      return { error: "invalid_secrets", message: `Secret "${key}" must be a non-empty string.` };
    }
    if (Buffer.byteLength(value, "utf8") > SECRET_MAX_VALUE_BYTES) {
      return { error: "value_too_large", message: `Secret "${key}" is too large.` };
    }
    out[key] = value;
  }
  return { secrets: out };
}

/**
 * Parse the `secretsScope` field from a clone request. Defaults to
 * global when omitted; validates the discriminated union shape.
 */
function parseCloneSecretsScope(raw: unknown): SecretScope | { error: string } {
  if (raw === undefined || raw === null) return SecretScope.global();
  if (typeof raw !== "object") return { error: "invalid_secretsScope" };
  const obj = raw as { kind?: unknown; namespaceId?: unknown; resourceId?: unknown };
  if (obj.kind === "global") return SecretScope.global();
  if (obj.kind === "namespace") {
    if (typeof obj.namespaceId !== "string" || obj.namespaceId.length === 0) {
      return { error: "missing_namespaceId" };
    }
    return SecretScope.namespace(obj.namespaceId);
  }
  if (obj.kind === "resource") {
    if (typeof obj.namespaceId !== "string" || obj.namespaceId.length === 0) {
      return { error: "missing_namespaceId" };
    }
    if (typeof obj.resourceId !== "string" || obj.resourceId.length === 0) {
      return { error: "missing_resourceId" };
    }
    return SecretScope.resource(obj.namespaceId, obj.resourceId);
  }
  return { error: "invalid_secretsScope_kind" };
}

// ---------------------------------------------------------------------------
// Extract role — "Save as role". Lifts the active version's INLINE role
// into the RoleRegistry under a NEW role id, then rebinds the agent to a
// `ref` so the two share a live link. Create-only on the role; if the
// rebind fails the freshly written role is removed again.
// ---------------------------------------------------------------------------

interface ExtractRoleRequest {
  readonly roleId?: unknown;
  readonly roleVersion?: unknown;
  readonly metadata?: unknown;
}

export function extractRole(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const agentId = params.id;
    if (!agentId) return jsonError(400, "missing_id");
    if (!deps.roles) {
      return jsonError(501, "roles_not_configured", "This server has no role registry wired.");
    }
    const roles = deps.roles;
    if (isFileManaged({ deps, id: agentId })) return fileManaged409(agentId);
    const body = await readJson<ExtractRoleRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.roleId !== "string" || body.roleId.length === 0) {
      return jsonError(400, "missing_roleId");
    }
    if (body.roleId.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Role ids starting with `_` are reserved.");
    }

    const recipe = await deps.registry.get(agentId);
    if (!recipe) return agentNotFound(agentId);
    if (recipe.backend.type !== "local" || !("inline" in recipe.backend.role)) {
      return jsonError(
        400,
        "no_inline_role",
        "Only an agent that carries an inline role can be extracted.",
      );
    }
    if ((await roles.versions(body.roleId)).length > 0) {
      return jsonError(409, "role_exists", `Role "${body.roleId}" already exists.`);
    }

    const roleVersion = optionalString(body.roleVersion);
    const metadata =
      typeof body.metadata === "object" && body.metadata !== null
        ? (body.metadata as { description?: string | null; tags?: string[] })
        : undefined;

    const role = await roles.register({
      id: body.roleId,
      ...(roleVersion !== undefined && { version: roleVersion }),
      definition: recipe.backend.role.inline,
      ...(metadata !== undefined && { metadata }),
    });
    try {
      // Rebind the agent to a ref at the same agent version. Pin the role
      // version just written so the link is reproducible.
      const agent = await deps.registry.register({
        id: recipe.id,
        version: recipe.version,
        backend: { ...recipe.backend, role: { ref: { id: role.id, version: role.version } } },
        metadata: recipe.metadata,
      });
      return json(201, { role, agent: redactRecipe(agent) });
    } catch (err) {
      await roles.unregister(role.id, role.version).catch(() => {});
      throw err;
    }
  };
}
