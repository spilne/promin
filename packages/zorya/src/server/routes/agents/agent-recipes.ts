// ---------------------------------------------------------------------------
// Agent recipes over HTTP: discovery (list, get, sources), CRUD, versions,
// promote, clone and extract-role. Versioning follows
// `versioned-resource.ts`: edits create versions, `If-Match` is the
// store's compare-and-set.
// ---------------------------------------------------------------------------

import { listAllVersioned, SecretScope, type RegisteredAgent } from "@promin/agent";
import { json, jsonError, readJson } from "../../router.ts";
import { requireActiveNamespace, secretScopeNamespace } from "../../namespace-guard.ts";
import { AgentListQuerySchema } from "../../api-types.ts";
import { parseQuery } from "../../request-validation.ts";
import {
  mergeAgentMetadata,
  parseBackend,
  parseMetadataPatch,
  redactRecipe,
  restoreRedactedSecrets,
  type AgentMetadataPatch,
} from "../agent-gateway-kit.ts";
import { forbidSecretScope, SECRET_KEY_PATTERN, SECRET_MAX_VALUE_BYTES } from "../secrets.ts";
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
} from "../versioned-resource.ts";
import type { AgentGatewayDeps, AgentsListResponse } from "./agent-gateway-deps.ts";
import { agentNotFound } from "./agent-scope.ts";

// ---------------------------------------------------------------------------
// Discovery — list + get
// ---------------------------------------------------------------------------

/**
 * Report which agent recipes are file-managed (their active version was
 * written by a scan) vs operator-authored. The editor uses this to disable
 * Save on a file-scanned recipe; the server refuses such edits as well.
 * Stored, so every replica answers the same from boot.
 */
export function listAgentSources(deps: { readonly registry: AgentGatewayDeps["registry"] }) {
  return listFileManagedRoute({ registry: deps.registry });
}

/**
 * GET /api/agents — one row per agent: its active version (the one `GET
 * /api/agents/:id` answers) plus how many versions it has, sorted by id.
 * `capability` / `tag` filter on the active version in the store, before
 * `limit` / `offset`, so `total` counts every matching agent and every page
 * is full. Designer drafts are hidden.
 */
export function listAgents(deps: AgentGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const { capability, tag, limit, offset } = parseQuery({ req, schema: AgentListQuerySchema });
    const matching = await listAllVersioned({
      registry: deps.registry,
      filter: {
        ...(capability !== undefined && { capability }),
        ...(tag !== undefined && { tag }),
        drafts: "exclude",
      },
    });
    const agents = await Promise.all(
      matching.slice(offset, offset + limit).map(async (recipe) => ({
        ...redactRecipe(recipe),
        versionCount: (await deps.registry.versions({ id: recipe.id })).length,
      })),
    );
    return json(200, { agents, total: matching.length } satisfies AgentsListResponse);
  };
}

function recipeResponse(params: { status: number; recipe: RegisteredAgent }): Response {
  return entryResponse({
    status: params.status,
    entry: params.recipe,
    body: redactRecipe(params.recipe),
  });
}

export function getAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const version = new URL(req.url).searchParams.get("version") ?? undefined;
    const recipe = await deps.registry.get({ id, ...(version !== undefined && { version }) });
    if (!recipe) return agentNotFound(id);
    return recipeResponse({ status: 200, recipe });
  };
}

// ---------------------------------------------------------------------------
// Recipe CRUD — author / edit / clone agents through HTTP.
//
//   - POST /api/agents creates a new agent (409 `agent_exists` when the id
//     is taken); its first version is active.
//   - PATCH /api/agents/:id needs `If-Match` (the active version's etag).
//     A `backend` / `metadata` change registers a new version built on the
//     active one (`version` names it; `publish: false` keeps it a draft
//     until promoted) — refused for a file-managed recipe. `enabled` flips
//     the agent's kill switch, file-managed or not.
//   - POST /api/agents/:id/promote makes a version active (rollback).
//   - Clone is create-only on the target id.
// ---------------------------------------------------------------------------

interface CreateAgentRequest {
  readonly id?: unknown;
  readonly version?: unknown;
  readonly backend?: unknown;
  readonly metadata?: unknown;
  readonly enabled?: unknown;
}

interface UpdateAgentRequest {
  /** Label of the new version. Default: the next `v<n>`. */
  readonly version?: unknown;
  /** Replaced wholesale when present. */
  readonly backend?: unknown;
  /** Merged field by field when present. */
  readonly metadata?: unknown;
  /** Make the new version active. Default `true`. */
  readonly publish?: unknown;
  /** The agent's kill switch. */
  readonly enabled?: unknown;
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

function fileManaged409(id: string): Response {
  return jsonError(
    409,
    "file_managed",
    `Agent "${id}" is managed by a recipe file; edit the file instead.`,
  );
}

function agentExists409(id: string): Response {
  return jsonError(409, "agent_exists", `Agent "${id}" already exists.`);
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
    if (body.backend === undefined || body.backend === null) {
      return jsonError(400, "missing_backend");
    }
    if (body.enabled !== undefined && typeof body.enabled !== "boolean") {
      return jsonError(400, "invalid_enabled");
    }
    const backend = parseBackend({ raw: body.backend, mcp: deps.mcp });
    if ("error" in backend) return jsonError(400, backend.error, backend.message);
    const restored = restoreRedactedSecrets({ next: backend.backend });
    if ("error" in restored) return jsonError(400, restored.error);
    let metadata: AgentMetadataPatch | undefined;
    if (body.metadata !== undefined && body.metadata !== null) {
      const patch = parseMetadataPatch(body.metadata);
      if ("error" in patch) return jsonError(400, patch.error, patch.message);
      metadata = patch.metadata;
    }
    const version = optionalVersion(body.version);
    const id = body.id;
    const created = await createOnly({
      register: () =>
        deps.registry.register({
          id,
          ...(version !== undefined && { version }),
          backend: restored,
          ...(metadata !== undefined && { metadata }),
          ...(body.enabled === false && { enabled: false }),
          expectedActive: null,
        }),
      onExists: () => agentExists409(id),
    });
    if (created instanceof Response) return created;
    return recipeResponse({ status: 201, recipe: created });
  };
}

export function updateAgent(deps: AgentGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<UpdateAgentRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    // A file owns the recipe's content (the next scan would undo an edit);
    // the kill switch is the operator's either way.
    const editsContent =
      (body.backend !== undefined && body.backend !== null) ||
      (body.metadata !== undefined && body.metadata !== null);
    const ifMatch = ifMatchVersions({ req, required: true });
    if (body.enabled !== undefined && typeof body.enabled !== "boolean") {
      return jsonError(400, "invalid_enabled");
    }
    if (body.publish !== undefined && typeof body.publish !== "boolean") {
      return jsonError(400, "invalid_publish");
    }

    const active = await deps.registry.get({ id });
    if (!active) return agentNotFound(id);
    if (editsContent && isFileManagedEntry(active)) return fileManaged409(id);
    matchActiveVersion({ tags: ifMatch, active: active.version });
    // The edit is built on the version just read; the store refuses it
    // (412) if another write moved the active version since.
    const base = active.version;

    let nextBackend = active.backend;
    if (body.backend !== undefined && body.backend !== null) {
      const parsed = parseBackend({ raw: body.backend, mcp: deps.mcp });
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
    const version = optionalVersion(body.version);
    // One write: the new version and the kill switch land together or not
    // at all (a lost compare-and-set leaves both untouched).
    const enabled = typeof body.enabled === "boolean" ? body.enabled : undefined;
    let current = active;
    if (nextBackend !== active.backend || nextMetadata !== active.metadata) {
      current = await registryCall(() =>
        deps.registry.register({
          id,
          ...(version !== undefined && { version }),
          backend: nextBackend,
          metadata: nextMetadata,
          promote: body.publish !== false,
          expectedActive: base,
          ...(enabled !== undefined && { enabled }),
        }),
      );
    } else if (enabled !== undefined) {
      current = await registryCall(() =>
        deps.registry.setEnabled({ id, enabled, expectedActive: base }),
      );
    }
    return recipeResponse({ status: 200, recipe: current });
  };
}

/** `DELETE /api/agents/:id[?version=]`. */
export function deleteAgent(deps: AgentGatewayDeps) {
  return deleteRoute({
    registry: deps.registry,
    notFound: agentNotFound,
    refuseFileManaged: true,
  });
}

/** `GET /api/agents/:id/versions` — every version, oldest first, secrets redacted. */
export function listAgentVersions(deps: AgentGatewayDeps) {
  return listVersionsRoute({
    registry: deps.registry,
    notFound: agentNotFound,
    present: redactRecipe,
  });
}

/** `POST /api/agents/:id/promote` `{ version }`. */
export function promoteAgent(deps: AgentGatewayDeps) {
  return promoteRoute({
    registry: deps.registry,
    notFound: agentNotFound,
    present: redactRecipe,
  });
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
    const targetId = body.targetId;

    const url = new URL(req.url);
    const sourceVersion = url.searchParams.get("version") ?? undefined;
    const source = await deps.registry.get({
      id: sourceId,
      ...(sourceVersion !== undefined && { version: sourceVersion }),
    });
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
      const forbidden = await forbidSecretScope({
        authorizeScope: deps.authorizeScope,
        req,
        scope: secretsScope,
      });
      if (forbidden) return forbidden;
      await requireActiveNamespace({
        registry: deps.namespaces,
        namespace: secretScopeNamespace(secretsScope),
      });
    }

    const targetVersion = optionalVersion(body.targetVersion);
    const clone = await createOnly({
      register: () =>
        deps.registry.register({
          id: targetId,
          ...(targetVersion !== undefined && { version: targetVersion }),
          backend: source.backend,
          metadata: source.metadata,
          expectedActive: null,
        }),
      onExists: () => agentExists409(targetId),
    });
    if (clone instanceof Response) return clone;
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
// into the RoleRegistry under a NEW role id, then publishes a new agent
// version bound to it by `ref` (pinned to the role version just written).
// Create-only on the role; if the agent write fails the role is removed
// again.
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
    const body = await readJson<ExtractRoleRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.roleId !== "string" || body.roleId.length === 0) {
      return jsonError(400, "missing_roleId");
    }
    if (body.roleId.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Role ids starting with `_` are reserved.");
    }
    const roleId = body.roleId;
    const ifMatch = ifMatchVersions({ req, required: false });

    const recipe = await deps.registry.get({ id: agentId });
    if (!recipe) return agentNotFound(agentId);
    if (isFileManagedEntry(recipe)) return fileManaged409(agentId);
    matchActiveVersion({ tags: ifMatch, active: recipe.version });
    if (recipe.backend.type !== "local" || !("inline" in recipe.backend.role)) {
      return jsonError(
        400,
        "no_inline_role",
        "Only an agent that carries an inline role can be extracted.",
      );
    }
    const backend = recipe.backend;
    const inline = recipe.backend.role.inline;

    const roleVersion = optionalVersion(body.roleVersion);
    const metadata =
      typeof body.metadata === "object" && body.metadata !== null
        ? (body.metadata as { description?: string | null; tags?: string[] })
        : undefined;

    const role = await createOnly({
      register: () =>
        roles.register({
          id: roleId,
          ...(roleVersion !== undefined && { version: roleVersion }),
          definition: inline,
          ...(metadata !== undefined && { metadata }),
          expectedActive: null,
        }),
      onExists: () => jsonError(409, "role_exists", `Role "${roleId}" already exists.`),
    });
    if (role instanceof Response) return role;
    try {
      const agent = await registryCall(() =>
        deps.registry.register({
          id: recipe.id,
          backend: { ...backend, role: { ref: { id: role.id, version: role.version } } },
          metadata: recipe.metadata,
          promote: true,
          expectedActive: recipe.version,
        }),
      );
      return json(201, { role, agent: redactRecipe(agent) });
    } catch (err) {
      await roles.unregister({ id: role.id }).catch(() => {});
      throw err;
    }
  };
}
