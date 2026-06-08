// ---------------------------------------------------------------------------
// Secrets HTTP routes — CRUD over the SecretsStorage primitive.
//
// Routes:
//   POST   /api/secrets              — store a secret at exact scope
//   GET    /api/secrets              — list keys at scope (NEVER values)
//   DELETE /api/secrets/:key         — delete from scope
//
// All routes require dashboard auth (apiKeys middleware). Read paths
// return key NAMES only; the stored values never leave the server
// outside of in-process resolve() calls by the agent runtime.
//
// Permission gradient: Phase 1 is coarse — anyone with dashboard auth
// can read/write at any scope. Per-namespace ACLs land later (multi-
// tenant work, promin-ybn9).
// ---------------------------------------------------------------------------

import type { SecretScope, SecretsStorage } from "@promin/agent";
import { json, jsonError, readJson } from "../router.ts";
import type { NamespaceService } from "../services/namespaces.ts";
import { resolveRequiredNamespaceId } from "./namespace-validation.ts";

export interface SecretsGatewayDeps {
  readonly secrets: SecretsStorage;
  readonly namespaces?: NamespaceService;
}

interface CreateSecretRequest {
  readonly scope?: unknown;
  readonly key?: unknown;
  readonly value?: unknown;
}

const KEY_PATTERN = /^[A-Za-z][A-Za-z0-9_\-.]{0,127}$/;
const MAX_VALUE_BYTES = 1_048_576; // 1 MB

/**
 * Parse a wire-format scope into a SecretScope. Accepts the discriminated
 * union shape ({ kind: 'global' | 'namespace' | 'resource', ... }) AND
 * a query-string convention for GET/DELETE — see `parseScopeFromQuery`.
 */
function parseScopeFromBody(raw: unknown): SecretScope | { error: string } {
  if (raw === undefined || raw === null) {
    // Default: global. Lets callers omit `scope` for the simplest case.
    return { kind: "global" };
  }
  if (typeof raw !== "object") return { error: "invalid_scope" };
  const obj = raw as { kind?: unknown; namespaceId?: unknown; resourceId?: unknown };
  if (obj.kind === "global") return { kind: "global" };
  if (obj.kind === "namespace") {
    if (typeof obj.namespaceId !== "string" || obj.namespaceId.length === 0) {
      return { error: "missing_namespaceId" };
    }
    return { kind: "namespace", namespaceId: obj.namespaceId };
  }
  if (obj.kind === "resource") {
    if (typeof obj.namespaceId !== "string" || obj.namespaceId.length === 0) {
      return { error: "missing_namespaceId" };
    }
    if (typeof obj.resourceId !== "string" || obj.resourceId.length === 0) {
      return { error: "missing_resourceId" };
    }
    return {
      kind: "resource",
      namespaceId: obj.namespaceId,
      resourceId: obj.resourceId,
    };
  }
  return { error: "invalid_scope_kind" };
}

function parseScopeFromQuery(url: URL): SecretScope | { error: string } {
  const kind = url.searchParams.get("scope") ?? "global";
  if (kind === "global") return { kind: "global" };
  if (kind === "namespace") {
    const ns = url.searchParams.get("namespaceId");
    if (!ns) return { error: "missing_namespaceId" };
    return { kind: "namespace", namespaceId: ns };
  }
  if (kind === "resource") {
    const ns = url.searchParams.get("namespaceId");
    const res = url.searchParams.get("resourceId");
    if (!ns) return { error: "missing_namespaceId" };
    if (!res) return { error: "missing_resourceId" };
    return { kind: "resource", namespaceId: ns, resourceId: res };
  }
  return { error: "invalid_scope_kind" };
}

export interface CreateSecretResponse {
  readonly scope: SecretScope;
  readonly key: string;
}

export interface ListSecretsResponse {
  readonly scope: SecretScope;
  readonly keys: ReadonlyArray<string>;
}

export function createSecret(deps: SecretsGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateSecretRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    const scope = parseScopeFromBody(body.scope);
    if ("error" in scope) return jsonError(400, scope.error);
    const resolvedScope = await resolveSecretScope(deps.namespaces, scope);
    if ("response" in resolvedScope) return resolvedScope.response;
    if (typeof body.key !== "string" || !KEY_PATTERN.test(body.key)) {
      return jsonError(400, "invalid_key", "Key must match [A-Za-z][A-Za-z0-9_\\-.]{0,127}.");
    }
    if (typeof body.value !== "string" || body.value.length === 0) {
      return jsonError(400, "missing_value");
    }
    if (Buffer.byteLength(body.value, "utf8") > MAX_VALUE_BYTES) {
      return jsonError(413, "value_too_large", `Secret value exceeds ${MAX_VALUE_BYTES} bytes.`);
    }
    try {
      await deps.secrets.set({ scope: resolvedScope.scope, key: body.key, value: body.value });
      const response: CreateSecretResponse = { scope: resolvedScope.scope, key: body.key };
      return json(201, response);
    } catch (err) {
      return jsonError(500, "set_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function listSecrets(deps: SecretsGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const scope = parseScopeFromQuery(url);
    if ("error" in scope) return jsonError(400, scope.error);
    const resolvedScope = await resolveSecretScope(deps.namespaces, scope);
    if ("response" in resolvedScope) return resolvedScope.response;
    try {
      const keys = await deps.secrets.list({ scope: resolvedScope.scope });
      const response: ListSecretsResponse = { scope: resolvedScope.scope, keys };
      return json(200, response);
    } catch (err) {
      return jsonError(500, "list_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export function deleteSecret(deps: SecretsGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const key = params.key;
    if (!key) return jsonError(400, "missing_key");
    const url = new URL(req.url);
    const scope = parseScopeFromQuery(url);
    if ("error" in scope) return jsonError(400, scope.error);
    const resolvedScope = await resolveSecretScope(deps.namespaces, scope);
    if ("response" in resolvedScope) return resolvedScope.response;
    try {
      await deps.secrets.delete({ scope: resolvedScope.scope, key });
      return new Response(null, { status: 204 });
    } catch (err) {
      return jsonError(500, "delete_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

async function resolveSecretScope(
  namespaces: NamespaceService | undefined,
  scope: SecretScope,
): Promise<{ scope: SecretScope } | { response: Response }> {
  if (scope.kind === "global") return { scope };
  const namespace = await resolveRequiredNamespaceId(namespaces, scope.namespaceId);
  if ("response" in namespace) return namespace;
  if (scope.kind === "namespace") {
    return { scope: { kind: "namespace", namespaceId: namespace.namespaceId } };
  }
  return {
    scope: {
      kind: "resource",
      namespaceId: namespace.namespaceId,
      resourceId: scope.resourceId,
    },
  };
}
