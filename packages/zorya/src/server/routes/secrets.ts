// ---------------------------------------------------------------------------
// Secrets HTTP routes — CRUD over the SecretsStorage primitive.
//
// Routes:
//   POST   /api/secrets              — store a secret at exact scope (201)
//   GET    /api/secrets              — list keys at scope (NEVER values)
//   DELETE /api/secrets/:key         — delete from scope (204; 404 if absent)
//
// All routes require dashboard auth (apiKeys middleware). Read paths
// return key NAMES only; the stored values never leave the server
// outside of in-process resolve() calls by the agent runtime.
//
// Permission gradient: coarse — anyone with dashboard auth can read/write
// at any scope, unless an `authorizeScope` hook is wired: then listing,
// storing or deleting a namespace- or resource-scoped secret needs the
// caller to be allowed that (namespace, resource?) — 403 `scope_forbidden`.
// The global scope is not a tenant scope and stays behind server auth only.
// ---------------------------------------------------------------------------

import type { SecretScope, SecretsStorage } from "@promin/agent";
import type { NamespaceRegistry } from "@promin/workflow";
import { z } from "zod";
import { json, jsonError, noContent } from "../router.ts";
import { parseBody, parseParams, parseQuery } from "../request-validation.ts";
import { requireActiveNamespace, secretScopeNamespace } from "../namespace-guard.ts";
import { internalError, type ScopeAuthorizer } from "./agent-gateway-kit.ts";

export interface SecretsGatewayDeps {
  readonly secrets: SecretsStorage;
  /** Binds a namespace- or resource-scoped secret to the caller. See `ScopeAuthorizer`. */
  readonly authorizeScope?: ScopeAuthorizer;
  /**
   * When set, storing a namespace- or resource-scoped secret needs a
   * registered, active namespace (404 `namespace_not_found`, 409
   * `namespace_archived`). Listing and deleting are not checked, so an
   * archived namespace's secrets can still be cleaned up.
   */
  readonly namespaces?: NamespaceRegistry;
}

/** Allowed secret key shape — shared with the clone route. */
export const SECRET_KEY_PATTERN = /^[A-Za-z][A-Za-z0-9_\-.]{0,127}$/;
export const SECRET_MAX_VALUE_BYTES = 1_048_576; // 1 MB

const ScopeId = z.string().min(1).max(256);

/** A secret scope on the wire. Omitted means global. */
export const SecretScopeSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("global") }).strict(),
  z.object({ kind: z.literal("namespace"), namespaceId: ScopeId }).strict(),
  z.object({ kind: z.literal("resource"), namespaceId: ScopeId, resourceId: ScopeId }).strict(),
]);

const SecretKeySchema = z.string().regex(SECRET_KEY_PATTERN, {
  message: "must match [A-Za-z][A-Za-z0-9_\\-.]{0,127}",
});

/** Body of `POST /api/secrets`. */
export const CreateSecretRequestSchema = z
  .object({
    scope: SecretScopeSchema.optional(),
    key: SecretKeySchema,
    value: z.string().min(1),
  })
  .strict();

/** Query of `GET /api/secrets` and `DELETE /api/secrets/:key`: `?scope=global|namespace|resource`. */
export const SecretScopeQuerySchema = z
  .object({
    scope: z.enum(["global", "namespace", "resource"]).default("global"),
    namespaceId: ScopeId.optional(),
    resourceId: ScopeId.optional(),
  })
  .transform((q, ctx): SecretScope => {
    if (q.scope === "global") return { kind: "global" };
    if (q.namespaceId === undefined) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ["namespaceId"], message: "Required" });
      return z.NEVER;
    }
    if (q.scope === "namespace") return { kind: "namespace", namespaceId: q.namespaceId };
    if (q.resourceId === undefined) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ["resourceId"], message: "Required" });
      return z.NEVER;
    }
    return { kind: "resource", namespaceId: q.namespaceId, resourceId: q.resourceId };
  });

const SecretParamsSchema = z.object({ key: SecretKeySchema });

/**
 * 403 `scope_forbidden` unless the caller may act in the secret scope's
 * namespace (and resource). Global scopes are not checked.
 */
export async function forbidSecretScope(params: {
  readonly authorizeScope: ScopeAuthorizer | undefined;
  readonly req: Request;
  readonly scope: SecretScope;
}): Promise<Response | null> {
  const { authorizeScope, req, scope } = params;
  if (!authorizeScope || scope.kind === "global") return null;
  const allowed = await authorizeScope({
    req,
    scope: {
      namespaceId: scope.namespaceId,
      ...(scope.kind === "resource" && { resourceId: scope.resourceId }),
    },
  });
  return allowed ? null : jsonError(403, "scope_forbidden");
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
    const body = await parseBody({ req, schema: CreateSecretRequestSchema });
    const scope: SecretScope = body.scope ?? { kind: "global" };
    const forbidden = await forbidSecretScope({ authorizeScope: deps.authorizeScope, req, scope });
    if (forbidden) return forbidden;
    if (Buffer.byteLength(body.value, "utf8") > SECRET_MAX_VALUE_BYTES) {
      return jsonError(
        413,
        "value_too_large",
        `Secret value exceeds ${SECRET_MAX_VALUE_BYTES} bytes.`,
      );
    }
    await requireActiveNamespace({
      registry: deps.namespaces,
      namespace: secretScopeNamespace(scope),
    });
    try {
      await deps.secrets.set({ scope, key: body.key, value: body.value });
      const response: CreateSecretResponse = { scope, key: body.key };
      return json(201, response);
    } catch (err) {
      return internalError({ code: "set_failed", err });
    }
  };
}

export function listSecrets(deps: SecretsGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const scope = parseQuery({ req, schema: SecretScopeQuerySchema });
    const forbidden = await forbidSecretScope({ authorizeScope: deps.authorizeScope, req, scope });
    if (forbidden) return forbidden;
    try {
      const keys = await deps.secrets.list({ scope });
      const response: ListSecretsResponse = { scope, keys };
      return json(200, response);
    } catch (err) {
      return internalError({ code: "list_failed", err });
    }
  };
}

export function deleteSecret(deps: SecretsGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { key } = parseParams({ params, schema: SecretParamsSchema });
    const scope = parseQuery({ req, schema: SecretScopeQuerySchema });
    const forbidden = await forbidSecretScope({ authorizeScope: deps.authorizeScope, req, scope });
    if (forbidden) return forbidden;
    try {
      const keys = await deps.secrets.list({ scope });
      if (!keys.includes(key)) return jsonError(404, "not_found");
      await deps.secrets.delete({ scope, key });
      return noContent();
    } catch (err) {
      return internalError({ code: "delete_failed", err });
    }
  };
}
