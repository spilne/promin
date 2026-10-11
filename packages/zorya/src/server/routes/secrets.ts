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
// at any scope.
// ---------------------------------------------------------------------------

import type { SecretScope, SecretsStorage } from "@promin/agent";
import { z } from "zod";
import { json, jsonError, noContent } from "../router.ts";
import { parseBody, parseParams, parseQuery } from "../request-validation.ts";
import { internalError } from "./agent-gateway-kit.ts";

export interface SecretsGatewayDeps {
  readonly secrets: SecretsStorage;
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
    if (Buffer.byteLength(body.value, "utf8") > SECRET_MAX_VALUE_BYTES) {
      return jsonError(
        413,
        "value_too_large",
        `Secret value exceeds ${SECRET_MAX_VALUE_BYTES} bytes.`,
      );
    }
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
