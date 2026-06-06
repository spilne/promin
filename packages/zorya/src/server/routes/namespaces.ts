// ---------------------------------------------------------------------------
// Namespaces HTTP routes — authoritative namespace list and lifecycle.
// ---------------------------------------------------------------------------

import { json, jsonError, readJson } from "../router.ts";
import type {
  Namespace,
  NamespaceCapabilities,
  NamespaceCreateInput,
  NamespaceService,
  NamespaceUpdateInput,
} from "../services/namespaces.ts";
import { NamespaceArchivedError, NamespaceNotFoundError } from "../services/namespaces.ts";

export interface NamespacesGatewayDeps {
  readonly namespaces: NamespaceService;
}

export interface NamespacesResponse {
  readonly namespaces: Namespace[];
  readonly defaultNamespaceId: string;
}

interface CreateNamespaceRequest {
  readonly id?: unknown;
  readonly displayName?: unknown;
  readonly description?: unknown;
  readonly capabilities?: unknown;
  readonly metadata?: unknown;
}

interface UpdateNamespaceRequest {
  readonly displayName?: unknown;
  readonly description?: unknown;
  readonly status?: unknown;
  readonly capabilities?: unknown;
  readonly metadata?: unknown;
}

export function listNamespaces(deps: NamespacesGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const status = url.searchParams.get("status");
    const allowedStatus = status === "active" || status === "archived" ? status : undefined;
    try {
      await deps.namespaces.ensureDefaultNamespace();
      const namespaces = await deps.namespaces.registry.list(
        allowedStatus ? { status: allowedStatus } : undefined,
      );
      const response: NamespacesResponse = {
        namespaces,
        defaultNamespaceId: deps.namespaces.defaultNamespaceId,
      };
      return json(200, response);
    } catch (err) {
      return namespaceRouteError(err, "list_failed");
    }
  };
}

export function createNamespace(deps: NamespacesGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateNamespaceRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.id !== "string") return jsonError(400, "missing_id");
    const input: NamespaceCreateInput = {
      id: body.id,
      ...(typeof body.displayName === "string" && { displayName: body.displayName }),
      ...(body.description === null || typeof body.description === "string"
        ? { description: body.description }
        : {}),
      ...(isRecord(body.capabilities) && {
        capabilities: body.capabilities as NamespaceCapabilities,
      }),
      ...(isRecord(body.metadata) && { metadata: body.metadata }),
    };
    try {
      const namespace = await deps.namespaces.registry.create(input);
      return json(201, { namespace });
    } catch (err) {
      return namespaceRouteError(err, "create_failed");
    }
  };
}

export function getNamespace(deps: NamespacesGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    try {
      const namespace = await deps.namespaces.registry.get(id);
      if (!namespace) return jsonError(404, "namespace_not_found");
      return json(200, { namespace });
    } catch (err) {
      return namespaceRouteError(err, "get_failed");
    }
  };
}

export function updateNamespace(deps: NamespacesGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const body = await readJson<UpdateNamespaceRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    const patch: MutableNamespaceUpdateInput = {};
    if (typeof body.displayName === "string") patch.displayName = body.displayName;
    if (body.description === null || typeof body.description === "string") {
      patch.description = body.description;
    }
    if (body.status === "active" || body.status === "archived") patch.status = body.status;
    if (isRecord(body.capabilities)) {
      patch.capabilities = body.capabilities as NamespaceCapabilities;
    }
    if (isRecord(body.metadata)) patch.metadata = body.metadata;
    if (Object.keys(patch).length === 0) return jsonError(400, "empty_patch");
    try {
      const namespace = await deps.namespaces.registry.update(id, patch);
      return json(200, { namespace });
    } catch (err) {
      return namespaceRouteError(err, "update_failed");
    }
  };
}

export function archiveNamespace(deps: NamespacesGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    try {
      const namespace = await deps.namespaces.registry.archive(id);
      return json(200, { namespace });
    } catch (err) {
      return namespaceRouteError(err, "archive_failed");
    }
  };
}

function namespaceRouteError(err: unknown, fallback: string): Response {
  if (err instanceof NamespaceNotFoundError) return jsonError(404, "namespace_not_found");
  if (err instanceof NamespaceArchivedError) return jsonError(409, "namespace_archived");
  const message = err instanceof Error ? err.message : String(err);
  if (message === "invalid_namespace_id") return jsonError(400, "invalid_namespace_id");
  if (message.startsWith("namespace already exists")) {
    return jsonError(409, "namespace_exists", message);
  }
  return jsonError(500, fallback, message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

type MutableNamespaceUpdateInput = {
  -readonly [K in keyof NamespaceUpdateInput]: NamespaceUpdateInput[K];
};
