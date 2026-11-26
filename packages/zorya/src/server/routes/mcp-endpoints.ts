// ---------------------------------------------------------------------------
// MCP endpoint routes — author endpoints, preview what they expose, and
// serve them over MCP.
//
// Registry (versioning per `versioned-resource.ts`, operator plane):
//   GET    /api/mcp-endpoints               active version per endpoint (tag, paging)
//   POST   /api/mcp-endpoints               create (409 when the id exists)
//   GET    /api/mcp-endpoints/_sources      file-managed ids
//   GET    /api/mcp-endpoints/_catalog      tools endpoints may expose: the shared
//                                           ones, and each endpoint's own
//   GET    /api/mcp-endpoints/:id           one (active, or ?version=)
//   PATCH  /api/mcp-endpoints/:id           new version and/or kill switch; If-Match
//   DELETE /api/mcp-endpoints/:id           all versions, or ?version=
//   GET    /api/mcp-endpoints/:id/versions
//   POST   /api/mcp-endpoints/:id/promote
//   GET    /api/mcp-endpoints/:id/tools     exposed tools + skipped bindings (?version=)
//
// Serving (MCP Streamable HTTP, stateless; POST only, GET/DELETE 405):
//   /ns/:namespace/mcp/:id   a tenant: authorized as `{ kind: "tenant",
//                            namespaceId, resourceId? }`, the namespace must
//                            be active, and the endpoint must allow it (its
//                            `namespaces`; otherwise 404, so a tenant can't
//                            probe for endpoints it may not use)
//   /mcp/:id                 the global scope: authorized as
//                            `{ kind: "operator", resource: "mcp-endpoint" }`
//
// The resource a call acts for comes from the `X-Zorya-Resource` header
// (MCP clients can set headers, not request bodies); scoped tools called
// without one fail with a tool error.
// ---------------------------------------------------------------------------

import {
  agentToolJsonSchema,
  type AgentTool,
  handleMcpEndpointRequest,
  type McpEndpointMetadata,
  type McpEndpointRecipeInput,
  type McpEndpointToolBinding,
  type RegisteredMcpEndpoint,
} from "@promin/agent";
import { NAMESPACE_ID_PATTERN, type NamespaceRegistry } from "@promin/workflow";
import { z } from "zod";
import { requireActiveNamespace } from "../namespace-guard.ts";
import { json, jsonError, parseIntParam, readJson } from "../router.ts";
import type { ZoryaMcpEndpoints } from "../services/mcp-endpoints/index.ts";
import type { ScopeAuthorizer } from "./agent-gateway-kit.ts";
import {
  createOnly,
  deleteRoute,
  entryResponse,
  ifMatchVersions,
  listFileManagedRoute,
  listVersionsRoute,
  matchActiveVersion,
  optionalVersion,
  promoteRoute,
  registryCall,
} from "./versioned-resource.ts";

export type {
  McpEndpointSkippedTool,
  McpEndpointToolBinding,
  RegisteredMcpEndpoint,
} from "@promin/agent";

/** Header naming the resource (end user, account) a call acts for. */
export const MCP_RESOURCE_HEADER = "x-zorya-resource";

/** Longest accepted `X-Zorya-Resource`. */
const MAX_RESOURCE_ID_LENGTH = 256;

export interface McpEndpointsListResponse {
  endpoints: RegisteredMcpEndpoint[];
  nextCursor: string | null;
}

export interface McpEndpointVersionsResponse {
  versions: RegisteredMcpEndpoint[];
}

/** One tool as the endpoint exposes it (no code). */
export interface McpEndpointToolView {
  readonly name: string;
  readonly tool: string;
  readonly description: string;
  readonly private: boolean;
  readonly parameters: Record<string, unknown>;
}

export interface McpEndpointToolsResponse {
  tools: McpEndpointToolView[];
  skipped: Array<{ tool: string; private: boolean; code: string }>;
}

/** A tool an endpoint may expose, as the builder's picker lists it. */
export interface McpEndpointCatalogTool {
  readonly name: string;
  readonly description: string;
  readonly requireApproval: boolean;
  readonly idempotent: boolean;
  readonly enabled: boolean;
  /** `scoped` / `elevated`: needs a tenant scope (and, elevated, a capability). */
  readonly kind: "plain" | "scoped" | "elevated";
}

export interface McpEndpointCatalogResponse {
  /** Shared tools, referenced by name from any endpoint. */
  shared: McpEndpointCatalogTool[];
  /** Each file-defined endpoint's own tools, by endpoint id. */
  private: Record<string, McpEndpointCatalogTool[]>;
}

export interface McpEndpointGatewayDeps {
  readonly endpoints: ZoryaMcpEndpoints;
}

function endpointNotFound(id: string): Response {
  return jsonError(404, "mcp_endpoint_not_found", `MCP endpoint "${id}" is not registered.`);
}

// ---------------------------------------------------------------------------
// Registry CRUD
// ---------------------------------------------------------------------------

export function listMcpEndpoints(deps: McpEndpointGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const tag = url.searchParams.get("tag") ?? undefined;
    const limit = parseIntParam({ raw: url.searchParams.get("limit"), min: 1, max: 1_000 });
    const cursor = url.searchParams.get("cursor") ?? undefined;
    const page = await deps.endpoints.registry.list({
      filter: tag !== undefined ? { tag } : {},
      ...(limit !== undefined && { limit }),
      ...(cursor !== undefined && { cursor }),
    });
    return json(200, {
      endpoints: page.items,
      nextCursor: page.nextCursor,
    } satisfies McpEndpointsListResponse);
  };
}

export function listMcpEndpointSources(deps: McpEndpointGatewayDeps) {
  return listFileManagedRoute({ registry: deps.endpoints.registry });
}

export function getMcpEndpointCatalog(deps: McpEndpointGatewayDeps) {
  return async (): Promise<Response> => {
    const view = (tool: AgentTool<any, any>): McpEndpointCatalogTool => ({
      name: tool.name,
      description: tool.description,
      requireApproval: tool.requireApproval === true,
      idempotent: tool.idempotent === true,
      enabled: tool.enabled !== false,
      kind: tool.kind ?? "plain",
    });
    const byName = (a: McpEndpointCatalogTool, b: McpEndpointCatalogTool) =>
      a.name.localeCompare(b.name);
    const shared = Object.entries(deps.endpoints.sharedTools())
      .map(([name, tool]) => ({ ...view(tool), name }))
      .sort(byName);
    const privateTools: Record<string, McpEndpointCatalogTool[]> = {};
    for (const id of deps.endpoints.privateTools.ids()) {
      privateTools[id] = Object.entries(deps.endpoints.privateTools.get(id))
        .map(([name, tool]) => ({ ...view(tool), name }))
        .sort(byName);
    }
    return json(200, { shared, private: privateTools } satisfies McpEndpointCatalogResponse);
  };
}

export function getMcpEndpoint(deps: McpEndpointGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const version = new URL(req.url).searchParams.get("version") ?? undefined;
    const entry = await deps.endpoints.registry.get({
      id,
      ...(version !== undefined && { version }),
    });
    if (!entry) return endpointNotFound(id);
    return entryResponse({ status: 200, entry });
  };
}

export function listMcpEndpointVersions(deps: McpEndpointGatewayDeps) {
  return listVersionsRoute({ registry: deps.endpoints.registry, notFound: endpointNotFound });
}

export function promoteMcpEndpoint(deps: McpEndpointGatewayDeps) {
  return promoteRoute({ registry: deps.endpoints.registry, notFound: endpointNotFound });
}

export function deleteMcpEndpoint(deps: McpEndpointGatewayDeps) {
  return deleteRoute({ registry: deps.endpoints.registry, notFound: endpointNotFound });
}

const BindingSchema = z
  .object({
    tool: z.string().min(1),
    private: z.boolean().optional(),
    name: z.string().min(1).optional(),
    description: z.string().optional(),
  })
  .strict();

/** Content fields a create or edit may carry; deep checks are the registry kind's. */
const ContentSchema = z
  .object({
    title: z.string().nullable().optional(),
    instructions: z.string().nullable().optional(),
    tools: z.array(BindingSchema).optional(),
    capabilities: z.array(z.string()).optional(),
    namespaces: z.array(z.string().regex(NAMESPACE_ID_PATTERN)).nullable().optional(),
    trustClientApproval: z.boolean().optional(),
    metadata: z
      .object({
        description: z.string().nullable().optional(),
        tags: z.array(z.string()).optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

const CreateSchema = ContentSchema.extend({
  id: z.string().min(1),
  version: z.string().optional(),
});

const UpdateSchema = ContentSchema.extend({
  version: z.string().optional(),
  /** Make the new version active. Default `true`. */
  publish: z.boolean().optional(),
  enabled: z.boolean().optional(),
});

type ContentPatch = z.infer<typeof ContentSchema>;

function invalidBody(error: z.ZodError): Response {
  const detail = error.issues
    .map((i) => `${i.path.length > 0 ? i.path.join(".") : "(body)"}: ${i.message}`)
    .join("; ");
  return jsonError(400, "invalid_body", detail);
}

/** Field-merge `patch` over `base` (an edit keeps what it does not mention). */
function mergeContent(base: McpEndpointRecipeInput, patch: ContentPatch): McpEndpointRecipeInput {
  const metadata: Partial<McpEndpointMetadata> | undefined =
    patch.metadata !== undefined
      ? {
          ...base.metadata,
          ...(patch.metadata.description !== undefined && {
            description: patch.metadata.description,
          }),
          ...(patch.metadata.tags !== undefined && { tags: patch.metadata.tags }),
        }
      : base.metadata;
  return {
    tools: (patch.tools as McpEndpointToolBinding[] | undefined) ?? base.tools,
    title: patch.title !== undefined ? patch.title : (base.title ?? null),
    instructions:
      patch.instructions !== undefined ? patch.instructions : (base.instructions ?? null),
    capabilities: patch.capabilities ?? base.capabilities ?? [],
    namespaces: patch.namespaces !== undefined ? patch.namespaces : (base.namespaces ?? null),
    trustClientApproval: patch.trustClientApproval ?? base.trustClientApproval ?? false,
    ...(metadata !== undefined && { metadata }),
  };
}

export function createMcpEndpoint(deps: McpEndpointGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const raw = await readJson<unknown>(req);
    if (raw === null) return jsonError(400, "missing_body");
    const parsed = CreateSchema.safeParse(raw);
    if (!parsed.success) return invalidBody(parsed.error);
    const { id, version: rawVersion, ...content } = parsed.data;
    if (id.startsWith("_")) {
      return jsonError(400, "reserved_id_prefix", "Endpoint ids starting with `_` are reserved.");
    }
    const version = optionalVersion(rawVersion);
    const input = mergeContent({ tools: [] }, content);
    const created = await createOnly({
      register: () =>
        deps.endpoints.registry.register({
          id,
          ...(version !== undefined && { version }),
          ...input,
          expectedActive: null,
        }),
      onExists: () => jsonError(409, "mcp_endpoint_exists", `MCP endpoint "${id}" already exists.`),
    });
    if (created instanceof Response) return created;
    return entryResponse({ status: 201, entry: created });
  };
}

export function updateMcpEndpoint(deps: McpEndpointGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const raw = await readJson<unknown>(req);
    if (raw === null) return jsonError(400, "missing_body");
    const parsed = UpdateSchema.safeParse(raw);
    if (!parsed.success) return invalidBody(parsed.error);
    const ifMatch = ifMatchVersions({ req, required: true });
    const { version: rawVersion, publish, enabled, ...content } = parsed.data;
    const editsContent = Object.keys(content).length > 0;

    const registry = deps.endpoints.registry;
    const active = await registry.get({ id });
    if (!active) return endpointNotFound(id);
    matchActiveVersion({ tags: ifMatch, active: active.version });

    const version = optionalVersion(rawVersion);
    const updated =
      !editsContent && enabled !== undefined
        ? await registryCall(() =>
            registry.setEnabled({ id, enabled, expectedActive: active.version }),
          )
        : await registryCall(() =>
            registry.register({
              id,
              ...(version !== undefined && { version }),
              ...mergeContent(active, content),
              promote: publish !== false,
              expectedActive: active.version,
              ...(enabled !== undefined && { enabled }),
            }),
          );
    return entryResponse({ status: 200, entry: updated });
  };
}

/** What an endpoint version exposes right now — the builder's preview. */
export function listMcpEndpointTools(deps: McpEndpointGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const version = new URL(req.url).searchParams.get("version") ?? undefined;
    const entry = await deps.endpoints.registry.get({
      id,
      ...(version !== undefined && { version }),
    });
    if (!entry) return endpointNotFound(id);
    const resolved = deps.endpoints.resolveTools(entry);
    return json(200, {
      tools: [...resolved.tools.values()].map((t) => ({
        name: t.name,
        tool: t.tool.name,
        description: t.description,
        private: t.private,
        parameters: agentToolJsonSchema(t.tool),
      })),
      skipped: resolved.skipped.map((s) => ({ ...s })),
    } satisfies McpEndpointToolsResponse);
  };
}

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

export interface ServeMcpEndpointDeps extends McpEndpointGatewayDeps {
  readonly namespaces?: NamespaceRegistry;
  readonly authorizeScope?: ScopeAuthorizer;
}

/** `/ns/:namespace/mcp/:id` (tenant) or, with `global`, `/mcp/:id`. */
export function serveMcpEndpoint(deps: ServeMcpEndpointDeps & { readonly global: boolean }) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    const namespaceId = deps.global ? undefined : params.namespace;
    if (!deps.global && (namespaceId === undefined || !NAMESPACE_ID_PATTERN.test(namespaceId))) {
      return jsonError(400, "invalid_namespace");
    }
    const rawResource = req.headers.get(MCP_RESOURCE_HEADER);
    const resourceId = rawResource !== null && rawResource.length > 0 ? rawResource : undefined;
    if (resourceId !== undefined && resourceId.length > MAX_RESOURCE_ID_LENGTH) {
      return jsonError(400, "invalid_resource");
    }

    // Authorize before looking anything up: a caller learns nothing about
    // endpoints in a scope it may not act in.
    if (deps.authorizeScope) {
      const allowed = await deps.authorizeScope({
        req,
        scope:
          namespaceId === undefined
            ? { kind: "operator", resource: "mcp-endpoint" }
            : {
                kind: "tenant",
                namespaceId,
                ...(resourceId !== undefined && { resourceId }),
              },
      });
      if (!allowed) return jsonError(403, "scope_forbidden");
    }

    const endpoint = await deps.endpoints.registry.get({ id });
    if (!endpoint) return endpointNotFound(id);
    if (
      namespaceId !== undefined &&
      endpoint.namespaces !== null &&
      !endpoint.namespaces.includes(namespaceId)
    ) {
      return endpointNotFound(id);
    }
    if (!endpoint.enabled) {
      return jsonError(410, "mcp_endpoint_disabled", `MCP endpoint "${id}" is disabled.`);
    }
    await requireActiveNamespace({ registry: deps.namespaces, namespace: namespaceId });

    const service = deps.endpoints;
    return handleMcpEndpointRequest({
      req,
      endpoint,
      tools: service.resolveTools(endpoint),
      scope: {
        ...(namespaceId !== undefined && { namespaceId }),
        ...(resourceId !== undefined && { resourceId }),
      },
      ...(service.defaultTimeoutMs !== undefined && { defaultTimeoutMs: service.defaultTimeoutMs }),
      ...(service.toolAuditLogger !== undefined && { toolAuditLogger: service.toolAuditLogger }),
      ...(service.clock !== undefined && { clock: service.clock }),
      scopeHint:
        "Call it through /ns/<namespace>/mcp/<endpoint> with an X-Zorya-Resource header naming the resource.",
      onToolError: ({ tool, error }) => service.onToolError({ endpoint: id, tool, error }),
    });
  };
}
