// ---------------------------------------------------------------------------
// MCP endpoints: the registry CRUD under `/api/mcp-endpoints` and the MCP
// serving paths `/ns/:namespace/mcp/:id` (tenant) and `/mcp/:id` (global).
// ---------------------------------------------------------------------------

import type { NamespaceRegistry } from "@promin/workflow";
import type { Router } from "../router.ts";
import type { ScopeAuthorizer } from "../routes/agent-gateway-kit.ts";
import {
  createMcpEndpoint,
  deleteMcpEndpoint,
  getMcpEndpoint,
  getMcpEndpointCatalog,
  listMcpEndpointSources,
  listMcpEndpointTools,
  listMcpEndpointVersions,
  listMcpEndpoints,
  promoteMcpEndpoint,
  serveMcpEndpoint,
  updateMcpEndpoint,
} from "../routes/mcp-endpoints.ts";
import type { ZoryaMcpEndpoints } from "../services/mcp-endpoints/index.ts";

export function mountMcpEndpoints(params: {
  readonly router: Router;
  readonly endpoints: ZoryaMcpEndpoints;
  readonly namespaces: NamespaceRegistry | undefined;
  /** The agents service's scope authorizer, applied to every MCP call. */
  readonly authorizeScope: ScopeAuthorizer | undefined;
}): void {
  const { router, endpoints } = params;
  const deps = { endpoints };
  router
    .get("/api/mcp-endpoints", listMcpEndpoints(deps))
    .post("/api/mcp-endpoints", createMcpEndpoint(deps))
    // `_sources` before `:id` (ids starting with `_` are rejected).
    .get("/api/mcp-endpoints/_sources", listMcpEndpointSources(deps))
    .get("/api/mcp-endpoints/_catalog", getMcpEndpointCatalog(deps))
    .get("/api/mcp-endpoints/:id", getMcpEndpoint(deps))
    .patch("/api/mcp-endpoints/:id", updateMcpEndpoint(deps))
    .delete("/api/mcp-endpoints/:id", deleteMcpEndpoint(deps))
    .get("/api/mcp-endpoints/:id/versions", listMcpEndpointVersions(deps))
    .post("/api/mcp-endpoints/:id/promote", promoteMcpEndpoint(deps))
    .get("/api/mcp-endpoints/:id/tools", listMcpEndpointTools(deps));

  const serveDeps = {
    endpoints,
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
    ...(params.authorizeScope !== undefined && { authorizeScope: params.authorizeScope }),
  };
  const tenant = serveMcpEndpoint({ ...serveDeps, global: false });
  const global = serveMcpEndpoint({ ...serveDeps, global: true });
  for (const method of ["POST", "GET", "DELETE"]) {
    router.add(method, "/ns/:namespace/mcp/:id", tenant);
    router.add(method, "/mcp/:id", global);
  }
}
