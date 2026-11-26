// ---------------------------------------------------------------------------
// Agent gateway API: recipes and their catalogs, drafts, invoke / stream,
// threads, plus the memory, instance and role surfaces that hang off the
// agents service.
//
// Literal `_`-prefixed segments (`_catalog`, `_sources`, `_draft`) are
// mounted before `/api/agents/:id` so an agent id can never shadow them
// (`_`-prefixed agent ids are reserved).
// ---------------------------------------------------------------------------

import type { SecretsStorage } from "@promin/agent";
import type { NamespaceRegistry, WallClock } from "@promin/workflow";
import { json, type Handler, type ProtectedRouteAccess, type Router } from "../router.ts";
import type { RetainedSubagents, ZoryaAgents } from "../services/agents/index.ts";
import type { ZoryaEvals } from "../services/evals/index.ts";
import { evalPromotionGate } from "../routes/evals.ts";
import { mountRetainedAgentRoutes } from "../routes/agents/agent-retained.ts";
import { forkAgentThread, getAgentThreadLineage } from "../routes/agents/agent-forks.ts";
import {
  deleteAgentFeedback,
  getAgentFeedback,
  getAgentFeedbackTrend,
  postAgentFeedback,
} from "../routes/agents/agent-feedback.ts";
import type { ZoryaFragments } from "../services/fragments/index.ts";
import type { ZoryaMcpServers } from "../services/mcp/index.ts";
import type { ZoryaSkills } from "../services/skills/index.ts";
import type { ZoryaSecurity } from "../security/index.ts";
import {
  getToolCatalogHealth,
  listCatalogFragments,
  listCatalogMcpServers,
  listCatalogModels,
  listCatalogRetrievers,
  listCatalogSkills,
  listCatalogTools,
  listToolHistory,
} from "../routes/agent-catalog.ts";
import { createDraft, deleteDraft } from "../routes/agent-drafts.ts";
import {
  archiveAgentThread,
  cancelThreadTurn,
  cloneAgent,
  compactThread,
  createAgent,
  deleteAgent,
  distillThread,
  extractRole,
  getAgent,
  getThreadTrace,
  getNamespaceUsage,
  getThreadUsage,
  invokeAgent,
  listAgentSources,
  listAgentThreads,
  listAgentVersions,
  promoteAgent,
  listAgents,
  listThreadApprovals,
  listThreadMessages,
  reattachThreadStream,
  renameAgentThread,
  sendThreadMessage,
  streamAgent,
  streamThreadApproval,
  streamThreadMessage,
  TurnStreamHub,
  updateAgent,
  type AgentGatewayDeps,
} from "../routes/agents/index.ts";
import {
  deleteAgentInstance,
  getAgentInstance,
  listAgentInstances,
  listInstancesAcrossAgents,
  resolveAgentInstance,
  updateAgentInstance,
} from "../routes/instances.ts";
import {
  addNamespaceFact,
  deleteNamespaceFact,
  inspectMemory,
  patchNamespace,
} from "../routes/memory.ts";
import {
  createRole,
  deleteRole,
  getRole,
  listRoleVersions,
  listRoles,
  promoteRole,
  updateRole,
} from "../routes/roles.ts";
import { protectedAccess, orgTarget } from "./route-security.ts";
import {
  agentBodyTarget,
  agentQueryTarget,
  approvalBodyTarget,
  namespaceQueryTarget,
} from "./agent-route-security.ts";
import type { SecurityAction, SecurityResourceKind } from "../security/index.ts";

function declareRoute(params: {
  readonly router: Router;
  readonly method: string;
  readonly path: string;
  readonly access: ProtectedRouteAccess;
  readonly handler: Handler;
}): void {
  params.router.route({
    method: params.method,
    path: params.path,
    access: params.access,
    handler: params.handler,
  });
}

function orgAccess<K extends SecurityResourceKind>(params: {
  readonly resource: K;
  readonly action: SecurityAction<K>;
  readonly idParam?: string;
}): ProtectedRouteAccess {
  return protectedAccess({
    resource: params.resource,
    action: params.action,
    target: orgTarget({
      kind: params.resource,
      ...(params.idParam !== undefined && { idParam: params.idParam }),
    }),
  });
}

export function mountAgents(params: {
  readonly router: Router;
  readonly agents: ZoryaAgents;
  readonly retainedSubagents: RetainedSubagents | undefined;
  readonly skills: ZoryaSkills | undefined;
  readonly fragments: ZoryaFragments | undefined;
  readonly secrets: SecretsStorage | undefined;
  /** MCP allowlist recipe writes are checked against; its tools join the catalog. */
  readonly mcp: ZoryaMcpServers | undefined;
  /** Registry the namespace of agent writes is checked against. */
  readonly namespaces: NamespaceRegistry | undefined;
  /** Evals service whose gates guard agent promotion. */
  readonly evals: ZoryaEvals | undefined;
  readonly security: ZoryaSecurity | undefined;
  readonly clock: WallClock;
}): void {
  const { router, agents, skills, fragments, clock } = params;
  const namespaced = params.namespaces !== undefined ? { namespaces: params.namespaces } : {};
  const agentDeps: AgentGatewayDeps = {
    registry: agents.registry,
    resolve: agents.resolve,
    ...(agents.approvals !== undefined && { approvals: agents.approvals }),
    ...(params.security !== undefined && { security: params.security }),
    ...(agents.instances !== undefined && { instanceRegistry: agents.instances }),
    ...(agents.roles !== undefined && { roles: agents.roles }),
    ...(agents.turnGate !== undefined && { turnGate: agents.turnGate }),
    ...(agents.workerId !== undefined && { workerId: agents.workerId }),
    ...(params.secrets !== undefined && { secrets: params.secrets }),
    ...(params.mcp !== undefined && { mcp: params.mcp }),
    ...(agents.authorizeScope !== undefined && { authorizeScope: agents.authorizeScope }),
    ...(agents.budgets !== undefined && { budgets: agents.budgets }),
    ...namespaced,
    ...(params.evals !== undefined && {
      promotionGate: evalPromotionGate({
        evals: params.evals,
        ...(agents.authorizeScope !== undefined && { authorizeScope: agents.authorizeScope }),
      }),
    }),
    clock,
    turnStreams: new TurnStreamHub({
      clock,
      ...(agents.streaming !== undefined && { limits: agents.streaming }),
    }),
  };
  if (params.retainedSubagents)
    mountRetainedAgentRoutes(agentDeps, params.retainedSubagents, router);
  // The standalone fragments service wins over the registry passed on
  // ZoryaAgents, for the catalog and the memory inspector alike.
  const fragmentRegistry = fragments?.registry ?? agents.fragments;

  const route = (method: string, path: string, access: ProtectedRouteAccess, handler: Handler) =>
    declareRoute({ router, method, path, access, handler });
  const org = <K extends SecurityResourceKind>(
    method: string,
    path: string,
    resource: K,
    action: SecurityAction<K>,
    handler: Handler,
    idParam?: string,
  ) => route(method, path, orgAccess({ resource, action, ...(idParam && { idParam }) }), handler);
  const body = <K extends SecurityResourceKind>(
    method: string,
    path: string,
    resource: K,
    action: SecurityAction<K>,
    handler: Handler,
    options: { idParam?: string; idField?: string; identityId?: boolean } = {},
  ) =>
    route(
      method,
      path,
      protectedAccess({
        resource,
        action,
        target: agentBodyTarget({ kind: resource, security: params.security, ...options }),
      }),
      handler,
    );
  const query = <K extends SecurityResourceKind>(
    method: string,
    path: string,
    resource: K,
    action: SecurityAction<K>,
    handler: Handler,
    options: { idParam?: string; identityId?: boolean } = {},
  ) =>
    route(
      method,
      path,
      protectedAccess({
        resource,
        action,
        target: agentQueryTarget({ kind: resource, security: params.security, ...options }),
      }),
      handler,
    );

  org("GET", "/api/agents", "agent", "read", listAgents(agentDeps));
  route(
    "GET",
    "/api/agents/_usage",
    protectedAccess({
      resource: "usage",
      action: "read",
      target: namespaceQueryTarget({ kind: "usage" }),
    }),
    getNamespaceUsage(agentDeps),
  );
  org(
    "GET",
    "/api/agents/_catalog/models",
    "toolset",
    "read",
    agents.models
      ? listCatalogModels({ models: agents.models })
      : async () => json(200, { models: [] }),
  );
  org(
    "GET",
    "/api/agents/_catalog/tools",
    "toolset",
    "read",
    listCatalogTools({
      ...(agents.toolCatalog !== undefined && { tools: agents.toolCatalog }),
      ...(params.mcp !== undefined && { mcp: params.mcp }),
    }),
  );
  org(
    "GET",
    "/api/agents/_catalog/mcp-servers",
    "connector",
    "read",
    listCatalogMcpServers({ mcp: params.mcp }),
  );
  org(
    "GET",
    "/api/agents/_catalog/tools/health",
    "toolset",
    "read",
    agents.toolCatalog
      ? getToolCatalogHealth({ tools: agents.toolCatalog, agents: agents.registry })
      : async () => json(200, { recipes: [], orphans: [] }),
  );
  org(
    "GET",
    "/api/agents/_catalog/tools/history",
    "audit",
    "search",
    agents.toolHistory
      ? listToolHistory({ history: agents.toolHistory })
      : async () => json(200, { history: [] }),
  );
  org(
    "GET",
    "/api/agents/_catalog/skills",
    "skill",
    "read",
    skills ? listCatalogSkills({ skills: skills.registry }) : async () => json(200, { skills: [] }),
  );
  org(
    "GET",
    "/api/agents/_catalog/fragments",
    "fragment",
    "read",
    fragmentRegistry
      ? listCatalogFragments({ fragments: fragmentRegistry })
      : async () => json(200, { fragments: [] }),
  );
  org(
    "GET",
    "/api/agents/_catalog/retrievers",
    "knowledge",
    "read",
    agents.retrievers
      ? listCatalogRetrievers({
          retrievers: agents.retrievers,
          ...(agents.authorizeScope !== undefined && { authorizeScope: agents.authorizeScope }),
        })
      : async () => json(200, { retrievers: [] }),
  );
  org(
    "GET",
    "/api/agents/_sources",
    "agent",
    "read",
    listAgentSources({ registry: agents.registry }),
  );
  org(
    "POST",
    "/api/agents/_draft",
    "agent",
    "create",
    createDraft({
      registry: agents.registry,
      clock,
      ...(params.mcp !== undefined && { mcp: params.mcp }),
    }),
  );
  org(
    "DELETE",
    "/api/agents/_draft/:id",
    "agent",
    "update",
    deleteDraft({ registry: agents.registry }),
    "id",
  );
  org("POST", "/api/agents", "agent", "create", createAgent(agentDeps));
  org("GET", "/api/agents/:id", "agent", "read", getAgent(agentDeps), "id");
  org("PATCH", "/api/agents/:id", "agent", "update", updateAgent(agentDeps), "id");
  org("DELETE", "/api/agents/:id", "agent", "disable", deleteAgent(agentDeps), "id");
  org("GET", "/api/agents/:id/versions", "agent", "read", listAgentVersions(agentDeps), "id");
  org("POST", "/api/agents/:id/promote", "agent", "promote", promoteAgent(agentDeps), "id");
  org("POST", "/api/agents/:id/clone", "agent", "create", cloneAgent(agentDeps), "id");
  org("POST", "/api/agents/:id/extract-role", "agent-role", "create", extractRole(agentDeps), "id");
  body("POST", "/api/agents/:id/invoke", "agent", "invoke", invokeAgent(agentDeps), {
    idParam: "id",
  });
  body("POST", "/api/agents/:id/stream", "agent", "invoke", streamAgent(agentDeps), {
    idParam: "id",
  });
  query("GET", "/api/agents/:id/threads", "thread", "read", listAgentThreads(agentDeps), {
    identityId: true,
  });
  body(
    "PATCH",
    "/api/agents/:id/threads/:threadId",
    "thread",
    "rename",
    renameAgentThread(agentDeps),
    { idParam: "threadId" },
  );
  body(
    "POST",
    "/api/agents/:id/threads/:threadId",
    "thread",
    "send",
    sendThreadMessage(agentDeps),
    { idParam: "threadId" },
  );
  body(
    "POST",
    "/api/agents/:id/threads/:threadId/stream",
    "thread",
    "send",
    streamThreadMessage(agentDeps),
    { idParam: "threadId" },
  );
  query(
    "GET",
    "/api/agents/:id/threads/:threadId/stream",
    "thread",
    "read",
    reattachThreadStream(agentDeps),
    { idParam: "threadId" },
  );
  body(
    "POST",
    "/api/agents/:id/threads/:threadId/cancel",
    "thread",
    "send",
    cancelThreadTurn(agentDeps),
    { idParam: "threadId" },
  );
  route(
    "POST",
    "/api/agents/:id/threads/:threadId/approve",
    {
      kind: "protected",
      permissions: [
        { resource: "approval", action: "decide" },
        { resource: "approval", action: "decide-elevated" },
      ],
      resolveTarget: approvalBodyTarget({ approvals: agents.approvals }),
    },
    streamThreadApproval(agentDeps),
  );
  query(
    "GET",
    "/api/agents/:id/threads/:threadId/approvals",
    "thread",
    "read",
    listThreadApprovals(agentDeps),
    { idParam: "threadId" },
  );
  query(
    "GET",
    "/api/agents/:id/threads/:threadId/messages",
    "thread",
    "read",
    listThreadMessages(agentDeps),
    { idParam: "threadId" },
  );
  query(
    "GET",
    "/api/agents/:id/threads/:threadId/trace",
    "thread",
    "view-trace",
    getThreadTrace(agentDeps),
    { idParam: "threadId" },
  );
  query(
    "GET",
    "/api/agents/:id/threads/:threadId/usage",
    "usage",
    "read",
    getThreadUsage(agentDeps),
    { idParam: "threadId" },
  );
  body(
    "POST",
    "/api/agents/:id/threads/:threadId/distill",
    "memory",
    "write",
    distillThread(agentDeps),
    { identityId: true },
  );
  body(
    "POST",
    "/api/agents/:id/threads/:threadId/compact",
    "memory",
    "write",
    compactThread(agentDeps),
    { identityId: true },
  );
  body(
    "POST",
    "/api/agents/:id/threads/:threadId/archive",
    "thread",
    "archive",
    archiveAgentThread(agentDeps),
    { idParam: "threadId" },
  );

  if (params.evals?.datasets) {
    org(
      "GET",
      "/api/agents/:id/feedback",
      "agent",
      "read",
      getAgentFeedbackTrend(agentDeps, params.evals.datasets),
      "id",
    );
    query(
      "GET",
      "/api/agents/:id/threads/:threadId/feedback",
      "thread",
      "read",
      getAgentFeedback(agentDeps, params.evals.datasets),
      { idParam: "threadId" },
    );
    body(
      "POST",
      "/api/agents/:id/threads/:threadId/feedback",
      "thread",
      "send",
      postAgentFeedback(agentDeps, params.evals.datasets),
      { idParam: "threadId" },
    );
    query(
      "DELETE",
      "/api/agents/:id/threads/:threadId/feedback",
      "thread",
      "send",
      deleteAgentFeedback(agentDeps, params.evals.datasets),
      { idParam: "threadId" },
    );
  }

  if (agents.memory) {
    body(
      "POST",
      "/api/agents/:id/threads/:threadId/fork",
      "thread",
      "send",
      forkAgentThread(agentDeps),
      { idParam: "threadId" },
    );
    query(
      "GET",
      "/api/agents/:id/threads/:threadId/lineage",
      "thread",
      "read",
      getAgentThreadLineage(agentDeps, agents.memory),
      { idParam: "threadId" },
    );
  }

  if (agents.memory) {
    // Persona resolution piggybacks on the recipe store and fragment
    // registry, so the Prompt tab can show the role prompt alongside the
    // memory cascade.
    const mem = {
      memory: agents.memory,
      registry: agents.registry,
      ...(fragmentRegistry && { fragments: fragmentRegistry }),
      ...(agents.roles !== undefined && { roles: agents.roles }),
      ...(agents.authorizeScope !== undefined && { authorizeScope: agents.authorizeScope }),
      ...namespaced,
    };
    route(
      "GET",
      "/api/memory/inspect",
      protectedAccess({
        resource: "memory",
        action: "read",
        target: namespaceQueryTarget({ kind: "memory", identityQuery: "resourceId" }),
      }),
      inspectMemory(mem),
    );
    route(
      "PATCH",
      "/api/memory/namespace/:namespaceId",
      protectedAccess({
        resource: "memory",
        action: "write",
        target: ({ params: route }) => ({
          kind: "memory",
          namespaceId: route.namespaceId!,
        }),
      }),
      patchNamespace(mem),
    );
    route(
      "POST",
      "/api/memory/namespace/:namespaceId/facts",
      protectedAccess({
        resource: "memory",
        action: "write",
        target: ({ params: route }) => ({
          kind: "memory",
          namespaceId: route.namespaceId!,
        }),
      }),
      addNamespaceFact(mem),
    );
    route(
      "DELETE",
      "/api/memory/namespace/:namespaceId/facts/:factId",
      protectedAccess({
        resource: "memory",
        action: "erase",
        target: ({ params: route }) => ({
          kind: "memory",
          namespaceId: route.namespaceId!,
          id: route.factId,
        }),
      }),
      deleteNamespaceFact(mem),
    );
  }

  if (agents.instances && agents.memory) {
    const inDeps = {
      registry: agents.instances,
      memory: agents.memory,
      agents: agents.registry,
      ...(agents.authorizeScope !== undefined && { authorizeScope: agents.authorizeScope }),
      ...namespaced,
    };
    const instancePointTarget = async ({
      params: routeParams,
    }: {
      params: Readonly<Record<string, string>>;
    }) => {
      const instanceId = routeParams.instanceId;
      const instance = instanceId ? await agents.instances!.get(instanceId) : undefined;
      return {
        kind: "memory" as const,
        namespaceId: instance?.namespaceId ?? "org",
        ...(instance?.ownerId !== undefined ? { id: instance.ownerId } : {}),
      };
    };
    route(
      "GET",
      "/api/agents/:id/instances",
      protectedAccess({
        resource: "memory",
        action: "read",
        target: namespaceQueryTarget({ kind: "memory", identityQuery: "ownerId" }),
      }),
      listAgentInstances(inDeps),
    );
    route(
      "POST",
      "/api/agents/:id/instances",
      protectedAccess({
        resource: "memory",
        action: "write",
        target: async ({ request }) => {
          const raw = (await request
            .clone()
            .json()
            .catch(() => null)) as {
            namespaceId?: unknown;
            ownerId?: unknown;
          } | null;
          return {
            kind: "memory",
            namespaceId:
              typeof raw?.namespaceId === "string" && raw.namespaceId.length > 0
                ? raw.namespaceId
                : "org",
            ...(typeof raw?.ownerId === "string" && raw.ownerId.length > 0
              ? { id: raw.ownerId }
              : {}),
          };
        },
      }),
      resolveAgentInstance(inDeps),
    );
    route(
      "GET",
      "/api/agents/:id/instances/:instanceId",
      protectedAccess({ resource: "memory", action: "read", target: instancePointTarget }),
      getAgentInstance(inDeps),
    );
    route(
      "PATCH",
      "/api/agents/:id/instances/:instanceId",
      protectedAccess({ resource: "memory", action: "write", target: instancePointTarget }),
      updateAgentInstance(inDeps),
    );
    route(
      "DELETE",
      "/api/agents/:id/instances/:instanceId",
      protectedAccess({ resource: "memory", action: "erase", target: instancePointTarget }),
      deleteAgentInstance(inDeps),
    );
    route(
      "GET",
      "/api/instances",
      protectedAccess({
        resource: "memory",
        action: "read",
        target: namespaceQueryTarget({ kind: "memory", identityQuery: "ownerId" }),
      }),
      listInstancesAcrossAgents(inDeps),
    );
  }

  if (agents.roles) {
    const roleDeps = { registry: agents.roles, agents: agents.registry };
    org("GET", "/api/roles", "agent-role", "read", listRoles(roleDeps));
    org("POST", "/api/roles", "agent-role", "create", createRole(roleDeps));
    org("GET", "/api/roles/:id", "agent-role", "read", getRole(roleDeps), "id");
    org("PATCH", "/api/roles/:id", "agent-role", "update", updateRole(roleDeps), "id");
    org("DELETE", "/api/roles/:id", "agent-role", "disable", deleteRole(roleDeps), "id");
    org("GET", "/api/roles/:id/versions", "agent-role", "read", listRoleVersions(roleDeps), "id");
    org("POST", "/api/roles/:id/promote", "agent-role", "update", promoteRole(roleDeps), "id");
  }
}
