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
import { json, type Router } from "../router.ts";
import type { ZoryaAgents } from "../services/agents/index.ts";
import type { ZoryaFragments } from "../services/fragments/index.ts";
import type { ZoryaMcpServers } from "../services/mcp/index.ts";
import type { ZoryaSkills } from "../services/skills/index.ts";
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

export function mountAgents(params: {
  readonly router: Router;
  readonly agents: ZoryaAgents;
  readonly skills: ZoryaSkills | undefined;
  readonly fragments: ZoryaFragments | undefined;
  readonly secrets: SecretsStorage | undefined;
  /** MCP allowlist recipe writes are checked against; its tools join the catalog. */
  readonly mcp: ZoryaMcpServers | undefined;
  /** Registry the namespace of agent writes is checked against. */
  readonly namespaces: NamespaceRegistry | undefined;
  readonly clock: WallClock;
}): void {
  const { router, agents, skills, fragments, clock } = params;
  const namespaced = params.namespaces !== undefined ? { namespaces: params.namespaces } : {};
  const agentDeps: AgentGatewayDeps = {
    registry: agents.registry,
    resolve: agents.resolve,
    ...(agents.instances !== undefined && { instanceRegistry: agents.instances }),
    ...(agents.roles !== undefined && { roles: agents.roles }),
    ...(agents.turnGate !== undefined && { turnGate: agents.turnGate }),
    ...(agents.workerId !== undefined && { workerId: agents.workerId }),
    ...(params.secrets !== undefined && { secrets: params.secrets }),
    ...(params.mcp !== undefined && { mcp: params.mcp }),
    ...(agents.authorizeScope !== undefined && { authorizeScope: agents.authorizeScope }),
    ...(agents.budgets !== undefined && { budgets: agents.budgets }),
    ...namespaced,
    clock,
    turnStreams: new TurnStreamHub({
      clock,
      ...(agents.streaming !== undefined && { limits: agents.streaming }),
    }),
  };
  // The standalone fragments service wins over the registry passed on
  // ZoryaAgents, for the catalog and the memory inspector alike.
  const fragmentRegistry = fragments?.registry ?? agents.fragments;

  router
    .get("/api/agents", listAgents(agentDeps))
    // A namespace's spend against its budget (before `/api/agents/:id`).
    .get("/api/agents/_usage", getNamespaceUsage(agentDeps))
    // Read-only metadata — the designer UI's model dropdown source.
    .get(
      "/api/agents/_catalog/models",
      agents.models
        ? listCatalogModels({ models: agents.models })
        : async () => json(200, { models: [] }),
    )
    .get(
      "/api/agents/_catalog/tools",
      listCatalogTools({
        ...(agents.toolCatalog !== undefined && { tools: agents.toolCatalog }),
        ...(params.mcp !== undefined && { mcp: params.mcp }),
      }),
    )
    // The agent editor's MCP picker: the operator's allowlisted servers.
    .get("/api/agents/_catalog/mcp-servers", listCatalogMcpServers({ mcp: params.mcp }))
    // Reconciliation health: which recipe tool refs don't resolve? A pure
    // query at view time — no persistence, no audit trail.
    .get(
      "/api/agents/_catalog/tools/health",
      agents.toolCatalog
        ? getToolCatalogHealth({ tools: agents.toolCatalog, agents: agents.registry })
        : async () => json(200, { recipes: [], orphans: [] }),
    )
    // Durable audit trail of exposed tools over time, when a tool-history
    // store is wired.
    .get(
      "/api/agents/_catalog/tools/history",
      agents.toolHistory
        ? listToolHistory({ history: agents.toolHistory })
        : async () => json(200, { history: [] }),
    )
    // The agent editor's skill picker, backed by the skills service.
    .get(
      "/api/agents/_catalog/skills",
      skills
        ? listCatalogSkills({ skills: skills.registry })
        : async () => json(200, { skills: [] }),
    )
    // The agent editor's layered-prompt editor source.
    .get(
      "/api/agents/_catalog/fragments",
      fragmentRegistry
        ? listCatalogFragments({ fragments: fragmentRegistry })
        : async () => json(200, { fragments: [] }),
    )
    // The agent editor's knowledge picker: host-wired retrievers.
    .get(
      "/api/agents/_catalog/retrievers",
      agents.retrievers
        ? listCatalogRetrievers({
            retrievers: agents.retrievers,
            ...(agents.authorizeScope !== undefined && { authorizeScope: agents.authorizeScope }),
          })
        : async () => json(200, { retrievers: [] }),
    )
    // Which recipes are backed by a file on disk (the editor disables
    // in-place Save for them and the list badges them).
    .get("/api/agents/_sources", listAgentSources({ registry: agents.registry }))
    // Draft recipes — test a recipe edit before committing it.
    .post(
      "/api/agents/_draft",
      createDraft({
        registry: agents.registry,
        clock,
        ...(params.mcp !== undefined && { mcp: params.mcp }),
      }),
    )
    .delete("/api/agents/_draft/:id", deleteDraft({ registry: agents.registry }))
    // Recipe CRUD — author, edit and clone agents over HTTP.
    .post("/api/agents", createAgent(agentDeps))
    .get("/api/agents/:id", getAgent(agentDeps))
    .patch("/api/agents/:id", updateAgent(agentDeps))
    .delete("/api/agents/:id", deleteAgent(agentDeps))
    .get("/api/agents/:id/versions", listAgentVersions(agentDeps))
    .post("/api/agents/:id/promote", promoteAgent(agentDeps))
    .post("/api/agents/:id/clone", cloneAgent(agentDeps))
    .post("/api/agents/:id/extract-role", extractRole(agentDeps))
    .post("/api/agents/:id/invoke", invokeAgent(agentDeps))
    .post("/api/agents/:id/stream", streamAgent(agentDeps))
    .get("/api/agents/:id/threads", listAgentThreads(agentDeps))
    .patch("/api/agents/:id/threads/:threadId", renameAgentThread(agentDeps))
    .post("/api/agents/:id/threads/:threadId", sendThreadMessage(agentDeps))
    .post("/api/agents/:id/threads/:threadId/stream", streamThreadMessage(agentDeps))
    .get("/api/agents/:id/threads/:threadId/stream", reattachThreadStream(agentDeps))
    .post("/api/agents/:id/threads/:threadId/cancel", cancelThreadTurn(agentDeps))
    .post("/api/agents/:id/threads/:threadId/approve", streamThreadApproval(agentDeps))
    .get("/api/agents/:id/threads/:threadId/approvals", listThreadApprovals(agentDeps))
    .get("/api/agents/:id/threads/:threadId/messages", listThreadMessages(agentDeps))
    .get("/api/agents/:id/threads/:threadId/trace", getThreadTrace(agentDeps))
    .get("/api/agents/:id/threads/:threadId/usage", getThreadUsage(agentDeps))
    .post("/api/agents/:id/threads/:threadId/distill", distillThread(agentDeps))
    .post("/api/agents/:id/threads/:threadId/compact", compactThread(agentDeps))
    .post("/api/agents/:id/threads/:threadId/archive", archiveAgentThread(agentDeps));

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
    router
      .get("/api/memory/inspect", inspectMemory(mem))
      .patch("/api/memory/namespace/:namespaceId", patchNamespace(mem))
      .post("/api/memory/namespace/:namespaceId/facts", addNamespaceFact(mem))
      .delete("/api/memory/namespace/:namespaceId/facts/:factId", deleteNamespaceFact(mem));
  }

  if (agents.instances && agents.memory) {
    const inDeps = {
      registry: agents.instances,
      memory: agents.memory,
      agents: agents.registry,
      ...(agents.authorizeScope !== undefined && { authorizeScope: agents.authorizeScope }),
      ...namespaced,
    };
    router
      .get("/api/agents/:id/instances", listAgentInstances(inDeps))
      .post("/api/agents/:id/instances", resolveAgentInstance(inDeps))
      .get("/api/agents/:id/instances/:instanceId", getAgentInstance(inDeps))
      .patch("/api/agents/:id/instances/:instanceId", updateAgentInstance(inDeps))
      .delete("/api/agents/:id/instances/:instanceId", deleteAgentInstance(inDeps))
      .get("/api/instances", listInstancesAcrossAgents(inDeps));
  }

  if (agents.roles) {
    const roleDeps = { registry: agents.roles, agents: agents.registry };
    router
      .get("/api/roles", listRoles(roleDeps))
      .post("/api/roles", createRole(roleDeps))
      .get("/api/roles/:id", getRole(roleDeps))
      .patch("/api/roles/:id", updateRole(roleDeps))
      .delete("/api/roles/:id", deleteRole(roleDeps))
      .get("/api/roles/:id/versions", listRoleVersions(roleDeps))
      .post("/api/roles/:id/promote", promoteRole(roleDeps));
  }
}
