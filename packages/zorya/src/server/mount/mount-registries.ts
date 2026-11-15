// ---------------------------------------------------------------------------
// Registry CRUD APIs: skills, fragments, secrets, DAGs and knowledge bases.
// Each is mounted only when its service is configured.
// ---------------------------------------------------------------------------

import type { SecretsStorage } from "@promin/agent";
import type { NamespaceRegistry } from "@promin/workflow";
import type { RouteTargetRequest, Router } from "../router.ts";
import { assertUniqueQueryParameters } from "../request-validation.ts";
import type { ScopeAuthorizer } from "../routes/agent-gateway-kit.ts";
import type { ZoryaAgents } from "../services/agents/index.ts";
import type { ZoryaDags } from "../services/dags/index.ts";
import type { ZoryaFragments } from "../services/fragments/index.ts";
import type { ZoryaKnowledgeBases } from "../services/knowledge/index.ts";
import type { ZoryaSkills } from "../services/skills/index.ts";
import {
  createDag,
  deleteDag,
  getDag,
  listDagVersions,
  listDags,
  promoteDag,
  runDag,
  updateDag,
  type DagGatewayDeps,
} from "../routes/dags.ts";
import {
  createFragment,
  deleteFragment,
  getFragment,
  listFragmentSources,
  listFragmentVersions,
  listFragments,
  promoteFragment,
  updateFragment,
} from "../routes/fragments.ts";
import {
  createKnowledgeBase,
  deleteKnowledgeBase,
  deleteKnowledgeSource,
  getKnowledgeBase,
  importKnowledgeSources,
  ingestKnowledgeSource,
  listKnowledgeBases,
  listKnowledgeChunks,
  listKnowledgeSourceAdapters,
  listKnowledgeSources,
  reindexKnowledgeSource,
  searchKnowledgeBase,
  updateKnowledgeBase,
  type KnowledgeBaseGatewayDeps,
} from "../routes/knowledge-bases.ts";
import { createSecret, deleteSecret, listSecrets } from "../routes/secrets.ts";
import {
  createSkill,
  deleteSkill,
  getSkill,
  listSkillSources,
  listSkillVersions,
  listSkills,
  promoteSkill,
  updateSkill,
} from "../routes/skills.ts";
import {
  bodyNamespaceTarget,
  orgTarget,
  protectedAccess,
  queryNamespaceTarget,
} from "./route-security.ts";

export function mountSkills(params: {
  readonly router: Router;
  readonly skills: ZoryaSkills;
  readonly agents: ZoryaAgents | undefined;
}): void {
  const { router, skills, agents } = params;
  const skillDeps = {
    registry: skills.registry,
    ...(agents !== undefined && { agents: agents.registry }),
    ...(agents?.roles !== undefined && { roles: agents.roles }),
  };
  const route = (
    method: string,
    path: string,
    access: Parameters<Router["route"]>[0]["access"],
    handler: Parameters<Router["route"]>[0]["handler"],
  ) => router.route({ method, path, access, handler });
  route(
    "GET",
    "/api/skills",
    protectedAccess({ resource: "skill", action: "read" }),
    listSkills(skillDeps),
  );
  route(
    "POST",
    "/api/skills",
    protectedAccess({ resource: "skill", action: "create" }),
    createSkill(skillDeps),
  );
  // `_sources` before `:id` so the literal segment isn't shadowed (skill
  // ids starting with `_` are rejected at create time).
  route(
    "GET",
    "/api/skills/_sources",
    protectedAccess({ resource: "skill", action: "read" }),
    listSkillSources({ registry: skills.registry }),
  );
  const skill = orgTarget({ kind: "skill", idParam: "id" });
  route(
    "GET",
    "/api/skills/:id",
    protectedAccess({ resource: "skill", action: "read", target: skill }),
    getSkill(skillDeps),
  );
  route(
    "PATCH",
    "/api/skills/:id",
    protectedAccess({ resource: "skill", action: "update", target: skill }),
    updateSkill(skillDeps),
  );
  // The vocabulary models removal through the kill-switch permission.
  route(
    "DELETE",
    "/api/skills/:id",
    protectedAccess({ resource: "skill", action: "disable", target: skill }),
    deleteSkill(skillDeps),
  );
  route(
    "GET",
    "/api/skills/:id/versions",
    protectedAccess({ resource: "skill", action: "read", target: skill }),
    listSkillVersions(skillDeps),
  );
  route(
    "POST",
    "/api/skills/:id/promote",
    protectedAccess({ resource: "skill", action: "update", target: skill }),
    promoteSkill(skillDeps),
  );
}

export function mountFragments(params: {
  readonly router: Router;
  readonly fragments: ZoryaFragments;
  readonly agents: ZoryaAgents | undefined;
}): void {
  const { router, fragments, agents } = params;
  const fragmentDeps = {
    registry: fragments.registry,
    ...(agents !== undefined && { agents: agents.registry }),
    ...(agents?.roles !== undefined && { roles: agents.roles }),
  };
  const route = (
    method: string,
    path: string,
    access: Parameters<Router["route"]>[0]["access"],
    handler: Parameters<Router["route"]>[0]["handler"],
  ) => router.route({ method, path, access, handler });
  route(
    "GET",
    "/api/fragments",
    protectedAccess({ resource: "fragment", action: "read" }),
    listFragments(fragmentDeps),
  );
  route(
    "POST",
    "/api/fragments",
    protectedAccess({ resource: "fragment", action: "create" }),
    createFragment(fragmentDeps),
  );
  // `_sources` before `:id` so the literal segment isn't shadowed
  // (operator-supplied fragment ids starting with `_` are rejected).
  route(
    "GET",
    "/api/fragments/_sources",
    protectedAccess({ resource: "fragment", action: "read" }),
    listFragmentSources({ registry: fragments.registry }),
  );
  const fragment = orgTarget({ kind: "fragment", idParam: "id" });
  route(
    "GET",
    "/api/fragments/:id",
    protectedAccess({ resource: "fragment", action: "read", target: fragment }),
    getFragment(fragmentDeps),
  );
  route(
    "PATCH",
    "/api/fragments/:id",
    protectedAccess({ resource: "fragment", action: "update", target: fragment }),
    updateFragment(fragmentDeps),
  );
  route(
    "DELETE",
    "/api/fragments/:id",
    // Fragment removal is governed by the vocabulary's lifecycle/disable permission.
    protectedAccess({ resource: "fragment", action: "disable", target: fragment }),
    deleteFragment(fragmentDeps),
  );
  route(
    "GET",
    "/api/fragments/:id/versions",
    protectedAccess({ resource: "fragment", action: "read", target: fragment }),
    listFragmentVersions(fragmentDeps),
  );
  route(
    "POST",
    "/api/fragments/:id/promote",
    protectedAccess({ resource: "fragment", action: "update", target: fragment }),
    promoteFragment(fragmentDeps),
  );
}

export function mountSecrets(params: {
  readonly router: Router;
  readonly secrets: SecretsStorage;
  /** Registry a stored secret's namespace is checked against. */
  readonly namespaces: NamespaceRegistry | undefined;
  /** The agents service's scope authorizer, applied to tenant-scoped secrets. */
  readonly authorizeScope: ScopeAuthorizer | undefined;
}): void {
  const sec = {
    secrets: params.secrets,
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
    ...(params.authorizeScope !== undefined && { authorizeScope: params.authorizeScope }),
  };
  const createTarget = async ({ request }: RouteTargetRequest) => {
    let namespaceId = "org";
    try {
      const body = (await request.clone().json()) as {
        scope?: { kind?: unknown; namespaceId?: unknown };
      };
      if (
        (body.scope?.kind === "namespace" || body.scope?.kind === "resource") &&
        typeof body.scope.namespaceId === "string" &&
        body.scope.namespaceId.length > 0
      )
        namespaceId = body.scope.namespaceId;
    } catch {}
    return { kind: "secret" as const, namespaceId };
  };
  const queryTarget = ({ request, params: route }: RouteTargetRequest) => {
    const url = new URL(request.url);
    assertUniqueQueryParameters({
      url,
      names: new Set(["scope", "namespaceId", "resourceId"]),
    });
    const query = url.searchParams;
    const scope = query.get("scope") ?? "global";
    const namedNamespace = query.get("namespaceId");
    const namespaceId =
      (scope === "namespace" || scope === "resource") && namedNamespace ? namedNamespace : "org";
    return {
      kind: "secret" as const,
      namespaceId,
      ...(route.key !== undefined && { id: route.key }),
    };
  };
  params.router.route({
    method: "POST",
    path: "/api/secrets",
    access: protectedAccess({ resource: "secret", action: "write", target: createTarget }),
    handler: createSecret(sec),
  });
  params.router.route({
    method: "GET",
    path: "/api/secrets",
    access: protectedAccess({ resource: "secret", action: "list", target: queryTarget }),
    handler: listSecrets(sec),
  });
  params.router.route({
    method: "DELETE",
    path: "/api/secrets/:key",
    access: protectedAccess({ resource: "secret", action: "delete", target: queryTarget }),
    handler: deleteSecret(sec),
  });
}

export function mountDags(params: {
  readonly router: Router;
  readonly dags: ZoryaDags;
  /** Registry a run's namespace is checked against. */
  readonly namespaces: NamespaceRegistry | undefined;
}): void {
  const { dags } = params;
  const { agents } = dags;
  const dagDeps: DagGatewayDeps = {
    registry: dags.registry,
    workflows: dags.workflows,
    workflowName: dags.workflow.name,
    agents: agents.registry,
    ...(agents.authorizeScope !== undefined && { authorizeScope: agents.authorizeScope }),
    ...(agents.budgets !== undefined && { budgets: agents.budgets }),
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
  };
  const dag = orgTarget({ kind: "dag", idParam: "id" });
  params.router.route({
    method: "GET",
    path: "/api/dags",
    access: protectedAccess({ resource: "dag", action: "read" }),
    handler: listDags(dagDeps),
  });
  params.router.route({
    method: "POST",
    path: "/api/dags",
    access: protectedAccess({ resource: "dag", action: "create" }),
    handler: createDag(dagDeps),
  });
  params.router.route({
    method: "GET",
    path: "/api/dags/:id",
    access: protectedAccess({ resource: "dag", action: "read", target: dag }),
    handler: getDag(dagDeps),
  });
  params.router.route({
    method: "PATCH",
    path: "/api/dags/:id",
    access: protectedAccess({ resource: "dag", action: "update", target: dag }),
    handler: updateDag(dagDeps),
  });
  params.router.route({
    method: "DELETE",
    path: "/api/dags/:id",
    // DAG removal changes the registered definition; the vocabulary has no separate delete verb.
    access: protectedAccess({ resource: "dag", action: "update", target: dag }),
    handler: deleteDag(dagDeps),
  });
  params.router.route({
    method: "GET",
    path: "/api/dags/:id/versions",
    access: protectedAccess({ resource: "dag", action: "read", target: dag }),
    handler: listDagVersions(dagDeps),
  });
  params.router.route({
    method: "POST",
    path: "/api/dags/:id/promote",
    access: protectedAccess({ resource: "dag", action: "update", target: dag }),
    handler: promoteDag(dagDeps),
  });
  params.router.route({
    method: "POST",
    path: "/api/dags/:id/run",
    access: protectedAccess({
      resource: "run",
      action: "trigger",
      target: bodyNamespaceTarget({ kind: "run", idParam: "id" }),
    }),
    handler: runDag(dagDeps),
  });
}

export function mountKnowledgeBases(params: {
  readonly router: Router;
  readonly knowledgeBases: ZoryaKnowledgeBases;
  readonly agents: ZoryaAgents | undefined;
  /** Registry a written base's namespace is checked against. */
  readonly namespaces: NamespaceRegistry | undefined;
}): void {
  const { agents } = params;
  const deps: KnowledgeBaseGatewayDeps = {
    knowledgeBases: params.knowledgeBases,
    ...(agents !== undefined && { agents: agents.registry }),
    ...(agents?.authorizeScope !== undefined && { authorizeScope: agents.authorizeScope }),
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
  };
  const knowledge = queryNamespaceTarget({ kind: "knowledge", idParam: "id" });
  params.router.route({
    method: "GET",
    path: "/api/knowledge-bases",
    access: protectedAccess({
      resource: "knowledge",
      action: "read",
      target: queryNamespaceTarget({ kind: "knowledge" }),
    }),
    handler: listKnowledgeBases(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/knowledge-bases",
    access: protectedAccess({
      resource: "knowledge",
      action: "create",
      target: queryNamespaceTarget({ kind: "knowledge" }),
    }),
    handler: createKnowledgeBase(deps),
  });
  // `_adapters` before `:id` so the literal segment isn't shadowed
  // (knowledge base ids cannot start with `_`).
  params.router.route({
    method: "GET",
    path: "/api/knowledge-bases/_adapters",
    access: protectedAccess({ resource: "knowledge", action: "read" }),
    handler: listKnowledgeSourceAdapters(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/knowledge-bases/:id",
    access: protectedAccess({ resource: "knowledge", action: "read", target: knowledge }),
    handler: getKnowledgeBase(deps),
  });
  params.router.route({
    method: "PATCH",
    path: "/api/knowledge-bases/:id",
    access: protectedAccess({ resource: "knowledge", action: "update", target: knowledge }),
    handler: updateKnowledgeBase(deps),
  });
  params.router.route({
    method: "DELETE",
    path: "/api/knowledge-bases/:id",
    access: protectedAccess({ resource: "knowledge", action: "delete", target: knowledge }),
    handler: deleteKnowledgeBase(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/knowledge-bases/:id/sources",
    access: protectedAccess({ resource: "knowledge", action: "read", target: knowledge }),
    handler: listKnowledgeSources(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/knowledge-bases/:id/sources",
    access: protectedAccess({ resource: "knowledge", action: "update", target: knowledge }),
    handler: ingestKnowledgeSource(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/knowledge-bases/:id/sources/import",
    access: protectedAccess({ resource: "knowledge", action: "update", target: knowledge }),
    handler: importKnowledgeSources(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/knowledge-bases/:id/sources/:sourceId/reindex",
    access: protectedAccess({ resource: "knowledge", action: "update", target: knowledge }),
    handler: reindexKnowledgeSource(deps),
  });
  params.router.route({
    method: "DELETE",
    path: "/api/knowledge-bases/:id/sources/:sourceId",
    access: protectedAccess({ resource: "knowledge", action: "delete", target: knowledge }),
    handler: deleteKnowledgeSource(deps),
  });
  params.router.route({
    method: "GET",
    path: "/api/knowledge-bases/:id/chunks",
    access: protectedAccess({ resource: "knowledge", action: "inspect-text", target: knowledge }),
    handler: listKnowledgeChunks(deps),
  });
  params.router.route({
    method: "POST",
    path: "/api/knowledge-bases/:id/search",
    access: protectedAccess({ resource: "knowledge", action: "inspect-text", target: knowledge }),
    handler: searchKnowledgeBase(deps),
  });
}
