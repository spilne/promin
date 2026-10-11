// ---------------------------------------------------------------------------
// Registry CRUD APIs: skills, fragments, secrets, DAGs and knowledge bases.
// Each is mounted only when its service is configured.
// ---------------------------------------------------------------------------

import type { SecretsStorage } from "@promin/agent";
import type { NamespaceRegistry } from "@promin/workflow";
import type { Router } from "../router.ts";
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
  runDag,
  type DagGatewayDeps,
} from "../routes/dags.ts";
import {
  createFragment,
  deleteFragment,
  getFragment,
  listFragmentSources,
  listFragments,
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
  updateSkill,
} from "../routes/skills.ts";

export function mountSkills(params: {
  readonly router: Router;
  readonly skills: ZoryaSkills;
  readonly agents: ZoryaAgents | undefined;
}): void {
  const { router, skills, agents } = params;
  const skillDeps = {
    registry: skills.registry,
    fileManagedIds: () => skills.fileManagedIds(),
    ...(agents !== undefined && { agents: agents.registry }),
    ...(agents?.roles !== undefined && { roles: agents.roles }),
  };
  router
    .get("/api/skills", listSkills(skillDeps))
    .post("/api/skills", createSkill(skillDeps))
    // `_sources` before `:id` so the literal segment isn't shadowed (skill
    // ids starting with `_` are rejected at create time).
    .get(
      "/api/skills/_sources",
      listSkillSources({ fileManagedIds: () => skills.fileManagedIds() }),
    )
    .get("/api/skills/:id", getSkill(skillDeps))
    .patch("/api/skills/:id", updateSkill(skillDeps))
    .delete("/api/skills/:id", deleteSkill(skillDeps))
    .get("/api/skills/:id/versions", listSkillVersions(skillDeps));
}

export function mountFragments(params: {
  readonly router: Router;
  readonly fragments: ZoryaFragments;
  readonly agents: ZoryaAgents | undefined;
}): void {
  const { router, fragments, agents } = params;
  const fragmentDeps = {
    registry: fragments.registry,
    setFragment: (key: string, content: string) => fragments.setFragment(key, content),
    deleteFragment: (key: string) => fragments.deleteFragment(key),
    ...(agents !== undefined && { agents: agents.registry }),
    ...(agents?.roles !== undefined && { roles: agents.roles }),
  };
  router
    .get("/api/fragments", listFragments(fragmentDeps))
    .post("/api/fragments", createFragment(fragmentDeps))
    // `_sources` before `:key` so the literal segment isn't shadowed
    // (operator-supplied fragment keys starting with `_` are rejected).
    .get(
      "/api/fragments/_sources",
      listFragmentSources({ fileManagedIds: () => fragments.fileManagedIds() }),
    )
    .get("/api/fragments/:key", getFragment(fragmentDeps))
    .patch("/api/fragments/:key", updateFragment(fragmentDeps))
    .delete("/api/fragments/:key", deleteFragment(fragmentDeps));
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
  params.router
    .post("/api/secrets", createSecret(sec))
    .get("/api/secrets", listSecrets(sec))
    .delete("/api/secrets/:key", deleteSecret(sec));
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
    ...(params.namespaces !== undefined && { namespaces: params.namespaces }),
  };
  params.router
    .get("/api/dags", listDags(dagDeps))
    .post("/api/dags", createDag(dagDeps))
    .get("/api/dags/:id", getDag(dagDeps))
    .delete("/api/dags/:id", deleteDag(dagDeps))
    .get("/api/dags/:id/versions", listDagVersions(dagDeps))
    .post("/api/dags/:id/run", runDag(dagDeps));
}

export function mountKnowledgeBases(params: {
  readonly router: Router;
  readonly knowledgeBases: ZoryaKnowledgeBases;
  readonly agents: ZoryaAgents | undefined;
}): void {
  const deps: KnowledgeBaseGatewayDeps = {
    knowledgeBases: params.knowledgeBases,
    ...(params.agents !== undefined && { agents: params.agents.registry }),
  };
  params.router
    .get("/api/knowledge-bases", listKnowledgeBases(deps))
    .post("/api/knowledge-bases", createKnowledgeBase(deps))
    // `_adapters` before `:id` so the literal segment isn't shadowed
    // (knowledge base ids cannot start with `_`).
    .get("/api/knowledge-bases/_adapters", listKnowledgeSourceAdapters(deps))
    .get("/api/knowledge-bases/:id", getKnowledgeBase(deps))
    .patch("/api/knowledge-bases/:id", updateKnowledgeBase(deps))
    .delete("/api/knowledge-bases/:id", deleteKnowledgeBase(deps))
    .get("/api/knowledge-bases/:id/sources", listKnowledgeSources(deps))
    .post("/api/knowledge-bases/:id/sources", ingestKnowledgeSource(deps))
    .post("/api/knowledge-bases/:id/sources/import", importKnowledgeSources(deps))
    .post("/api/knowledge-bases/:id/sources/:sourceId/reindex", reindexKnowledgeSource(deps))
    .delete("/api/knowledge-bases/:id/sources/:sourceId", deleteKnowledgeSource(deps))
    .get("/api/knowledge-bases/:id/chunks", listKnowledgeChunks(deps))
    .post("/api/knowledge-bases/:id/search", searchKnowledgeBase(deps));
}
