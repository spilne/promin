// ---------------------------------------------------------------------------
// Registry CRUD APIs: skills, fragments, secrets and DAGs. Each is mounted
// only when its service is configured.
// ---------------------------------------------------------------------------

import type { SecretsStorage } from "@promin/agent";
import type { Router } from "../router.ts";
import type { ZoryaAgents } from "../services/agents/index.ts";
import type { ZoryaDags } from "../services/dags/index.ts";
import type { ZoryaFragments } from "../services/fragments/index.ts";
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
}): void {
  const sec = { secrets: params.secrets };
  params.router
    .post("/api/secrets", createSecret(sec))
    .get("/api/secrets", listSecrets(sec))
    .delete("/api/secrets/:key", deleteSecret(sec));
}

export function mountDags(params: { readonly router: Router; readonly dags: ZoryaDags }): void {
  const { dags } = params;
  const dagDeps: DagGatewayDeps = {
    registry: dags.registry,
    resolver: dags.resolver,
    runner: dags.runner,
  };
  params.router
    .get("/api/dags", listDags(dagDeps))
    .post("/api/dags", createDag(dagDeps))
    .get("/api/dags/:id", getDag(dagDeps))
    .delete("/api/dags/:id", deleteDag(dagDeps))
    .get("/api/dags/:id/versions", listDagVersions(dagDeps))
    .post("/api/dags/:id/run", runDag(dagDeps));
}
