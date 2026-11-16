import { describe, expect, it } from "bun:test";
import { mountAccessRoutes } from "../../../security-module/access-routes.ts";
import { Router } from "../../router.ts";
import { mountEvals } from "../mount-evals.ts";
import { mountNamespaces } from "../mount-namespaces.ts";
import {
  mountDags,
  mountFragments,
  mountKnowledgeBases,
  mountSecrets,
  mountSkills,
} from "../mount-registries.ts";
import { mountWorkflowBuilder } from "../mount-workflow-builder.ts";

const routes = [
  ["GET", "/api/skills"],
  ["POST", "/api/skills"],
  ["GET", "/api/skills/_sources"],
  ["GET", "/api/skills/one"],
  ["PATCH", "/api/skills/one"],
  ["DELETE", "/api/skills/one"],
  ["GET", "/api/skills/one/versions"],
  ["POST", "/api/skills/one/promote"],
  ["GET", "/api/fragments"],
  ["POST", "/api/fragments"],
  ["GET", "/api/fragments/_sources"],
  ["GET", "/api/fragments/one"],
  ["PATCH", "/api/fragments/one"],
  ["DELETE", "/api/fragments/one"],
  ["GET", "/api/fragments/one/versions"],
  ["POST", "/api/fragments/one/promote"],
  ["POST", "/api/secrets"],
  ["GET", "/api/secrets"],
  ["DELETE", "/api/secrets/key"],
  ["GET", "/api/dags"],
  ["POST", "/api/dags"],
  ["GET", "/api/dags/one"],
  ["PATCH", "/api/dags/one"],
  ["DELETE", "/api/dags/one"],
  ["GET", "/api/dags/one/versions"],
  ["POST", "/api/dags/one/promote"],
  ["POST", "/api/dags/one/run"],
  ["GET", "/api/knowledge-bases?namespaceId=team-a"],
  ["POST", "/api/knowledge-bases?namespaceId=team-a"],
  ["GET", "/api/knowledge-bases/_adapters"],
  ["GET", "/api/knowledge-bases/one?namespaceId=team-a"],
  ["PATCH", "/api/knowledge-bases/one?namespaceId=team-a"],
  ["DELETE", "/api/knowledge-bases/one?namespaceId=team-a"],
  ["GET", "/api/knowledge-bases/one/sources?namespaceId=team-a"],
  ["POST", "/api/knowledge-bases/one/sources?namespaceId=team-a"],
  ["POST", "/api/knowledge-bases/one/sources/import?namespaceId=team-a"],
  ["POST", "/api/knowledge-bases/one/sources/source/reindex?namespaceId=team-a"],
  ["DELETE", "/api/knowledge-bases/one/sources/source?namespaceId=team-a"],
  ["GET", "/api/knowledge-bases/one/chunks?namespaceId=team-a"],
  ["POST", "/api/knowledge-bases/one/search?namespaceId=team-a"],
  ["GET", "/api/evals/suites"],
  ["POST", "/api/evals/suites"],
  ["GET", "/api/evals/suites/one"],
  ["PATCH", "/api/evals/suites/one"],
  ["DELETE", "/api/evals/suites/one"],
  ["GET", "/api/evals/suites/one/versions"],
  ["POST", "/api/evals/suites/one/promote"],
  ["POST", "/api/evals/runs"],
  ["GET", "/api/evals/runs?namespaceId=team-a"],
  ["GET", "/api/evals/runs/run"],
  ["POST", "/api/evals/runs/run/cancel"],
  ["GET", "/api/evals/compare"],
  ["GET", "/api/evals/gates"],
  ["GET", "/api/evals/gates/agent"],
  ["PUT", "/api/evals/gates/agent"],
  ["DELETE", "/api/evals/gates/agent"],
  ["GET", "/api/namespaces"],
  ["POST", "/api/namespaces"],
  ["GET", "/api/namespaces/team-a"],
  ["PATCH", "/api/namespaces/team-a"],
  ["DELETE", "/api/namespaces/team-a"],
  ["GET", "/api/workflow-builder/activities"],
  ["POST", "/api/workflow-builder/validate"],
  ["GET", "/api/workflow-builder/workflows"],
  ["POST", "/api/workflow-builder/workflows"],
  ["GET", "/api/workflow-builder/workflows/one"],
  ["PUT", "/api/workflow-builder/workflows/one"],
  ["DELETE", "/api/workflow-builder/workflows/one"],
  ["POST", "/api/workflow-builder/workflows/one/publish"],
  ["GET", "/api/access/users"],
  ["PUT", "/api/access/users/user/bindings"],
  ["DELETE", "/api/access/users/user/bindings"],
  ["POST", "/api/access/users/user/disable"],
  ["PUT", "/api/access/knowledge-grants"],
  ["DELETE", "/api/access/knowledge-grants"],
] as const;

describe("registry and administration route security", () => {
  it("declares protected access for every migrated route", async () => {
    const seen: string[] = [];
    const router = new Router({
      accessGuard: ({ request, access }) => {
        if (access.kind === "protected") {
          seen.push(`${request.method} ${new URL(request.url).pathname}`);
        }
        return new Response(null, { status: 403 });
      },
    });
    const registry = {} as never;
    mountSkills({ router, skills: { registry } as never, agents: undefined });
    mountFragments({ router, fragments: { registry } as never, agents: undefined });
    mountSecrets({
      router,
      secrets: {} as never,
      namespaces: undefined,
      authorizeScope: undefined,
    });
    mountDags({
      router,
      dags: {
        registry,
        workflows: {},
        workflow: { name: "dag" },
        agents: { registry: {}, authorizeScope: undefined, budgets: undefined },
      } as never,
      namespaces: undefined,
    });
    mountKnowledgeBases({
      router,
      knowledgeBases: {} as never,
      agents: undefined,
      namespaces: undefined,
    });
    mountEvals({
      router,
      evals: { getRun: async () => undefined } as never,
      namespaces: undefined,
      authorizeScope: undefined,
    });
    mountNamespaces({ router, registry, storage: {} as never, schedulerStorage: undefined });
    mountWorkflowBuilder({ router, builder: {} as never });
    mountAccessRoutes({
      router,
      store: {} as never,
      credentialPepper: "test",
      clock: {} as never,
    });

    expect(router.uncoveredRoutes()).toEqual([]);
    for (const [method, path] of routes) {
      const response = await router.handle(
        new Request(`http://zorya.test${path}`, {
          method,
          ...(method !== "GET" && {
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ namespaceId: "team-a", scope: { namespaceId: "team-a" } }),
          }),
        }),
      );
      expect(response.status, `${method} ${path}`).toBe(403);
    }
    expect(seen).toHaveLength(routes.length);
  });

  it("does not let a stray namespace query re-scope global secrets", async () => {
    const targets: string[] = [];
    const router = new Router({
      accessGuard: ({ access }) => {
        if (access.kind === "protected") targets.push(access.target.resource.namespaceId);
        return new Response(null, { status: 403 });
      },
    });
    mountSecrets({
      router,
      secrets: {} as never,
      namespaces: undefined,
      authorizeScope: undefined,
    });
    await router.handle(
      new Request("http://zorya.test/api/secrets?scope=global&namespaceId=team-a"),
    );
    await router.handle(
      new Request("http://zorya.test/api/secrets?scope=namespace&namespaceId=team-a"),
    );
    await router.handle(
      new Request("http://zorya.test/api/secrets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          scope: { kind: "global", namespaceId: "team-a" },
          key: "token",
          value: "secret",
        }),
      }),
    );
    expect(targets).toEqual(["org", "team-a", "org"]);
  });
});
