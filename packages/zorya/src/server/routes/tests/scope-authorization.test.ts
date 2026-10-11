// ---------------------------------------------------------------------------
// `authorizeScope` across the tenant-scoped surfaces that hang off the
// agents service: namespace memory writes, instance lists, scoped secrets
// (direct and via clone), DAG runs, and the owner a gateway binds into the
// scoped agent. The test authorizer admits a caller only into the
// namespace named by its `x-tenant` header, and never for agent
// "restricted".
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { composeAgentInstanceId, type AgentScope } from "@promin/agent";
import type { ScopeAuthorizer } from "../agent-gateway-kit.ts";
import {
  bootAgents,
  jsonOf,
  localBackend,
  req,
  type HarnessOpts,
} from "./agent-gateway-harness.ts";

const authorizeScope: ScopeAuthorizer = ({ req: r, scope }) =>
  r.headers.get("x-tenant") === scope.namespaceId && scope.agentId !== "restricted";

const asAcme = { "x-tenant": "acme" };

/** Boot with the test authorizer; `bound` collects every scope an agent is bound to. */
async function boot(opts: HarnessOpts = {}) {
  const bound: AgentScope[] = [];
  const h = await bootAgents({
    authorizeScope,
    wrapAgent: (agent) =>
      new Proxy(agent, {
        get(target, prop) {
          if (prop === "withScope") {
            return (scope: AgentScope) => {
              bound.push(scope);
              return target.withScope(scope);
            };
          }
          const value = Reflect.get(target, prop, target);
          return typeof value === "function" ? value.bind(target) : value;
        },
      }),
    ...opts,
  });
  return { ...h, bound };
}

async function errorOf(res: Response): Promise<string> {
  return (await jsonOf<{ error: string }>(res)).error;
}

describe("namespace memory writes", () => {
  it("PATCH, add fact and delete fact need the caller to own the namespace", async () => {
    const { server, memory } = await boot();
    const fact = await memory.appendNamespaceFact("globex", {
      text: "globex secret",
      source: "operator",
    });

    const patch = await server.handle(
      req("PATCH", "/api/memory/namespace/globex", { staticRules: "pwned" }, asAcme),
    );
    expect(patch.status).toBe(403);
    expect(await errorOf(patch)).toBe("scope_forbidden");
    const add = await server.handle(
      req("POST", "/api/memory/namespace/globex/facts", { text: "injected" }, asAcme),
    );
    expect(add.status).toBe(403);
    const del = await server.handle(
      req("DELETE", `/api/memory/namespace/globex/facts/${fact.id}`, undefined, asAcme),
    );
    expect(del.status).toBe(403);

    expect((await memory.getNamespace("globex"))?.staticRules ?? null).toBeNull();
    expect((await memory.listNamespaceFacts("globex")).map((f) => f.text)).toEqual([
      "globex secret",
    ]);

    const own = await server.handle(
      req("POST", "/api/memory/namespace/acme/facts", { text: "ours" }, asAcme),
    );
    expect(own.status).toBe(201);
    const ownPatch = await server.handle(
      req("PATCH", "/api/memory/namespace/acme", { staticRules: "rules" }, asAcme),
    );
    expect(ownPatch.status).toBe(200);
  });
});

describe("instance lists", () => {
  it("must name a namespace the caller owns", async () => {
    const { server, instances } = await boot();
    await instances.resolveOrCreate({
      registeredAgentId: "support",
      namespaceId: "globex",
      ownerId: "carol",
    });
    for (const path of ["/api/instances", "/api/agents/support/instances"]) {
      const unscoped = await server.handle(req("GET", path, undefined, asAcme));
      expect([path, unscoped.status]).toEqual([path, 400]);
      expect(await errorOf(unscoped)).toBe("missing_namespaceId");
      const foreign = await server.handle(
        req("GET", `${path}?namespaceId=globex`, undefined, asAcme),
      );
      expect([path, foreign.status]).toEqual([path, 403]);
      const own = await server.handle(req("GET", `${path}?namespaceId=acme`, undefined, asAcme));
      expect([path, own.status]).toEqual([path, 200]);
      expect((await jsonOf<{ instances: unknown[] }>(own)).instances).toEqual([]);
    }
  });
});

describe("scoped secrets", () => {
  it("list, store and delete need the caller to own the secret's namespace", async () => {
    const { server, secrets } = await boot({ secrets: true });
    const globex = { kind: "namespace" as const, namespaceId: "globex" };
    await secrets!.set({ scope: globex, key: "API_KEY", value: "globex-key" });

    const store = await server.handle(
      req("POST", "/api/secrets", { scope: globex, key: "API_KEY", value: "evil" }, asAcme),
    );
    expect(store.status).toBe(403);
    const list = await server.handle(
      req("GET", "/api/secrets?scope=namespace&namespaceId=globex", undefined, asAcme),
    );
    expect(list.status).toBe(403);
    const del = await server.handle(
      req("DELETE", "/api/secrets/API_KEY?scope=namespace&namespaceId=globex", undefined, asAcme),
    );
    expect(del.status).toBe(403);
    expect(await secrets!.list({ scope: globex })).toEqual(["API_KEY"]);

    const own = await server.handle(
      req(
        "POST",
        "/api/secrets",
        {
          scope: { kind: "resource", namespaceId: "acme", resourceId: "alice" },
          key: "K",
          value: "v",
        },
        asAcme,
      ),
    );
    expect(own.status).toBe(201);
  });

  it("a clone cannot provision secrets into another namespace", async () => {
    const { server, registry, secrets } = await boot({ secrets: true });
    const res = await server.handle(
      req(
        "POST",
        "/api/agents/support/clone",
        {
          targetId: "support-2",
          secrets: { API_KEY: "evil" },
          secretsScope: { kind: "namespace", namespaceId: "globex" },
        },
        asAcme,
      ),
    );
    expect(res.status).toBe(403);
    expect(await registry.get("support-2")).toBeNull();
    expect(await secrets!.list({ scope: { kind: "namespace", namespaceId: "globex" } })).toEqual(
      [],
    );
  });
});

describe("owner binding", () => {
  it("an ownerId invocation binds the authorized owner into the agent scope", async () => {
    const { server, bound } = await boot();
    const res = await server.handle(
      req(
        "POST",
        "/api/agents/support/invoke",
        { task: "hi", namespaceId: "acme", ownerId: "alice" },
        asAcme,
      ),
    );
    expect(res.status).toBe(200);
    expect(bound).toEqual([
      {
        namespaceId: "acme",
        resourceId: composeAgentInstanceId({
          namespaceId: "acme",
          registeredAgentId: "support",
          ownerId: "alice",
        }),
        ownerId: "alice",
      },
    ]);
  });

  it("a raw resourceId shaped like an instance id binds no owner", async () => {
    const { server, bound } = await boot();
    const res = await server.handle(
      req(
        "POST",
        "/api/agents/support/invoke",
        { task: "hi", namespaceId: "acme", resourceId: "acme::support::bob" },
        asAcme,
      ),
    );
    expect(res.status).toBe(200);
    expect(bound).toEqual([{ namespaceId: "acme", resourceId: "acme::support::bob" }]);
  });
});

describe("DAG runs", () => {
  const dagOf = (agentId: string) => ({
    id: `dag-${agentId}`,
    version: "v1",
    nodes: [{ id: "n1", agentId, inputs: { task: { kind: "initial", path: "task" } } }],
    edges: [],
    entry: ["n1"],
    terminals: ["n1"],
  });

  async function bootDags() {
    const h = await boot({ dags: true });
    await h.registry.register({ id: "restricted", backend: localBackend });
    await h.registry.register({
      id: "off",
      backend: localBackend,
      metadata: { capabilities: [], tags: [], enabled: false },
    });
    for (const id of ["support", "restricted", "off"]) await h.dags!.registry.register(dagOf(id));
    return h;
  }

  const run = (id: string, body: Record<string, unknown>) =>
    req("POST", `/api/dags/dag-${id}/run`, { initialInput: { task: "hi" }, ...body }, asAcme);

  it("needs a scope identity", async () => {
    const { server } = await bootDags();
    const noNs = await server.handle(run("support", {}));
    expect(noNs.status).toBe(400);
    expect(await errorOf(noNs)).toBe("missing_namespaceId");
    const noIdentity = await server.handle(run("support", { namespaceId: "acme" }));
    expect(noIdentity.status).toBe(400);
    expect(await errorOf(noIdentity)).toBe("missing_scope_identity");
  });

  it("refuses a foreign namespace, a node agent the caller may not run, and a disabled one", async () => {
    const { server, bound } = await bootDags();
    const foreign = await server.handle(
      run("support", { namespaceId: "globex", resourceId: "alice" }),
    );
    expect(foreign.status).toBe(403);
    const restricted = await server.handle(
      run("restricted", { namespaceId: "acme", resourceId: "alice" }),
    );
    expect(restricted.status).toBe(403);
    const off = await server.handle(run("off", { namespaceId: "acme", resourceId: "alice" }));
    expect(off.status).toBe(410);
    expect(await errorOf(off)).toBe("agent_disabled");
    expect(bound).toEqual([]);
  });

  it("runs every node bound to the caller's scope", async () => {
    const { server, bound } = await bootDags();
    const res = await server.handle(run("support", { namespaceId: "acme", resourceId: "alice" }));
    expect(res.status).toBe(200);
    const body = await jsonOf<{ result: { ok: boolean; outputs: Record<string, unknown> } }>(res);
    expect(body.result.ok).toBe(true);
    expect(bound).toEqual([{ namespaceId: "acme", resourceId: "alice" }]);
  });
});
