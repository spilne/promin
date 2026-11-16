import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentInstanceRegistry,
  InMemoryAgentRegistry,
  type Agent,
  type RegisteredAgent,
} from "@promin/agent";
import {
  InMemoryWorkflowStorage,
  createWorkflowRunner,
  type RunSecurityContext,
} from "@promin/workflow";
import { ZoryaServer } from "../../../server.ts";
import { ZoryaAgents } from "../../../services/agents/zorya-agents.ts";
import { LocalWorkflows } from "../../../services/workflows/local-workflows.ts";
import {
  principalRef,
  requestSecurityContext,
  type Principal,
  type ZoryaSecurity,
} from "../../../security/index.ts";
import { deriveScope } from "../agent-scope.ts";

const operator: Principal = {
  kind: "operator",
  id: "operator-1",
  accessRoles: { org: ["admin"] },
  canActFor: true,
};

const user: Principal = {
  kind: "user",
  id: "user-1",
  accessRoles: { acme: ["member"] },
  canActFor: false,
};

const serviceAccount: Principal = {
  kind: "service-account",
  id: "slack-bot",
  accessRoles: { acme: ["member"] },
  canActFor: true,
};

describe("deriveScope", () => {
  it("preserves explicitly named scope for an operator", () => {
    expect(
      deriveScope({
        principal: operator,
        requested: { namespaceId: "acme", ownerId: "team-1" },
      }),
    ).toEqual({ namespaceId: "acme", ownerId: "team-1" });
  });

  it("derives a normal user's resource identity", () => {
    expect(deriveScope({ principal: user, requested: { namespaceId: "acme" } })).toEqual({
      namespaceId: "acme",
      resourceId: "user-1",
    });
  });

  it("forbids another resource identity and every owner identity for a normal user", () => {
    expect(
      deriveScope({
        principal: user,
        requested: { namespaceId: "acme", resourceId: "user-2" },
      }),
    ).toEqual({ error: "identity_forbidden", status: 403 });
    expect(
      deriveScope({
        principal: user,
        requested: { namespaceId: "acme", ownerId: "user-1" },
      }),
    ).toEqual({ error: "identity_forbidden", status: 403 });
  });

  it("requires a namespace for a normal user", () => {
    expect(deriveScope({ principal: user, requested: {} })).toEqual({
      error: "missing_namespaceId",
      status: 400,
    });
  });

  it("keeps the legacy explicit-scope contract without a principal", () => {
    expect(
      deriveScope({
        principal: undefined,
        requested: { namespaceId: "acme", resourceId: "legacy-user" },
      }),
    ).toEqual({ namespaceId: "acme", resourceId: "legacy-user" });
    expect(deriveScope({ principal: undefined, requested: { namespaceId: "acme" } })).toEqual({
      error: "missing_scope_identity",
      status: 400,
    });
  });
});

interface ResolveScope {
  readonly namespaceId?: string;
  readonly resourceId?: string;
  readonly principal?: Principal;
  readonly securityContext?: RunSecurityContext;
}

function fakeAgent(): Agent {
  const agent = {
    withScope: () => agent,
    invoke: () => ({
      text: Promise.resolve("ok"),
      finishReason: Promise.resolve("stop"),
      usage: Promise.resolve({ inputTokens: 0, outputTokens: 0, totalTokens: 0 }),
    }),
  } as unknown as Agent;
  return agent;
}

async function boot(params: {
  readonly security?: ZoryaSecurity;
  readonly apiKeys?: ReadonlyArray<string>;
}) {
  const storage = new InMemoryWorkflowStorage();
  const workflows = new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const registry = new InMemoryAgentRegistry();
  await registry.register({
    id: "support",
    backend: {
      type: "local",
      model: { provider: "anthropic", id: "claude-sonnet-4-6" },
      role: { inline: { systemPrompt: "Helpful", tools: [] } },
    },
  });
  const resolved: ResolveScope[] = [];
  const agents = new ZoryaAgents({
    registry,
    instances: new InMemoryAgentInstanceRegistry(),
    resolve: (_recipe: RegisteredAgent, scope?: ResolveScope) => {
      resolved.push(scope ?? {});
      return fakeAgent();
    },
    turnGate: false,
  });
  const server = new ZoryaServer({
    workflows,
    agents,
    ...(params.security !== undefined && { security: params.security }),
    ...(params.apiKeys !== undefined && { apiKeys: params.apiKeys }),
  });
  return { server, resolved };
}

function request(body: Record<string, unknown>, key?: string): Request {
  return new Request("http://zorya/api/agents/support/invoke", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(key !== undefined && { authorization: `Bearer ${key}` }),
    },
    body: JSON.stringify({ task: "help", ...body }),
  });
}

function security(params: {
  readonly actor: Principal;
  readonly users?: ReadonlyMap<string, Principal>;
  readonly authorize?: (principal: Principal, namespaceId: string) => boolean;
}): ZoryaSecurity {
  return {
    authenticate: async () => params.actor,
    resolvePrincipal: async ({ id }) => params.users?.get(id),
    resolveIdentity: async ({ actor, asserted }) => {
      if (!asserted) return { actor, effective: principalRef(actor) };
      if (!actor.canActFor) return undefined;
      if (actor.kind === "operator") return { actor, effective: asserted };
      const effective = params.users?.get(asserted.id);
      return effective ? { actor, effective: principalRef(effective) } : undefined;
    },
    identityFilter: async ({ namespaceId }) => ({ kind: "namespace", namespaceId }),
    authorize: async ({ context, resource }) => {
      const principal =
        context.effective.id === context.actor.id
          ? context.actor
          : params.users?.get(context.effective.id);
      return params.authorize?.(principal ?? context.actor, resource.namespaceId) === false
        ? { allowed: false, reason: "namespace_denied" }
        : { allowed: true, reason: "access_role_allow" };
    },
    recordAudit: async () => {},
  };
}

async function errorOf(response: Response): Promise<string | undefined> {
  return ((await response.json()) as { error?: string }).error;
}

describe("agent HTTP scope derives from the authenticated principal", () => {
  it("preserves an operator's requested identity", async () => {
    const { server, resolved } = await boot({ security: security({ actor: operator }) });

    const response = await server.handle(request({ namespaceId: "acme", resourceId: "user-7" }));

    expect(response.status).toBe(200);
    expect(resolved).toEqual([
      {
        namespaceId: "acme",
        resourceId: "user-7",
        principal: operator,
        securityContext: {
          actor: { kind: "operator", id: "operator-1" },
          effective: { kind: "user", id: "user-7" },
        },
      },
    ]);
  });

  it("derives a normal user's own identity", async () => {
    const { server, resolved } = await boot({ security: security({ actor: user }) });

    const response = await server.handle(request({ namespaceId: "acme" }));

    expect(response.status).toBe(200);
    expect(resolved).toEqual([
      {
        namespaceId: "acme",
        resourceId: "user-1",
        principal: user,
        securityContext: { actor: { kind: "user", id: "user-1" } },
      },
    ]);
  });

  it("forbids a normal user's other resource identity", async () => {
    const { server, resolved } = await boot({ security: security({ actor: user }) });

    const response = await server.handle(request({ namespaceId: "acme", resourceId: "user-2" }));

    expect(response.status).toBe(403);
    expect(await errorOf(response)).toBe("identity_forbidden");
    expect(resolved).toHaveLength(0);
  });

  it("forbids a normal user's owner identity", async () => {
    const { server, resolved } = await boot({ security: security({ actor: user }) });

    const response = await server.handle(request({ namespaceId: "acme", ownerId: "user-1" }));

    expect(response.status).toBe(403);
    expect(await errorOf(response)).toBe("identity_forbidden");
    expect(resolved).toHaveLength(0);
  });

  it("requires a namespace for a normal user", async () => {
    const { server, resolved } = await boot({ security: security({ actor: user }) });

    const response = await server.handle(request({}));

    expect(response.status).toBe(400);
    expect(await errorOf(response)).toBe("missing_namespaceId");
    expect(resolved).toHaveLength(0);
  });

  it("refuses a principal denied by namespace policy", async () => {
    const { server, resolved } = await boot({
      security: security({ actor: user, authorize: () => false }),
    });

    const response = await server.handle(request({ namespaceId: "acme" }));

    expect(response.status).toBe(403);
    expect(await errorOf(response)).toBe("forbidden");
    expect(resolved).toHaveLength(0);
  });

  it("passes the represented user principal for a service account", async () => {
    const represented = { ...user, id: "represented-user" };
    const { server, resolved } = await boot({
      security: security({
        actor: serviceAccount,
        users: new Map([[represented.id, represented]]),
      }),
    });

    const response = await server.handle(
      request({ namespaceId: "acme", resourceId: represented.id }),
    );

    expect(response.status).toBe(200);
    expect(resolved).toEqual([
      {
        namespaceId: "acme",
        resourceId: represented.id,
        principal: represented,
        securityContext: {
          actor: { kind: "service-account", id: "slack-bot" },
          effective: { kind: "user", id: represented.id },
        },
      },
    ]);
  });

  it("resolves an act-for service account's ownerId to the represented user", async () => {
    const represented = { ...user, id: "alice" };
    const { server, resolved } = await boot({
      security: security({
        actor: serviceAccount,
        users: new Map([[represented.id, represented]]),
      }),
    });

    const response = await server.handle(request({ namespaceId: "acme", ownerId: "alice" }));

    expect(response.status).toBe(200);
    expect(resolved).toHaveLength(1);
    expect(resolved[0]?.principal).toEqual(represented);
    expect(resolved[0]?.principal).not.toEqual(serviceAccount);
  });

  it("rejects an act-for service account's unknown owner before agent resolution", async () => {
    const { server, resolved } = await boot({
      security: security({ actor: serviceAccount, users: new Map() }),
    });

    const response = await server.handle(request({ namespaceId: "acme", ownerId: "alice" }));

    expect(response.status).toBe(403);
    expect(resolved).toHaveLength(0);
  });

  it("refuses unknown or disabled represented users", async () => {
    for (const resourceId of ["unknown-user", "disabled-user"]) {
      const { server, resolved } = await boot({
        security: security({ actor: serviceAccount, users: new Map() }),
      });

      const response = await server.handle(request({ namespaceId: "acme", resourceId }));

      expect(response.status).toBe(403);
      expect(await errorOf(response)).toBe("unknown_user");
      expect(resolved).toHaveLength(0);
    }
  });

  it("keeps named scope and the operator principal for API keys", async () => {
    const key = "operator-key-0123456789";
    const { server, resolved } = await boot({ apiKeys: [key] });

    const req = request({ namespaceId: "acme", resourceId: "named-user" }, key);
    const response = await server.handle(req);

    expect(response.status).toBe(200);
    expect(resolved).toEqual([
      {
        namespaceId: "acme",
        resourceId: "named-user",
        principal: expect.objectContaining({ kind: "operator", id: "operator" }),
        securityContext: {
          actor: { kind: "operator", id: "operator" },
          effective: { kind: "user", id: "named-user" },
        },
      },
    ]);
    expect(requestSecurityContext(req)).toEqual({
      actor: expect.objectContaining({ kind: "operator", id: "operator" }),
      effective: { kind: "user", id: "named-user" },
    });
  });
});
