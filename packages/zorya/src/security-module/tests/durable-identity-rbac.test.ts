import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { Router } from "../../server/router.ts";
import {
  createSecurityRouteAccessGuard,
  ORG_WIDE_NAMESPACE,
  principalRef,
  setRequestPrincipal,
  type Principal,
} from "../../server/security/index.ts";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import type { LoginProvider } from "../login-provider.ts";
import { createZoryaSecurity } from "../zorya-security.ts";

const BASE_URL = "https://zorya.test";
const SECRET = "0123456789abcdef0123456789abcdef";
const provider: LoginProvider = {
  id: "test",
  authorizationUrl: async () => "https://identity.test/authorize",
  exchange: async () => {
    throw new Error("not used");
  },
};

function environment() {
  const clock = FakeWallClock.create(1_000);
  const store = new InMemorySecurityStore({ clock });
  const security = createZoryaSecurity({
    store,
    loginProviders: [provider],
    publicBaseUrl: BASE_URL,
    allowedDomains: ["example.com"],
    auditSink: { recordAudit: async () => {} },
    secret: SECRET,
    clock,
  });
  const logger = { error() {} };
  const router = new Router({
    logger,
    accessGuard: createSecurityRouteAccessGuard({ security, clock, logger }),
  });
  security.mount?.(router);
  return { clock, store, security, router };
}

async function createUser(env: ReturnType<typeof environment>, email: string, role?: string) {
  return (
    await env.store.findOrCreateUser({
      email,
      bindings: role ? [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: role }] : [],
    })
  ).user;
}

async function resolve(
  env: ReturnType<typeof environment>,
  kind: "user" | "service-account",
  id: string,
) {
  const value = await env.security.resolvePrincipal({ kind, id });
  if (!value) throw new Error(`missing ${kind} principal ${id}`);
  return value;
}

async function handleAs(params: {
  readonly env: ReturnType<typeof environment>;
  readonly principal: Principal;
  readonly method?: string;
  readonly path: string;
  readonly body?: unknown;
}) {
  const request = new Request(`${BASE_URL}${params.path}`, {
    method: params.method ?? "GET",
    ...(params.body !== undefined && {
      headers: { "content-type": "application/json" },
      body: JSON.stringify(params.body),
    }),
  });
  setRequestPrincipal({ req: request, principal: params.principal });
  return params.env.router.handle(request);
}

function authenticate(env: ReturnType<typeof environment>, token: string) {
  return env.security.authenticate({
    request: new Request(BASE_URL, {
      headers: { authorization: `Bearer ${token}` },
    }),
    plane: "operator",
  });
}

describe("durable identity, groups, and service accounts", () => {
  it("applies and revokes direct, group, org, and knowledge audiences on the next request", async () => {
    const env = environment();
    const user = await createUser(env, "user@example.com");
    const subject = { kind: "user", id: user.id } as const;
    await env.store.putGroup({ id: "finance", status: "active" });
    await env.store.putGroupMembership({ groupId: "finance", subject });
    await env.store.putSubjectBinding({
      subject,
      namespaceId: "team-a",
      accessRole: "member",
    });
    await env.store.putSubjectBinding({
      subject: { kind: "group", id: "finance" },
      namespaceId: ORG_WIDE_NAMESPACE,
      accessRole: "auditor",
    });
    await env.store.putKnowledgeAudienceGrant({
      namespaceId: "team-a",
      baseId: "finance-docs",
      audience: { kind: "group", id: "finance" },
    });

    const current = await resolve(env, "user", user.id);
    expect(new Set(current.accessRoles.org)).toEqual(new Set(["auditor"]));
    expect(new Set(current.accessRoles["team-a"])).toEqual(new Set(["member"]));
    expect(current.teams).toEqual(["finance"]);
    expect(
      await env.security.authorize({
        context: { actor: current, effective: principalRef(current) },
        permission: { resource: "knowledge", action: "use" },
        resource: {
          kind: "knowledge",
          namespaceId: "team-a",
          id: "finance-docs",
        },
      }),
    ).toEqual({ allowed: true, reason: "knowledge_grant_allow" });

    await env.store.removeGroupMembership({ groupId: "finance", subject });
    expect(
      await env.security.authorize({
        context: { actor: current, effective: principalRef(current) },
        permission: { resource: "knowledge", action: "use" },
        resource: {
          kind: "knowledge",
          namespaceId: "team-a",
          id: "finance-docs",
        },
      }),
    ).toEqual({ allowed: false, reason: "permission_denied" });
    expect((await resolve(env, "user", user.id)).accessRoles.org).toBeUndefined();
  });

  it("keeps colliding user and service-account IDs isolated", async () => {
    const env = environment();
    const user = await createUser(env, "collision@example.com");
    await env.store.putServiceAccount({
      id: user.id,
      status: "active",
      canActFor: true,
    });
    await env.store.putSubjectBinding({
      subject: { kind: "user", id: user.id },
      namespaceId: "team-a",
      accessRole: "member",
    });
    await env.store.putSubjectBinding({
      subject: { kind: "service-account", id: user.id },
      namespaceId: "team-a",
      accessRole: "agent-manager",
    });
    expect((await resolve(env, "user", user.id)).accessRoles["team-a"]).toEqual(["member"]);
    const account = await resolve(env, "service-account", user.id);
    expect(account.accessRoles["team-a"]).toEqual(["agent-manager"]);
    expect(account.canActFor).toBeTrue();
  });

  it("creates, overlaps, revokes, expires, and disables hash-only credentials", async () => {
    const env = environment();
    const adminUser = await createUser(env, "admin@example.com", "admin");
    const admin = await resolve(env, "user", adminUser.id);
    const created = await handleAs({
      env,
      principal: admin,
      method: "POST",
      path: "/api/access/service-accounts",
      body: { id: "slack-agent", canActFor: true },
    });
    expect(created.status).toBe(201);
    const first = (await created.json()) as {
      credential: { id: string; token: string };
    };
    expect(await authenticate(env, first.credential.token)).toMatchObject({
      id: "slack-agent",
      canActFor: true,
    });
    await env.store.putServiceAccount({
      id: "slack-agent",
      status: "active",
      canActFor: false,
    });
    expect((await authenticate(env, first.credential.token))?.canActFor).toBeFalse();
    const firstStored = await env.store.getServiceAccountCredential(first.credential.id);
    expect(firstStored?.secretHash).toHaveLength(64);
    expect(JSON.stringify(firstStored)).not.toContain(first.credential.token);

    const binding = await handleAs({
      env,
      principal: admin,
      method: "PUT",
      path: "/api/access/bindings",
      body: {
        subject: { kind: "service-account", id: "slack-agent" },
        namespaceId: "team-a",
        accessRole: "member",
      },
    });
    expect(binding.status).toBe(204);
    expect((await resolve(env, "service-account", "slack-agent")).accessRoles["team-a"]).toEqual([
      "member",
    ]);

    const rotated = await handleAs({
      env,
      principal: admin,
      method: "POST",
      path: "/api/access/service-accounts/slack-agent/credentials",
      body: { expiresAt: 1_100 },
    });
    const second = (await rotated.json()) as {
      credential: { id: string; token: string };
    };
    expect(await authenticate(env, first.credential.token)).toBeDefined();
    expect(await authenticate(env, second.credential.token)).toBeDefined();

    const revoked = await handleAs({
      env,
      principal: admin,
      method: "DELETE",
      path: `/api/access/service-accounts/slack-agent/credentials/${first.credential.id}`,
    });
    expect(revoked.status).toBe(204);
    expect(await authenticate(env, first.credential.token)).toBeUndefined();
    expect(await authenticate(env, second.credential.token)).toBeDefined();

    env.clock.advance(100);
    expect(await authenticate(env, second.credential.token)).toBeUndefined();
    const disabled = await handleAs({
      env,
      principal: admin,
      method: "DELETE",
      path: "/api/access/service-accounts/slack-agent",
    });
    expect(disabled.status).toBe(204);
    expect(
      await env.security.resolvePrincipal({
        kind: "service-account",
        id: "slack-agent",
      }),
    ).toBeUndefined();
    expect(
      await env.security.resolveIdentity({
        actor: {
          kind: "service-account",
          id: "slack-agent",
          accessRoles: {},
          canActFor: false,
        },
      }),
    ).toBeUndefined();
  });
});
