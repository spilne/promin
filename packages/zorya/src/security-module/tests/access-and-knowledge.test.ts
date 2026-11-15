import { describe, expect, it } from "bun:test";
import { Router } from "../../server/router.ts";
import {
  ORG_WIDE_NAMESPACE,
  setRequestPrincipal,
  type Principal,
  type ZoryaSecurity,
} from "../../server/security/index.ts";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import type { LoginProvider } from "../login-provider.ts";
import type { SecurityUser } from "../security-store.ts";
import { hashSessionToken } from "../sessions.ts";
import { createZoryaSecurity } from "../zorya-security.ts";

const SECRET = "0123456789abcdef0123456789abcdef";
const BASE_URL = "http://zorya.test";

const unusedLoginProvider: LoginProvider = {
  id: "test",
  authorizationUrl: async () => "http://identity.test/authorize",
  exchange: async () => {
    throw new Error("not used");
  },
};

interface Environment {
  readonly store: InMemorySecurityStore;
  readonly security: ZoryaSecurity;
  readonly router: Router;
}

function environment(): Environment {
  const store = new InMemorySecurityStore();
  const security = createZoryaSecurity({
    store,
    loginProviders: [unusedLoginProvider],
    publicBaseUrl: BASE_URL,
    allowedDomains: ["example.com"],
    secret: SECRET,
  });
  const router = new Router({ logger: { error() {} } });
  security.mount?.(router);
  return { store, security, router };
}

async function createUser(params: {
  readonly env: Environment;
  readonly email: string;
  readonly bindings?: ReadonlyArray<{ readonly namespaceId: string; readonly accessRole: string }>;
}): Promise<SecurityUser> {
  return (
    await params.env.store.findOrCreateUser({
      email: params.email,
      bindings: params.bindings ?? [],
    })
  ).user;
}

async function principal(env: Environment, user: SecurityUser): Promise<Principal> {
  const result = await env.security.principalById(user.id);
  if (!result) throw new Error(`user ${user.id} has no principal`);
  return result;
}

async function handleAs(params: {
  readonly env: Environment;
  readonly principal: Principal;
  readonly method?: string;
  readonly path: string;
  readonly body?: unknown;
}): Promise<Response> {
  const req = new Request(`${BASE_URL}${params.path}`, {
    method: params.method ?? "GET",
    ...(params.body !== undefined && {
      headers: { "content-type": "application/json" },
      body: JSON.stringify(params.body),
    }),
  });
  setRequestPrincipal({ req, principal: params.principal });
  return params.env.router.handle(req);
}

describe("access-role administration", () => {
  it("a member cannot call the access routes; an admin can", async () => {
    const env = environment();
    const memberUser = await createUser({
      env,
      email: "member@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "member" }],
    });
    const adminUser = await createUser({
      env,
      email: "admin@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "admin" }],
    });
    const member = await principal(env, memberUser);
    const admin = await principal(env, adminUser);
    const binding = { namespaceId: "team-a", accessRole: "analyst" };
    const grant = { ...binding, baseId: "docs" };

    const unauthenticated = await env.router.handle(new Request(`${BASE_URL}/api/access/users`));
    expect(unauthenticated.status).toBe(403);
    expect(await unauthenticated.json()).toEqual({ error: "forbidden" });

    for (const request of [
      { path: "/api/access/users" },
      {
        method: "PUT",
        path: `/api/access/users/${memberUser.id}/bindings`,
        body: binding,
      },
      {
        method: "DELETE",
        path: `/api/access/users/${memberUser.id}/bindings`,
        body: binding,
      },
      { method: "POST", path: `/api/access/users/${memberUser.id}/disable` },
      { method: "PUT", path: "/api/access/knowledge-grants", body: grant },
      { method: "DELETE", path: "/api/access/knowledge-grants", body: grant },
    ]) {
      const response = await handleAs({ env, principal: member, ...request });
      expect(response.status).toBe(403);
      expect(await response.json()).toEqual({ error: "forbidden" });
    }

    const response = await handleAs({ env, principal: admin, path: "/api/access/users" });
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      users: ReadonlyArray<SecurityUser & { bindings: ReadonlyArray<unknown> }>;
    };
    expect(body.users).toHaveLength(2);
    expect(body.users.find((user) => user.id === adminUser.id)?.bindings).toEqual([
      { userId: adminUser.id, namespaceId: ORG_WIDE_NAMESPACE, accessRole: "admin" },
    ]);
  });

  it("a new binding applies to the next request", async () => {
    const env = environment();
    const adminUser = await createUser({
      env,
      email: "admin@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "admin" }],
    });
    const user = await createUser({ env, email: "user@example.com" });
    const admin = await principal(env, adminUser);
    const body = { namespaceId: "team-a", accessRole: "analyst" };

    const put = await handleAs({
      env,
      principal: admin,
      method: "PUT",
      path: `/api/access/users/${user.id}/bindings`,
      body,
    });
    expect(put.status).toBe(204);
    expect((await env.security.principalById(user.id))?.accessRoles).toEqual({
      "team-a": ["analyst"],
    });

    const remove = await handleAs({
      env,
      principal: admin,
      method: "DELETE",
      path: `/api/access/users/${user.id}/bindings`,
      body,
    });
    expect(remove.status).toBe(204);
    expect((await env.security.principalById(user.id))?.accessRoles).toEqual({});
  });

  it("a bad body answers 400 with issues", async () => {
    const env = environment();
    const adminUser = await createUser({
      env,
      email: "admin@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "admin" }],
    });
    const user = await createUser({ env, email: "user@example.com" });
    const response = await handleAs({
      env,
      principal: await principal(env, adminUser),
      method: "PUT",
      path: `/api/access/users/${user.id}/bindings`,
      body: { namespaceId: "team-a" },
    });

    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({
      error: "invalid_request",
      issues: [{ path: "accessRole" }],
    });
  });
});

describe("namespace and knowledge access", () => {
  it("canActIn: bound namespace and org-wide are allowed, others are not", async () => {
    const env = environment();
    const scopedUser = await createUser({
      env,
      email: "scoped@example.com",
      bindings: [{ namespaceId: "team-a", accessRole: "analyst" }],
    });
    const orgUser = await createUser({
      env,
      email: "org@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "member" }],
    });
    const scoped = await principal(env, scopedUser);
    const org = await principal(env, orgUser);

    expect(await env.security.canActIn({ principal: scoped, namespaceId: "team-a" })).toBeTrue();
    expect(await env.security.canActIn({ principal: scoped, namespaceId: "team-b" })).toBeFalse();
    expect(await env.security.canActIn({ principal: org, namespaceId: "team-b" })).toBeTrue();
  });

  it("canUseKnowledge: no grant means no access for a member", async () => {
    const env = environment();
    const user = await createUser({
      env,
      email: "member@example.com",
      bindings: [{ namespaceId: "team-a", accessRole: "member" }],
    });
    expect(
      await env.security.canUseKnowledge({
        principal: await principal(env, user),
        namespaceId: "team-a",
        baseId: "docs",
      }),
    ).toBeFalse();
  });

  it("canUseKnowledge: admin and knowledge-manager always have access", async () => {
    const env = environment();
    const adminUser = await createUser({
      env,
      email: "admin@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "admin" }],
    });
    const managerUser = await createUser({
      env,
      email: "manager@example.com",
      bindings: [{ namespaceId: "team-a", accessRole: "knowledge-manager" }],
    });
    const admin = await principal(env, adminUser);
    const manager = await principal(env, managerUser);

    expect(
      await env.security.canUseKnowledge({
        principal: admin,
        namespaceId: "any-team",
        baseId: "ungranted",
      }),
    ).toBeTrue();
    expect(
      await env.security.canUseKnowledge({
        principal: manager,
        namespaceId: "team-a",
        baseId: "ungranted",
      }),
    ).toBeTrue();
    expect(
      await env.security.canUseKnowledge({
        principal: manager,
        namespaceId: "team-b",
        baseId: "ungranted",
      }),
    ).toBeFalse();
  });

  it("canUseKnowledge: granting a caller role allows it; revoking denies the next call", async () => {
    const env = environment();
    const adminUser = await createUser({
      env,
      email: "admin@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "admin" }],
    });
    const memberUser = await createUser({
      env,
      email: "member@example.com",
      bindings: [{ namespaceId: "team-a", accessRole: "analyst" }],
    });
    const admin = await principal(env, adminUser);
    const member = await principal(env, memberUser);
    const grant = { namespaceId: "team-a", baseId: "docs", accessRole: "analyst" };

    expect(
      await env.security.canUseKnowledge({
        principal: member,
        namespaceId: "team-a",
        baseId: "docs",
      }),
    ).toBeFalse();
    expect(
      (
        await handleAs({
          env,
          principal: admin,
          method: "PUT",
          path: "/api/access/knowledge-grants",
          body: grant,
        })
      ).status,
    ).toBe(204);
    expect(
      await env.security.canUseKnowledge({
        principal: member,
        namespaceId: "team-a",
        baseId: "docs",
      }),
    ).toBeTrue();
    expect(
      (
        await handleAs({
          env,
          principal: admin,
          method: "DELETE",
          path: "/api/access/knowledge-grants",
          body: grant,
        })
      ).status,
    ).toBe(204);
    expect(
      await env.security.canUseKnowledge({
        principal: member,
        namespaceId: "team-a",
        baseId: "docs",
      }),
    ).toBeFalse();
  });

  it("canUseKnowledge: an org-wide grant applies in every namespace", async () => {
    const env = environment();
    const user = await createUser({
      env,
      email: "member@example.com",
      bindings: [
        { namespaceId: "team-a", accessRole: "member" },
        { namespaceId: "team-b", accessRole: "member" },
      ],
    });
    const member = await principal(env, user);
    await env.store.putKnowledgeGrant({
      namespaceId: ORG_WIDE_NAMESPACE,
      baseId: "handbook",
      accessRole: "member",
    });

    expect(
      await env.security.canUseKnowledge({
        principal: member,
        namespaceId: "team-a",
        baseId: "handbook",
      }),
    ).toBeTrue();
    expect(
      await env.security.canUseKnowledge({
        principal: member,
        namespaceId: "team-b",
        baseId: "handbook",
      }),
    ).toBeTrue();
    expect(
      await env.security.canUseKnowledge({
        principal: member,
        namespaceId: "team-c",
        baseId: "handbook",
      }),
    ).toBeFalse();
  });
});

describe("disabled users", () => {
  it("a disabled user is no longer authenticated and has no principal", async () => {
    const env = environment();
    const adminUser = await createUser({
      env,
      email: "admin@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "admin" }],
    });
    const user = await createUser({
      env,
      email: "user@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "member" }],
    });
    const token = "active-session-token";
    await env.store.createSession({
      tokenHash: hashSessionToken(token),
      userId: user.id,
      ttlMs: 60_000,
    });
    const authenticated = () =>
      env.security.authenticate(
        new Request(`${BASE_URL}/api/agents`, {
          headers: { cookie: `zorya_session=${token}` },
        }),
      );
    expect((await authenticated())?.id).toBe(user.id);

    const response = await handleAs({
      env,
      principal: await principal(env, adminUser),
      method: "POST",
      path: `/api/access/users/${user.id}/disable`,
    });
    expect(response.status).toBe(204);
    expect(await authenticated()).toBeUndefined();
    expect(await env.security.principalById(user.id)).toBeUndefined();
  });
});
