import { describe, expect, it } from "bun:test";
import { SystemWallClock } from "@promin/workflow";
import { Router } from "../../server/router.ts";
import {
  createSecurityRouteAccessGuard,
  ORG_WIDE_NAMESPACE,
  principalRef,
  setRequestPrincipal,
  type AuthorizationRequest,
  type Principal,
  type ZoryaSecurity,
} from "../../server/security/index.ts";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import type { LoginProvider } from "../login-provider.ts";
import type { SecurityUser } from "../security-store.ts";
import { hashSessionToken } from "../sessions.ts";
import { createZoryaSecurity, type ServiceAccountConfig } from "../zorya-security.ts";

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

function environment(
  params: { readonly serviceAccounts?: ReadonlyArray<ServiceAccountConfig> } = {},
): Environment {
  const store = new InMemorySecurityStore();
  const security = createZoryaSecurity({
    store,
    loginProviders: [unusedLoginProvider],
    publicBaseUrl: BASE_URL,
    allowedDomains: ["example.com"],
    ...(params.serviceAccounts && { serviceAccounts: params.serviceAccounts }),
    secret: SECRET,
  });
  const logger = { error() {} };
  const router = new Router({
    logger,
    accessGuard: createSecurityRouteAccessGuard({ security, clock: SystemWallClock, logger }),
  });
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
  const result = await env.security.resolvePrincipal({ kind: "user", id: user.id });
  if (!result) throw new Error(`user ${user.id} has no principal`);
  return result;
}

async function canInvokeAgent(
  env: Environment,
  value: Principal,
  namespaceId: string,
): Promise<boolean> {
  return (
    await env.security.authorize({
      context: { actor: value, effective: principalRef(value) },
      permission: { resource: "agent", action: "invoke" },
      resource: { kind: "agent", namespaceId },
    })
  ).allowed;
}

async function canUseKnowledgeBase(
  env: Environment,
  value: Principal,
  namespaceId: string,
  baseId: string,
): Promise<boolean> {
  return (
    await env.security.authorize({
      context: { actor: value, effective: principalRef(value) },
      permission: { resource: "knowledge", action: "use" },
      resource: { kind: "knowledge", namespaceId, id: baseId },
    })
  ).allowed;
}

function authenticate(env: Environment, request: Request): Promise<Principal | undefined> {
  return env.security.authenticate({ request, plane: "operator" });
}

function rolesAt(
  value: Principal | undefined,
  namespaceId: string,
): ReadonlyArray<string> | undefined {
  return value?.accessRoles[namespaceId];
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

    const inheritedAdmin = {
      kind: "service-account",
      id: "inherited-admin",
      accessRoles: Object.create({ [ORG_WIDE_NAMESPACE]: ["admin"] }) as Record<
        string,
        ReadonlyArray<string>
      >,
      canActFor: false,
    } satisfies Principal;
    const inheritedAdminResponse = await handleAs({
      env,
      principal: inheritedAdmin,
      path: "/api/access/users",
    });
    expect(inheritedAdminResponse.status).toBe(403);
    expect(await inheritedAdminResponse.json()).toEqual({ error: "forbidden" });

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
    expect(
      (await env.security.resolvePrincipal({ kind: "user", id: user.id }))?.accessRoles,
    ).toEqual({
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
    expect(
      (await env.security.resolvePrincipal({ kind: "user", id: user.id }))?.accessRoles,
    ).toEqual({});
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

  it("invalid namespace bodies answer 400 with structured issues; org remains valid", async () => {
    const env = environment();
    const adminUser = await createUser({
      env,
      email: "admin@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "admin" }],
    });
    const user = await createUser({ env, email: "user@example.com" });
    const admin = await principal(env, adminUser);

    for (const request of [
      {
        method: "PUT",
        path: `/api/access/users/${user.id}/bindings`,
        body: { namespaceId: "_private", accessRole: "member" },
      },
      {
        method: "DELETE",
        path: `/api/access/users/${user.id}/bindings`,
        body: { namespaceId: "bad namespace", accessRole: "member" },
      },
      {
        method: "PUT",
        path: "/api/access/knowledge-grants",
        body: { namespaceId: "__proto__", baseId: "docs", accessRole: "member" },
      },
      {
        method: "DELETE",
        path: "/api/access/knowledge-grants",
        body: { namespaceId: "/root", baseId: "docs", accessRole: "member" },
      },
    ]) {
      const response = await handleAs({ env, principal: admin, ...request });
      expect(response.status).toBe(400);
      expect(await response.json()).toMatchObject({
        error: "invalid_request",
        issues: [{ path: "namespaceId" }],
      });
    }

    const orgWide = await handleAs({
      env,
      principal: admin,
      method: "PUT",
      path: "/api/access/knowledge-grants",
      body: { namespaceId: ORG_WIDE_NAMESPACE, baseId: "docs", accessRole: "member" },
    });
    expect(orgWide.status).toBe(204);

    const prototypeNamed = await handleAs({
      env,
      principal: admin,
      method: "PUT",
      path: `/api/access/users/${user.id}/bindings`,
      body: { namespaceId: "constructor", accessRole: "builder" },
    });
    expect(prototypeNamed.status).toBe(204);
    expect(
      rolesAt(await env.security.resolvePrincipal({ kind: "user", id: user.id }), "constructor"),
    ).toEqual(["builder"]);
  });

  it("a knowledge-manager grants only inside the bound namespace", async () => {
    const env = environment();
    const user = await createUser({
      env,
      email: "manager@example.com",
      bindings: [{ namespaceId: "team-a", accessRole: "knowledge-manager" }],
    });
    const manager = await principal(env, user);
    const allowed = await handleAs({
      env,
      principal: manager,
      method: "PUT",
      path: "/api/access/knowledge-grants",
      body: { namespaceId: "team-a", baseId: "docs", accessRole: "member" },
    });
    expect(allowed.status).toBe(204);
    const denied = await handleAs({
      env,
      principal: manager,
      method: "PUT",
      path: "/api/access/knowledge-grants",
      body: { namespaceId: "team-b", baseId: "docs", accessRole: "member" },
    });
    expect(denied.status).toBe(403);
    expect(await env.store.listKnowledgeGrants({ namespaceId: "team-b", baseId: "docs" })).toEqual(
      [],
    );
  });
});

describe("namespace and knowledge access", () => {
  it("revalidates stale roles and disabled users on every authorization", async () => {
    const env = environment();
    const user = await createUser({
      env,
      email: "stale@example.com",
      bindings: [{ namespaceId: "team-a", accessRole: "member" }],
    });
    const stale = await principal(env, user);
    const request = {
      context: { actor: stale, effective: principalRef(stale) },
      permission: { resource: "agent", action: "invoke" },
      resource: { kind: "agent", namespaceId: "team-a" },
    } as const;

    expect(await env.security.authorize(request)).toEqual({
      allowed: true,
      reason: "access_role_allow",
    });
    await env.store.removeBinding({
      userId: user.id,
      namespaceId: "team-a",
      accessRole: "member",
    });
    expect(await env.security.authorize(request)).toEqual({
      allowed: false,
      reason: "namespace_denied",
    });
    await env.store.setUserStatus({ id: user.id, status: "disabled" });
    expect(await env.security.authorize(request)).toEqual({
      allowed: false,
      reason: "principal_disabled",
    });
  });

  it("denies mismatched permission and resource kinds before role evaluation", async () => {
    const env = environment();
    const user = await createUser({
      env,
      email: "admin@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "admin" }],
    });
    const admin = await principal(env, user);
    expect(
      await env.security.authorize({
        context: { actor: admin, effective: principalRef(admin) },
        permission: { resource: "agent", action: "invoke" },
        resource: { kind: "knowledge", namespaceId: "team-a", id: "docs" },
      } as unknown as AuthorizationRequest),
    ).toEqual({ allowed: false, reason: "permission_denied" });
  });

  it("revalidates the actor and its current act-for authority", async () => {
    const account = {
      id: "channel",
      token: "channel-token",
      canActFor: true,
      accessRoles: { "team-a": ["member"] },
    };
    const env = environment({ serviceAccounts: [account] });
    const user = await createUser({
      env,
      email: "represented@example.com",
      bindings: [{ namespaceId: "team-a", accessRole: "member" }],
    });
    const effective = await principal(env, user);
    const actor = await env.security.resolvePrincipal({ kind: "service-account", id: account.id });
    if (!actor) throw new Error("configured service account did not resolve");
    const context = await env.security.resolveIdentity({
      actor,
      asserted: principalRef(effective),
    });
    if (!context) throw new Error("configured act-for identity did not resolve");
    const request = {
      context,
      permission: { resource: "agent", action: "invoke" },
      resource: { kind: "agent", namespaceId: "team-a" },
    } as const;

    expect(await env.security.authorize(request)).toEqual({
      allowed: true,
      reason: "access_role_allow",
    });
    account.canActFor = false;
    expect(
      await env.security.resolveIdentity({ actor, asserted: principalRef(effective) }),
    ).toBeUndefined();
    expect(await env.security.authorize(request)).toEqual({
      allowed: false,
      reason: "act_for_required",
    });

    const missingActor: Principal = {
      kind: "service-account",
      id: "removed-channel",
      accessRoles: { "team-a": ["admin"] },
      canActFor: true,
    };
    expect(
      await env.security.authorize({
        ...request,
        context: { actor: missingActor, effective: principalRef(effective) },
      }),
    ).toEqual({ allowed: false, reason: "identity_unknown" });
  });

  it("principal construction safely preserves stored prototype-like namespace keys", async () => {
    const env = environment();
    const user = await createUser({
      env,
      email: "legacy@example.com",
      bindings: [
        { namespaceId: "constructor", accessRole: "build" },
        { namespaceId: "__proto__", accessRole: "legacy" },
        { namespaceId: "toString", accessRole: "view" },
      ],
    });
    const byId = await principal(env, user);

    expect(Object.getPrototypeOf(byId.accessRoles)).toBeNull();
    expect(rolesAt(byId, "constructor")).toEqual(["build"]);
    expect(rolesAt(byId, "__proto__")).toEqual(["legacy"]);
    expect(rolesAt(byId, "toString")).toEqual(["view"]);
    for (const namespaceId of ["constructor", "__proto__", "toString"]) {
      expect(await canInvokeAgent(env, byId, namespaceId)).toBeTrue();
    }

    const token = "prototype-namespace-session";
    await env.store.createSession({
      tokenHash: hashSessionToken(token),
      userId: user.id,
      ttlMs: 60_000,
    });
    const authenticated = await authenticate(
      env,
      new Request(`${BASE_URL}/api/agents`, {
        headers: { cookie: `zorya_session=${token}` },
      }),
    );
    expect(authenticated?.id).toBe(user.id);
    expect(Object.getPrototypeOf(authenticated?.accessRoles)).toBeNull();
    expect(rolesAt(authenticated, "constructor")).toEqual(["build"]);
    expect(rolesAt(authenticated, "__proto__")).toEqual(["legacy"]);
    expect(rolesAt(authenticated, "toString")).toEqual(["view"]);
  });

  it("authorization never treats inherited prototype properties as roles", async () => {
    const ownAccessRoles = JSON.parse(
      '{"constructor":["build"],"__proto__":["legacy"],"toString":["view"]}',
    ) as Record<string, ReadonlyArray<string>>;
    const env = environment({
      serviceAccounts: [
        { id: "plain-object", token: "plain-token", accessRoles: {} },
        { id: "own-properties", token: "own-token", accessRoles: ownAccessRoles },
      ],
    });
    const inherited: Principal = {
      kind: "service-account",
      id: "plain-object",
      accessRoles: {},
      canActFor: false,
    };
    for (const namespaceId of ["constructor", "__proto__", "toString"]) {
      expect(await canInvokeAgent(env, inherited, namespaceId)).toBeFalse();
      expect(await canUseKnowledgeBase(env, inherited, namespaceId, "docs")).toBeFalse();
    }

    const own: Principal = {
      ...inherited,
      id: "own-properties",
      accessRoles: ownAccessRoles,
    };
    for (const namespaceId of ["constructor", "__proto__", "toString"]) {
      expect(await canInvokeAgent(env, own, namespaceId)).toBeTrue();
    }
  });

  it("agent.invoke: bound namespace and org-wide are allowed, others are not", async () => {
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

    expect(await canInvokeAgent(env, scoped, "team-a")).toBeTrue();
    expect(await canInvokeAgent(env, scoped, "team-b")).toBeFalse();
    expect(await canInvokeAgent(env, org, "team-b")).toBeTrue();
  });

  it("knowledge.use: no grant means no access for a member", async () => {
    const env = environment();
    const user = await createUser({
      env,
      email: "member@example.com",
      bindings: [{ namespaceId: "team-a", accessRole: "member" }],
    });
    expect(
      await canUseKnowledgeBase(env, await principal(env, user), "team-a", "docs"),
    ).toBeFalse();
  });

  it("knowledge.use: admin is unrestricted; knowledge-manager still needs a text grant", async () => {
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

    expect(await canUseKnowledgeBase(env, admin, "any-team", "ungranted")).toBeTrue();
    expect(await canUseKnowledgeBase(env, manager, "team-a", "ungranted")).toBeFalse();
    await env.store.putKnowledgeGrant({
      namespaceId: "team-a",
      baseId: "docs",
      accessRole: "knowledge-manager",
    });
    expect(await canUseKnowledgeBase(env, manager, "team-a", "docs")).toBeTrue();
    expect(await canUseKnowledgeBase(env, manager, "team-b", "ungranted")).toBeFalse();
  });

  it("knowledge-manager may manage knowledge only in the bound namespace", async () => {
    const env = environment();
    const managerUser = await createUser({
      env,
      email: "manager@example.com",
      bindings: [{ namespaceId: "team-a", accessRole: "knowledge-manager" }],
    });
    const unboundUser = await createUser({ env, email: "unbound@example.com" });
    const manager = await principal(env, managerUser);
    const unbound = await principal(env, unboundUser);
    for (const action of ["read", "create", "update", "delete", "grant"] as const) {
      const permission = { resource: "knowledge" as const, action };
      expect(
        await env.security.authorize({
          context: { actor: manager, effective: principalRef(manager) },
          permission,
          resource: { kind: "knowledge", namespaceId: "team-a", id: "docs" },
        }),
      ).toEqual({ allowed: true, reason: "access_role_allow" });
      expect(
        await env.security.authorize({
          context: { actor: manager, effective: principalRef(manager) },
          permission,
          resource: { kind: "knowledge", namespaceId: "team-b", id: "docs" },
        }),
      ).toEqual({ allowed: false, reason: "permission_denied" });
      expect(
        await env.security.authorize({
          context: { actor: unbound, effective: principalRef(unbound) },
          permission,
          resource: { kind: "knowledge", namespaceId: "team-a", id: "docs" },
        }),
      ).toEqual({ allowed: false, reason: "permission_denied" });
    }
    for (const action of ["use", "inspect-text"] as const) {
      expect(
        await env.security.authorize({
          context: { actor: manager, effective: principalRef(manager) },
          permission: { resource: "knowledge", action },
          resource: { kind: "knowledge", namespaceId: "team-a", id: "docs" },
        }),
      ).toEqual({ allowed: false, reason: "permission_denied" });
    }
  });

  it("agent-manager and auditor follow the built-in route permission matrix", async () => {
    const env = environment();
    const managerUser = await createUser({
      env,
      email: "agents@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "agent-manager" }],
    });
    const auditorUser = await createUser({
      env,
      email: "auditor@example.com",
      bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: "auditor" }],
    });
    const manager = await principal(env, managerUser);
    const auditor = await principal(env, auditorUser);
    const authorize = (value: Principal, permission: AuthorizationRequest["permission"]) =>
      env.security.authorize({
        context: { actor: value, effective: principalRef(value) },
        permission,
        resource: { kind: permission.resource, namespaceId: ORG_WIDE_NAMESPACE } as never,
      } as AuthorizationRequest);

    expect(await authorize(manager, { resource: "skill", action: "update" })).toEqual({
      allowed: true,
      reason: "access_role_allow",
    });
    expect(await authorize(manager, { resource: "namespace", action: "update" })).toEqual({
      allowed: false,
      reason: "permission_denied",
    });
    expect(await authorize(auditor, { resource: "workflow", action: "read" })).toEqual({
      allowed: true,
      reason: "access_role_allow",
    });
    expect(await authorize(auditor, { resource: "workflow", action: "update" })).toEqual({
      allowed: false,
      reason: "permission_denied",
    });
    expect(await authorize(auditor, { resource: "secret", action: "list" })).toEqual({
      allowed: false,
      reason: "permission_denied",
    });
  });

  it("knowledge.use: granting a caller role allows it; revoking denies the next call", async () => {
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

    expect(await canUseKnowledgeBase(env, member, "team-a", "docs")).toBeFalse();
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
    expect(await canUseKnowledgeBase(env, member, "team-a", "docs")).toBeTrue();
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
    expect(await canUseKnowledgeBase(env, member, "team-a", "docs")).toBeFalse();
  });

  it("knowledge.use: an org-wide grant applies in every namespace", async () => {
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

    expect(await canUseKnowledgeBase(env, member, "team-a", "handbook")).toBeTrue();
    expect(await canUseKnowledgeBase(env, member, "team-b", "handbook")).toBeTrue();
    expect(await canUseKnowledgeBase(env, member, "team-c", "handbook")).toBeFalse();
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
      authenticate(
        env,
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
    expect(await env.security.resolvePrincipal({ kind: "user", id: user.id })).toBeUndefined();
  });
});
