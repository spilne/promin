import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { Auth } from "../../server/auth.ts";
import { Router } from "../../server/router.ts";
import {
  OPERATOR_PRINCIPAL,
  WORKER_PRINCIPAL,
  createApiKeySecurity,
  createSecurityRouteAccessGuard,
  principalRef,
  setRequestPrincipal,
  type AuthorizationDecision,
  type AuthorizationRequest,
  type Principal,
  type SecurityAuditEvent,
  type SecurityAuditSink,
  type SecurityContext,
  type SecurityPermission,
  type SecurityResourceRef,
} from "../../server/security/index.ts";
import {
  securityConformanceSuite,
  selfContext,
  type SecurityConformanceFactory,
} from "../../server/security/tests/security-conformance-suite.ts";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import type { DashboardLoginProvider } from "../../server/security/index.ts";
import { DurableSecurityAudit } from "../security-audit.ts";
import { createZoryaSecurity } from "../zorya-security.ts";

const SECRET = "0123456789abcdef0123456789abcdef";
const SERVICE_TOKEN = "channel-token";

const unusedLoginProvider: DashboardLoginProvider = {
  id: "test",
  authorizationUrl: async () => "http://identity.test/authorize",
  exchange: async () => {
    throw new Error("not used");
  },
};

const apiKeyFactory: SecurityConformanceFactory = {
  create: async (params = {}) => {
    const auditEvents: SecurityAuditEvent[] = [];
    const security = createApiKeySecurity({
      operatorAuth: new Auth({ apiKeys: ["operator-key"] }),
      workerAuth: new Auth({ apiKeys: ["worker-key"] }),
      logger: {
        log(message) {
          if (params.auditFailure) throw params.auditFailure;
          const parsed = JSON.parse(message) as SecurityAuditEvent & { component?: string };
          const { component: _component, ...event } = parsed;
          auditEvents.push(event);
        },
      },
    });
    return {
      security,
      recordsAllowedAudit: false,
      validRequest: bearerRequest("operator-key"),
      invalidRequest: bearerRequest("invalid"),
      authenticated: principalRef(OPERATOR_PRINCIPAL),
      actor: OPERATOR_PRINCIPAL,
      nonActingPrincipal: WORKER_PRINCIPAL,
      asserted: { kind: "user", id: "represented-user" },
      allowed: {
        context: selfContext(OPERATOR_PRINCIPAL),
        permission: { resource: "agent", action: "invoke" },
        resource: { kind: "agent", namespaceId: "team-a", id: "assistant" },
      },
      allowedReason: "operator_allow",
      allowedEffective: principalRef(OPERATOR_PRINCIPAL),
      denied: {
        context: selfContext(WORKER_PRINCIPAL),
        permission: { resource: "agent", action: "invoke" },
        resource: { kind: "agent", namespaceId: "team-a", id: "assistant" },
      },
      deniedReason: "wrong_plane",
      permissionMatrix: [
        permissionCase(
          "agent invoke",
          selfContext(OPERATOR_PRINCIPAL),
          { resource: "agent", action: "invoke" },
          { kind: "agent", namespaceId: "team-a", id: "assistant" },
          { allowed: true, reason: "operator_allow" },
        ),
        permissionCase(
          "agent point read",
          selfContext(OPERATOR_PRINCIPAL),
          { resource: "agent", action: "read" },
          { kind: "agent", namespaceId: "team-a", id: "assistant" },
          { allowed: true, reason: "operator_allow" },
        ),
        permissionCase(
          "agent list",
          selfContext(OPERATOR_PRINCIPAL),
          { resource: "agent", action: "read" },
          { kind: "agent", namespaceId: "team-a" },
          { allowed: true, reason: "operator_allow" },
        ),
        permissionCase(
          "knowledge use",
          selfContext(OPERATOR_PRINCIPAL),
          { resource: "knowledge", action: "use" },
          { kind: "knowledge", namespaceId: "team-a", id: "docs" },
          { allowed: true, reason: "operator_allow" },
        ),
        permissionCase(
          "ordinary approval",
          selfContext(OPERATOR_PRINCIPAL),
          { resource: "approval", action: "decide" },
          {
            kind: "approval",
            namespaceId: "team-a",
            id: "approval-1",
            principal: principalRef(OPERATOR_PRINCIPAL),
          },
          { allowed: true, reason: "operator_allow" },
        ),
        permissionCase(
          "elevated approval",
          selfContext(OPERATOR_PRINCIPAL),
          { resource: "approval", action: "decide-elevated" },
          { kind: "approval", namespaceId: "team-a", id: "approval-1" },
          { allowed: true, reason: "operator_allow" },
        ),
      ],
      auditEvents,
    };
  },
};

const moduleFactory: SecurityConformanceFactory = {
  create: async (params = {}) => {
    const store = new InMemorySecurityStore();
    const represented = (
      await store.findOrCreateProviderUser({
        identity: { providerId: "test", issuer: "https://id.test", subject: "represented" },
        email: "represented@example.com",
        bindings: [{ namespaceId: "team-a", accessRole: "member" }],
      })
    ).user;
    const unbound = (
      await store.findOrCreateProviderUser({
        identity: { providerId: "test", issuer: "https://id.test", subject: "unbound" },
        email: "unbound@example.com",
      })
    ).user;
    await store.putKnowledgeGrant({
      namespaceId: "team-a",
      baseId: "docs",
      accessRole: "member",
    });
    const auditEvents: SecurityAuditEvent[] = [];
    const security = createZoryaSecurity({
      store,
      loginProviders: [unusedLoginProvider],
      publicBaseUrl: "http://zorya.test",
      allowInsecureHttp: true,
      allowedDomains: ["example.com"],
      serviceAccounts: [
        {
          id: "channel",
          token: SERVICE_TOKEN,
          canActFor: true,
          accessRoles: { "team-a": ["member"] },
        },
      ],
      auditSink: {
        recordAudit: async (event) => {
          if (params.auditFailure) throw params.auditFailure;
          auditEvents.push(event);
        },
      },
      secret: SECRET,
    });
    const actor = await requiredPrincipal(security, { kind: "service-account", id: "channel" });
    const effective = await requiredPrincipal(security, { kind: "user", id: represented.id });
    const denied = await requiredPrincipal(security, { kind: "user", id: unbound.id });
    return {
      security,
      validRequest: bearerRequest(SERVICE_TOKEN),
      invalidRequest: bearerRequest("invalid"),
      authenticated: principalRef(actor),
      actor,
      nonActingPrincipal: denied,
      asserted: principalRef(effective),
      allowed: {
        context: { actor, effective: principalRef(effective) },
        permission: { resource: "agent", action: "invoke" },
        resource: { kind: "agent", namespaceId: "team-a", id: "assistant" },
      },
      allowedReason: "access_role_allow",
      allowedEffective: principalRef(effective),
      denied: {
        context: selfContext(denied),
        permission: { resource: "agent", action: "invoke" },
        resource: { kind: "agent", namespaceId: "team-a", id: "assistant" },
      },
      deniedReason: "namespace_denied",
      permissionMatrix: [
        permissionCase(
          "agent invoke",
          { actor, effective: principalRef(effective) },
          { resource: "agent", action: "invoke" },
          { kind: "agent", namespaceId: "team-a", id: "assistant" },
          { allowed: true, reason: "access_role_allow" },
        ),
        permissionCase(
          "agent point read",
          { actor, effective: principalRef(effective) },
          { resource: "agent", action: "read" },
          { kind: "agent", namespaceId: "team-a", id: "assistant" },
          { allowed: true, reason: "self_identity_allow" },
        ),
        permissionCase(
          "agent list",
          { actor, effective: principalRef(effective) },
          { resource: "agent", action: "read" },
          { kind: "agent", namespaceId: "team-a" },
          { allowed: true, reason: "self_identity_allow" },
        ),
        permissionCase(
          "knowledge use",
          { actor, effective: principalRef(effective) },
          { resource: "knowledge", action: "use" },
          { kind: "knowledge", namespaceId: "team-a", id: "docs" },
          { allowed: true, reason: "knowledge_grant_allow" },
        ),
        permissionCase(
          "ordinary approval",
          { actor, effective: principalRef(effective) },
          { resource: "approval", action: "decide" },
          {
            kind: "approval",
            namespaceId: "team-a",
            id: "approval-1",
            principal: principalRef(actor),
          },
          { allowed: true, reason: "self_identity_allow" },
        ),
        permissionCase(
          "elevated approval",
          { actor, effective: principalRef(effective) },
          { resource: "approval", action: "decide-elevated" },
          { kind: "approval", namespaceId: "team-a", id: "approval-1" },
          { allowed: false, reason: "permission_denied" },
        ),
        permissionCase(
          "member run trigger",
          { actor, effective: principalRef(effective) },
          { resource: "run", action: "trigger" },
          { kind: "run", namespaceId: "team-a", id: "other-run" },
          { allowed: true, reason: "self_identity_allow" },
        ),
        permissionCase(
          "member run administration",
          { actor, effective: principalRef(effective) },
          { resource: "run", action: "cancel" },
          { kind: "run", namespaceId: "team-a", id: "other-run" },
          { allowed: false, reason: "permission_denied" },
        ),
      ],
      auditEvents,
      revokeAllowedIdentity: async () =>
        store.setUserStatus({ id: represented.id, status: "disabled" }),
    };
  },
};

securityConformanceSuite("API-key adapter", apiKeyFactory);
securityConformanceSuite("optional security module", moduleFactory);

describe("optional security module audit sink", () => {
  it("requires an audit sink instead of silently discarding events", () => {
    expect(() =>
      createZoryaSecurity({
        store: new InMemorySecurityStore(),
        loginProviders: [unusedLoginProvider],
        publicBaseUrl: "http://zorya.test",
        allowInsecureHttp: true,
        allowedDomains: ["example.com"],
        secret: SECRET,
      } as never),
    ).toThrow(/auditSink is required/);
  });

  it("detaches and freezes forwarded audit records", async () => {
    let retained: SecurityAuditEvent | undefined;
    const security = createZoryaSecurity({
      store: new InMemorySecurityStore(),
      loginProviders: [unusedLoginProvider],
      publicBaseUrl: "http://zorya.test",
      allowInsecureHttp: true,
      allowedDomains: ["example.com"],
      auditSink: { recordAudit: async (event) => void (retained = event) },
      secret: SECRET,
    });
    const roles = ["member"];
    const metadata = { source: "test" };
    await security.recordAudit({
      event: "authorization",
      occurredAt: "2026-11-16T00:00:00.000Z",
      context: {
        actor: {
          kind: "user",
          id: "alice",
          accessRoles: { "team-a": roles },
          canActFor: false,
        },
        effective: { kind: "user", id: "alice" },
      },
      namespaceId: "team-a",
      decision: { allowed: true, reason: "self_identity_allow" },
      metadata,
    });
    roles.push("admin");
    metadata.source = "mutated";

    expect(retained?.context?.actor.accessRoles["team-a"]).toEqual(["member"]);
    expect(retained?.metadata).toEqual({ source: "test" });
    expect(Object.isFrozen(retained)).toBeTrue();
    expect(Object.isFrozen(retained?.context?.actor.accessRoles["team-a"])).toBeTrue();
    expect(Object.isFrozen(retained?.metadata)).toBeTrue();
  });

  it("bounds denial emission in the security adapter and separates resource buckets", async () => {
    const clock = FakeWallClock.create("2026-11-16T00:00:00.000Z");
    const events: SecurityAuditEvent[] = [];
    const security = createZoryaSecurity({
      store: new InMemorySecurityStore({ clock }),
      loginProviders: [unusedLoginProvider],
      publicBaseUrl: "http://zorya.test",
      allowInsecureHttp: true,
      allowedDomains: ["example.com"],
      auditSink: { recordAudit: async (event) => void events.push(event) },
      denialAudit: { windowMs: 100, emitLimit: 1 },
      clock,
      secret: SECRET,
    });
    const denied = {
      event: "authorization",
      occurredAt: clock.now().toISOString(),
      namespaceId: "team-a",
      context: selfContext(OPERATOR_PRINCIPAL),
      permission: { resource: "agent", action: "invoke" },
      resource: { kind: "agent", namespaceId: "team-a", id: "agent-1" },
      decision: { allowed: false, reason: "permission_denied" },
    } as const satisfies SecurityAuditEvent;

    await security.recordAudit(denied);
    await security.recordAudit(denied);
    await security.recordAudit(denied);
    expect(events).toHaveLength(1);
    await security.recordAudit({
      ...denied,
      resource: { ...denied.resource, id: "agent-2" },
    });
    expect(events).toHaveLength(2);

    clock.advance(100);
    await security.recordAudit({ ...denied, occurredAt: clock.now().toISOString() });
    expect(events.map((event) => event.event)).toEqual([
      "authorization",
      "authorization",
      "authorization.denial-suppressed",
      "authorization",
    ]);
    expect(events[2]?.metadata).toMatchObject({
      totalCount: 3,
      detailedCount: 1,
      suppressedCount: 2,
    });
  });

  it("starts and stops configured durable outbox delivery with the security lifecycle", async () => {
    const clock = FakeWallClock.create(1_000);
    const store = new InMemorySecurityStore({ clock });
    const audit = new DurableSecurityAudit({
      store,
      activeKeyId: "key",
      keys: { key: "a".repeat(32) },
      clock,
      idFactory: () => "lifecycle-event",
      outbox: {},
    });
    const delivered: string[] = [];
    const security = createZoryaSecurity({
      store,
      loginProviders: [unusedLoginProvider],
      publicBaseUrl: "http://zorya.test",
      allowInsecureHttp: true,
      allowedDomains: ["example.com"],
      auditSink: audit,
      auditOutbox: {
        owner: "security-1",
        destination: {
          deliver: async ({ idempotencyKey }) => void delivered.push(idempotencyKey),
        },
      },
      clock,
      secret: SECRET,
    });
    await security.recordAudit({
      event: "server.started",
      occurredAt: clock.now().toISOString(),
      namespaceId: "org",
    });

    await security.start?.();
    await security.stop?.();
    expect(delivered).toEqual(["lifecycle-event"]);
    expect(await store.getAuditOutbox("lifecycle-event")).toMatchObject({ status: "delivered" });
  });

  it("snapshots the sink method and event fields once", async () => {
    const events: SecurityAuditEvent[] = [];
    let sinkReads = 0;
    const sink = Object.defineProperty({}, "recordAudit", {
      get() {
        sinkReads += 1;
        if (sinkReads > 1) throw new Error("audit sink method reread");
        return async (event: SecurityAuditEvent) => void events.push(event);
      },
    }) as SecurityAuditSink;
    const security = createZoryaSecurity({
      store: new InMemorySecurityStore(),
      loginProviders: [unusedLoginProvider],
      publicBaseUrl: "http://zorya.test",
      allowInsecureHttp: true,
      allowedDomains: ["example.com"],
      auditSink: sink,
      secret: SECRET,
    });
    const reads = new Map<string, number>();
    const once = <T>(field: string, value: T): T => {
      const count = (reads.get(field) ?? 0) + 1;
      reads.set(field, count);
      if (count > 1) throw new Error(`audit ${field} reread`);
      return value;
    };
    let permissionAction = "read";
    let resourceId = "assistant";
    let principalId = "alice";
    const permission = {
      get resource() {
        return once("permission.resource", "agent");
      },
      get action() {
        return once("permission.action", permissionAction);
      },
    } as SecurityPermission;
    const principal = {
      get kind() {
        return once("resource.principal.kind", "user");
      },
      get id() {
        return once("resource.principal.id", principalId);
      },
    };
    const resource = {
      get kind() {
        return once("resource.kind", "agent");
      },
      get namespaceId() {
        return once("resource.namespaceId", "team-a");
      },
      get id() {
        return once("resource.id", resourceId);
      },
      get principal() {
        return once("resource.principal", principal);
      },
    } as SecurityResourceRef;
    const source = {
      get event() {
        return once("event", "authorization");
      },
      get occurredAt() {
        return once("occurredAt", "2026-11-16T00:00:00.000Z");
      },
      get context() {
        return once("context", undefined);
      },
      get namespaceId() {
        return once("namespaceId", "team-a");
      },
      get resource() {
        return once("resource", resource);
      },
      get permission() {
        return once("permission", permission);
      },
      get decision() {
        return once("decision", undefined);
      },
      get metadata() {
        return once("metadata", undefined);
      },
    } as SecurityAuditEvent;

    await security.recordAudit(source);
    permissionAction = "create";
    resourceId = "mutated";
    principalId = "mallory";
    expect(sinkReads).toBe(1);
    expect([...reads.values()].every((count) => count === 1)).toBeTrue();
    expect(events).toHaveLength(1);
    expect(events[0]?.permission).toEqual({ resource: "agent", action: "read" });
    expect(events[0]?.resource).toEqual({
      kind: "agent",
      namespaceId: "team-a",
      id: "assistant",
      principal: { kind: "user", id: "alice" },
    });
    expect(Object.isFrozen(events[0]?.permission)).toBeTrue();
    expect(Object.isFrozen(events[0]?.resource)).toBeTrue();
    expect(Object.isFrozen(events[0]?.resource?.principal)).toBeTrue();
  });

  it("rejects malformed or uncorrelated audit permission and resource fields before the sink", async () => {
    let sinkCalls = 0;
    const security = createZoryaSecurity({
      store: new InMemorySecurityStore(),
      loginProviders: [unusedLoginProvider],
      publicBaseUrl: "http://zorya.test",
      allowInsecureHttp: true,
      allowedDomains: ["example.com"],
      auditSink: { recordAudit: async () => void (sinkCalls += 1) },
      secret: SECRET,
    });
    const base = {
      event: "authorization",
      occurredAt: "2026-11-16T00:00:00.000Z",
      namespaceId: "team-a",
    };
    const malformed = [
      { permission: { resource: "agent", action: "unknown" } },
      { permission: { resource: "unknown", action: "read" } },
      { resource: { kind: "unknown", namespaceId: "team-a" } },
      { resource: { kind: "agent", namespaceId: "" } },
      { resource: { kind: "agent", namespaceId: "team-a", id: "" } },
      { resource: { kind: "agent", namespaceId: "team-a", id: 42 } },
      {
        resource: {
          kind: "agent",
          namespaceId: "team-a",
          principal: { kind: "unknown", id: "alice" },
        },
      },
      {
        resource: {
          kind: "agent",
          namespaceId: "team-a",
          principal: { kind: "user", id: "" },
        },
      },
      {
        permission: { resource: "agent", action: "read" },
        resource: { kind: "knowledge", namespaceId: "team-a", id: "docs" },
      },
    ];

    for (const fields of malformed) {
      await expect(
        security.recordAudit({ ...base, ...fields } as unknown as SecurityAuditEvent),
      ).rejects.toBeInstanceOf(TypeError);
    }
    expect(sinkCalls).toBe(0);
  });

  it("fails closed for allowed audit failure and preserves denial", async () => {
    const store = new InMemorySecurityStore();
    const adminUser = (
      await store.findOrCreateProviderUser({
        identity: { providerId: "test", issuer: "https://id.test", subject: "admin" },
        email: "admin@example.com",
        bindings: [{ namespaceId: "team-a", accessRole: "admin" }],
      })
    ).user;
    const deniedUser = (
      await store.findOrCreateProviderUser({
        identity: { providerId: "test", issuer: "https://id.test", subject: "denied" },
        email: "denied@example.com",
      })
    ).user;
    const security = createZoryaSecurity({
      store,
      loginProviders: [unusedLoginProvider],
      publicBaseUrl: "http://zorya.test",
      allowInsecureHttp: true,
      allowedDomains: ["example.com"],
      auditSink: {
        recordAudit: async () => {
          throw new Error("audit unavailable");
        },
      },
      secret: SECRET,
    });
    let handled = 0;
    const router = new Router({
      logger: { error() {} },
      accessGuard: createSecurityRouteAccessGuard({
        security,
        clock: { now: () => new Date("2026-11-16T00:00:00.000Z") },
        logger: { error() {} },
      }),
    }).route({
      method: "POST",
      path: "/api/agents/:id/invoke",
      access: {
        kind: "protected",
        permission: { resource: "agent", action: "invoke" },
        resolveTarget: ({ params }) => ({
          resource: { kind: "agent", namespaceId: "team-a", id: params.id },
        }),
      },
      handler: () => {
        handled += 1;
        return new Response(null, { status: 204 });
      },
    });

    const admin = await requiredPrincipal(security, { kind: "user", id: adminUser.id });
    const allowed = new Request("http://zorya.test/api/agents/helper/invoke", { method: "POST" });
    setRequestPrincipal({ req: allowed, principal: admin });
    expect((await router.handle(allowed)).status).toBe(503);

    const deniedPrincipal = await requiredPrincipal(security, {
      kind: "user",
      id: deniedUser.id,
    });
    const denied = new Request("http://zorya.test/api/agents/helper/invoke", { method: "POST" });
    setRequestPrincipal({ req: denied, principal: deniedPrincipal });
    expect((await router.handle(denied)).status).toBe(403);
    expect(handled).toBe(0);
  });
});

function bearerRequest(token: string): Request {
  return new Request("http://zorya.test/api/agents", {
    headers: { authorization: `Bearer ${token}` },
  });
}

async function requiredPrincipal(
  security: ReturnType<typeof createZoryaSecurity>,
  ref: { readonly kind: "user" | "service-account"; readonly id: string },
): Promise<Principal> {
  const value = await security.resolvePrincipal(ref);
  if (!value) throw new Error(`missing principal ${ref.kind}:${ref.id}`);
  return value;
}

function permissionCase(
  name: string,
  context: SecurityContext,
  permission: SecurityPermission,
  resource: SecurityResourceRef,
  expected: AuthorizationDecision,
): {
  readonly name: string;
  readonly request: AuthorizationRequest;
  readonly expected: AuthorizationDecision;
} {
  return {
    name,
    request: { context, permission, resource } as unknown as AuthorizationRequest,
    expected,
  };
}
