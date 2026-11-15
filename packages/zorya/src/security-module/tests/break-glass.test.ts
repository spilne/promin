import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { Router } from "../../server/router.ts";
import { ORG_WIDE_NAMESPACE } from "../../server/security/security.ts";
import { BreakGlassAccessManager, breakGlassGrantIdFromToken } from "../break-glass.ts";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import { DurableSecurityAudit } from "../security-audit.ts";
import { CSRF_HEADER, hashSessionToken } from "../sessions.ts";
import { createZoryaSecurity } from "../zorya-security.ts";

const BASE_URL = "http://zorya.test";
const SECRET = "0123456789abcdef0123456789abcdef";
const AUDIT_KEY = "audit-key-material-that-is-at-least-32-bytes";
const TOKEN_SECRET = "s".repeat(43);

async function fixture(role = "admin") {
  const clock = FakeWallClock.create(1_000);
  const store = new InMemorySecurityStore({ clock });
  const { user } = await store.findOrCreateProviderUser({
    identity: { providerId: "test", issuer: "https://identity.test", subject: "alice" },
    email: "alice@example.com",
    bindings: [{ namespaceId: ORG_WIDE_NAMESPACE, accessRole: role }],
  });
  let nextAuditId = 0;
  const audit = new DurableSecurityAudit({
    store,
    activeKeyId: "key-1",
    keys: { "key-1": AUDIT_KEY },
    clock,
    idFactory: () => `audit-${++nextAuditId}`,
  });
  const manager = new BreakGlassAccessManager({
    store,
    audit,
    publicBaseUrl: BASE_URL,
    allowInsecureHttp: true,
    clock,
    idFactory: () => "grant_1",
    tokenSecretFactory: () => TOKEN_SECRET,
  });
  const security = (sessionSseMaxAgeMs?: number) =>
    createZoryaSecurity({
      store,
      loginProviders: [
        {
          id: "unused",
          authorizationUrl: async () => "https://identity.test/authorize",
          exchange: async () => {
            throw new Error("not used");
          },
        },
      ],
      publicBaseUrl: BASE_URL,
      allowInsecureHttp: true,
      allowedDomains: ["example.com"],
      auditSink: audit,
      secret: SECRET,
      ...(sessionSseMaxAgeMs !== undefined && { sessionSseMaxAgeMs }),
      clock,
    });
  return { clock, store, user, audit, manager, security };
}

describe("host-local break-glass access", () => {
  it("issues hash-only access and authenticates bearer and CSRF-bound cookie requests", async () => {
    const env = await fixture();
    const issued = await env.manager.issue({
      principal: { kind: "user", id: env.user.id },
      reason: "identity provider outage",
      createdBy: "host-operator",
    });
    expect(breakGlassGrantIdFromToken(issued.token)).toBe(issued.grantId);
    expect(issued.authorizationHeader).toBe(`Bearer ${issued.token}`);
    expect(issued.setCookieHeaders).toHaveLength(2);

    const grant = await env.store.getBreakGlassGrant(issued.grantId);
    expect(grant).toMatchObject({
      principal: { kind: "user", id: env.user.id },
      reason: "identity provider outage",
      createdBy: "host-operator",
      tokenHash: hashSessionToken(issued.token),
    });
    expect(JSON.stringify(grant)).not.toContain(issued.token);
    expect(await env.store.getSessionRecord(hashSessionToken(issued.token))).toMatchObject({
      authMethod: "break-glass",
    });

    const security = env.security();
    const bearer = new Request(`${BASE_URL}/api/agents`, {
      headers: { authorization: issued.authorizationHeader },
    });
    expect(await security.authenticate({ request: bearer, plane: "operator" })).toMatchObject({
      id: env.user.id,
      accessRoles: { [ORG_WIDE_NAMESPACE]: ["admin"] },
    });

    const missingCsrf = new Request(`${BASE_URL}/api/agents`, {
      method: "POST",
      headers: { cookie: issued.cookieHeader, origin: BASE_URL },
    });
    expect(
      await security.authenticate({ request: missingCsrf, plane: "operator" }),
    ).toBeUndefined();
    const cookie = new Request(`${BASE_URL}/api/agents`, {
      method: "POST",
      headers: {
        cookie: issued.cookieHeader,
        origin: BASE_URL,
        [CSRF_HEADER]: issued.csrfToken,
      },
    });
    expect(await security.authenticate({ request: cookie, plane: "operator" })).toMatchObject({
      id: env.user.id,
    });

    expect((await env.audit.verify(ORG_WIDE_NAMESPACE)).valid).toBe(true);
    const events = (
      await env.store.listAuditRecords({ namespaceId: ORG_WIDE_NAMESPACE, limit: 100 })
    ).items;
    expect(events.map((event) => event.event)).toEqual([
      "break-glass.created",
      "break-glass.used",
      "break-glass.used",
    ]);
    for (const event of events) expect(JSON.stringify(event)).not.toContain(issued.token);
  });

  it("survives adapter restart and denies revoke, expiry, disable and role removal", async () => {
    const env = await fixture();
    const issued = await env.manager.issue({
      principal: { kind: "user", id: env.user.id },
      reason: "provider unavailable",
      createdBy: "operator-a",
      ttlMs: 5_000,
    });
    const request = () =>
      new Request(`${BASE_URL}/api/agents`, {
        headers: { authorization: issued.authorizationHeader },
      });
    expect(
      await env.security().authenticate({ request: request(), plane: "operator" }),
    ).toBeDefined();
    expect(await env.manager.revoke({ grantId: issued.grantId, revokedBy: "operator-b" })).toBe(
      "revoked",
    );
    expect(await env.manager.revoke({ grantId: issued.grantId, revokedBy: "operator-b" })).toBe(
      "already-revoked",
    );
    expect(
      await env.security().authenticate({ request: request(), plane: "operator" }),
    ).toBeUndefined();

    const expiring = await fixture();
    const short = await expiring.manager.issue({
      principal: { kind: "user", id: expiring.user.id },
      reason: "short test",
      createdBy: "operator",
      ttlMs: 1,
    });
    expiring.clock.advance(1);
    expect(
      await expiring.security().authenticate({
        request: new Request(`${BASE_URL}/api/agents`, {
          headers: { authorization: short.authorizationHeader },
        }),
        plane: "operator",
      }),
    ).toBeUndefined();

    const disabled = await fixture();
    const disabledAccess = await disabled.manager.issue({
      principal: { kind: "user", id: disabled.user.id },
      reason: "disable test",
      createdBy: "operator",
    });
    await disabled.store.setUserStatus({ id: disabled.user.id, status: "disabled" });
    expect(
      await disabled.security().authenticate({
        request: new Request(`${BASE_URL}/api/agents`, {
          headers: { authorization: disabledAccess.authorizationHeader },
        }),
        plane: "operator",
      }),
    ).toBeUndefined();

    const demoted = await fixture();
    const demotedAccess = await demoted.manager.issue({
      principal: { kind: "user", id: demoted.user.id },
      reason: "role test",
      createdBy: "operator",
    });
    await demoted.store.removeSubjectBinding({
      subject: { kind: "user", id: demoted.user.id },
      namespaceId: ORG_WIDE_NAMESPACE,
      accessRole: "admin",
    });
    expect(
      await demoted.security().authenticate({
        request: new Request(`${BASE_URL}/api/agents`, {
          headers: { authorization: demotedAccess.authorizationHeader },
        }),
        plane: "operator",
      }),
    ).toBeUndefined();
  });

  it("exposes no network mint route and bounds cookie-authenticated SSE lifetime", async () => {
    const env = await fixture();
    const issued = await env.manager.issue({
      principal: { kind: "user", id: env.user.id },
      reason: "identity provider outage",
      createdBy: "host-operator",
    });
    const security = env.security(25);
    const router = new Router({ logger: { error() {} } });
    security.mount?.(router);
    for (const path of ["/auth/break-glass", "/api/break-glass"]) {
      const response = await router.handle(new Request(`${BASE_URL}${path}`, { method: "POST" }));
      expect(response.status).toBe(404);
    }

    const request = new Request(`${BASE_URL}/api/runs/id/events`, {
      headers: { cookie: issued.cookieHeader },
    });
    expect(await security.authenticate({ request, plane: "operator" })).toBeDefined();
    const response = security.secureResponse?.({
      request,
      response: new Response(new ReadableStream<Uint8Array>({}), {
        headers: { "content-type": "text/event-stream" },
      }),
    });
    if (!response?.body) throw new Error("missing secured SSE body");
    const read = response.body.getReader().read();
    env.clock.advance(24);
    expect(await Promise.race([read.then(() => "closed"), Promise.resolve("open")])).toBe("open");
    env.clock.advance(1);
    expect(await read).toEqual({ value: undefined, done: true });
  });

  it("refuses non-admin principals, overlong TTLs and implicit insecure cookies", async () => {
    const member = await fixture("member");
    await expect(
      member.manager.issue({
        principal: { kind: "user", id: member.user.id },
        reason: "not allowed",
        createdBy: "operator",
      }),
    ).rejects.toThrow("org-wide admin");
    expect(
      () =>
        new BreakGlassAccessManager({
          store: member.store,
          audit: member.audit,
          publicBaseUrl: BASE_URL,
        }),
    ).toThrow("allowInsecureHttp");
    await expect(
      member.manager.issue({
        principal: { kind: "user", id: member.user.id },
        reason: "too long",
        createdBy: "operator",
        ttlMs: 60 * 60 * 1_000 + 1,
      }),
    ).rejects.toThrow("exceeds maximum");
  });
});
