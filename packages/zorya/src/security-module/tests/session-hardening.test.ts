import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { Router } from "../../server/router.ts";
import type { ZoryaSecurity } from "../../server/security/index.ts";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import type { LoginProvider } from "../login-provider.ts";
import {
  CSRF_COOKIE,
  CSRF_HEADER,
  SECURE_CSRF_COOKIE,
  SECURE_SESSION_COOKIE,
  csrfCookie,
  hashCsrfToken,
  hashSessionToken,
  sessionCookie,
} from "../sessions.ts";
import { createZoryaSecurity } from "../zorya-security.ts";

const BASE_URL = "http://zorya.test";
const SECRET = "0123456789abcdef0123456789abcdef";
const SESSION_TOKEN = "browser-session-token";
const CSRF_TOKEN = "browser-csrf-token";
const SESSION_TTL_MS = 60_000;

const unusedLoginProvider: LoginProvider = {
  id: "test",
  authorizationUrl: async () => "https://identity.test/authorize",
  exchange: async () => {
    throw new Error("not used");
  },
};

interface Fixture {
  readonly clock: ReturnType<typeof FakeWallClock.create>;
  readonly store: InMemorySecurityStore;
  readonly security: ZoryaSecurity;
  readonly userId: string;
  readonly cookie: string;
}

async function fixture(params: { readonly sseMaxAgeMs?: number } = {}): Promise<Fixture> {
  const clock = FakeWallClock.create(1_000);
  const store = new InMemorySecurityStore({ clock });
  const { user } = await store.findOrCreateProviderUser({
    identity: { providerId: "test", issuer: "https://identity.test", subject: "alice" },
    email: "alice@example.com",
    bindings: [{ namespaceId: "team-a", accessRole: "member" }],
  });
  await store.putSession({
    tokenHash: hashSessionToken(SESSION_TOKEN),
    principal: { kind: "user", id: user.id },
    createdAt: clock.currentTimeMs(),
    expiresAt: clock.currentTimeMs() + SESSION_TTL_MS,
    csrfTokenHash: hashCsrfToken(CSRF_TOKEN),
    authMethod: "oidc",
  });
  const security = makeSecurity({ store, clock, sseMaxAgeMs: params.sseMaxAgeMs });
  return {
    clock,
    store,
    security,
    userId: user.id,
    cookie: `zorya_session=${SESSION_TOKEN}; ${CSRF_COOKIE}=${CSRF_TOKEN}`,
  };
}

function makeSecurity(params: {
  readonly store: InMemorySecurityStore;
  readonly clock: ReturnType<typeof FakeWallClock.create>;
  readonly sseMaxAgeMs?: number;
}): ZoryaSecurity {
  return createZoryaSecurity({
    store: params.store,
    loginProviders: [unusedLoginProvider],
    publicBaseUrl: BASE_URL,
    allowInsecureHttp: true,
    allowedDomains: ["example.com"],
    serviceAccounts: [{ id: "automation", token: "automation-token" }],
    auditSink: { recordAudit: async () => {} },
    secret: SECRET,
    sessionTtlMs: SESSION_TTL_MS,
    ...(params.sseMaxAgeMs !== undefined && { sessionSseMaxAgeMs: params.sseMaxAgeMs }),
    clock: params.clock,
  });
}

function cookieRequest(params: {
  readonly cookie: string;
  readonly method?: string;
  readonly origin?: string;
  readonly csrf?: string;
}): Request {
  const headers = new Headers({ cookie: params.cookie });
  if (params.origin !== undefined) headers.set("origin", params.origin);
  if (params.csrf !== undefined) headers.set(CSRF_HEADER, params.csrf);
  return new Request(`${BASE_URL}/api/agents`, {
    method: params.method ?? "GET",
    headers,
  });
}

async function authenticate(security: ZoryaSecurity, request: Request) {
  return security.authenticate({ request, plane: "operator" });
}

describe("durable browser sessions", () => {
  it("survives adapter restart and immediately denies expiry, revoke, and disable", async () => {
    const env = await fixture();
    const request = () => cookieRequest({ cookie: env.cookie });
    expect(await authenticate(env.security, request())).toMatchObject({ id: env.userId });

    const restarted = makeSecurity({ store: env.store, clock: env.clock });
    expect(await authenticate(restarted, request())).toMatchObject({ id: env.userId });

    await env.store.revokeSession({
      tokenHash: hashSessionToken(SESSION_TOKEN),
      revokedAt: env.clock.currentTimeMs(),
    });
    expect(await authenticate(restarted, request())).toBeUndefined();

    const expiring = await fixture();
    expiring.clock.advance(SESSION_TTL_MS);
    expect(await authenticate(expiring.security, cookieRequest({ cookie: expiring.cookie }))).toBe(
      undefined,
    );

    const disabled = await fixture();
    await disabled.store.setUserStatus({ id: disabled.userId, status: "disabled" });
    expect(await authenticate(disabled.security, cookieRequest({ cookie: disabled.cookie }))).toBe(
      undefined,
    );
  });

  it("requires exact Origin and the session-bound double-submit token on unsafe requests", async () => {
    const env = await fixture();
    const valid = () =>
      cookieRequest({
        cookie: env.cookie,
        method: "POST",
        origin: BASE_URL,
        csrf: CSRF_TOKEN,
      });
    expect(await authenticate(env.security, valid())).toBeDefined();

    for (const request of [
      cookieRequest({ cookie: env.cookie, method: "POST", csrf: CSRF_TOKEN }),
      cookieRequest({ cookie: env.cookie, method: "POST", origin: BASE_URL }),
      cookieRequest({ cookie: env.cookie, method: "POST", origin: BASE_URL, csrf: "wrong" }),
      cookieRequest({
        cookie: env.cookie,
        method: "POST",
        origin: "http://evil.test",
        csrf: CSRF_TOKEN,
      }),
      cookieRequest({
        cookie: env.cookie,
        method: "POST",
        origin: "http://admin.zorya.test",
        csrf: CSRF_TOKEN,
      }),
    ]) {
      expect(await authenticate(env.security, request)).toBeUndefined();
    }
  });

  it("rejects duplicate session and CSRF cookies without bearer fallback", async () => {
    const env = await fixture();
    const duplicateSession = cookieRequest({
      cookie: `${env.cookie}; zorya_session=attacker`,
    });
    duplicateSession.headers.set("authorization", "Bearer automation-token");
    expect(await authenticate(env.security, duplicateSession)).toBeUndefined();

    expect(
      await authenticate(
        env.security,
        cookieRequest({
          cookie: `${env.cookie}; ${CSRF_COOKIE}=attacker`,
          method: "POST",
          origin: BASE_URL,
          csrf: CSRF_TOKEN,
        }),
      ),
    ).toBeUndefined();
  });

  it("does not impose browser CSRF checks on bearer-authenticated writes", async () => {
    const env = await fixture();
    const request = new Request(`${BASE_URL}/api/agents`, {
      method: "POST",
      headers: { authorization: "Bearer automation-token" },
    });
    expect(await authenticate(env.security, request)).toMatchObject({
      kind: "service-account",
      id: "automation",
    });
  });

  it("protects logout with the same Origin and CSRF contract and revokes durably", async () => {
    const env = await fixture();
    const router = new Router({ logger: { error() {} } });
    env.security.mount?.(router);

    const missingCsrf = await router.handle(
      new Request(`${BASE_URL}/auth/logout`, {
        method: "POST",
        headers: { cookie: env.cookie, origin: BASE_URL },
      }),
    );
    expect(missingCsrf.status).toBe(403);
    expect(await authenticate(env.security, cookieRequest({ cookie: env.cookie }))).toBeDefined();

    const logout = await router.handle(
      new Request(`${BASE_URL}/auth/logout`, {
        method: "POST",
        headers: { cookie: env.cookie, origin: BASE_URL, [CSRF_HEADER]: CSRF_TOKEN },
      }),
    );
    expect(logout.status).toBe(204);
    expect(logout.headers.get("set-cookie")).toContain(`${CSRF_COOKIE}=;`);
    expect(await authenticate(env.security, cookieRequest({ cookie: env.cookie }))).toBeUndefined();
  });
});

describe("browser cookie policy and SSE reauthorization", () => {
  it("uses Secure __Host cookies for HTTPS and refuses implicit insecure operation", () => {
    expect(sessionCookie({ token: "token", ttlMs: 1_000, secure: true })).toBe(
      `${SECURE_SESSION_COOKIE}=token; Path=/; Max-Age=1; HttpOnly; SameSite=Lax; Secure`,
    );
    expect(csrfCookie({ token: "csrf", ttlMs: 1_000, secure: true })).toBe(
      `${SECURE_CSRF_COOKIE}=csrf; Path=/; Max-Age=1; SameSite=Strict; Secure`,
    );
    expect(() =>
      createZoryaSecurity({
        store: new InMemorySecurityStore(),
        loginProviders: [unusedLoginProvider],
        publicBaseUrl: BASE_URL,
        allowedDomains: ["example.com"],
        auditSink: { recordAudit: async () => {} },
        secret: SECRET,
      }),
    ).toThrow(/allowInsecureHttp/);
  });

  it("ends every cookie-authenticated SSE response at the configured reauthorization bound", async () => {
    const env = await fixture({ sseMaxAgeMs: 25 });
    const request = cookieRequest({ cookie: env.cookie });
    expect(await authenticate(env.security, request)).toBeDefined();
    const source = new ReadableStream<Uint8Array>({});
    const response = env.security.secureResponse?.({
      request,
      response: new Response(source, { headers: { "content-type": "text/event-stream" } }),
    });
    if (!response?.body) throw new Error("missing secured SSE body");
    const read = response.body.getReader().read();

    // Any expiry, disable, or role removal is observed on the forced reconnect,
    // which cannot be delayed beyond this connection-age bound.
    await env.store.removeSubjectBinding({
      subject: { kind: "user", id: env.userId },
      namespaceId: "team-a",
      accessRole: "member",
    });
    env.clock.advance(24);
    expect(await Promise.race([read.then(() => "closed"), Promise.resolve("open")])).toBe("open");
    env.clock.advance(1);
    expect(await read).toEqual({ value: undefined, done: true });

    const reconnected = await authenticate(env.security, cookieRequest({ cookie: env.cookie }));
    expect(reconnected?.accessRoles["team-a"]).toBeUndefined();
  });

  it("does not cap bearer-authenticated SSE responses", async () => {
    const env = await fixture({ sseMaxAgeMs: 10 });
    const request = new Request(`${BASE_URL}/api/runs/id/events`, {
      headers: { authorization: "Bearer automation-token" },
    });
    expect(await authenticate(env.security, request)).toBeDefined();
    const response = new Response(new ReadableStream<Uint8Array>({}), {
      headers: { "content-type": "text/event-stream" },
    });
    expect(env.security.secureResponse?.({ request, response })).toBe(response);
  });

  it("does not cancel cookie-authenticated POST agent streams mid-turn", async () => {
    const env = await fixture({ sseMaxAgeMs: 10 });
    const request = cookieRequest({
      cookie: env.cookie,
      method: "POST",
      origin: BASE_URL,
      csrf: CSRF_TOKEN,
    });
    expect(await authenticate(env.security, request)).toBeDefined();
    let cancelled = false;
    const response = new Response(
      new ReadableStream<Uint8Array>({
        cancel: () => {
          cancelled = true;
        },
      }),
      { headers: { "content-type": "text/event-stream" } },
    );

    expect(env.security.secureResponse?.({ request, response })).toBe(response);
    env.clock.advance(10);
    expect(cancelled).toBe(false);
  });
});
