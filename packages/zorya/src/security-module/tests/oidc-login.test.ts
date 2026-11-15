import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { Router } from "../../server/router.ts";
import type { ZoryaSecurity } from "../../server/security/index.ts";
import { InMemorySecurityStore } from "../in-memory-security-store.ts";
import { oidcLoginProvider } from "../oidc-login-provider.ts";
import { hashSessionToken } from "../sessions.ts";
import { createZoryaSecurity } from "../zorya-security.ts";
import {
  startFakeOidcIssuer,
  type FakeOidcIssuer,
  type FakeTokenMode,
} from "./fake-oidc-issuer.ts";

const SECRET = "0123456789abcdef0123456789abcdef";
const PUBLIC_BASE_URL = "http://zorya.test";
const SESSION_TTL_MS = 60_000;

let issuer: FakeOidcIssuer;

beforeAll(async () => {
  issuer = await startFakeOidcIssuer();
});

afterAll(async () => {
  await issuer.stop();
});

interface Environment {
  readonly store: InMemorySecurityStore;
  readonly security: ZoryaSecurity;
  readonly router: Router;
  readonly clock: ReturnType<typeof FakeWallClock.create>;
  readonly publicBaseUrl: string;
}

function environment(
  params: {
    readonly adminEmails?: ReadonlyArray<string>;
    readonly serviceAccounts?: ReadonlyArray<{
      readonly id: string;
      readonly token: string;
      readonly canActFor?: boolean;
    }>;
    readonly publicBaseUrl?: string;
  } = {},
): Environment {
  const clock = FakeWallClock.create(1_000);
  const store = new InMemorySecurityStore({ clock });
  const security = createZoryaSecurity({
    store,
    loginProviders: [
      oidcLoginProvider({
        issuer: issuer.issuer,
        clientId: issuer.clientId,
        clientSecret: issuer.clientSecret,
      }),
    ],
    publicBaseUrl: params.publicBaseUrl ?? PUBLIC_BASE_URL,
    allowedDomains: ["example.com"],
    ...(params.adminEmails !== undefined && { adminEmails: params.adminEmails }),
    ...(params.serviceAccounts !== undefined && { serviceAccounts: params.serviceAccounts }),
    secret: SECRET,
    sessionTtlMs: SESSION_TTL_MS,
    clock,
  });
  const router = new Router({ logger: { error() {} } });
  security.mount?.(router);
  return {
    store,
    security,
    router,
    clock,
    publicBaseUrl: params.publicBaseUrl ?? PUBLIC_BASE_URL,
  };
}

async function login(params: {
  readonly env: Environment;
  readonly email: string;
  readonly mode?: FakeTokenMode;
  readonly emailVerified?: boolean;
}): Promise<{ readonly response: Response; readonly sessionCookie?: string }> {
  issuer.nextLogin({ email: params.email, emailVerified: params.emailVerified });
  issuer.signWith(params.mode ?? "valid");
  const start = await params.env.router.handle(
    new Request(`${params.env.publicBaseUrl}/auth/login/oidc`),
  );
  const stateCookie = cookiePair(start, "zorya_login_state");
  const authorizationUrl = start.headers.get("location");
  if (!stateCookie || !authorizationUrl) throw new Error("login route did not start OIDC");
  const authorization = await fetch(authorizationUrl, { redirect: "manual" });
  const callbackUrl = authorization.headers.get("location");
  if (!callbackUrl) throw new Error("issuer did not redirect to callback");
  const response = await params.env.router.handle(
    new Request(callbackUrl, { headers: { cookie: stateCookie } }),
  );
  return { response, sessionCookie: cookiePair(response, "zorya_session") };
}

function cookiePair(response: Response, name: string): string | undefined {
  const header = response.headers.get("set-cookie");
  const match = header?.match(new RegExp(`(?:^|[, ]+)${name}=([^;,]*)`));
  return match ? `${name}=${match[1]}` : undefined;
}

async function errorCode(response: Response): Promise<string | undefined> {
  const body: unknown = await response.json();
  if (!body || typeof body !== "object" || !("error" in body)) return undefined;
  return typeof body.error === "string" ? body.error : undefined;
}

describe("OIDC security module", () => {
  it("first login creates the user with the default access role and a session", async () => {
    const env = environment();
    const result = await login({ env, email: "user@example.com" });

    expect(result.response.status).toBe(302);
    expect(result.sessionCookie).toBeDefined();
    expect(result.response.headers.get("set-cookie")).toMatch(
      /zorya_session=[^;]+; Path=\/; Max-Age=60; HttpOnly; SameSite=Lax/,
    );
    const principal = await env.security.authenticate(
      new Request(`${PUBLIC_BASE_URL}/api/agents`, {
        headers: { cookie: result.sessionCookie! },
      }),
    );
    expect(principal).toMatchObject({
      kind: "user",
      email: "user@example.com",
      accessRoles: { org: ["member"] },
      canActFor: false,
    });
    expect(await env.store.listUsers()).toHaveLength(1);
  });

  it("an admin email gets the admin access role", async () => {
    const env = environment({ adminEmails: ["Admin@Example.com"] });
    const result = await login({ env, email: "ADMIN@example.COM" });
    const principal = await env.security.authenticate(
      new Request(`${PUBLIC_BASE_URL}/`, { headers: { cookie: result.sessionCookie! } }),
    );
    expect(principal?.accessRoles).toEqual({ org: ["admin"] });
  });

  it("second login reuses the user", async () => {
    const env = environment();
    const first = await login({ env, email: "user@example.com" });
    const second = await login({ env, email: "USER@EXAMPLE.COM" });
    const firstPrincipal = await env.security.authenticate(
      new Request(`${PUBLIC_BASE_URL}/`, { headers: { cookie: first.sessionCookie! } }),
    );
    const secondPrincipal = await env.security.authenticate(
      new Request(`${PUBLIC_BASE_URL}/`, { headers: { cookie: second.sessionCookie! } }),
    );
    expect(secondPrincipal?.id).toBe(firstPrincipal?.id);
    expect(await env.store.listUsers()).toHaveLength(1);
  });

  it("domains", async () => {
    expect((await login({ env: environment(), email: "a@example.com" })).response.status).toBe(302);
    expect((await login({ env: environment(), email: "A@EXAMPLE.COM" })).response.status).toBe(302);
    for (const email of [
      "a@evil.com.example.com",
      "a@example.com.evil.com",
      "example.com@evil.com",
    ]) {
      const env = environment();
      const result = await login({ env, email });
      expect(result.response.status).toBe(403);
      expect(await errorCode(result.response)).toBe("domain_not_allowed");
      expect(await env.store.listUsers()).toEqual([]);
    }
  });

  it("bad tokens", async () => {
    for (const mode of ["wrong-key", "wrong-issuer", "wrong-audience", "expired"] as const) {
      const env = environment();
      const result = await login({ env, email: `${mode}@example.com`, mode });
      expect(result.response.status).toBe(401);
      expect(await errorCode(result.response)).toBe("invalid_identity_token");
      expect(await env.store.listUsers()).toEqual([]);
    }
    const env = environment();
    const unverified = await login({
      env,
      email: "unverified@example.com",
      emailVerified: false,
    });
    expect(unverified.response.status).toBe(403);
    expect(await errorCode(unverified.response)).toBe("email_not_verified");
    expect(await env.store.listUsers()).toEqual([]);
  });

  it("state mismatch answers 400 invalid_state", async () => {
    const env = environment();
    issuer.nextLogin({ email: "user@example.com" });
    const start = await env.router.handle(new Request(`${PUBLIC_BASE_URL}/auth/login/oidc`));
    const stateCookie = cookiePair(start, "zorya_login_state")!;
    const authorization = await fetch(start.headers.get("location")!, { redirect: "manual" });
    const callback = new URL(authorization.headers.get("location")!);
    callback.searchParams.set("state", "attacker-state");

    const response = await env.router.handle(
      new Request(callback, { headers: { cookie: stateCookie } }),
    );
    expect(response.status).toBe(400);
    expect(await errorCode(response)).toBe("invalid_state");
    expect(await env.store.listUsers()).toEqual([]);
  });

  it("the state cookie is signed, short-lived, and httpOnly", async () => {
    const env = environment({ publicBaseUrl: "https://zorya.test" });
    issuer.nextLogin({ email: "user@example.com" });
    const start = await env.router.handle(new Request(`${env.publicBaseUrl}/auth/login/oidc`));
    const stateCookie = cookiePair(start, "zorya_login_state");
    const setCookie = start.headers.get("set-cookie");
    const authorizationUrl = start.headers.get("location");
    expect(stateCookie).toBeDefined();
    expect(setCookie).toContain("Path=/auth");
    expect(setCookie).toContain("Max-Age=600");
    expect(setCookie).toContain("HttpOnly");
    expect(setCookie).toContain("SameSite=Lax");
    expect(setCookie).toContain("Secure");
    if (!stateCookie || !authorizationUrl) throw new Error("login did not start");

    const authorization = await fetch(authorizationUrl, { redirect: "manual" });
    const callbackUrl = authorization.headers.get("location");
    if (!callbackUrl) throw new Error("issuer did not redirect to callback");
    const tampered = stateCookie.replace("zorya_login_state=", "zorya_login_state=x");
    const tamperedResponse = await env.router.handle(
      new Request(callbackUrl, { headers: { cookie: tampered } }),
    );
    expect(tamperedResponse.status).toBe(400);
    expect(await errorCode(tamperedResponse)).toBe("invalid_state");

    env.clock.advance(10 * 60 * 1_000 + 1);
    const expiredResponse = await env.router.handle(
      new Request(callbackUrl, { headers: { cookie: stateCookie } }),
    );
    expect(expiredResponse.status).toBe(400);
    expect(await errorCode(expiredResponse)).toBe("invalid_state");
    expect(await env.store.listUsers()).toEqual([]);

    const secureLogin = await login({ env, email: "user@example.com" });
    expect(secureLogin.response.headers.get("set-cookie")).toMatch(
      /zorya_session=[^;]+; Path=\/; Max-Age=60; HttpOnly; SameSite=Lax; Secure/,
    );
  });

  it("unknown login providers answer 404", async () => {
    const env = environment();
    const response = await env.router.handle(
      new Request(`${env.publicBaseUrl}/auth/login/not-configured`),
    );
    expect(response.status).toBe(404);
  });

  it("an expired session is not authenticated", async () => {
    const env = environment();
    const result = await login({ env, email: "user@example.com" });
    const request = () =>
      new Request(`${PUBLIC_BASE_URL}/`, { headers: { cookie: result.sessionCookie! } });
    expect(await env.security.authenticate(request())).toBeDefined();
    env.clock.advance(SESSION_TTL_MS + 1);
    expect(await env.security.authenticate(request())).toBeUndefined();
  });

  it("a disabled user cannot authenticate, be resolved, or log in again", async () => {
    const env = environment();
    const active = await login({ env, email: "user@example.com" });
    const user = (await env.store.listUsers())[0]!;
    await env.store.setUserStatus({ id: user.id, status: "disabled" });
    const rawToken = active.sessionCookie?.slice("zorya_session=".length);
    if (!rawToken) throw new Error("login did not return a session cookie");
    // Recreate a stale session to prove authenticate checks user status even
    // when a backing store has not yet completed session invalidation.
    await env.store.createSession({
      tokenHash: hashSessionToken(rawToken),
      userId: user.id,
      ttlMs: SESSION_TTL_MS,
    });
    expect(
      await env.security.authenticate(
        new Request(`${PUBLIC_BASE_URL}/`, { headers: { cookie: active.sessionCookie! } }),
      ),
    ).toBeUndefined();
    expect(await env.security.principalById(user.id)).toBeUndefined();

    const retry = await login({ env, email: "USER@EXAMPLE.COM" });
    expect(retry.response.status).toBe(403);
    expect(await errorCode(retry.response)).toBe("user_disabled");
  });

  it("logout ends the session", async () => {
    const env = environment();
    const result = await login({ env, email: "user@example.com" });
    const request = () =>
      new Request(`${PUBLIC_BASE_URL}/`, { headers: { cookie: result.sessionCookie! } });
    expect(await env.security.authenticate(request())).toBeDefined();
    const logout = await env.router.handle(
      new Request(`${PUBLIC_BASE_URL}/auth/logout`, {
        method: "POST",
        headers: { cookie: result.sessionCookie! },
      }),
    );
    expect(logout.status).toBe(204);
    expect(await env.security.authenticate(request())).toBeUndefined();
  });

  it("a service account token authenticates; a wrong token does not", async () => {
    const env = environment({
      serviceAccounts: [{ id: "slack", token: "correct-token", canActFor: true }],
    });
    const accepted = await env.security.authenticate(
      new Request(`${PUBLIC_BASE_URL}/api/agents`, {
        headers: { authorization: "Bearer correct-token" },
      }),
    );
    const denied = await env.security.authenticate(
      new Request(`${PUBLIC_BASE_URL}/api/agents`, {
        headers: { authorization: "Bearer wrong-token" },
      }),
    );
    expect(accepted).toEqual({
      kind: "service-account",
      id: "slack",
      accessRoles: {},
      canActFor: true,
    });
    expect(denied).toBeUndefined();
  });

  it("does not fall through from an invalid session to a bearer token", async () => {
    const env = environment({
      serviceAccounts: [{ id: "service", token: "correct-token" }],
    });
    const principal = await env.security.authenticate(
      new Request(`${PUBLIC_BASE_URL}/api/agents`, {
        headers: {
          cookie: "zorya_session=stale",
          authorization: "Bearer correct-token",
        },
      }),
    );
    expect(principal).toBeUndefined();
  });

  it("config: empty allowedDomains, no providers, or a short secret throws", () => {
    const provider = oidcLoginProvider({
      issuer: issuer.issuer,
      clientId: issuer.clientId,
      clientSecret: issuer.clientSecret,
    });
    const store = new InMemorySecurityStore();
    expect(() =>
      createZoryaSecurity({
        store,
        loginProviders: [provider],
        publicBaseUrl: PUBLIC_BASE_URL,
        allowedDomains: [],
        secret: SECRET,
      }),
    ).toThrow(/allowedDomains/);
    expect(() =>
      createZoryaSecurity({
        store,
        loginProviders: [],
        publicBaseUrl: PUBLIC_BASE_URL,
        allowedDomains: ["example.com"],
        secret: SECRET,
      }),
    ).toThrow(/loginProviders/);
    expect(() =>
      createZoryaSecurity({
        store,
        loginProviders: [provider],
        publicBaseUrl: PUBLIC_BASE_URL,
        allowedDomains: ["example.com"],
        secret: "too-short",
      }),
    ).toThrow(/secret/);
  });
});
