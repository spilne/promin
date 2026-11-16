import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import type { WallClock } from "@promin/workflow";

export const SESSION_COOKIE = "zorya_session";
export const SECURE_SESSION_COOKIE = "__Host-zorya_session";
export const CSRF_COOKIE = "zorya_csrf";
export const SECURE_CSRF_COOKIE = "__Host-zorya_csrf";
export const CSRF_HEADER = "x-zorya-csrf";
export const LOGIN_STATE_COOKIE = "zorya_login_state";
const LOGIN_STATE_TTL_MS = 10 * 60 * 1_000;

interface LoginStatePayload {
  readonly state: string;
  readonly codeVerifier: string;
  readonly issuedAt: number;
}

export type CookieValue =
  | { readonly kind: "absent" }
  | { readonly kind: "duplicate" }
  | { readonly kind: "value"; readonly value: string };

export function createLoginState(params: {
  readonly secret: string;
  readonly clock: WallClock;
  readonly secure: boolean;
}): {
  readonly state: string;
  readonly codeVerifier: string;
  readonly codeChallenge: string;
  readonly cookie: string;
} {
  const state = randomBytes(32).toString("base64url");
  const codeVerifier = randomBytes(32).toString("base64url");
  const payload: LoginStatePayload = {
    state,
    codeVerifier,
    issuedAt: params.clock.currentTimeMs(),
  };
  return {
    state,
    codeVerifier,
    codeChallenge: createHash("sha256").update(codeVerifier).digest("base64url"),
    cookie: cookie({
      name: LOGIN_STATE_COOKIE,
      value: signPayload({ payload, secret: params.secret }),
      maxAgeSeconds: LOGIN_STATE_TTL_MS / 1_000,
      path: "/auth",
      secure: params.secure,
      httpOnly: true,
      sameSite: "Lax",
    }),
  };
}

export function readLoginState(params: {
  readonly cookieHeader: string | null;
  readonly secret: string;
  readonly clock: WallClock;
}): LoginStatePayload | undefined {
  const parsed = readCookie(params.cookieHeader, LOGIN_STATE_COOKIE);
  if (parsed.kind !== "value" || parsed.value.length === 0) return undefined;
  const payload = verifyPayload({ value: parsed.value, secret: params.secret });
  if (!payload) return undefined;
  if (payload.issuedAt > params.clock.currentTimeMs()) return undefined;
  if (params.clock.currentTimeMs() - payload.issuedAt > LOGIN_STATE_TTL_MS) return undefined;
  return payload;
}

export function clearLoginStateCookie(secure: boolean): string {
  return cookie({
    name: LOGIN_STATE_COOKIE,
    value: "",
    maxAgeSeconds: 0,
    path: "/auth",
    secure,
    httpOnly: true,
    sameSite: "Lax",
  });
}

export function newSessionToken(): string {
  return randomBytes(32).toString("base64url");
}

export function newCsrfToken(): string {
  return randomBytes(32).toString("base64url");
}

export function hashSessionToken(token: string): string {
  return createHash("sha256").update(token, "utf8").digest("hex");
}

export function hashCsrfToken(token: string): string {
  return createHash("sha256").update(token, "utf8").digest("hex");
}

export function sessionCookie(params: {
  readonly token: string;
  readonly ttlMs: number;
  readonly secure: boolean;
}): string {
  return cookie({
    name: sessionCookieName(params.secure),
    value: params.token,
    maxAgeSeconds: Math.max(0, Math.floor(params.ttlMs / 1_000)),
    path: "/",
    secure: params.secure,
    httpOnly: true,
    sameSite: "Lax",
  });
}

export function csrfCookie(params: {
  readonly token: string;
  readonly ttlMs: number;
  readonly secure: boolean;
}): string {
  return cookie({
    name: csrfCookieName(params.secure),
    value: params.token,
    maxAgeSeconds: Math.max(0, Math.floor(params.ttlMs / 1_000)),
    path: "/",
    secure: params.secure,
    // The dashboard must copy this value into CSRF_HEADER. It is not a credential.
    httpOnly: false,
    sameSite: "Strict",
  });
}

export function clearSessionCookies(secure: boolean): ReadonlyArray<string> {
  return [
    cookie({
      name: sessionCookieName(secure),
      value: "",
      maxAgeSeconds: 0,
      path: "/",
      secure,
      httpOnly: true,
      sameSite: "Lax",
    }),
    cookie({
      name: csrfCookieName(secure),
      value: "",
      maxAgeSeconds: 0,
      path: "/",
      secure,
      httpOnly: false,
      sameSite: "Strict",
    }),
  ];
}

/** @deprecated Use clearSessionCookies so the readable CSRF cookie is cleared too. */
export function clearSessionCookie(secure: boolean): string {
  return clearSessionCookies(secure)[0]!;
}

/**
 * Reads exactly one configured session cookie. Duplicate cookies are kept
 * distinct from an absent cookie so callers cannot fall through to bearer auth.
 */
export function sessionToken(cookieHeader: string | null, secure = false): CookieValue {
  return readCookie(cookieHeader, sessionCookieName(secure));
}

export function csrfToken(cookieHeader: string | null, secure = false): CookieValue {
  return readCookie(cookieHeader, csrfCookieName(secure));
}

export function sessionCookieName(secure: boolean): string {
  return secure ? SECURE_SESSION_COOKIE : SESSION_COOKIE;
}

export function csrfCookieName(secure: boolean): string {
  return secure ? SECURE_CSRF_COOKIE : CSRF_COOKIE;
}

/** Constant-time verification of a presented CSRF token against its session digest. */
export function csrfTokenMatches(token: string, expectedHash: string | undefined): boolean {
  if (!expectedHash || token.length > 256 || !/^[0-9a-f]{64}$/.test(expectedHash)) return false;
  const actual = Buffer.from(hashCsrfToken(token), "hex");
  const expected = Buffer.from(expectedHash, "hex");
  return expected.length === actual.length && timingSafeEqual(expected, actual);
}

function signPayload(params: {
  readonly payload: LoginStatePayload;
  readonly secret: string;
}): string {
  const encoded = Buffer.from(JSON.stringify(params.payload), "utf8").toString("base64url");
  const signature = createHmac("sha256", params.secret).update(encoded).digest("base64url");
  return `${encoded}.${signature}`;
}

function verifyPayload(params: {
  readonly value: string;
  readonly secret: string;
}): LoginStatePayload | undefined {
  const splitAt = params.value.lastIndexOf(".");
  if (splitAt <= 0) return undefined;
  const encoded = params.value.slice(0, splitAt);
  const signature = params.value.slice(splitAt + 1);
  const expected = createHmac("sha256", params.secret).update(encoded).digest();
  let presented: Buffer;
  try {
    presented = Buffer.from(signature, "base64url");
  } catch {
    return undefined;
  }
  if (presented.length !== expected.length || !timingSafeEqual(presented, expected))
    return undefined;
  try {
    const raw: unknown = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
    if (!isLoginStatePayload(raw)) return undefined;
    return raw;
  } catch {
    return undefined;
  }
}

function isLoginStatePayload(value: unknown): value is LoginStatePayload {
  if (!value || typeof value !== "object") return false;
  return (
    "state" in value &&
    typeof value.state === "string" &&
    "codeVerifier" in value &&
    typeof value.codeVerifier === "string" &&
    "issuedAt" in value &&
    typeof value.issuedAt === "number"
  );
}

function readCookie(header: string | null, name: string): CookieValue {
  if (!header) return { kind: "absent" };
  let value: string | undefined;
  for (const part of header.split(";")) {
    const trimmed = part.trim();
    const splitAt = trimmed.indexOf("=");
    if (splitAt < 0 || trimmed.slice(0, splitAt) !== name) continue;
    if (value !== undefined) return { kind: "duplicate" };
    value = trimmed.slice(splitAt + 1);
  }
  return value === undefined ? { kind: "absent" } : { kind: "value", value };
}

function cookie(params: {
  readonly name: string;
  readonly value: string;
  readonly maxAgeSeconds: number;
  readonly path: string;
  readonly secure: boolean;
  readonly httpOnly: boolean;
  readonly sameSite: "Lax" | "Strict";
}): string {
  return [
    `${params.name}=${params.value}`,
    `Path=${params.path}`,
    `Max-Age=${params.maxAgeSeconds}`,
    ...(params.httpOnly ? ["HttpOnly"] : []),
    `SameSite=${params.sameSite}`,
    ...(params.secure ? ["Secure"] : []),
  ].join("; ");
}
