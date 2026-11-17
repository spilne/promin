import { randomBytes } from "node:crypto";
import { ORG_WIDE_NAMESPACE } from "../server/security/security.ts";
import type { DurableSecurityAudit } from "./security-audit.ts";
import type { BreakGlassGrant, SecurityPrincipalRef, SecurityStore } from "./security-store.ts";
import {
  csrfCookie,
  csrfCookieName,
  hashCsrfToken,
  hashSessionToken,
  newCsrfToken,
  sessionCookie,
  sessionCookieName,
} from "./sessions.ts";

const DEFAULT_TTL_MS = 15 * 60 * 1_000;
const DEFAULT_MAX_TTL_MS = 60 * 60 * 1_000;
const TOKEN_PREFIX = "zbg_";
const TOKEN_SECRET_CHARACTERS = 43;

export interface BreakGlassAccessManagerConfig {
  readonly store: SecurityStore;
  readonly audit: DurableSecurityAudit;
  readonly publicBaseUrl: string;
  readonly allowInsecureHttp?: boolean;
  readonly defaultTtlMs?: number;
  readonly maxTtlMs?: number;
  /** Test seam. Production defaults to a 128-bit random identifier. */
  readonly idFactory?: () => string;
  /** Test seam. Production defaults to a 256-bit random credential. */
  readonly tokenSecretFactory?: () => string;
}

export interface IssueBreakGlassAccessParams {
  readonly principal: SecurityPrincipalRef;
  readonly reason: string;
  readonly createdBy: string;
  readonly ttlMs?: number;
}

/** Plaintext material returned exactly once by a host-local issuance command. */
export interface IssuedBreakGlassAccess {
  readonly grantId: string;
  readonly principal: SecurityPrincipalRef;
  readonly createdAt: number;
  readonly expiresAt: number;
  readonly token: string;
  readonly csrfToken: string;
  readonly authorizationHeader: string;
  readonly cookieHeader: string;
  readonly setCookieHeaders: ReadonlyArray<string>;
}

/**
 * Host-side break-glass lifecycle. It has no HTTP mount and therefore cannot
 * mint emergency access over the network.
 */
export class BreakGlassAccessManager {
  private readonly store: SecurityStore;
  private readonly audit: DurableSecurityAudit;
  private readonly secure: boolean;
  private readonly defaultTtlMs: number;
  private readonly maxTtlMs: number;
  private readonly idFactory: () => string;
  private readonly tokenSecretFactory: () => string;

  constructor(config: BreakGlassAccessManagerConfig) {
    this.store = config.store;
    this.audit = config.audit;
    const publicBaseUrl = new URL(config.publicBaseUrl);
    if (publicBaseUrl.protocol !== "https:" && publicBaseUrl.protocol !== "http:") {
      throw new TypeError("BreakGlassAccessManager: publicBaseUrl must use http or https");
    }
    this.secure = publicBaseUrl.protocol === "https:";
    if (!this.secure && config.allowInsecureHttp !== true) {
      throw new TypeError(
        "BreakGlassAccessManager: an HTTP publicBaseUrl requires allowInsecureHttp: true",
      );
    }
    this.defaultTtlMs = positiveSafeInteger(config.defaultTtlMs ?? DEFAULT_TTL_MS, "default TTL");
    this.maxTtlMs = positiveSafeInteger(config.maxTtlMs ?? DEFAULT_MAX_TTL_MS, "maximum TTL");
    if (this.defaultTtlMs > this.maxTtlMs) {
      throw new TypeError("BreakGlassAccessManager: default TTL exceeds maximum TTL");
    }
    this.idFactory = config.idFactory ?? (() => randomBytes(16).toString("base64url"));
    this.tokenSecretFactory =
      config.tokenSecretFactory ?? (() => randomBytes(32).toString("base64url"));
  }

  async issue(params: IssueBreakGlassAccessParams): Promise<IssuedBreakGlassAccess> {
    const ttlMs = positiveSafeInteger(params.ttlMs ?? this.defaultTtlMs, "break-glass TTL");
    if (ttlMs > this.maxTtlMs) {
      throw new TypeError(`BreakGlassAccessManager: TTL exceeds maximum ${this.maxTtlMs}ms`);
    }
    const access = await this.store.resolveAccess({
      subject: params.principal,
      namespaceId: ORG_WIDE_NAMESPACE,
    });
    if (!access.accessRoles.includes("admin")) {
      throw new Error("BreakGlassAccessManager: principal must hold org-wide admin access");
    }

    const grantId = safeTokenPart(this.idFactory(), "grant id", 128);
    const secret = safeTokenPart(
      this.tokenSecretFactory(),
      "token secret",
      TOKEN_SECRET_CHARACTERS,
      TOKEN_SECRET_CHARACTERS,
    );
    const token = `${TOKEN_PREFIX}${grantId}.${secret}`;
    const csrfToken = newCsrfToken();
    const createdAt = await this.store.currentTimeMs();
    if (!Number.isSafeInteger(createdAt) || createdAt < 0) {
      throw new TypeError("BreakGlassAccessManager: clock returned an invalid time");
    }
    const expiresAt = createdAt + ttlMs;
    if (!Number.isSafeInteger(expiresAt)) {
      throw new TypeError("BreakGlassAccessManager: expiry exceeds the safe integer range");
    }
    const tokenHash = hashSessionToken(token);
    const grant: BreakGlassGrant = {
      id: grantId,
      principal: params.principal,
      tokenHash,
      reason: params.reason,
      createdBy: params.createdBy,
      createdAt,
      expiresAt,
    };
    await this.audit.createBreakGlassAccess({
      grant,
      session: {
        tokenHash,
        principal: params.principal,
        createdAt,
        expiresAt,
        csrfTokenHash: hashCsrfToken(csrfToken),
        authMethod: "break-glass",
      },
    });
    const setCookieHeaders = Object.freeze([
      sessionCookie({ token, ttlMs, secure: this.secure }),
      csrfCookie({ token: csrfToken, ttlMs, secure: this.secure }),
    ]);
    return Object.freeze({
      grantId,
      principal: Object.freeze({ ...params.principal }),
      createdAt,
      expiresAt,
      token,
      csrfToken,
      authorizationHeader: `Bearer ${token}`,
      cookieHeader: `${sessionCookieName(this.secure)}=${token}; ${csrfCookieName(this.secure)}=${csrfToken}`,
      setCookieHeaders,
    });
  }

  async revoke(params: {
    readonly grantId: string;
    readonly revokedBy: string;
  }): Promise<"revoked" | "already-revoked"> {
    const grant = await this.store.getBreakGlassGrant(params.grantId);
    if (!grant) throw new Error(`Unknown break-glass grant: ${params.grantId}`);
    if (grant.revokedAt !== undefined) return "already-revoked";
    const revokedAt = await this.store.currentTimeMs();
    try {
      return await this.audit.revokeBreakGlassAccess({
        grant,
        revokedAt,
        revokedBy: params.revokedBy,
      });
    } catch (error) {
      // A concurrent host command may have completed after our read.
      const current = await this.store.getBreakGlassGrant(params.grantId);
      if (current?.revokedAt !== undefined) return "already-revoked";
      throw error;
    }
  }
}

/** Returns the embedded grant id without exposing or logging the credential. */
export function breakGlassGrantIdFromToken(token: string): string | undefined {
  if (token.length > 256 || !token.startsWith(TOKEN_PREFIX)) return undefined;
  const separator = token.indexOf(".", TOKEN_PREFIX.length);
  if (separator < 0) return undefined;
  const grantId = token.slice(TOKEN_PREFIX.length, separator);
  const secret = token.slice(separator + 1);
  if (!isTokenPart(grantId, 1, 128) || !isTokenPart(secret, 43, 43)) return undefined;
  return grantId;
}

function positiveSafeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`BreakGlassAccessManager: ${label} must be a positive safe integer`);
  }
  return value;
}

function safeTokenPart(value: string, label: string, max: number, min = 1): string {
  if (!isTokenPart(value, min, max)) {
    throw new TypeError(`BreakGlassAccessManager: ${label} is not a safe token component`);
  }
  return value;
}

function isTokenPart(value: string, min: number, max: number): boolean {
  return value.length >= min && value.length <= max && /^[A-Za-z0-9_-]+$/.test(value);
}
