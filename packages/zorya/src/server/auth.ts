// ---------------------------------------------------------------------------
// Auth — bearer API-key checks.
//
// Keys are compared as SHA-256 digests with `timingSafeEqual`, against every
// configured key without an early exit, so neither the key's contents nor
// which key matched leak through timing. Keys are never logged or echoed.
//
// An `Auth` with no keys is open. ZoryaServer only builds one when the
// operator passed `insecure: true`; otherwise it refuses to start without
// keys (see `ZoryaServerConfig.insecure`).
// ---------------------------------------------------------------------------

import { createHash, timingSafeEqual } from "node:crypto";

export interface AuthConfig {
  /** Allowed API keys. Empty means open, which the server only allows in insecure mode. */
  apiKeys?: ReadonlyArray<string>;
}

export class Auth {
  private readonly digests: ReadonlyArray<Buffer>;

  constructor(config: AuthConfig) {
    const keys = config.apiKeys ?? [];
    for (const key of keys) {
      if (typeof key !== "string" || key.length === 0) {
        throw new Error("Auth: API keys must be non-empty strings");
      }
    }
    this.digests = keys.map(digest);
  }

  get isEnabled(): boolean {
    return this.digests.length > 0;
  }

  /** Whether `key` is one of the configured keys. Always false when auth is disabled. */
  matches(key: string | undefined): boolean {
    if (key === undefined || key.length === 0) return false;
    const presented = digest(key);
    let ok = false;
    for (const d of this.digests) {
      // Non-short-circuiting OR: every configured key is compared.
      ok = timingSafeEqual(presented, d) || ok;
    }
    return ok;
  }

  /** Returns true if the request carries a valid bearer key, or auth is disabled. */
  check(req: Request): boolean {
    if (!this.isEnabled) return true;
    return this.matches(bearerToken(req));
  }
}

/** The token of an `Authorization: Bearer <token>` header, if present. */
export function bearerToken(req: Request): string | undefined {
  const header = req.headers.get("authorization");
  if (!header) return undefined;
  const parts = header.trim().split(/\s+/);
  if (parts.length !== 2 || parts[0]!.toLowerCase() !== "bearer") return undefined;
  return parts[1];
}

function digest(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest();
}
