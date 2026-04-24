// ---------------------------------------------------------------------------
// Auth — simple API key middleware.
//
// Keys are configured at ZoryaServer construction time. If no keys are
// configured the server runs unauthenticated (suitable for local dev only).
// ---------------------------------------------------------------------------

export interface AuthConfig {
  /** Allowed API keys. When empty or undefined, auth is disabled. */
  apiKeys?: ReadonlyArray<string>;
}

export class Auth {
  private readonly keys: Set<string>;

  constructor(config: AuthConfig) {
    this.keys = new Set(config.apiKeys ?? []);
  }

  get isEnabled(): boolean {
    return this.keys.size > 0;
  }

  /** Returns true if the request carries a valid key, or auth is disabled. */
  check(req: Request): boolean {
    if (!this.isEnabled) return true;
    const header = req.headers.get("authorization");
    if (!header) return false;
    const parts = header.split(/\s+/);
    if (parts.length !== 2 || parts[0].toLowerCase() !== "bearer") return false;
    return this.keys.has(parts[1]!);
  }
}
