// ---------------------------------------------------------------------------
// Scoped secrets storage — distinct from the flat `SecretStore` in
// secret-store.ts (which exists for in-process bootstrap utilities).
// `SecretsStorage` adds the namespace / resource scoping that the
// multi-tenant SaaS direction needs:
//
//   global       — host-wide defaults (e.g. platform's pooled API key)
//   namespace    — team / workspace scope; one tier per tenant
//   resource     — per-(namespace, user) scope; the most specific tier
//
// Two read paths:
//   - `get({ scope, key })`   → exact-scope lookup. Returns null when
//                                that scope doesn't have the key, even
//                                if a parent scope does.
//   - `resolve({ ns?, res?, key })` → cascade resource → namespace →
//                                global, returning the FIRST hit. This
//                                is the agent-resolver call site.
//
// Writes go to the exact scope the caller specifies; there is no
// implicit cascade on write.
//
// Implementations:
//   - InMemorySecretsStorage — single-process tests + dev
//   - PostgresSecretsStorage — production (in @promin/postgres),
//                              encrypts at rest with AES-256-GCM
//
// Both must pass `secretsStorageTestSuite`.
// ---------------------------------------------------------------------------

/**
 * Discriminated scope reference. The `kind` field selects which tier
 * the secret lives at; the storage layer treats each tier independently
 * for writes, and walks up for resolve().
 */
export type SecretScope =
  | { readonly kind: "global" }
  | { readonly kind: "namespace"; readonly namespaceId: string }
  | { readonly kind: "resource"; readonly namespaceId: string; readonly resourceId: string };

/** Convenience builders so callers don't have to spell out the discriminant. */
export const SecretScope = {
  global(): SecretScope {
    return { kind: "global" };
  },
  namespace(namespaceId: string): SecretScope {
    return { kind: "namespace", namespaceId };
  },
  resource(namespaceId: string, resourceId: string): SecretScope {
    return { kind: "resource", namespaceId, resourceId };
  },
};

/**
 * Result of a cascading resolve(). Surfaces the scope the value was
 * found at so callers can decide policy ('reject if global', etc.).
 */
export interface ResolvedSecret {
  readonly value: string;
  readonly scope: SecretScope;
}

export interface SecretsStorage {
  /**
   * Exact-scope lookup. Reads ONLY at the given scope; does NOT walk
   * up. Returns null if that scope doesn't have the key, even if a
   * parent scope does.
   */
  get(params: { readonly scope: SecretScope; readonly key: string }): Promise<string | null>;

  /**
   * Cascading lookup. Walks resource → namespace → global, returning
   * the first match along with the scope it was found at. Returns
   * null when no scope has the key.
   *
   * `namespaceId` and `resourceId` are optional — when both are unset
   * this is just a global lookup; when only `namespaceId` is set it
   * walks namespace → global.
   */
  resolve(params: {
    readonly namespaceId?: string;
    readonly resourceId?: string;
    readonly key: string;
  }): Promise<ResolvedSecret | null>;

  /**
   * Write to the exact scope. Replaces existing value. Encrypted at
   * rest by implementations that persist (FileSecret pattern is
   * AES-256-GCM with scrypt-stretched passphrase).
   */
  set(params: {
    readonly scope: SecretScope;
    readonly key: string;
    readonly value: string;
  }): Promise<void>;

  /**
   * Delete from exact scope. No-op when the key isn't there.
   */
  delete(params: { readonly scope: SecretScope; readonly key: string }): Promise<void>;

  /**
   * List keys at exact scope. Returns key names ONLY — never values.
   * Used by the dashboard to show 'what's stored' without exposing
   * secrets.
   */
  list(params: { readonly scope: SecretScope }): Promise<string[]>;
}
