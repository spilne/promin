// ---------------------------------------------------------------------------
// SqliteSecretsStorage — encrypted scoped secret vault on SQLite.
//
// Mirror of PostgresSecretsStorage; same encryption (AES-256-GCM with
// scrypt-stretched passphrase, same scrypt salt as FileSecretStore so
// passphrases interop) so all three backends round-trip plaintext
// identically through the conformance suite.
//
// Schema (auto-created on first use): one table for all three scopes,
// scope_kind discriminator + empty-string defaults on namespace_id /
// resource_id. Same null-coalescing convention as the PG schema so the
// PRIMARY KEY enforces uniqueness without SQLite's NULL-aware quirks.
// ---------------------------------------------------------------------------

import { createCipheriv, createDecipheriv, randomBytes, scryptSync } from "node:crypto";
import type { ResolvedSecret, SecretScope, SecretsStorage } from "@promin/agent";
import type { SqliteDatabase } from "./sqlite-database.ts";

export interface SqliteSecretsStorageConfig {
  readonly db: SqliteDatabase;
  /**
   * Host-supplied passphrase. Stretched via scrypt to a 32-byte AES
   * key on construction. Same salt as FileSecretStore for migration
   * compatibility.
   */
  readonly passphrase: string;
  /** Override the table name (default: `promin_secrets`). */
  readonly table?: string;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

interface DbRow {
  scope_kind: string;
  namespace_id: string;
  resource_id: string;
  secret_key: string;
  iv: string;
  auth_tag: string;
  ciphertext: string;
  created_at: number;
  updated_at: number;
}

export class SqliteSecretsStorage implements SecretsStorage {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: () => number;
  private readonly key: Buffer;

  private constructor(config: SqliteSecretsStorageConfig) {
    this.db = config.db;
    this.table = config.table ?? "promin_secrets";
    this.clock = config.now ?? (() => Date.now());
    this.key = scryptSync(config.passphrase, "promin-agent-secrets-v1", 32);
    this._setup();
  }

  static make(config: SqliteSecretsStorageConfig): SqliteSecretsStorage {
    return new SqliteSecretsStorage(config);
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        scope_kind   TEXT NOT NULL,
        namespace_id TEXT NOT NULL DEFAULT '',
        resource_id  TEXT NOT NULL DEFAULT '',
        secret_key   TEXT NOT NULL,
        iv           TEXT NOT NULL,
        auth_tag     TEXT NOT NULL,
        ciphertext   TEXT NOT NULL,
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL,
        PRIMARY KEY (scope_kind, namespace_id, resource_id, secret_key)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_scope_idx ON ${this.table} (scope_kind, namespace_id, resource_id)`,
    );
  }

  async get(params: { scope: SecretScope; key: string }): Promise<string | null> {
    const { scopeKind, namespaceId, resourceId } = encodeScope(params.scope);
    const row = this.db
      .query<DbRow>(
        `SELECT * FROM ${this.table}
         WHERE scope_kind = ? AND namespace_id = ? AND resource_id = ? AND secret_key = ?`,
      )
      .get(scopeKind, namespaceId, resourceId, params.key);
    return row ? this.decrypt(row) : null;
  }

  async resolve(params: {
    namespaceId?: string;
    resourceId?: string;
    key: string;
  }): Promise<ResolvedSecret | null> {
    // Walk most-specific to least: try resource → namespace → global,
    // return first hit. Three sequential queries are cheap on SQLite
    // (in-process, no network); the union approach used by PG offers
    // no real win here.
    if (params.namespaceId && params.resourceId) {
      const v = await this.get({
        scope: { kind: "resource", namespaceId: params.namespaceId, resourceId: params.resourceId },
        key: params.key,
      });
      if (v !== null) {
        return {
          value: v,
          scope: {
            kind: "resource",
            namespaceId: params.namespaceId,
            resourceId: params.resourceId,
          },
        };
      }
    }
    if (params.namespaceId) {
      const v = await this.get({
        scope: { kind: "namespace", namespaceId: params.namespaceId },
        key: params.key,
      });
      if (v !== null) {
        return { value: v, scope: { kind: "namespace", namespaceId: params.namespaceId } };
      }
    }
    const v = await this.get({ scope: { kind: "global" }, key: params.key });
    if (v !== null) return { value: v, scope: { kind: "global" } };
    return null;
  }

  async set(params: { scope: SecretScope; key: string; value: string }): Promise<void> {
    const { scopeKind, namespaceId, resourceId } = encodeScope(params.scope);
    const enc = this.encrypt(params.value);
    const now = this.clock();
    this.db
      .query(
        `INSERT INTO ${this.table}
           (scope_kind, namespace_id, resource_id, secret_key, iv, auth_tag, ciphertext, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(scope_kind, namespace_id, resource_id, secret_key) DO UPDATE SET
           iv = excluded.iv,
           auth_tag = excluded.auth_tag,
           ciphertext = excluded.ciphertext,
           updated_at = excluded.updated_at`,
      )
      .run(
        scopeKind,
        namespaceId,
        resourceId,
        params.key,
        enc.iv,
        enc.authTag,
        enc.ciphertext,
        now,
        now,
      );
  }

  async delete(params: { scope: SecretScope; key: string }): Promise<void> {
    const { scopeKind, namespaceId, resourceId } = encodeScope(params.scope);
    this.db
      .query(
        `DELETE FROM ${this.table}
         WHERE scope_kind = ? AND namespace_id = ? AND resource_id = ? AND secret_key = ?`,
      )
      .run(scopeKind, namespaceId, resourceId, params.key);
  }

  async list(params: { scope: SecretScope }): Promise<string[]> {
    const { scopeKind, namespaceId, resourceId } = encodeScope(params.scope);
    const rows = this.db
      .query<{ secret_key: string }>(
        `SELECT secret_key FROM ${this.table}
         WHERE scope_kind = ? AND namespace_id = ? AND resource_id = ?
         ORDER BY secret_key ASC`,
      )
      .all(scopeKind, namespaceId, resourceId);
    return rows.map((r) => r.secret_key);
  }

  // --- crypto helpers --------------------------------------------------

  private encrypt(plaintext: string): { iv: string; authTag: string; ciphertext: string } {
    const iv = randomBytes(12);
    const cipher = createCipheriv("aes-256-gcm", this.key, iv);
    const ciphertext = cipher.update(plaintext, "utf8", "hex") + cipher.final("hex");
    return {
      iv: iv.toString("hex"),
      authTag: cipher.getAuthTag().toString("hex"),
      ciphertext,
    };
  }

  private decrypt(row: { iv: string; auth_tag: string; ciphertext: string }): string {
    const iv = Buffer.from(row.iv, "hex");
    const tag = Buffer.from(row.auth_tag, "hex");
    const decipher = createDecipheriv("aes-256-gcm", this.key, iv);
    decipher.setAuthTag(tag);
    return decipher.update(row.ciphertext, "hex", "utf8") + decipher.final("utf8");
  }
}

function encodeScope(scope: SecretScope): {
  scopeKind: string;
  namespaceId: string;
  resourceId: string;
} {
  switch (scope.kind) {
    case "global":
      return { scopeKind: "global", namespaceId: "", resourceId: "" };
    case "namespace":
      return { scopeKind: "namespace", namespaceId: scope.namespaceId, resourceId: "" };
    case "resource":
      return {
        scopeKind: "resource",
        namespaceId: scope.namespaceId,
        resourceId: scope.resourceId,
      };
  }
}
