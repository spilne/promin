// ---------------------------------------------------------------------------
// PostgresSecretsStorage — encrypted scoped secret vault on Postgres.
//
// Encryption: AES-256-GCM, key derived from a host-supplied passphrase
// via scrypt. Mirrors the FileSecretStore pattern from @promin/agent so
// the security primitives are already battle-tested. Secrets-at-rest
// stay opaque without the passphrase; a DB-only leak doesn't expose
// values.
//
// Three scopes (global / namespace / resource) share one table — the
// `scope_kind` column discriminates. Empty-string defaults on
// namespace_id / resource_id let the PK enforce uniqueness without
// PG's null-aware semantics (NULLs are not equal under UNIQUE).
//
// Cascade on `resolve()`: query all relevant scope tiers in one round
// trip with three OR'd subqueries, then pick the most-specific match
// in JS (resource > namespace > global).
// ---------------------------------------------------------------------------

import { createCipheriv, createDecipheriv, randomBytes, scryptSync } from "node:crypto";
import { and, eq, inArray, sql } from "drizzle-orm";
import type { ResolvedSecret, SecretScope, SecretsStorage } from "@promin/agent";
import type { DrizzleDb } from "../drizzle-db.ts";
import { agentSecret } from "../schema.ts";

export interface PostgresSecretsStorageConfig {
  readonly db: DrizzleDb;
  /**
   * Host-supplied passphrase. Stretched via scrypt to a 32-byte AES
   * key on construction. Rotating the passphrase requires re-encrypting
   * every existing row — call `migrate({ oldPassphrase, newPassphrase })`
   * for that flow (not yet implemented; tracked separately).
   */
  readonly passphrase: string;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class PostgresSecretsStorage implements SecretsStorage {
  private readonly db: DrizzleDb;
  private readonly clock: () => number;
  private readonly key: Buffer;

  constructor(config: PostgresSecretsStorageConfig) {
    this.db = config.db;
    this.clock = config.now ?? (() => Date.now());
    // Same salt as FileSecretStore — a passphrase set under the file
    // store reads under the PG store and vice versa, so operators can
    // migrate without re-collecting passwords from users.
    this.key = scryptSync(config.passphrase, "promin-agent-secrets-v1", 32);
  }

  async get(params: { scope: SecretScope; key: string }): Promise<string | null> {
    const { scopeKind, namespaceId, resourceId } = encodeScope(params.scope);
    const rows = await this.db
      .select()
      .from(agentSecret)
      .where(
        and(
          eq(agentSecret.scopeKind, scopeKind),
          eq(agentSecret.namespaceId, namespaceId),
          eq(agentSecret.resourceId, resourceId),
          eq(agentSecret.secretKey, params.key),
        ),
      )
      .limit(1);
    const row = rows[0];
    return row ? this.decrypt(row) : null;
  }

  async resolve(params: {
    namespaceId?: string;
    resourceId?: string;
    key: string;
  }): Promise<ResolvedSecret | null> {
    // Build the union of scope tuples to look up in one query, then
    // pick the most-specific match in JS. At most three rows come back.
    const tuples: Array<[string, string, string, "global" | "namespace" | "resource"]> = [];
    if (params.namespaceId && params.resourceId) {
      tuples.push(["resource", params.namespaceId, params.resourceId, "resource"]);
    }
    if (params.namespaceId) {
      tuples.push(["namespace", params.namespaceId, "", "namespace"]);
    }
    tuples.push(["global", "", "", "global"]);

    const orClauses = tuples.map(
      ([kind, ns, res]) =>
        sql`(${agentSecret.scopeKind} = ${kind} AND ${agentSecret.namespaceId} = ${ns} AND ${agentSecret.resourceId} = ${res})`,
    );
    const rows = await this.db
      .select()
      .from(agentSecret)
      .where(and(eq(agentSecret.secretKey, params.key), sql.join(orClauses, sql` OR `)));

    // Rank by specificity: resource > namespace > global.
    const rank = { resource: 0, namespace: 1, global: 2 } as const;
    let best: { row: (typeof rows)[number]; kind: "global" | "namespace" | "resource" } | null =
      null;
    for (const r of rows) {
      const kind = r.scopeKind as "global" | "namespace" | "resource";
      if (!best || rank[kind] < rank[best.kind]) {
        best = { row: r, kind };
      }
    }
    if (!best) return null;
    return {
      value: this.decrypt(best.row),
      scope: scopeFromRow(best.row),
    };
  }

  async set(params: { scope: SecretScope; key: string; value: string }): Promise<void> {
    const { scopeKind, namespaceId, resourceId } = encodeScope(params.scope);
    const enc = this.encrypt(params.value);
    const now = this.clock();
    await this.db
      .insert(agentSecret)
      .values({
        scopeKind,
        namespaceId,
        resourceId,
        secretKey: params.key,
        iv: enc.iv,
        authTag: enc.authTag,
        ciphertext: enc.ciphertext,
        createdAt: now,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: [
          agentSecret.scopeKind,
          agentSecret.namespaceId,
          agentSecret.resourceId,
          agentSecret.secretKey,
        ],
        set: {
          iv: enc.iv,
          authTag: enc.authTag,
          ciphertext: enc.ciphertext,
          updatedAt: now,
        },
      });
  }

  async delete(params: { scope: SecretScope; key: string }): Promise<void> {
    const { scopeKind, namespaceId, resourceId } = encodeScope(params.scope);
    await this.db
      .delete(agentSecret)
      .where(
        and(
          eq(agentSecret.scopeKind, scopeKind),
          eq(agentSecret.namespaceId, namespaceId),
          eq(agentSecret.resourceId, resourceId),
          eq(agentSecret.secretKey, params.key),
        ),
      );
  }

  async list(params: { scope: SecretScope }): Promise<string[]> {
    const { scopeKind, namespaceId, resourceId } = encodeScope(params.scope);
    const rows = await this.db
      .select({ key: agentSecret.secretKey })
      .from(agentSecret)
      .where(
        and(
          eq(agentSecret.scopeKind, scopeKind),
          eq(agentSecret.namespaceId, namespaceId),
          eq(agentSecret.resourceId, resourceId),
        ),
      );
    return rows.map((r) => r.key).sort();
  }

  // --- crypto helpers ----------------------------------------------------

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

  private decrypt(row: { iv: string; authTag: string; ciphertext: string }): string {
    const iv = Buffer.from(row.iv, "hex");
    const tag = Buffer.from(row.authTag, "hex");
    const decipher = createDecipheriv("aes-256-gcm", this.key, iv);
    decipher.setAuthTag(tag);
    return decipher.update(row.ciphertext, "hex", "utf8") + decipher.final("utf8");
  }
}

// --- scope encoding -----------------------------------------------------

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

function scopeFromRow(row: {
  scopeKind: string;
  namespaceId: string;
  resourceId: string;
}): SecretScope {
  if (row.scopeKind === "global") return { kind: "global" };
  if (row.scopeKind === "namespace") {
    return { kind: "namespace", namespaceId: row.namespaceId };
  }
  return { kind: "resource", namespaceId: row.namespaceId, resourceId: row.resourceId };
}

// Suppress unused import warning — `inArray` may land in a future query
// optimization (single-roundtrip multi-key resolve). Keep imported so
// the file's intent is clear.
void inArray;
