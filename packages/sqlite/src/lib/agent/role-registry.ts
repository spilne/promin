// ---------------------------------------------------------------------------
// `SqliteRoleRegistry` — `RoleRegistry` over SQLite. Sibling of
// `SqliteAgentRegistry`: one row per (role_id, version). The behavioral
// `definition` (persona prompt + tools + skills + capabilities) and the
// `metadata` (description / tags / suggestedSecrets) are JSON blobs so the
// shapes evolve without schema migrations.
//
// Schema (auto-created on first use):
//
//   CREATE TABLE promin_role_registry (
//     id         TEXT NOT NULL,
//     version    TEXT NOT NULL,
//     definition TEXT NOT NULL,    -- JSON blob
//     metadata   TEXT NOT NULL,    -- JSON blob
//     created_at INTEGER NOT NULL,
//     updated_at INTEGER NOT NULL,
//     PRIMARY KEY (id, version)
//   )
// ---------------------------------------------------------------------------

import type {
  ListRolesParams,
  RegisterRoleInput,
  RegisteredRole,
  RoleDefinition,
  RoleMetadata,
  RoleRegistry,
} from "@promin/agent";
import { DEFAULT_ROLE_VERSION } from "@promin/agent";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteRoleRegistryConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `promin_role_registry`). */
  readonly table?: string;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class SqliteRoleRegistry implements RoleRegistry {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: () => number;

  private constructor(config: SqliteRoleRegistryConfig) {
    this.db = config.db;
    this.table = config.table ?? "promin_role_registry";
    this.clock = config.now ?? (() => Date.now());
    this._setup();
  }

  static make(config: SqliteRoleRegistryConfig): SqliteRoleRegistry {
    return new SqliteRoleRegistry(config);
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id         TEXT NOT NULL,
        version    TEXT NOT NULL,
        definition TEXT NOT NULL,
        metadata   TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (id, version)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_id_updated ON ${this.table} (id, updated_at)`,
    );
  }

  async register(input: RegisterRoleInput): Promise<RegisteredRole> {
    const version = input.version ?? DEFAULT_ROLE_VERSION;
    const now = this.clock();
    const existing = await this.get(input.id, version);
    const metadata: RoleMetadata = {
      description: input.metadata?.description ?? null,
      tags: input.metadata?.tags ? [...input.metadata.tags] : [],
      ...(input.metadata?.suggestedSecrets !== undefined && {
        suggestedSecrets: [...input.metadata.suggestedSecrets],
      }),
    };
    const next: RegisteredRole = {
      id: input.id,
      version,
      definition: input.definition,
      metadata,
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    this.db
      .query(
        `INSERT INTO ${this.table}
           (id, version, definition, metadata, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(id, version) DO UPDATE SET
           definition = excluded.definition,
           metadata = excluded.metadata,
           updated_at = excluded.updated_at`,
      )
      .run(
        next.id,
        next.version,
        JSON.stringify(next.definition),
        JSON.stringify(next.metadata),
        next.createdAt,
        next.updatedAt,
      );
    return next;
  }

  async get(id: string, version?: string): Promise<RegisteredRole | null> {
    if (version !== undefined) {
      const row = this.db
        .query<DbRoleRow>(`SELECT * FROM ${this.table} WHERE id = ? AND version = ?`)
        .get(id, version);
      return row ? toRegisteredRole(row) : null;
    }
    const row = this.db
      .query<DbRoleRow>(
        `SELECT * FROM ${this.table} WHERE id = ?
         ORDER BY updated_at DESC, rowid DESC LIMIT 1`,
      )
      .get(id);
    return row ? toRegisteredRole(row) : null;
  }

  async list(params: ListRolesParams = {}): Promise<RegisteredRole[]> {
    const rows = this.db.query<DbRoleRow>(`SELECT * FROM ${this.table}`).all();
    let roles = rows.map(toRegisteredRole);

    // capability + tag filters happen in JS — both live inside JSON blobs,
    // so no SQL index would help.
    if (params.capability !== undefined) {
      roles = roles.filter((r) => (r.definition.capabilities ?? []).includes(params.capability!));
    }
    if (params.tag !== undefined) {
      roles = roles.filter((r) => r.metadata.tags.includes(params.tag!));
    }

    const order = params.order ?? "createdDesc";
    roles.sort((a, b) => {
      switch (order) {
        case "createdAsc":
          return a.createdAt - b.createdAt;
        case "createdDesc":
          return b.createdAt - a.createdAt;
        case "idAsc":
          if (a.id !== b.id) return a.id < b.id ? -1 : 1;
          return a.version < b.version ? -1 : 1;
      }
    });

    const offset = params.cursor ? parseInt(params.cursor, 10) || 0 : 0;
    const limit = params.limit ?? roles.length;
    return roles.slice(offset, offset + limit);
  }

  async versions(id: string): Promise<RegisteredRole[]> {
    const rows = this.db
      .query<DbRoleRow>(
        `SELECT * FROM ${this.table} WHERE id = ? ORDER BY created_at ASC, rowid ASC`,
      )
      .all(id);
    return rows.map(toRegisteredRole);
  }

  async unregister(id: string, version?: string): Promise<void> {
    if (version !== undefined) {
      this.db.query(`DELETE FROM ${this.table} WHERE id = ? AND version = ?`).run(id, version);
      return;
    }
    this.db.query(`DELETE FROM ${this.table} WHERE id = ?`).run(id);
  }
}

// ---------------------------------------------------------------------------
// DB row + decoder
// ---------------------------------------------------------------------------

interface DbRoleRow {
  id: string;
  version: string;
  definition: string;
  metadata: string;
  created_at: number;
  updated_at: number;
}

function toRegisteredRole(r: DbRoleRow): RegisteredRole {
  return {
    id: r.id,
    version: r.version,
    definition: JSON.parse(r.definition) as RoleDefinition,
    metadata: JSON.parse(r.metadata) as RoleMetadata,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}
