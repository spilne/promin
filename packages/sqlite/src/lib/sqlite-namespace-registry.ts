// ---------------------------------------------------------------------------
// SqliteNamespaceRegistry — durable NamespaceRegistry over SQLite.
// ---------------------------------------------------------------------------

import {
  NamespaceNotFoundError,
  normalizeNamespaceDisplayName,
  normalizeNamespaceId,
  normalizeNamespaceStatus,
  sanitizeNamespaceCapabilities,
  sanitizeNamespaceRecord,
  type Namespace,
  type NamespaceCapabilities,
  type NamespaceCreateInput,
  type NamespaceRegistry,
  type NamespaceUpdateInput,
} from "@promin/core";
import type { SqliteDatabase } from "./sqlite-database.ts";

interface Row {
  id: string;
  display_name: string;
  description: string | null;
  status: string;
  capabilities: string | null;
  metadata: string | null;
  created_at: number;
  updated_at: number;
}

export interface SqliteNamespaceRegistryConfig {
  readonly db: SqliteDatabase;
  readonly tableName?: string;
  readonly now?: () => number;
}

export class SqliteNamespaceRegistry implements NamespaceRegistry {
  private readonly table: string;
  private readonly clock: () => number;

  private constructor(
    private readonly db: SqliteDatabase,
    config: SqliteNamespaceRegistryConfig,
  ) {
    this.table = normalizeSqliteIdentifier(config.tableName ?? "promin_namespace");
    this.clock = config.now ?? (() => Date.now());
    this.setup();
  }

  static make(config: SqliteNamespaceRegistryConfig): SqliteNamespaceRegistry {
    return new SqliteNamespaceRegistry(config.db, config);
  }

  private setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id           TEXT NOT NULL PRIMARY KEY,
        display_name TEXT NOT NULL,
        description  TEXT,
        status       TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived')),
        capabilities TEXT NOT NULL DEFAULT '{}',
        metadata     TEXT NOT NULL DEFAULT '{}',
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_status_display ON ${this.table} (status, display_name, id)`,
    );
  }

  async create(input: NamespaceCreateInput): Promise<Namespace> {
    const id = normalizeNamespaceId(input.id);
    const now = this.clock();
    const row: Namespace = {
      id,
      displayName: normalizeNamespaceDisplayName(input.displayName, id),
      description: input.description ?? null,
      status: "active",
      capabilities: sanitizeNamespaceCapabilities(input.capabilities),
      metadata: sanitizeNamespaceRecord(input.metadata),
      createdAt: now,
      updatedAt: now,
    };
    try {
      this.db
        .query(
          `INSERT INTO ${this.table}
             (id, display_name, description, status, capabilities, metadata, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          row.id,
          row.displayName,
          row.description,
          row.status,
          JSON.stringify(row.capabilities),
          JSON.stringify(row.metadata),
          row.createdAt,
          row.updatedAt,
        );
    } catch (err) {
      if (String(err).includes("UNIQUE") || String(err).includes("constraint")) {
        throw new Error(`namespace already exists: ${id}`);
      }
      throw err;
    }
    return row;
  }

  async get(id: string): Promise<Namespace | null> {
    const row = this.db
      .query<Row>(`SELECT * FROM ${this.table} WHERE id = ?`)
      .get(normalizeNamespaceId(id));
    return row ? toNamespace(row) : null;
  }

  async list(params: { status?: Namespace["status"] } = {}): Promise<Namespace[]> {
    const status = params.status ? normalizeNamespaceStatus(params.status) : undefined;
    const rows = status
      ? this.db
          .query<Row>(
            `SELECT * FROM ${this.table} WHERE status = ? ORDER BY display_name ASC, id ASC`,
          )
          .all(status)
      : this.db.query<Row>(`SELECT * FROM ${this.table} ORDER BY display_name ASC, id ASC`).all();
    return rows.map(toNamespace);
  }

  async update(id: string, patch: NamespaceUpdateInput): Promise<Namespace> {
    const key = normalizeNamespaceId(id);
    const existing = await this.get(key);
    if (!existing) throw new NamespaceNotFoundError(key);
    const next: Namespace = {
      ...existing,
      ...(patch.displayName !== undefined && {
        displayName: normalizeNamespaceDisplayName(patch.displayName, key),
      }),
      ...(patch.description !== undefined && { description: patch.description }),
      ...(patch.status !== undefined && { status: normalizeNamespaceStatus(patch.status) }),
      ...(patch.capabilities !== undefined && {
        capabilities: sanitizeNamespaceCapabilities(patch.capabilities),
      }),
      ...(patch.metadata !== undefined && { metadata: sanitizeNamespaceRecord(patch.metadata) }),
      updatedAt: this.clock(),
    };
    this.db
      .query(
        `UPDATE ${this.table}
           SET display_name = ?, description = ?, status = ?, capabilities = ?, metadata = ?, updated_at = ?
         WHERE id = ?`,
      )
      .run(
        next.displayName,
        next.description,
        next.status,
        JSON.stringify(next.capabilities),
        JSON.stringify(next.metadata),
        next.updatedAt,
        key,
      );
    return next;
  }

  async archive(id: string): Promise<Namespace> {
    return this.update(id, { status: "archived" });
  }
}

function toNamespace(row: Row): Namespace {
  return {
    id: row.id,
    displayName: row.display_name,
    description: row.description,
    status: normalizeNamespaceStatus(row.status),
    capabilities: parseJsonObject<NamespaceCapabilities>(row.capabilities),
    metadata: parseJsonObject<Record<string, unknown>>(row.metadata),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function normalizeSqliteIdentifier(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error("invalid_sqlite_identifier");
  }
  return value;
}

function parseJsonObject<T extends object>(raw: string | null): T {
  if (!raw) return {} as T;
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as T)
      : ({} as T);
  } catch {
    return {} as T;
  }
}
