// ---------------------------------------------------------------------------
// `SqliteAgentIdentityRegistry` — persistent `AgentIdentityRegistry`. The
// id is composite-keyed on (namespace_id, registered_agent_id, user_id),
// stored as the deterministic `composeAgentIdentityId` slug for fast
// point lookups.
//
// Schema (auto-created on first use):
//
//   CREATE TABLE promin_agent_identity (
//     id                  TEXT PRIMARY KEY,
//     registered_agent_id TEXT NOT NULL,
//     namespace_id        TEXT NOT NULL,
//     user_id             TEXT NOT NULL,
//     display_name        TEXT,
//     metadata            TEXT NOT NULL,    -- JSON blob
//     created_at          INTEGER NOT NULL,
//     last_active_at      INTEGER NOT NULL
//   )
// ---------------------------------------------------------------------------

import {
  composeAgentIdentityId,
  type AgentIdentity,
  type AgentIdentityRegistry,
  type CreateAgentIdentityInput,
  type ListAgentIdentitiesParams,
  type UpdateAgentIdentityPatch,
} from "@promin/agent";
import type { SqliteDatabase } from "./sqlite-database.ts";

export interface SqliteAgentIdentityRegistryConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `promin_agent_identity`). */
  readonly table?: string;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

interface Row {
  id: string;
  registered_agent_id: string;
  namespace_id: string;
  user_id: string;
  display_name: string | null;
  metadata: string;
  created_at: number;
  last_active_at: number;
}

export class SqliteAgentIdentityRegistry implements AgentIdentityRegistry {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: () => number;

  private constructor(config: SqliteAgentIdentityRegistryConfig) {
    this.db = config.db;
    this.table = config.table ?? "promin_agent_identity";
    this.clock = config.now ?? (() => Date.now());
    this._setup();
  }

  static make(config: SqliteAgentIdentityRegistryConfig): SqliteAgentIdentityRegistry {
    return new SqliteAgentIdentityRegistry(config);
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id                  TEXT PRIMARY KEY,
        registered_agent_id TEXT NOT NULL,
        namespace_id        TEXT NOT NULL,
        user_id             TEXT NOT NULL,
        display_name        TEXT,
        metadata            TEXT NOT NULL,
        created_at          INTEGER NOT NULL,
        last_active_at      INTEGER NOT NULL
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_ns_user ON ${this.table} (namespace_id, user_id)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_agent ON ${this.table} (registered_agent_id)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_last_active ON ${this.table} (last_active_at DESC)`,
    );
  }

  async resolveOrCreate(input: CreateAgentIdentityInput): Promise<AgentIdentity> {
    validateNonEmpty("registeredAgentId", input.registeredAgentId);
    validateNonEmpty("namespaceId", input.namespaceId);
    validateNonEmpty("userId", input.userId);
    const id = composeAgentIdentityId(input);
    const existing = await this.get(id);
    if (existing) return existing;

    const now = this.clock();
    const metadata = JSON.stringify(input.metadata ?? {});
    this.db
      .query(
        `INSERT INTO ${this.table}
           (id, registered_agent_id, namespace_id, user_id, display_name, metadata, created_at, last_active_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO NOTHING`,
      )
      .run(
        id,
        input.registeredAgentId,
        input.namespaceId,
        input.userId,
        input.displayName ?? null,
        metadata,
        now,
        now,
      );

    // Re-read so concurrent inserts converge on the same row.
    const after = await this.get(id);
    if (!after) {
      throw new Error(`SqliteAgentIdentityRegistry: failed to create identity for ${id}`);
    }
    return after;
  }

  async get(id: string): Promise<AgentIdentity | null> {
    const row = this.db.query<Row>(`SELECT * FROM ${this.table} WHERE id = ?`).get(id);
    return row ? rowToIdentity(row) : null;
  }

  async list(params: ListAgentIdentitiesParams = {}): Promise<AgentIdentity[]> {
    const where: string[] = [];
    const args: Array<string | number> = [];
    if (params.namespaceId !== undefined) {
      where.push("namespace_id = ?");
      args.push(params.namespaceId);
    }
    if (params.userId !== undefined) {
      where.push("user_id = ?");
      args.push(params.userId);
    }
    if (params.registeredAgentId !== undefined) {
      where.push("registered_agent_id = ?");
      args.push(params.registeredAgentId);
    }
    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
    const orderClause = orderByClause(params.order ?? "lastActiveDesc");
    const limitClause = params.limit !== undefined ? "LIMIT ?" : "";
    if (params.limit !== undefined) args.push(params.limit);

    const rows = this.db
      .query<Row>(`SELECT * FROM ${this.table} ${whereClause} ${orderClause} ${limitClause}`)
      .all(...args);
    return rows.map(rowToIdentity);
  }

  async touch(id: string, lastActiveAt?: number): Promise<void> {
    const ts = lastActiveAt ?? this.clock();
    this.db.query(`UPDATE ${this.table} SET last_active_at = ? WHERE id = ?`).run(ts, id);
  }

  async update(id: string, patch: UpdateAgentIdentityPatch): Promise<AgentIdentity> {
    const existing = await this.get(id);
    if (!existing) {
      throw new Error(`AgentIdentity not found: ${id}`);
    }
    const next: AgentIdentity = {
      ...existing,
      displayName: "displayName" in patch ? (patch.displayName ?? null) : existing.displayName,
      metadata: patch.metadata !== undefined ? { ...patch.metadata } : existing.metadata,
    };
    this.db
      .query(`UPDATE ${this.table} SET display_name = ?, metadata = ? WHERE id = ?`)
      .run(next.displayName ?? null, JSON.stringify(next.metadata), id);
    return next;
  }

  async delete(id: string): Promise<void> {
    this.db.query(`DELETE FROM ${this.table} WHERE id = ?`).run(id);
  }
}

function validateNonEmpty(name: string, value: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`AgentIdentity: ${name} must be a non-empty string`);
  }
}

function orderByClause(order: NonNullable<ListAgentIdentitiesParams["order"]>): string {
  switch (order) {
    case "createdAsc":
      return "ORDER BY created_at ASC";
    case "createdDesc":
      return "ORDER BY created_at DESC";
    case "lastActiveDesc":
      return "ORDER BY last_active_at DESC";
  }
}

function rowToIdentity(row: Row): AgentIdentity {
  let metadata: Record<string, unknown> = {};
  try {
    const parsed = JSON.parse(row.metadata) as unknown;
    if (parsed && typeof parsed === "object") metadata = parsed as Record<string, unknown>;
  } catch {
    // Persisted blob got corrupted somehow — fall back to empty rather
    // than throwing. The registry's job is to surface what's there.
  }
  return {
    id: row.id,
    registeredAgentId: row.registered_agent_id,
    namespaceId: row.namespace_id,
    userId: row.user_id,
    displayName: row.display_name,
    metadata,
    createdAt: row.created_at,
    lastActiveAt: row.last_active_at,
  };
}
