// ---------------------------------------------------------------------------
// `SqliteAgentInstanceRegistry` — persistent `AgentInstanceRegistry`. The
// id is composite-keyed on (namespace_id, registered_agent_id, owner_id),
// stored as the deterministic `composeAgentInstanceId` slug for fast
// point lookups.
//
// Schema (auto-created on first use):
//
//   CREATE TABLE promin_agent_instance (
//     id                  TEXT PRIMARY KEY,
//     registered_agent_id TEXT NOT NULL,
//     namespace_id        TEXT NOT NULL,
//     owner_id            TEXT NOT NULL,
//     display_name        TEXT,
//     metadata            TEXT NOT NULL,    -- JSON blob
//     created_at          INTEGER NOT NULL
//   )
// ---------------------------------------------------------------------------

import {
  composeAgentInstanceId,
  type AgentInstance,
  type AgentInstanceRegistry,
  type CreateAgentInstanceInput,
  type ListAgentInstancesParams,
  type UpdateAgentInstancePatch,
} from "@promin/agent";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteAgentInstanceRegistryConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `promin_agent_instance`). */
  readonly table?: string;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

interface Row {
  id: string;
  registered_agent_id: string;
  namespace_id: string;
  owner_id: string;
  display_name: string | null;
  metadata: string;
  created_at: number;
}

export class SqliteAgentInstanceRegistry implements AgentInstanceRegistry {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: () => number;

  private constructor(config: SqliteAgentInstanceRegistryConfig) {
    this.db = config.db;
    this.table = config.table ?? "promin_agent_instance";
    this.clock = config.now ?? (() => Date.now());
    this._setup();
  }

  static make(config: SqliteAgentInstanceRegistryConfig): SqliteAgentInstanceRegistry {
    return new SqliteAgentInstanceRegistry(config);
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id                  TEXT PRIMARY KEY,
        registered_agent_id TEXT NOT NULL,
        namespace_id        TEXT NOT NULL,
        owner_id            TEXT NOT NULL,
        display_name        TEXT,
        metadata            TEXT NOT NULL,
        created_at          INTEGER NOT NULL
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_ns_owner ON ${this.table} (namespace_id, owner_id)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_agent ON ${this.table} (registered_agent_id)`,
    );
  }

  async resolveOrCreate(input: CreateAgentInstanceInput): Promise<AgentInstance> {
    validateNonEmpty("registeredAgentId", input.registeredAgentId);
    validateNonEmpty("namespaceId", input.namespaceId);
    validateNonEmpty("ownerId", input.ownerId);
    const id = composeAgentInstanceId(input);
    const existing = await this.get(id);
    if (existing) return existing;

    const now = this.clock();
    const metadata = JSON.stringify(input.metadata ?? {});
    this.db
      .query(
        `INSERT INTO ${this.table}
           (id, registered_agent_id, namespace_id, owner_id, display_name, metadata, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO NOTHING`,
      )
      .run(
        id,
        input.registeredAgentId,
        input.namespaceId,
        input.ownerId,
        input.displayName ?? null,
        metadata,
        now,
      );

    // Re-read so concurrent inserts converge on the same row.
    const after = await this.get(id);
    if (!after) {
      throw new Error(`SqliteAgentInstanceRegistry: failed to create instance for ${id}`);
    }
    return after;
  }

  async get(id: string): Promise<AgentInstance | null> {
    const row = this.db.query<Row>(`SELECT * FROM ${this.table} WHERE id = ?`).get(id);
    return row ? rowToInstance(row) : null;
  }

  async list(params: ListAgentInstancesParams = {}): Promise<AgentInstance[]> {
    const where: string[] = [];
    const args: Array<string | number> = [];
    if (params.namespaceId !== undefined) {
      where.push("namespace_id = ?");
      args.push(params.namespaceId);
    }
    if (params.ownerId !== undefined) {
      where.push("owner_id = ?");
      args.push(params.ownerId);
    }
    if (params.registeredAgentId !== undefined) {
      where.push("registered_agent_id = ?");
      args.push(params.registeredAgentId);
    }
    const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
    const orderClause = orderByClause(params.order ?? "createdDesc");
    const limitClause = params.limit !== undefined ? "LIMIT ?" : "";
    if (params.limit !== undefined) args.push(params.limit);

    const rows = this.db
      .query<Row>(`SELECT * FROM ${this.table} ${whereClause} ${orderClause} ${limitClause}`)
      .all(...args);
    return rows.map(rowToInstance);
  }

  async update(id: string, patch: UpdateAgentInstancePatch): Promise<AgentInstance> {
    const existing = await this.get(id);
    if (!existing) {
      throw new Error(`AgentInstance not found: ${id}`);
    }
    const next: AgentInstance = {
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
    throw new Error(`AgentInstance: ${name} must be a non-empty string`);
  }
}

function orderByClause(order: NonNullable<ListAgentInstancesParams["order"]>): string {
  switch (order) {
    case "createdAsc":
      return "ORDER BY created_at ASC";
    case "createdDesc":
      return "ORDER BY created_at DESC";
  }
}

function rowToInstance(row: Row): AgentInstance {
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
    ownerId: row.owner_id,
    displayName: row.display_name,
    metadata,
    createdAt: row.created_at,
  };
}
