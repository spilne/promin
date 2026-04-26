// ---------------------------------------------------------------------------
// `SqliteAgentRegistry` — `AgentRegistry` over SQLite. JSON-encodes the
// `backend` and `metadata` blobs so future backend variants land without
// schema migrations.
//
// Schema (auto-created on first use):
//
//   CREATE TABLE promin_agent_registry (
//     id           TEXT NOT NULL,
//     version      TEXT NOT NULL,
//     backend_type TEXT NOT NULL,
//     backend      TEXT NOT NULL,    -- JSON blob
//     metadata     TEXT NOT NULL,    -- JSON blob
//     created_at   INTEGER NOT NULL,
//     updated_at   INTEGER NOT NULL,
//     PRIMARY KEY (id, version)
//   )
// ---------------------------------------------------------------------------

import type {
  AgentBackend,
  AgentMetadata,
  AgentRegistry,
  ListAgentsParams,
  RegisterAgentInput,
  RegisteredAgent,
} from "@promin/agent";
import { DEFAULT_AGENT_VERSION } from "@promin/agent";
import type { SqliteDatabase } from "./sqlite-database.ts";

export interface SqliteAgentRegistryConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `promin_agent_registry`). */
  readonly table?: string;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class SqliteAgentRegistry implements AgentRegistry {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: () => number;

  private constructor(config: SqliteAgentRegistryConfig) {
    this.db = config.db;
    this.table = config.table ?? "promin_agent_registry";
    this.clock = config.now ?? (() => Date.now());
    this._setup();
  }

  static make(config: SqliteAgentRegistryConfig): SqliteAgentRegistry {
    return new SqliteAgentRegistry(config);
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id           TEXT NOT NULL,
        version      TEXT NOT NULL,
        backend_type TEXT NOT NULL,
        backend      TEXT NOT NULL,
        metadata     TEXT NOT NULL,
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL,
        PRIMARY KEY (id, version)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_id_updated ON ${this.table} (id, updated_at)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_backend_type ON ${this.table} (backend_type)`,
    );
  }

  async register(input: RegisterAgentInput): Promise<RegisteredAgent> {
    const version = input.version ?? DEFAULT_AGENT_VERSION;
    const now = this.clock();
    const existing = await this.get(input.id, version);
    const metadata: AgentMetadata = {
      description: input.metadata?.description ?? null,
      capabilities: input.metadata?.capabilities ? [...input.metadata.capabilities] : [],
      tags: input.metadata?.tags ? [...input.metadata.tags] : [],
    };
    const next: RegisteredAgent = {
      id: input.id,
      version,
      backend: input.backend,
      metadata,
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    this.db
      .query(
        `INSERT INTO ${this.table}
           (id, version, backend_type, backend, metadata, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id, version) DO UPDATE SET
           backend_type = excluded.backend_type,
           backend = excluded.backend,
           metadata = excluded.metadata,
           updated_at = excluded.updated_at`,
      )
      .run(
        next.id,
        next.version,
        next.backend.type,
        JSON.stringify(next.backend),
        JSON.stringify(next.metadata),
        next.createdAt,
        next.updatedAt,
      );
    return next;
  }

  async get(id: string, version?: string): Promise<RegisteredAgent | null> {
    if (version !== undefined) {
      const row = this.db
        .query<DbAgentRow>(`SELECT * FROM ${this.table} WHERE id = ? AND version = ?`)
        .get(id, version);
      return row ? toRegisteredAgent(row) : null;
    }
    const row = this.db
      .query<DbAgentRow>(
        `SELECT * FROM ${this.table} WHERE id = ?
         ORDER BY updated_at DESC, rowid DESC LIMIT 1`,
      )
      .get(id);
    return row ? toRegisteredAgent(row) : null;
  }

  async list(params: ListAgentsParams = {}): Promise<RegisteredAgent[]> {
    const where: string[] = [];
    const args: unknown[] = [];
    if (params.backendType) {
      where.push("backend_type = ?");
      args.push(params.backendType);
    }
    const whereClause = where.length > 0 ? ` WHERE ${where.join(" AND ")}` : "";
    const rows = this.db
      .query<DbAgentRow>(`SELECT * FROM ${this.table}${whereClause}`)
      .all(...args);

    let agents = rows.map(toRegisteredAgent);

    // capability + tag filters happen in JS — capabilities/tags live inside
    // the metadata JSON blob, no SQL index will help.
    if (params.capability !== undefined) {
      agents = agents.filter((a) => a.metadata.capabilities.includes(params.capability!));
    }
    if (params.tag !== undefined) {
      agents = agents.filter((a) => a.metadata.tags.includes(params.tag!));
    }

    const order = params.order ?? "createdDesc";
    agents.sort((a, b) => {
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
    const limit = params.limit ?? agents.length;
    return agents.slice(offset, offset + limit);
  }

  async versions(id: string): Promise<RegisteredAgent[]> {
    const rows = this.db
      .query<DbAgentRow>(
        `SELECT * FROM ${this.table} WHERE id = ? ORDER BY created_at ASC, rowid ASC`,
      )
      .all(id);
    return rows.map(toRegisteredAgent);
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

interface DbAgentRow {
  id: string;
  version: string;
  backend_type: string;
  backend: string;
  metadata: string;
  created_at: number;
  updated_at: number;
}

function toRegisteredAgent(r: DbAgentRow): RegisteredAgent {
  return {
    id: r.id,
    version: r.version,
    backend: JSON.parse(r.backend) as AgentBackend,
    metadata: JSON.parse(r.metadata) as AgentMetadata,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}
