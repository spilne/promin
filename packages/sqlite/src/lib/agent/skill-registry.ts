// ---------------------------------------------------------------------------
// `SqliteSkillRegistry` — `SkillRegistry` over SQLite. Sibling of
// `SqliteAgentRegistry`. One row per (id, version); the content fields
// (description / when_to_use / body) are plain columns and `metadata` is a
// JSON blob so capability/tag/enabled changes land without migrations.
//
// Schema (auto-created on first use):
//
//   CREATE TABLE promin_skill_registry (
//     id          TEXT NOT NULL,
//     version     TEXT NOT NULL,
//     description TEXT NOT NULL,
//     when_to_use TEXT NOT NULL,
//     body        TEXT NOT NULL,
//     metadata    TEXT NOT NULL,   -- JSON blob
//     created_at  INTEGER NOT NULL,
//     updated_at  INTEGER NOT NULL,
//     PRIMARY KEY (id, version)
//   )
// ---------------------------------------------------------------------------

import type {
  ListSkillsParams,
  RegisterSkillInput,
  RegisteredSkill,
  SkillMetadata,
  SkillRegistry,
} from "@promin/agent";
import { DEFAULT_SKILL_VERSION } from "@promin/agent";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteSkillRegistryConfig {
  readonly db: SqliteDatabase;
  /** Override the table name (default: `promin_skill_registry`). */
  readonly table?: string;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class SqliteSkillRegistry implements SkillRegistry {
  private readonly db: SqliteDatabase;
  private readonly table: string;
  private readonly clock: () => number;

  private constructor(config: SqliteSkillRegistryConfig) {
    this.db = config.db;
    this.table = config.table ?? "promin_skill_registry";
    this.clock = config.now ?? (() => Date.now());
    this._setup();
  }

  static make(config: SqliteSkillRegistryConfig): SqliteSkillRegistry {
    return new SqliteSkillRegistry(config);
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id          TEXT NOT NULL,
        version     TEXT NOT NULL,
        description TEXT NOT NULL,
        when_to_use TEXT NOT NULL,
        body        TEXT NOT NULL,
        metadata    TEXT NOT NULL,
        created_at  INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL,
        PRIMARY KEY (id, version)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.table}_id_updated ON ${this.table} (id, updated_at)`,
    );
  }

  async register(input: RegisterSkillInput): Promise<RegisteredSkill> {
    const version = input.version ?? DEFAULT_SKILL_VERSION;
    const now = this.clock();
    const existing = await this.get(input.id, version);
    const metadata = buildMetadata(input.metadata);
    const next: RegisteredSkill = {
      id: input.id,
      version,
      description: input.description,
      // Fall back to description when an author folds the trigger into it.
      whenToUse: input.whenToUse ?? input.description,
      body: input.body,
      metadata,
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    this.db
      .query(
        `INSERT INTO ${this.table}
           (id, version, description, when_to_use, body, metadata, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id, version) DO UPDATE SET
           description = excluded.description,
           when_to_use = excluded.when_to_use,
           body = excluded.body,
           metadata = excluded.metadata,
           updated_at = excluded.updated_at`,
      )
      .run(
        next.id,
        next.version,
        next.description,
        next.whenToUse,
        next.body,
        JSON.stringify(next.metadata),
        next.createdAt,
        next.updatedAt,
      );
    return next;
  }

  async get(id: string, version?: string): Promise<RegisteredSkill | null> {
    if (version !== undefined) {
      const row = this.db
        .query<DbSkillRow>(`SELECT * FROM ${this.table} WHERE id = ? AND version = ?`)
        .get(id, version);
      return row ? toRegisteredSkill(row) : null;
    }
    const row = this.db
      .query<DbSkillRow>(
        `SELECT * FROM ${this.table} WHERE id = ?
         ORDER BY updated_at DESC, rowid DESC LIMIT 1`,
      )
      .get(id);
    return row ? toRegisteredSkill(row) : null;
  }

  async list(params: ListSkillsParams = {}): Promise<RegisteredSkill[]> {
    const rows = this.db.query<DbSkillRow>(`SELECT * FROM ${this.table}`).all();
    let skills = rows.map(toRegisteredSkill);

    // capability + tag live inside the metadata JSON blob — filter in JS.
    if (params.capability !== undefined) {
      skills = skills.filter((s) => s.metadata.capabilities.includes(params.capability!));
    }
    if (params.tag !== undefined) {
      skills = skills.filter((s) => s.metadata.tags.includes(params.tag!));
    }

    const order = params.order ?? "createdDesc";
    skills.sort((a, b) => {
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
    const limit = params.limit ?? skills.length;
    return skills.slice(offset, offset + limit);
  }

  async versions(id: string): Promise<RegisteredSkill[]> {
    const rows = this.db
      .query<DbSkillRow>(
        `SELECT * FROM ${this.table} WHERE id = ? ORDER BY created_at ASC, rowid ASC`,
      )
      .all(id);
    return rows.map(toRegisteredSkill);
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
// DB row + helpers
// ---------------------------------------------------------------------------

interface DbSkillRow {
  id: string;
  version: string;
  description: string;
  when_to_use: string;
  body: string;
  metadata: string;
  created_at: number;
  updated_at: number;
}

function buildMetadata(patch?: Partial<SkillMetadata>): SkillMetadata {
  return {
    capabilities: patch?.capabilities ? [...patch.capabilities] : [],
    tags: patch?.tags ? [...patch.tags] : [],
    ...(patch?.enabled !== undefined && { enabled: patch.enabled }),
    // Pass trust through — the scanner's needs-review default and an
    // operator's Approve both flow via metadata.trust on register.
    ...(patch?.trust !== undefined && { trust: patch.trust }),
  };
}

function toRegisteredSkill(r: DbSkillRow): RegisteredSkill {
  return {
    id: r.id,
    version: r.version,
    description: r.description,
    whenToUse: r.when_to_use,
    body: r.body,
    metadata: JSON.parse(r.metadata) as SkillMetadata,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}
