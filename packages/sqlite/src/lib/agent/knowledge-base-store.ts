// ---------------------------------------------------------------------------
// `SqliteKnowledgeBaseStore` — `KnowledgeBaseStore` over SQLite. Managed
// knowledge bases (zorya's Knowledge page) persist their metadata and every
// source's text here, so the index can be rebuilt at boot.
//
// Schema (auto-created on first use; `<prefix>` defaults to `promin_knowledge`):
//
//   CREATE TABLE <prefix>_bases (
//     id TEXT PRIMARY KEY, description TEXT, tags TEXT, metadata TEXT,
//     created_at INTEGER, updated_at INTEGER
//   )
//   CREATE TABLE <prefix>_sources (
//     base_id TEXT, source_id TEXT, title TEXT, uri TEXT, mime_type TEXT,
//     tags TEXT, metadata TEXT, text TEXT, status TEXT, error TEXT,
//     chunk_count INTEGER, created_at INTEGER, updated_at INTEGER,
//     PRIMARY KEY (base_id, source_id)
//   )
// ---------------------------------------------------------------------------

import type {
  KnowledgeBaseRecord,
  KnowledgeBaseStore,
  KnowledgeSourceRecord,
  KnowledgeSourceStatus,
  StoredKnowledgeBase,
} from "@promin/agent";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteKnowledgeBaseStoreConfig {
  readonly db: SqliteDatabase;
  /** Table name prefix (default `promin_knowledge`): `<prefix>_bases`, `<prefix>_sources`. */
  readonly tablePrefix?: string;
}

interface BaseRow {
  id: string;
  description: string | null;
  tags: string;
  metadata: string;
  created_at: number;
  updated_at: number;
}

interface SourceRow {
  base_id: string;
  source_id: string;
  title: string | null;
  uri: string | null;
  mime_type: string | null;
  tags: string;
  metadata: string;
  text: string;
  status: KnowledgeSourceStatus;
  error: string | null;
  chunk_count: number;
  created_at: number;
  updated_at: number;
}

export class SqliteKnowledgeBaseStore implements KnowledgeBaseStore {
  private readonly db: SqliteDatabase;
  private readonly bases: string;
  private readonly sources: string;

  private constructor(config: SqliteKnowledgeBaseStoreConfig) {
    this.db = config.db;
    const prefix = config.tablePrefix ?? "promin_knowledge";
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(prefix)) {
      throw new Error(`SqliteKnowledgeBaseStore: invalid tablePrefix "${prefix}"`);
    }
    this.bases = `${prefix}_bases`;
    this.sources = `${prefix}_sources`;
    this.setup();
  }

  static make(config: SqliteKnowledgeBaseStoreConfig): SqliteKnowledgeBaseStore {
    return new SqliteKnowledgeBaseStore(config);
  }

  private setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.bases} (
        id          TEXT PRIMARY KEY,
        description TEXT,
        tags        TEXT NOT NULL,
        metadata    TEXT NOT NULL,
        created_at  INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL
      )
    `);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.sources} (
        base_id     TEXT NOT NULL,
        source_id   TEXT NOT NULL,
        title       TEXT,
        uri         TEXT,
        mime_type   TEXT,
        tags        TEXT NOT NULL,
        metadata    TEXT NOT NULL,
        text        TEXT NOT NULL,
        status      TEXT NOT NULL,
        error       TEXT,
        chunk_count INTEGER NOT NULL,
        created_at  INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL,
        PRIMARY KEY (base_id, source_id)
      )
    `);
  }

  async loadAll(): Promise<ReadonlyArray<StoredKnowledgeBase>> {
    const bases = this.db.query<BaseRow>(`SELECT * FROM ${this.bases} ORDER BY id`).all();
    const sources = this.db
      .query<SourceRow>(`SELECT * FROM ${this.sources} ORDER BY base_id, source_id`)
      .all();
    const byBase = new Map<string, KnowledgeSourceRecord[]>();
    for (const row of sources) {
      const list = byBase.get(row.base_id) ?? [];
      list.push(toSource(row));
      byBase.set(row.base_id, list);
    }
    return bases.map((row) => ({ base: toBase(row), sources: byBase.get(row.id) ?? [] }));
  }

  async saveBase(base: KnowledgeBaseRecord): Promise<void> {
    this.db
      .query(
        `INSERT INTO ${this.bases} (id, description, tags, metadata, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
           description = excluded.description,
           tags = excluded.tags,
           metadata = excluded.metadata,
           created_at = excluded.created_at,
           updated_at = excluded.updated_at`,
      )
      .run(
        base.id,
        base.description ?? null,
        JSON.stringify(base.tags),
        JSON.stringify(base.metadata),
        base.createdAt,
        base.updatedAt,
      );
  }

  async deleteBase(id: string): Promise<void> {
    this.db.transaction(() => {
      this.db.query(`DELETE FROM ${this.sources} WHERE base_id = ?`).run(id);
      this.db.query(`DELETE FROM ${this.bases} WHERE id = ?`).run(id);
    })();
  }

  async saveSource(params: { baseId: string; source: KnowledgeSourceRecord }): Promise<void> {
    const { baseId, source } = params;
    this.db
      .query(
        `INSERT INTO ${this.sources}
           (base_id, source_id, title, uri, mime_type, tags, metadata, text, status, error,
            chunk_count, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(base_id, source_id) DO UPDATE SET
           title = excluded.title,
           uri = excluded.uri,
           mime_type = excluded.mime_type,
           tags = excluded.tags,
           metadata = excluded.metadata,
           text = excluded.text,
           status = excluded.status,
           error = excluded.error,
           chunk_count = excluded.chunk_count,
           created_at = excluded.created_at,
           updated_at = excluded.updated_at`,
      )
      .run(
        baseId,
        source.id,
        source.title ?? null,
        source.uri ?? null,
        source.mimeType ?? null,
        JSON.stringify(source.tags),
        JSON.stringify(source.metadata),
        source.text,
        source.status,
        source.error ?? null,
        source.chunkCount,
        source.createdAt,
        source.updatedAt,
      );
  }

  async deleteSource(params: { baseId: string; sourceId: string }): Promise<void> {
    this.db
      .query(`DELETE FROM ${this.sources} WHERE base_id = ? AND source_id = ?`)
      .run(params.baseId, params.sourceId);
  }
}

function toBase(row: BaseRow): KnowledgeBaseRecord {
  return {
    id: row.id,
    ...(row.description !== null && { description: row.description }),
    tags: JSON.parse(row.tags) as string[],
    metadata: JSON.parse(row.metadata) as Record<string, unknown>,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function toSource(row: SourceRow): KnowledgeSourceRecord {
  return {
    id: row.source_id,
    ...(row.title !== null && { title: row.title }),
    ...(row.uri !== null && { uri: row.uri }),
    ...(row.mime_type !== null && { mimeType: row.mime_type }),
    tags: JSON.parse(row.tags) as string[],
    metadata: JSON.parse(row.metadata) as Record<string, unknown>,
    text: row.text,
    status: row.status,
    ...(row.error !== null && { error: row.error }),
    chunkCount: row.chunk_count,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}
