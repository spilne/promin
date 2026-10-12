// ---------------------------------------------------------------------------
// `SqliteKnowledgeBaseStore` — `KnowledgeBaseStore` over SQLite. Managed
// knowledge bases (zorya's Knowledge page) persist their metadata and every
// source's text and ingestion state here.
//
// Schema (auto-created on first use; `<prefix>` defaults to `promin_knowledge`):
//
//   CREATE TABLE <prefix>_bases (
//     namespace_id TEXT, id TEXT, description TEXT, tags TEXT, metadata TEXT,
//     created_at INTEGER, updated_at INTEGER,
//     PRIMARY KEY (namespace_id, id)
//   )
//   CREATE TABLE <prefix>_sources (
//     namespace_id TEXT, base_id TEXT, source_id TEXT, title TEXT, uri TEXT,
//     mime_type TEXT, tags TEXT, metadata TEXT, text TEXT, status TEXT,
//     error TEXT, chunk_count INTEGER, created_at INTEGER, updated_at INTEGER,
//     claimed_by TEXT, claimed_at INTEGER,
//     PRIMARY KEY (namespace_id, base_id, source_id)
//   )
//
// One SQLite file has one host, so claim ages use the injected `WallClock`.
// A claim is one `UPDATE … RETURNING` statement, atomic under SQLite's
// write lock.
// ---------------------------------------------------------------------------

import type {
  ClaimedKnowledgeSource,
  KnowledgeBaseKey,
  KnowledgeBaseRecord,
  KnowledgeBaseStore,
  KnowledgeSourceKey,
  KnowledgeSourceRecord,
  KnowledgeSourceStatus,
  KnowledgeSourceSummary,
  StoredKnowledgeBase,
} from "@promin/agent";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { SqliteDatabase } from "../sqlite-database.ts";

export interface SqliteKnowledgeBaseStoreConfig {
  readonly db: SqliteDatabase;
  /** Table name prefix (default `promin_knowledge`): `<prefix>_bases`, `<prefix>_sources`. */
  readonly tablePrefix?: string;
  /** Time source of claim ages. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

interface BaseRow {
  namespace_id: string;
  id: string;
  description: string | null;
  tags: string;
  metadata: string;
  created_at: number;
  updated_at: number;
}

interface SourceRow {
  namespace_id: string;
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

type SummaryRow = Omit<SourceRow, "text">;

const SUMMARY_COLUMNS =
  "namespace_id, base_id, source_id, title, uri, mime_type, tags, metadata, status, error, " +
  "chunk_count, created_at, updated_at";

export class SqliteKnowledgeBaseStore implements KnowledgeBaseStore {
  private readonly db: SqliteDatabase;
  private readonly bases: string;
  private readonly sources: string;
  private readonly clock: WallClock;

  private constructor(config: SqliteKnowledgeBaseStoreConfig) {
    this.db = config.db;
    const prefix = config.tablePrefix ?? "promin_knowledge";
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(prefix)) {
      throw new Error(`SqliteKnowledgeBaseStore: invalid tablePrefix "${prefix}"`);
    }
    this.bases = `${prefix}_bases`;
    this.sources = `${prefix}_sources`;
    this.clock = config.clock ?? SystemWallClock;
    this.setup();
  }

  static make(config: SqliteKnowledgeBaseStoreConfig): SqliteKnowledgeBaseStore {
    return new SqliteKnowledgeBaseStore(config);
  }

  private setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.bases} (
        namespace_id TEXT NOT NULL,
        id           TEXT NOT NULL,
        description  TEXT,
        tags         TEXT NOT NULL,
        metadata     TEXT NOT NULL,
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL,
        PRIMARY KEY (namespace_id, id)
      )
    `);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.sources} (
        namespace_id TEXT NOT NULL,
        base_id      TEXT NOT NULL,
        source_id    TEXT NOT NULL,
        title        TEXT,
        uri          TEXT,
        mime_type    TEXT,
        tags         TEXT NOT NULL,
        metadata     TEXT NOT NULL,
        text         TEXT NOT NULL,
        status       TEXT NOT NULL,
        error        TEXT,
        chunk_count  INTEGER NOT NULL,
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL,
        claimed_by   TEXT,
        claimed_at   INTEGER,
        PRIMARY KEY (namespace_id, base_id, source_id)
      )
    `);
    this.db.run(`
      CREATE INDEX IF NOT EXISTS ${this.sources}_queue_idx ON ${this.sources} (updated_at)
        WHERE status IN ('pending', 'ingesting')
    `);
  }

  async listBases(): Promise<ReadonlyArray<StoredKnowledgeBase>> {
    const bases = this.db
      .query<BaseRow>(`SELECT * FROM ${this.bases} ORDER BY namespace_id, id`)
      .all();
    const sources = this.db
      .query<SummaryRow>(
        `SELECT ${SUMMARY_COLUMNS} FROM ${this.sources} ORDER BY namespace_id, base_id, source_id`,
      )
      .all();
    const byBase = new Map<string, KnowledgeSourceSummary[]>();
    for (const row of sources) {
      const key = JSON.stringify([row.namespace_id, row.base_id]);
      const list = byBase.get(key) ?? [];
      list.push(toSummary(row));
      byBase.set(key, list);
    }
    return bases.map((row) => ({
      base: toBase(row),
      sources: byBase.get(JSON.stringify([row.namespace_id, row.id])) ?? [],
    }));
  }

  async getBase(key: KnowledgeBaseKey): Promise<StoredKnowledgeBase | null> {
    const row = this.db
      .query<BaseRow>(`SELECT * FROM ${this.bases} WHERE namespace_id = ? AND id = ?`)
      .get(key.namespaceId, key.id);
    if (!row) return null;
    const sources = this.db
      .query<SummaryRow>(
        `SELECT ${SUMMARY_COLUMNS} FROM ${this.sources}
         WHERE namespace_id = ? AND base_id = ? ORDER BY source_id`,
      )
      .all(key.namespaceId, key.id);
    return { base: toBase(row), sources: sources.map(toSummary) };
  }

  async getSource(key: KnowledgeSourceKey): Promise<KnowledgeSourceRecord | null> {
    const row = this.db
      .query<SourceRow>(
        `SELECT * FROM ${this.sources} WHERE namespace_id = ? AND base_id = ? AND source_id = ?`,
      )
      .get(key.namespaceId, key.baseId, key.sourceId);
    return row ? toSource(row) : null;
  }

  async saveBase(base: KnowledgeBaseRecord): Promise<void> {
    this.db
      .query(
        `INSERT INTO ${this.bases}
           (namespace_id, id, description, tags, metadata, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(namespace_id, id) DO UPDATE SET
           description = excluded.description,
           tags = excluded.tags,
           metadata = excluded.metadata,
           created_at = excluded.created_at,
           updated_at = excluded.updated_at`,
      )
      .run(
        base.namespaceId,
        base.id,
        base.description ?? null,
        JSON.stringify(base.tags),
        JSON.stringify(base.metadata),
        base.createdAt,
        base.updatedAt,
      );
  }

  async deleteBase(key: KnowledgeBaseKey): Promise<void> {
    this.db.transaction(() => {
      this.db
        .query(`DELETE FROM ${this.sources} WHERE namespace_id = ? AND base_id = ?`)
        .run(key.namespaceId, key.id);
      this.db
        .query(`DELETE FROM ${this.bases} WHERE namespace_id = ? AND id = ?`)
        .run(key.namespaceId, key.id);
    })();
  }

  async saveSource(params: {
    namespaceId: string;
    baseId: string;
    source: KnowledgeSourceRecord;
  }): Promise<void> {
    const { namespaceId, baseId, source } = params;
    this.db
      .query(
        `INSERT INTO ${this.sources}
           (namespace_id, base_id, source_id, title, uri, mime_type, tags, metadata, text, status,
            error, chunk_count, created_at, updated_at, claimed_by, claimed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
         ON CONFLICT(namespace_id, base_id, source_id) DO UPDATE SET
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
           updated_at = excluded.updated_at,
           claimed_by = NULL,
           claimed_at = NULL`,
      )
      .run(namespaceId, baseId, source.id, ...sourceColumns(source));
  }

  async deleteSource(key: KnowledgeSourceKey): Promise<void> {
    this.db
      .query(`DELETE FROM ${this.sources} WHERE namespace_id = ? AND base_id = ? AND source_id = ?`)
      .run(key.namespaceId, key.baseId, key.sourceId);
  }

  async claimSource(params: {
    workerId: string;
    staleAfterMs: number;
  }): Promise<ClaimedKnowledgeSource | null> {
    const now = this.clock.currentTimeMs();
    const row = this.db
      .query<SourceRow>(
        `UPDATE ${this.sources}
         SET status = 'ingesting', claimed_by = ?, claimed_at = ?
         WHERE rowid = (
           SELECT rowid FROM ${this.sources}
           WHERE status = 'pending' OR (status = 'ingesting' AND claimed_at < ?)
           ORDER BY updated_at, namespace_id, base_id, source_id
           LIMIT 1
         )
         RETURNING *`,
      )
      .get(params.workerId, now, now - params.staleAfterMs);
    if (!row) return null;
    return { namespaceId: row.namespace_id, baseId: row.base_id, source: toSource(row) };
  }

  async completeSource(params: {
    namespaceId: string;
    baseId: string;
    workerId: string;
    source: KnowledgeSourceRecord;
  }): Promise<boolean> {
    const { source } = params;
    const rows = this.db
      .query<{ source_id: string }>(
        `UPDATE ${this.sources}
         SET title = ?, uri = ?, mime_type = ?, tags = ?, metadata = ?, text = ?, status = ?,
             error = ?, chunk_count = ?, created_at = ?, updated_at = ?,
             claimed_by = NULL, claimed_at = NULL
         WHERE namespace_id = ? AND base_id = ? AND source_id = ?
           AND status = 'ingesting' AND claimed_by = ?
         RETURNING source_id`,
      )
      .all(...sourceColumns(source), params.namespaceId, params.baseId, source.id, params.workerId);
    return rows.length > 0;
  }

  async heartbeatClaim(params: KnowledgeSourceKey & { workerId: string }): Promise<boolean> {
    const rows = this.db
      .query<{ source_id: string }>(
        `UPDATE ${this.sources} SET claimed_at = ?
         WHERE namespace_id = ? AND base_id = ? AND source_id = ?
           AND status = 'ingesting' AND claimed_by = ?
         RETURNING source_id`,
      )
      .all(
        this.clock.currentTimeMs(),
        params.namespaceId,
        params.baseId,
        params.sourceId,
        params.workerId,
      );
    return rows.length > 0;
  }

  async releaseClaims(params: { workerId: string }): Promise<number> {
    const rows = this.db
      .query<{ source_id: string }>(
        `UPDATE ${this.sources}
         SET status = 'pending', claimed_by = NULL, claimed_at = NULL
         WHERE status = 'ingesting' AND claimed_by = ?
         RETURNING source_id`,
      )
      .all(params.workerId);
    return rows.length;
  }
}

/** The source's columns from `title` through `updated_at`, in table order. */
function sourceColumns(
  source: KnowledgeSourceRecord,
): [
  string | null,
  string | null,
  string | null,
  string,
  string,
  string,
  KnowledgeSourceStatus,
  string | null,
  number,
  number,
  number,
] {
  return [
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
  ];
}

function toBase(row: BaseRow): KnowledgeBaseRecord {
  return {
    namespaceId: row.namespace_id,
    id: row.id,
    ...(row.description !== null && { description: row.description }),
    tags: JSON.parse(row.tags) as string[],
    metadata: JSON.parse(row.metadata) as Record<string, unknown>,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function toSummary(row: SummaryRow): KnowledgeSourceSummary {
  return {
    id: row.source_id,
    ...(row.title !== null && { title: row.title }),
    ...(row.uri !== null && { uri: row.uri }),
    ...(row.mime_type !== null && { mimeType: row.mime_type }),
    tags: JSON.parse(row.tags) as string[],
    metadata: JSON.parse(row.metadata) as Record<string, unknown>,
    status: row.status,
    ...(row.error !== null && { error: row.error }),
    chunkCount: row.chunk_count,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function toSource(row: SourceRow): KnowledgeSourceRecord {
  return { ...toSummary(row), text: row.text };
}
