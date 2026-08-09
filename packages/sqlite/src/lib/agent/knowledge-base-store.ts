import type { SqliteDatabase } from "../sqlite-database.ts";

interface KnowledgeBaseDefinition {
  readonly id: string;
  readonly namespace: string;
  readonly description?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
  readonly provider: "memory" | "external";
  readonly status: "ready" | "degraded";
  readonly sourceCount: number;
  readonly chunkCount: number;
  readonly createdAt: number;
  readonly updatedAt: number;
}

interface KnowledgeBaseSourceRecord {
  readonly id: string;
  readonly title?: string;
  readonly uri?: string;
  readonly mimeType?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
  readonly text: string;
  readonly status: "ready" | "failed";
  readonly error?: string;
  readonly chunkCount: number;
  readonly createdAt: number;
  readonly updatedAt: number;
}

interface StoredKnowledgeBase {
  readonly definition: KnowledgeBaseDefinition;
  readonly sources: ReadonlyArray<KnowledgeBaseSourceRecord>;
}

export interface SqliteKnowledgeBaseStoreConfig {
  readonly db: SqliteDatabase;
  readonly tablePrefix?: string;
}

interface BaseRow {
  id: string;
  namespace: string;
  description: string | null;
  tags: string;
  metadata: string;
  provider: "memory" | "external";
  status: "ready" | "degraded";
  source_count: number;
  chunk_count: number;
  created_at: number;
  updated_at: number;
}

interface SourceRow {
  base_id: string;
  namespace: string;
  id: string;
  title: string | null;
  uri: string | null;
  mime_type: string | null;
  tags: string;
  metadata: string;
  text: string;
  status: "ready" | "failed";
  error: string | null;
  chunk_count: number;
  created_at: number;
  updated_at: number;
}

/** SQLite persistence adapter for Zorya's KnowledgeBaseStore contract. */
export class SqliteKnowledgeBaseStore {
  private readonly db: SqliteDatabase;
  private readonly bases: string;
  private readonly sources: string;

  private constructor(config: SqliteKnowledgeBaseStoreConfig) {
    this.db = config.db;
    const prefix = config.tablePrefix ?? "promin_knowledge";
    assertIdentifier(prefix);
    this.bases = `${prefix}_bases`;
    this.sources = `${prefix}_sources`;
    this.setup();
  }

  static make(config: SqliteKnowledgeBaseStoreConfig): SqliteKnowledgeBaseStore {
    return new SqliteKnowledgeBaseStore(config);
  }

  async load(): Promise<ReadonlyArray<StoredKnowledgeBase>> {
    const bases = this.db
      .query<BaseRow>(`SELECT * FROM ${this.bases} ORDER BY namespace, id`)
      .all();
    const sources = this.db
      .query<SourceRow>(`SELECT * FROM ${this.sources} ORDER BY namespace, base_id, id`)
      .all();
    const byBase = new Map<string, KnowledgeBaseSourceRecord[]>();
    for (const row of sources) {
      const list = byBase.get(key(row.namespace, row.base_id)) ?? [];
      list.push(toSource(row));
      byBase.set(key(row.namespace, row.base_id), list);
    }
    return bases.map((row) => ({
      definition: toDefinition(row),
      sources: byBase.get(key(row.namespace, row.id)) ?? [],
    }));
  }

  async save(record: StoredKnowledgeBase): Promise<void> {
    const definition = record.definition;
    const baseParams = [
      definition.id,
      definition.namespace,
      definition.description ?? null,
      JSON.stringify(definition.tags),
      JSON.stringify(definition.metadata),
      definition.provider,
      definition.status,
      definition.sourceCount,
      definition.chunkCount,
      definition.createdAt,
      definition.updatedAt,
    ];
    this.db
      .query(
        `INSERT INTO ${this.bases}
       (id, namespace, description, tags, metadata, provider, status, source_count, chunk_count, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(namespace, id) DO UPDATE SET
         description = excluded.description,
         tags = excluded.tags,
         metadata = excluded.metadata,
         provider = excluded.provider,
         status = excluded.status,
         source_count = excluded.source_count,
         chunk_count = excluded.chunk_count,
         updated_at = excluded.updated_at`,
      )
      .run(...baseParams);
    this.db
      .query(`DELETE FROM ${this.sources} WHERE namespace = ? AND base_id = ?`)
      .run(definition.namespace, definition.id);
    const insert = this.db.query(
      `INSERT INTO ${this.sources}
       (base_id, namespace, id, title, uri, mime_type, tags, metadata, text, status, error, chunk_count, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    );
    for (const source of record.sources) {
      insert.run(
        definition.id,
        definition.namespace,
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
  }

  async delete(namespace: string, id: string): Promise<void> {
    this.db.query(`DELETE FROM ${this.bases} WHERE namespace = ? AND id = ?`).run(namespace, id);
  }

  private setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.bases} (
        id TEXT NOT NULL,
        namespace TEXT NOT NULL,
        description TEXT,
        tags TEXT NOT NULL,
        metadata TEXT NOT NULL,
        provider TEXT NOT NULL,
        status TEXT NOT NULL,
        source_count INTEGER NOT NULL,
        chunk_count INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (namespace, id)
      )
    `);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.sources} (
        base_id TEXT NOT NULL,
        namespace TEXT NOT NULL,
        id TEXT NOT NULL,
        title TEXT,
        uri TEXT,
        mime_type TEXT,
        tags TEXT NOT NULL,
        metadata TEXT NOT NULL,
        text TEXT NOT NULL,
        status TEXT NOT NULL,
        error TEXT,
        chunk_count INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (namespace, base_id, id),
        FOREIGN KEY (namespace, base_id) REFERENCES ${this.bases}(namespace, id) ON DELETE CASCADE
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this.sources}_namespace ON ${this.sources} (namespace)`,
    );
  }
}

function toDefinition(row: BaseRow): KnowledgeBaseDefinition {
  return {
    id: row.id,
    namespace: row.namespace,
    ...(row.description !== null && { description: row.description }),
    tags: parseJson<string[]>(row.tags, []),
    metadata: parseJson<Record<string, unknown>>(row.metadata, {}),
    provider: row.provider,
    status: row.status,
    sourceCount: row.source_count,
    chunkCount: row.chunk_count,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function toSource(row: SourceRow): KnowledgeBaseSourceRecord {
  return {
    id: row.id,
    ...(row.title !== null && { title: row.title }),
    ...(row.uri !== null && { uri: row.uri }),
    ...(row.mime_type !== null && { mimeType: row.mime_type }),
    tags: parseJson<string[]>(row.tags, []),
    metadata: parseJson<Record<string, unknown>>(row.metadata, {}),
    text: row.text,
    status: row.status,
    ...(row.error !== null && { error: row.error }),
    chunkCount: row.chunk_count,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function parseJson<T>(value: string, fallback: T): T {
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function key(namespace: string, id: string): string {
  return `${namespace}\u0000${id}`;
}

function assertIdentifier(value: string): void {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value))
    throw new Error("invalid_knowledge_base_table_prefix");
}
