import { randomUUID } from "node:crypto";
import { sql, type SQL } from "drizzle-orm";
import {
  FlatTextChunker,
  type EmbeddingProvider,
  type KnowledgeChunk,
  type KnowledgeChunker,
  type KnowledgeIngestDocument,
  type KnowledgeSource,
  type RetrieveRequest,
  type RetrieveResult,
  type Retriever,
  type RetrieverFilter,
} from "@promin/agent";
import type { DrizzleDb } from "../drizzle-db.ts";
import { execRaw } from "../drizzle-db.ts";

export interface PgVectorRetrieverConfig {
  readonly db: DrizzleDb;
  readonly embeddings: EmbeddingProvider;
  /** Fully qualified table name. Defaults to `agent_knowledge_chunk`. */
  readonly tableName?: string;
  /** Embedding vector dimension used by the configured provider. */
  readonly dimensions: number;
  /** Chunker used by `addDocument`. Defaults to `FlatTextChunker`. */
  readonly chunker?: KnowledgeChunker;
  /** Default result count. Default: 8. */
  readonly defaultTopK?: number;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export interface PgVectorEnsureSchemaOptions {
  /**
   * Create the pgvector extension before creating the table.
   *
   * Keep this explicit because stock Postgres images/databases do not always
   * have the extension installed on disk. Managed providers often require the
   * operator to enable it once at the database level.
   */
  readonly createExtension?: boolean;
  /** Vector index kind. HNSW is a strong default on modern pgvector. */
  readonly index?: "hnsw" | "ivfflat" | false;
}

interface PgVectorRetrieverRow {
  id: string;
  text: string;
  source_id: string;
  source_title: string | null;
  source_uri: string | null;
  source_mime_type: string | null;
  source_tags: string[] | null;
  source_metadata: Record<string, unknown> | null;
  chunk_index: number;
  parent_id: string | null;
  chunk_metadata: Record<string, unknown> | null;
  score: number | string;
}

export class PgVectorRetriever implements Retriever {
  private readonly db: DrizzleDb;
  private readonly embeddings: EmbeddingProvider;
  private readonly table: SQL;
  private readonly tableName: string;
  private readonly dimensions: number;
  private readonly chunker: KnowledgeChunker;
  private readonly defaultTopK: number;
  private readonly clock: () => number;

  constructor(config: PgVectorRetrieverConfig) {
    if (!Number.isInteger(config.dimensions) || config.dimensions < 1) {
      throw new Error("PgVectorRetriever: dimensions must be a positive integer");
    }
    this.db = config.db;
    this.embeddings = config.embeddings;
    this.tableName = config.tableName ?? "agent_knowledge_chunk";
    this.table = qualifiedIdentifier(this.tableName);
    this.dimensions = config.dimensions;
    this.chunker = config.chunker ?? new FlatTextChunker();
    this.defaultTopK = config.defaultTopK ?? 8;
    this.clock = config.now ?? (() => Date.now());
  }

  async ensureSchema(options: PgVectorEnsureSchemaOptions = {}): Promise<void> {
    if (options.createExtension) {
      await this.db.execute(sql`CREATE EXTENSION IF NOT EXISTS vector`);
    }

    await this.db.execute(sql`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id text PRIMARY KEY,
        text text NOT NULL,
        source_id text NOT NULL,
        source_title text,
        source_uri text,
        source_mime_type text,
        source_tags text[] NOT NULL DEFAULT '{}',
        source_metadata jsonb NOT NULL DEFAULT '{}',
        chunk_index integer NOT NULL,
        parent_id text,
        chunk_metadata jsonb NOT NULL DEFAULT '{}',
        embedding vector(${sql.raw(String(this.dimensions))}) NOT NULL,
        created_at bigint NOT NULL,
        updated_at bigint NOT NULL
      )
    `);
    await this.db.execute(
      sql`CREATE INDEX IF NOT EXISTS ${rawIdentifier(indexName(this.tableName, "source_id"))} ON ${this.table} (source_id)`,
    );
    await this.db.execute(
      sql`CREATE INDEX IF NOT EXISTS ${rawIdentifier(indexName(this.tableName, "source_tags_gin"))} ON ${this.table} USING gin (source_tags)`,
    );
    await this.db.execute(
      sql`CREATE INDEX IF NOT EXISTS ${rawIdentifier(indexName(this.tableName, "source_metadata_gin"))} ON ${this.table} USING gin (source_metadata)`,
    );
    await this.db.execute(
      sql`CREATE INDEX IF NOT EXISTS ${rawIdentifier(indexName(this.tableName, "chunk_metadata_gin"))} ON ${this.table} USING gin (chunk_metadata)`,
    );

    if (options.index !== false) {
      const indexKind = options.index ?? "hnsw";
      const method =
        indexKind === "hnsw"
          ? sql.raw("hnsw (embedding vector_cosine_ops)")
          : sql.raw("ivfflat (embedding vector_cosine_ops)");
      await this.db.execute(sql`
        CREATE INDEX IF NOT EXISTS ${rawIdentifier(indexName(this.tableName, `embedding_${indexKind}`))}
        ON ${this.table}
        USING ${method}
      `);
    }
  }

  async addDocument(document: KnowledgeIngestDocument): Promise<KnowledgeChunk[]> {
    const chunks = this.chunker.chunk(document);
    await this.addChunks(chunks);
    return chunks;
  }

  async addChunks(chunks: ReadonlyArray<KnowledgeChunk>): Promise<void> {
    for (const chunk of chunks) {
      await this.upsertChunk(chunk);
    }
  }

  async deleteSource(sourceId: string): Promise<void> {
    await this.db.execute(sql`DELETE FROM ${this.table} WHERE source_id = ${sourceId}`);
  }

  async retrieve(request: RetrieveRequest): Promise<RetrieveResult[]> {
    const query = request.query.trim();
    if (!query) return [];

    const embedding = await this.embeddings.embed(query);
    const vector = vectorParam(embedding, this.dimensions);
    const where = filterSql(request.filter);
    const limit = request.topK ?? this.defaultTopK;
    const rows = (await execRaw(
      this.db,
      sql`
        SELECT
          id,
          text,
          source_id,
          source_title,
          source_uri,
          source_mime_type,
          source_tags,
          source_metadata,
          chunk_index,
          parent_id,
          chunk_metadata,
          1 - (embedding <=> ${vector}) AS score
        FROM ${this.table}
        ${where}
        ORDER BY embedding <=> ${vector}, id ASC
        LIMIT ${limit}
      `,
    )) as PgVectorRetrieverRow[];

    return rows.map((row) => ({
      chunk: truncateChunk(rowToChunk(row), request.maxChunkCharacters),
      score: Number(row.score),
    }));
  }

  private async upsertChunk(chunk: KnowledgeChunk): Promise<void> {
    const now = this.clock();
    const embedding = await this.embeddings.embed(chunk.text);
    const vector = vectorParam(embedding, this.dimensions);
    await this.db.execute(sql`
      INSERT INTO ${this.table} (
        id,
        text,
        source_id,
        source_title,
        source_uri,
        source_mime_type,
        source_tags,
        source_metadata,
        chunk_index,
        parent_id,
        chunk_metadata,
        embedding,
        created_at,
        updated_at
      )
      VALUES (
        ${chunk.id || randomUUID()},
        ${chunk.text},
        ${chunk.source.id},
        ${chunk.source.title ?? null},
        ${chunk.source.uri ?? null},
        ${chunk.source.mimeType ?? null},
        ${textArrayParam(chunk.source.tags ?? [])},
        ${JSON.stringify(chunk.source.metadata ?? {})}::jsonb,
        ${chunk.index},
        ${chunk.parentId ?? null},
        ${JSON.stringify(chunk.metadata ?? {})}::jsonb,
        ${vector},
        ${now},
        ${now}
      )
      ON CONFLICT (id) DO UPDATE SET
        text = EXCLUDED.text,
        source_id = EXCLUDED.source_id,
        source_title = EXCLUDED.source_title,
        source_uri = EXCLUDED.source_uri,
        source_mime_type = EXCLUDED.source_mime_type,
        source_tags = EXCLUDED.source_tags,
        source_metadata = EXCLUDED.source_metadata,
        chunk_index = EXCLUDED.chunk_index,
        parent_id = EXCLUDED.parent_id,
        chunk_metadata = EXCLUDED.chunk_metadata,
        embedding = EXCLUDED.embedding,
        updated_at = EXCLUDED.updated_at
    `);
  }
}

export function createPgVectorRetriever(config: PgVectorRetrieverConfig): PgVectorRetriever {
  return new PgVectorRetriever(config);
}

function rowToChunk(row: PgVectorRetrieverRow): KnowledgeChunk {
  const source: KnowledgeSource = {
    id: row.source_id,
    title: row.source_title ?? undefined,
    uri: row.source_uri ?? undefined,
    mimeType: row.source_mime_type ?? undefined,
    tags: row.source_tags ?? [],
    metadata: row.source_metadata ?? {},
  };
  return {
    id: row.id,
    text: row.text,
    source,
    index: row.chunk_index,
    parentId: row.parent_id ?? undefined,
    metadata: row.chunk_metadata ?? {},
  };
}

function filterSql(filter: RetrieverFilter | undefined): SQL {
  const clauses: SQL[] = [];
  if (filter?.tags && filter.tags.length > 0) {
    clauses.push(sql`source_tags @> ${textArrayParam(filter.tags)}`);
  }
  if (filter?.metadata && Object.keys(filter.metadata).length > 0) {
    const metadata = JSON.stringify(filter.metadata);
    clauses.push(
      sql`(source_metadata @> ${metadata}::jsonb OR chunk_metadata @> ${metadata}::jsonb)`,
    );
  }
  if (clauses.length === 0) return sql``;
  return sql`WHERE ${sql.join(clauses, sql` AND `)}`;
}

function truncateChunk(chunk: KnowledgeChunk, max: number | undefined): KnowledgeChunk {
  if (max === undefined || chunk.text.length <= max) return chunk;
  return { ...chunk, text: chunk.text.slice(0, Math.max(0, max)).trimEnd() };
}

function vectorParam(vector: ReadonlyArray<number>, dimensions: number): SQL {
  if (vector.length !== dimensions) {
    throw new Error(
      `PgVectorRetriever: embedding dimension mismatch; expected ${dimensions}, got ${vector.length}`,
    );
  }
  for (const value of vector) {
    if (!Number.isFinite(value)) {
      throw new Error("PgVectorRetriever: embedding vectors must contain only finite numbers");
    }
  }
  return sql`${JSON.stringify(vector)}::vector`;
}

function textArrayParam(values: ReadonlyArray<string>): SQL {
  if (values.length === 0) return sql`ARRAY[]::text[]`;
  return sql`ARRAY[${sql.join(
    values.map((value) => sql`${value}`),
    sql`, `,
  )}]::text[]`;
}

function qualifiedIdentifier(name: string): SQL {
  return sql.raw(
    name
      .split(".")
      .map((part) => quoteIdentifier(part))
      .join("."),
  );
}

function rawIdentifier(name: string): SQL {
  return sql.raw(quoteIdentifier(name));
}

function quoteIdentifier(part: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(part)) {
    throw new Error(`PgVectorRetriever: unsafe SQL identifier '${part}'`);
  }
  return `"${part}"`;
}

function indexName(tableName: string, suffix: string): string {
  return `${tableName.replace(/\./g, "_")}_${suffix}_idx`;
}
