// ---------------------------------------------------------------------------
// PgVectorRetriever — `Retriever` over a pgvector table of knowledge chunks.
//
// One row per chunk: text, source fields, chunk metadata and an
// `embedding vector(<dimensions>)` column. Search embeds the query and
// orders by cosine distance (`<=>`); the score is cosine similarity
// (`1 - distance`). Tag filters use array containment on `source_tags`;
// metadata filters use jsonb containment on the source metadata merged
// with the chunk metadata (chunk keys win), matching `InMemoryRetriever`.
//
// Every value travels as a bind parameter. The table name comes from
// config, so it is validated as a (optionally schema-qualified) plain
// identifier and quoted; it is never interpolated raw.
// ---------------------------------------------------------------------------

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
  type IngestibleRetriever,
  type RetrieverFilter,
} from "@promin/agent";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { execRaw } from "../exec-raw.ts";

export interface PgVectorRetrieverConfig {
  readonly db: DrizzleDb;
  readonly embeddings: EmbeddingProvider;
  /** Embedding vector dimension produced by `embeddings`. */
  readonly dimensions: number;
  /** Table name, optionally schema-qualified (`kb.chunks`). Default `agent_knowledge_chunk`. */
  readonly tableName?: string;
  /** Chunker used by `addDocument`. Default `FlatTextChunker`. */
  readonly chunker?: KnowledgeChunker;
  /** Result count when a request sets no `topK`. Default 8. */
  readonly defaultTopK?: number;
  /** Time source for `created_at` / `updated_at`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

export interface PgVectorEnsureSchemaOptions {
  /**
   * Run `CREATE EXTENSION IF NOT EXISTS vector` first. Off by default:
   * creating an extension needs elevated privileges, and managed providers
   * usually have an operator enable it once per database.
   */
  readonly createExtension?: boolean;
  /** Vector index kind. Default `hnsw`; `false` creates none. */
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

/** Postgres truncates identifiers past this length, which could merge index names. */
const MAX_IDENTIFIER_LENGTH = 63;

export class PgVectorRetriever implements IngestibleRetriever {
  private readonly db: DrizzleDb;
  private readonly embeddings: EmbeddingProvider;
  private readonly tableName: string;
  private readonly table: SQL;
  private readonly dimensions: number;
  private readonly chunker: KnowledgeChunker;
  private readonly defaultTopK: number;
  private readonly clock: WallClock;

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
    this.clock = config.clock ?? SystemWallClock;
  }

  /** Create the chunk table and its indexes when missing. Idempotent. */
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
      sql`CREATE INDEX IF NOT EXISTS ${this.indexName("source_id")} ON ${this.table} (source_id)`,
    );
    await this.db.execute(
      sql`CREATE INDEX IF NOT EXISTS ${this.indexName("source_tags")} ON ${this.table} USING gin (source_tags)`,
    );
    await this.db.execute(
      sql`CREATE INDEX IF NOT EXISTS ${this.indexName("metadata")} ON ${this.table} USING gin ((source_metadata || chunk_metadata))`,
    );

    const index = options.index ?? "hnsw";
    if (index !== false) {
      const method = index === "hnsw" ? sql.raw("hnsw") : sql.raw("ivfflat");
      await this.db.execute(sql`
        CREATE INDEX IF NOT EXISTS ${this.indexName(`embedding_${index}`)}
        ON ${this.table}
        USING ${method} (embedding vector_cosine_ops)
      `);
    }
  }

  /** Chunk a document, embed each chunk, and upsert the rows. */
  async addDocument(document: KnowledgeIngestDocument): Promise<KnowledgeChunk[]> {
    const chunks = this.chunker.chunk(document);
    await this.addChunks(chunks);
    return chunks;
  }

  /** Embed and upsert chunks by id (a re-ingested chunk replaces its row). */
  async addChunks(chunks: ReadonlyArray<KnowledgeChunk>): Promise<void> {
    for (const chunk of chunks) {
      await this.upsertChunk(chunk);
    }
  }

  /** Delete every chunk of a source (e.g. before re-ingesting it). */
  async deleteSource(sourceId: string): Promise<void> {
    await this.db.execute(sql`DELETE FROM ${this.table} WHERE source_id = ${sourceId}`);
  }

  async retrieve(request: RetrieveRequest): Promise<RetrieveResult[]> {
    const query = request.query.trim();
    if (!query) return [];
    const limit = request.topK ?? this.defaultTopK;
    if (!Number.isInteger(limit) || limit < 1) {
      throw new Error("PgVectorRetriever: topK must be a positive integer");
    }

    const vector = this.vectorParam(await this.embeddings.embed(query));
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
        ${filterSql(request.filter)}
        ORDER BY embedding <=> ${vector}, id ASC
        LIMIT ${limit}
      `,
    )) as PgVectorRetrieverRow[];

    return rows.map((row) => ({
      chunk: truncateChunk({ chunk: rowToChunk(row), max: request.maxChunkCharacters }),
      score: Number(row.score),
    }));
  }

  private async upsertChunk(chunk: KnowledgeChunk): Promise<void> {
    if (!chunk.id) throw new Error("PgVectorRetriever: chunk id must not be empty");
    const vector = this.vectorParam(await this.embeddings.embed(chunk.text));
    const now = this.clock.currentTimeMs();
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
        ${chunk.id},
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

  /** Validate an embedding and bind it as a `vector` parameter. */
  private vectorParam(vector: ReadonlyArray<number>): SQL {
    if (vector.length !== this.dimensions) {
      throw new Error(
        `PgVectorRetriever: embedding dimension mismatch; expected ${this.dimensions}, got ${vector.length}`,
      );
    }
    if (!vector.every((value) => Number.isFinite(value))) {
      throw new Error("PgVectorRetriever: embedding vectors must contain only finite numbers");
    }
    return sql`${JSON.stringify(vector)}::vector`;
  }

  /** Quoted, unqualified index name (Postgres creates it in the table's schema). */
  private indexName(suffix: string): SQL {
    const name = `${this.tableName.replace(/\./g, "_")}_${suffix}_idx`;
    if (name.length > MAX_IDENTIFIER_LENGTH) {
      throw new Error(
        `PgVectorRetriever: index name '${name}' exceeds ${MAX_IDENTIFIER_LENGTH} characters; use a shorter tableName`,
      );
    }
    return sql.raw(quoteIdentifier(name));
  }
}

function rowToChunk(row: PgVectorRetrieverRow): KnowledgeChunk {
  const source: KnowledgeSource = {
    id: row.source_id,
    ...(row.source_title !== null && { title: row.source_title }),
    ...(row.source_uri !== null && { uri: row.source_uri }),
    ...(row.source_mime_type !== null && { mimeType: row.source_mime_type }),
    tags: row.source_tags ?? [],
    metadata: row.source_metadata ?? {},
  };
  return {
    id: row.id,
    text: row.text,
    source,
    index: row.chunk_index,
    ...(row.parent_id !== null && { parentId: row.parent_id }),
    metadata: row.chunk_metadata ?? {},
  };
}

function filterSql(filter: RetrieverFilter | undefined): SQL {
  const clauses: SQL[] = [];
  if (filter?.tags && filter.tags.length > 0) {
    clauses.push(sql`source_tags @> ${textArrayParam(filter.tags)}`);
  }
  if (filter?.metadata && Object.keys(filter.metadata).length > 0) {
    clauses.push(
      sql`(source_metadata || chunk_metadata) @> ${JSON.stringify(filter.metadata)}::jsonb`,
    );
  }
  if (clauses.length === 0) return sql``;
  return sql`WHERE ${sql.join(clauses, sql` AND `)}`;
}

function truncateChunk(params: { chunk: KnowledgeChunk; max: number | undefined }): KnowledgeChunk {
  const { chunk, max } = params;
  if (max === undefined || chunk.text.length <= max) return chunk;
  return { ...chunk, text: chunk.text.slice(0, Math.max(0, max)).trimEnd() };
}

function textArrayParam(values: ReadonlyArray<string>): SQL {
  if (values.length === 0) return sql`ARRAY[]::text[]`;
  return sql`ARRAY[${sql.join(
    values.map((value) => sql`${value}`),
    sql`, `,
  )}]::text[]`;
}

function qualifiedIdentifier(name: string): SQL {
  const parts = name.split(".");
  if (parts.length > 2) {
    throw new Error(`PgVectorRetriever: unsafe SQL identifier '${name}'`);
  }
  return sql.raw(parts.map((part) => quoteIdentifier(part)).join("."));
}

function quoteIdentifier(part: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(part) || part.length > MAX_IDENTIFIER_LENGTH) {
    throw new Error(`PgVectorRetriever: unsafe SQL identifier '${part}'`);
  }
  return `"${part}"`;
}
