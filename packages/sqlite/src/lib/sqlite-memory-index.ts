import { randomUUID } from "node:crypto";
import type { MemoryIndex, MemoryEntry, MemoryScope, EmbeddingProvider } from "@promin/agent";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent memory store backed by SQLite.
 *
 * Entries are stored as JSON and searched by keyword overlap scoring.
 * An optional store-level `namespace` acts as the default `namespaceId`
 * when no per-call scope is passed — useful for isolating separate agents
 * that share one database file.
 *
 * When an `embeddings` provider is supplied, `save()` stores a vector
 * alongside each entry and `search()` uses cosine-similarity ranking
 * instead of keyword overlap. Keyword search is used as a fallback when
 * no embedding rows exist for the scoped entries.
 *
 * Schema (auto-created on first use):
 *   CREATE TABLE promin_memory (
 *     id           TEXT    NOT NULL PRIMARY KEY,
 *     content      TEXT    NOT NULL,
 *     metadata     TEXT,
 *     namespace_id TEXT,
 *     session_id   TEXT,
 *     created_at   INTEGER NOT NULL,
 *     updated_at   INTEGER
 *   )
 *
 *   CREATE TABLE promin_memory_embeddings (
 *     id        TEXT NOT NULL PRIMARY KEY REFERENCES promin_memory(id) ON DELETE CASCADE,
 *     embedding TEXT NOT NULL
 *   )
 *
 * @example
 * ```ts
 * import { Database } from "bun:sqlite";
 * const db = new Database("agent.db");
 * // Scoped to "agent-42" — all unscoped calls resolve to that namespace.
 * const store = SqliteMemoryIndex.make({ db, namespace: "agent-42" });
 * await store.save({ content: "The capital of France is Paris." });
 * const results = await store.search("France");
 * ```
 */
export class SqliteMemoryIndex implements MemoryIndex {
  private readonly _table: string;
  private readonly _namespace: string | null;
  private readonly _embeddings: EmbeddingProvider | null;

  private constructor(
    private readonly db: SqliteDatabase,
    table: string,
    namespace: string | null,
    embeddings: EmbeddingProvider | null,
  ) {
    this._table = table;
    this._namespace = namespace;
    this._embeddings = embeddings;
    this._setup();
  }

  static make(params: {
    db: SqliteDatabase;
    /**
     * Default namespace for this store instance. When provided, all calls
     * without an explicit scope use `{ namespaceId: namespace }` as their
     * effective scope. Useful for giving each agent its own isolated memory
     * partition in a shared database.
     */
    namespace?: string;
    /** Override the table name (default: `promin_memory`). */
    table?: string;
    /**
     * Optional embedding provider for semantic search. When supplied, each
     * saved entry gets an embedding vector stored in a companion table, and
     * `search()` ranks by cosine similarity instead of keyword overlap.
     */
    embeddings?: EmbeddingProvider;
  }): SqliteMemoryIndex {
    return new SqliteMemoryIndex(
      params.db,
      params.table ?? "promin_memory",
      params.namespace ?? null,
      params.embeddings ?? null,
    );
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this._table} (
        id           TEXT    NOT NULL PRIMARY KEY,
        content      TEXT    NOT NULL,
        metadata     TEXT,
        namespace_id TEXT,
        session_id   TEXT,
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this._table}_scope ON ${this._table} (namespace_id, session_id)`,
    );
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this._table}_embeddings (
        id        TEXT NOT NULL PRIMARY KEY REFERENCES ${this._table}(id) ON DELETE CASCADE,
        embedding TEXT NOT NULL
      )
    `);
  }

  /** Merge the store-level default namespace into a per-call scope. */
  private _resolveScope(scope?: MemoryScope): MemoryScope | undefined {
    if (this._namespace === null) return scope;
    // Explicit per-call namespaceId takes precedence over the store default.
    return {
      namespaceId: scope?.namespaceId ?? this._namespace,
      sessionId: scope?.sessionId,
    };
  }

  async save(
    input: { content: string; metadata?: Record<string, unknown> },
    scope?: MemoryScope,
  ): Promise<string> {
    const resolved = this._resolveScope(scope);
    const id = randomUUID();
    this.db
      .query(
        `INSERT INTO ${this._table} (id, content, metadata, namespace_id, session_id, created_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(
        id,
        input.content,
        input.metadata != null ? JSON.stringify(input.metadata) : null,
        resolved?.namespaceId ?? null,
        resolved?.sessionId ?? null,
        Date.now(),
      );

    if (this._embeddings) {
      const embedding = await this._embeddings.embed(input.content);
      this.db
        .query(`INSERT INTO ${this._table}_embeddings (id, embedding) VALUES (?, ?)`)
        .run(id, JSON.stringify(embedding));
    }

    return id;
  }

  /**
   * Update an existing memory entry's content and/or metadata.
   *
   * @throws {Error} If no entry with the given `id` exists.
   */
  async update(
    id: string,
    patch: { content?: string; metadata?: Record<string, unknown> },
  ): Promise<void> {
    const setClauses: string[] = [];
    const params: unknown[] = [];

    if (patch.content !== undefined) {
      setClauses.push("content = ?");
      params.push(patch.content);
    }

    if (patch.metadata !== undefined) {
      setClauses.push("metadata = ?");
      params.push(JSON.stringify(patch.metadata));
    }

    // Always touch updated_at.
    setClauses.push("updated_at = ?");
    params.push(Date.now());

    // WHERE param.
    params.push(id);

    const exists = this.db
      .query<{ id: string }>(`SELECT id FROM ${this._table} WHERE id = ?`)
      .get(id);
    if (!exists) throw new Error(`Memory entry not found: ${id}`);

    this.db.query(`UPDATE ${this._table} SET ${setClauses.join(", ")} WHERE id = ?`).run(...params);

    if (patch.content !== undefined && this._embeddings) {
      const embedding = await this._embeddings.embed(patch.content);
      this.db
        .query(`INSERT OR REPLACE INTO ${this._table}_embeddings (id, embedding) VALUES (?, ?)`)
        .run(id, JSON.stringify(embedding));
    }
  }

  async search(query: string, limit = 5, scope?: MemoryScope): Promise<MemoryEntry[]> {
    const resolved = this._resolveScope(scope);

    // ---- semantic search ------------------------------------------------
    if (this._embeddings) {
      const queryEmbedding = await this._embeddings.embed(query);

      // Load all scoped rows that also have an embedding.
      const embRows = this._listRowsWithEmbeddings(resolved);

      if (embRows.length > 0) {
        return embRows
          .map(({ row, embedding }) => ({
            row,
            score: cosineSimilarity(queryEmbedding, embedding),
          }))
          .sort((a, b) => b.score - a.score)
          .slice(0, limit)
          .map(({ row }) => toEntry(row));
      }
      // Fall through to keyword search when no embeddings exist yet.
    }

    // ---- keyword search -------------------------------------------------
    const rows = this._listRows(resolved);
    if (rows.length === 0) return [];

    const hasTerms = query
      .toLowerCase()
      .split(/\W+/)
      .some((w) => w.length > 2);

    if (!hasTerms) {
      return rows.slice(0, limit).map(toEntry);
    }

    const queryWords = new Set(
      query
        .toLowerCase()
        .split(/\W+/)
        .filter((w) => w.length > 2),
    );

    return rows
      .map((row) => ({ row, score: keywordScore(queryWords, row.content) }))
      .filter(({ score }) => score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, limit)
      .map(({ row }) => toEntry(row));
  }

  async list(limit?: number, scope?: MemoryScope): Promise<MemoryEntry[]> {
    const rows = this._listRows(this._resolveScope(scope));
    const limited = limit ? rows.slice(0, limit) : rows;
    return limited.map(toEntry);
  }

  async delete(id: string): Promise<void> {
    this.db.query(`DELETE FROM ${this._table} WHERE id = ?`).run(id);
  }

  private _buildScopeConditions(scope?: MemoryScope): {
    conditions: string[];
    params: unknown[];
  } {
    const conditions: string[] = [];
    const params: unknown[] = [];

    if (!scope) {
      conditions.push(`namespace_id IS NULL`, `session_id IS NULL`);
    } else {
      if (scope.namespaceId !== undefined) {
        conditions.push(`namespace_id = ?`);
        params.push(scope.namespaceId);
      } else {
        conditions.push(`namespace_id IS NULL`);
      }
      if (scope.sessionId !== undefined) {
        conditions.push(`session_id = ?`);
        params.push(scope.sessionId);
      } else {
        conditions.push(`session_id IS NULL`);
      }
    }

    return { conditions, params };
  }

  private _listRows(scope?: MemoryScope): DbMemoryRow[] {
    const { conditions, params } = this._buildScopeConditions(scope);
    const where = conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
    return this.db
      .query<DbMemoryRow>(
        `SELECT id, content, metadata, namespace_id, session_id, created_at, updated_at
         FROM ${this._table}${where} ORDER BY created_at DESC, rowid DESC`,
      )
      .all(...params);
  }

  /** Load scoped rows that have a matching embedding, together with the parsed vector. */
  private _listRowsWithEmbeddings(
    scope?: MemoryScope,
  ): Array<{ row: DbMemoryRow; embedding: number[] }> {
    const { conditions, params } = this._buildScopeConditions(scope);
    const tableAlias = "m";
    const scopedConditions = conditions.map((c) =>
      // Prefix unqualified column references with the table alias.
      c.replace(/^(namespace_id|session_id)/, `${tableAlias}.$1`),
    );
    const where = scopedConditions.length > 0 ? ` WHERE ${scopedConditions.join(" AND ")}` : "";

    const rows = this.db
      .query<DbMemoryRowWithEmbedding>(
        `SELECT m.id, m.content, m.metadata, m.namespace_id, m.session_id,
                m.created_at, m.updated_at, e.embedding
         FROM ${this._table} m
         INNER JOIN ${this._table}_embeddings e ON e.id = m.id
         ${where}
         ORDER BY m.created_at DESC, m.rowid DESC`,
      )
      .all(...params);

    return rows.map((r) => ({
      row: {
        id: r.id,
        content: r.content,
        metadata: r.metadata,
        namespace_id: r.namespace_id,
        session_id: r.session_id,
        created_at: r.created_at,
        updated_at: r.updated_at,
      },
      embedding: JSON.parse(r.embedding) as number[],
    }));
  }
}

// ---------------------------------------------------------------------------
// DB row types
// ---------------------------------------------------------------------------

interface DbMemoryRow {
  id: string;
  content: string;
  metadata: string | null;
  namespace_id: string | null;
  session_id: string | null;
  created_at: number;
  updated_at: number | null;
}

interface DbMemoryRowWithEmbedding extends DbMemoryRow {
  embedding: string;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function toEntry(row: DbMemoryRow): MemoryEntry {
  return {
    id: row.id,
    content: row.content,
    metadata: row.metadata ? JSON.parse(row.metadata) : undefined,
    createdAt: new Date(row.created_at),
    updatedAt: row.updated_at ? new Date(row.updated_at) : undefined,
  };
}

function cosineSimilarity(a: number[], b: number[]): number {
  let dot = 0;
  let magA = 0;
  let magB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    magA += a[i] ** 2;
    magB += b[i] ** 2;
  }
  const denom = Math.sqrt(magA) * Math.sqrt(magB);
  return denom === 0 ? 0 : dot / denom;
}

function keywordScore(queryWords: Set<string>, content: string): number {
  const contentWords = new Set(
    content
      .toLowerCase()
      .split(/\W+/)
      .filter((w) => w.length > 2),
  );
  let matches = 0;
  for (const w of queryWords) {
    if (contentWords.has(w)) matches++;
  }
  return matches / queryWords.size;
}
