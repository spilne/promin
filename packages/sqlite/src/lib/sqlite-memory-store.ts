import { randomUUID } from "node:crypto";
import type { MemoryStore, MemoryEntry, MemoryScope } from "@promin/agent";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent memory store backed by SQLite.
 *
 * Entries are stored as JSON and searched by keyword overlap scoring.
 * Scope filtering (namespaceId, sessionId) maps to nullable columns.
 *
 * Schema (auto-created on first use):
 *   CREATE TABLE promin_memory (
 *     id           TEXT NOT NULL PRIMARY KEY,
 *     content      TEXT NOT NULL,
 *     metadata     TEXT,
 *     namespace_id TEXT,
 *     session_id   TEXT,
 *     created_at   INTEGER NOT NULL
 *   )
 *
 * @example
 * ```ts
 * import { Database } from "bun:sqlite";
 * const db = new Database("agent.db");
 * const store = SqliteMemoryStore.make({ db });
 * await store.save({ content: "The capital of France is Paris." });
 * const results = await store.search("France");
 * ```
 */
export class SqliteMemoryStore implements MemoryStore {
  private readonly _table: string;

  private constructor(
    private readonly db: SqliteDatabase,
    table: string,
  ) {
    this._table = table;
    this._setup();
  }

  static make(params: {
    db: SqliteDatabase;
    /** Override the table name (default: `promin_memory`). */
    table?: string;
  }): SqliteMemoryStore {
    return new SqliteMemoryStore(params.db, params.table ?? "promin_memory");
  }

  private _setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this._table} (
        id           TEXT    NOT NULL PRIMARY KEY,
        content      TEXT    NOT NULL,
        metadata     TEXT,
        namespace_id TEXT,
        session_id   TEXT,
        created_at   INTEGER NOT NULL
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${this._table}_scope ON ${this._table} (namespace_id, session_id)`,
    );
  }

  async save(
    input: { content: string; metadata?: Record<string, unknown> },
    scope?: MemoryScope,
  ): Promise<string> {
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
        scope?.namespaceId ?? null,
        scope?.sessionId ?? null,
        Date.now(),
      );
    return id;
  }

  async search(query: string, limit = 5, scope?: MemoryScope): Promise<MemoryEntry[]> {
    const rows = this._listRows(scope);
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
    const rows = this._listRows(scope);
    const limited = limit ? rows.slice(0, limit) : rows;
    return limited.map(toEntry);
  }

  async delete(id: string): Promise<void> {
    this.db.query(`DELETE FROM ${this._table} WHERE id = ?`).run(id);
  }

  private _listRows(scope?: MemoryScope): DbMemoryRow[] {
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

    const where = conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
    return this.db
      .query<DbMemoryRow>(
        `SELECT id, content, metadata, namespace_id, session_id, created_at
         FROM ${this._table}${where} ORDER BY created_at DESC, rowid DESC`,
      )
      .all(...params);
  }
}

interface DbMemoryRow {
  id: string;
  content: string;
  metadata: string | null;
  namespace_id: string | null;
  session_id: string | null;
  created_at: number;
}

function toEntry(row: DbMemoryRow): MemoryEntry {
  return {
    id: row.id,
    content: row.content,
    metadata: row.metadata ? JSON.parse(row.metadata) : undefined,
    createdAt: new Date(row.created_at),
  };
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
