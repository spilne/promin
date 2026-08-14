import type { WorkflowSchema } from "@promin/workflow";
import type { SqliteDatabase } from "../sqlite-database.ts";

interface AuthoredWorkflowRecord {
  readonly name: string;
  readonly version: string;
  readonly schema: WorkflowSchema;
  readonly status: "draft" | "published";
  readonly contentHash: string;
  readonly createdAt: number;
  readonly updatedAt: number;
  readonly publishedAt?: number;
}

export interface SqliteAuthoredWorkflowStoreConfig {
  readonly db: SqliteDatabase;
  readonly table?: string;
}

interface AuthoredWorkflowRow {
  name: string;
  version: string;
  schema: string;
  status: "draft" | "published";
  content_hash: string;
  created_at: number;
  updated_at: number;
  published_at: number | null;
}

/** SQLite persistence adapter for Zorya authored workflow definitions. */
export class SqliteAuthoredWorkflowStore {
  private readonly db: SqliteDatabase;
  private readonly table: string;

  private constructor(config: SqliteAuthoredWorkflowStoreConfig) {
    this.db = config.db;
    this.table = config.table ?? "promin_authored_workflows";
    assertIdentifier(this.table);
    this.setup();
  }

  static make(config: SqliteAuthoredWorkflowStoreConfig): SqliteAuthoredWorkflowStore {
    return new SqliteAuthoredWorkflowStore(config);
  }

  async list(): Promise<ReadonlyArray<AuthoredWorkflowRecord>> {
    return this.db
      .query<AuthoredWorkflowRow>(`SELECT * FROM ${this.table} ORDER BY name ASC, updated_at DESC`)
      .all()
      .map(toRecord);
  }

  async get(name: string, version?: string): Promise<AuthoredWorkflowRecord | null> {
    const row =
      version !== undefined
        ? this.db
            .query<AuthoredWorkflowRow>(
              `SELECT * FROM ${this.table} WHERE name = ? AND version = ?`,
            )
            .get(name, version)
        : this.db
            .query<AuthoredWorkflowRow>(
              `SELECT * FROM ${this.table} WHERE name = ? ORDER BY updated_at DESC LIMIT 1`,
            )
            .get(name);
    return row ? toRecord(row) : null;
  }

  async save(record: AuthoredWorkflowRecord): Promise<void> {
    this.db
      .query(
        `INSERT INTO ${this.table}
         (name, version, schema, status, content_hash, created_at, updated_at, published_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(name, version) DO UPDATE SET
           schema = excluded.schema,
           status = excluded.status,
           content_hash = excluded.content_hash,
           updated_at = excluded.updated_at,
           published_at = excluded.published_at`,
      )
      .run(
        record.name,
        record.version,
        JSON.stringify(record.schema),
        record.status,
        record.contentHash,
        record.createdAt,
        record.updatedAt,
        record.publishedAt ?? null,
      );
  }

  async delete(name: string, version: string): Promise<void> {
    this.db.query(`DELETE FROM ${this.table} WHERE name = ? AND version = ?`).run(name, version);
  }

  private setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        name TEXT NOT NULL,
        version TEXT NOT NULL,
        schema TEXT NOT NULL,
        status TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        published_at INTEGER,
        PRIMARY KEY (name, version)
      )
    `);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${this.table}_updated ON ${this.table} (updated_at)`);
  }
}

function toRecord(row: AuthoredWorkflowRow): AuthoredWorkflowRecord {
  return {
    name: row.name,
    version: row.version,
    schema: JSON.parse(row.schema) as AuthoredWorkflowRecord["schema"],
    status: row.status,
    contentHash: row.content_hash,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    ...(row.published_at !== null && { publishedAt: row.published_at }),
  };
}

function assertIdentifier(value: string): void {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`Invalid SQLite identifier: ${value}`);
  }
}
