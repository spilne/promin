// ---------------------------------------------------------------------------
// `SqliteWorkflowSchemaStore` — `WorkflowSchemaStore` over SQLite. Workflows
// authored in a visual editor (zorya's workflow builder) keep their draft
// and every published version here, so they can be recompiled at boot.
//
// Schema (auto-created on first use; `<prefix>` defaults to
// `promin_workflow_schema`):
//
//   CREATE TABLE <prefix>_drafts (
//     name TEXT PRIMARY KEY, schema TEXT, revision INTEGER,
//     created_at INTEGER, updated_at INTEGER
//   )
//   CREATE TABLE <prefix>_versions (
//     name TEXT, version TEXT, sequence INTEGER, schema TEXT,
//     content_hash TEXT, published_at INTEGER,
//     PRIMARY KEY (name, version), UNIQUE (name, sequence)
//   )
//
// The compare-and-set of `saveDraft` and the duplicate check of
// `addVersion` each run in one transaction.
// ---------------------------------------------------------------------------

import type {
  StoredWorkflowSchema,
  WorkflowSchema,
  WorkflowSchemaDraft,
  WorkflowSchemaStore,
  WorkflowSchemaVersion,
} from "@promin/workflow";
import type { SqliteDatabase } from "./sqlite-database.ts";

export interface SqliteWorkflowSchemaStoreConfig {
  readonly db: SqliteDatabase;
  /** Table name prefix (default `promin_workflow_schema`): `<prefix>_drafts`, `<prefix>_versions`. */
  readonly tablePrefix?: string;
}

interface DraftRow {
  name: string;
  schema: string;
  revision: number;
  created_at: number;
  updated_at: number;
}

interface VersionRow {
  name: string;
  version: string;
  sequence: number;
  schema: string;
  content_hash: string;
  published_at: number;
}

export class SqliteWorkflowSchemaStore implements WorkflowSchemaStore {
  private readonly db: SqliteDatabase;
  private readonly drafts: string;
  private readonly versions: string;

  private constructor(config: SqliteWorkflowSchemaStoreConfig) {
    this.db = config.db;
    const prefix = config.tablePrefix ?? "promin_workflow_schema";
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(prefix)) {
      throw new Error(`SqliteWorkflowSchemaStore: invalid tablePrefix "${prefix}"`);
    }
    this.drafts = `${prefix}_drafts`;
    this.versions = `${prefix}_versions`;
    this.setup();
  }

  static make(config: SqliteWorkflowSchemaStoreConfig): SqliteWorkflowSchemaStore {
    return new SqliteWorkflowSchemaStore(config);
  }

  private setup(): void {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.drafts} (
        name       TEXT PRIMARY KEY,
        schema     TEXT NOT NULL,
        revision   INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${this.versions} (
        name         TEXT NOT NULL,
        version      TEXT NOT NULL,
        sequence     INTEGER NOT NULL,
        schema       TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        published_at INTEGER NOT NULL,
        PRIMARY KEY (name, version),
        UNIQUE (name, sequence)
      )
    `);
  }

  async list(): Promise<ReadonlyArray<StoredWorkflowSchema>> {
    const drafts = this.db.query<DraftRow>(`SELECT * FROM ${this.drafts} ORDER BY name`).all();
    const versions = this.db
      .query<VersionRow>(`SELECT * FROM ${this.versions} ORDER BY name, sequence`)
      .all();
    const byName = new Map<string, WorkflowSchemaVersion[]>();
    for (const row of versions) {
      const list = byName.get(row.name) ?? [];
      list.push(toVersion(row));
      byName.set(row.name, list);
    }
    return drafts.map((row) => ({ draft: toDraft(row), versions: byName.get(row.name) ?? [] }));
  }

  async get(name: string): Promise<StoredWorkflowSchema | null> {
    const draft = this.db.query<DraftRow>(`SELECT * FROM ${this.drafts} WHERE name = ?`).get(name);
    if (!draft) return null;
    const versions = this.db
      .query<VersionRow>(`SELECT * FROM ${this.versions} WHERE name = ? ORDER BY sequence`)
      .all(name);
    return { draft: toDraft(draft), versions: versions.map(toVersion) };
  }

  async saveDraft(params: {
    draft: WorkflowSchemaDraft;
    expectedRevision: number | null;
  }): Promise<boolean> {
    const { draft, expectedRevision } = params;
    return this.db.transaction(() => {
      const current = this.db
        .query<{ revision: number }>(`SELECT revision FROM ${this.drafts} WHERE name = ?`)
        .get(draft.name);
      if ((current?.revision ?? null) !== expectedRevision) return false;
      this.db
        .query(
          `INSERT INTO ${this.drafts} (name, schema, revision, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(name) DO UPDATE SET
             schema = excluded.schema,
             revision = excluded.revision,
             created_at = excluded.created_at,
             updated_at = excluded.updated_at`,
        )
        .run(
          draft.name,
          JSON.stringify(draft.schema),
          draft.revision,
          draft.createdAt,
          draft.updatedAt,
        );
      return true;
    })();
  }

  async addVersion(version: WorkflowSchemaVersion): Promise<boolean> {
    return this.db.transaction(() => {
      const taken = this.db
        .query<{ n: number }>(
          `SELECT COUNT(*) AS n FROM ${this.versions}
           WHERE name = ? AND (version = ? OR sequence = ?)`,
        )
        .get(version.name, version.version, version.sequence);
      if ((taken?.n ?? 0) > 0) return false;
      this.db
        .query(
          `INSERT INTO ${this.versions}
             (name, version, sequence, schema, content_hash, published_at)
           VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .run(
          version.name,
          version.version,
          version.sequence,
          JSON.stringify(version.schema),
          version.contentHash,
          version.publishedAt,
        );
      return true;
    })();
  }

  async delete(name: string): Promise<void> {
    this.db.transaction(() => {
      this.db.query(`DELETE FROM ${this.versions} WHERE name = ?`).run(name);
      this.db.query(`DELETE FROM ${this.drafts} WHERE name = ?`).run(name);
    })();
  }
}

function toDraft(row: DraftRow): WorkflowSchemaDraft {
  return {
    name: row.name,
    schema: JSON.parse(row.schema) as WorkflowSchema,
    revision: row.revision,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function toVersion(row: VersionRow): WorkflowSchemaVersion {
  return {
    name: row.name,
    version: row.version,
    sequence: row.sequence,
    schema: JSON.parse(row.schema) as WorkflowSchema,
    contentHash: row.content_hash,
    publishedAt: row.published_at,
  };
}
