// ---------------------------------------------------------------------------
// `SqliteWorkflowAdvertisementRegistry` — `WorkflowAdvertisementRegistry`
// over SQLite.
//
// Workers advertise their workflow definitions on connect (`upsert`) and
// the dashboard reads `distinct()` to populate the Workflows page. The
// in-memory impl loses everything on restart; this Sqlite impl persists
// across boots so workers don't have to re-advertise on every reconnect
// and the dashboard's Workflows tab keeps showing the catalog while
// workers are momentarily disconnected.
//
// Schema (auto-created on first use):
//
//   CREATE TABLE promin_workflow_advertisements (
//     worker_id     TEXT NOT NULL,
//     workflow_name TEXT NOT NULL,
//     version       TEXT,
//     steps         TEXT NOT NULL,    -- JSON array
//     sample_input  TEXT,             -- JSON
//     advertised_at INTEGER NOT NULL,
//     PRIMARY KEY (worker_id, workflow_name, COALESCE(version, ''))
//   );
// ---------------------------------------------------------------------------

import type {
  AdvertisedWorkflow,
  AdvertisementEntry,
  WorkflowAdvertisementRegistry,
} from "@promin/workflow";
import type { SqliteDatabase } from "./sqlite-database.ts";

interface Row {
  worker_id: string;
  workflow_name: string;
  version: string | null;
  steps: string;
  sample_input: string | null;
  advertised_at: number;
}

export interface SqliteWorkflowAdvertisementRegistryOptions {
  db: SqliteDatabase;
  /** Override the table name (default: `promin_workflow_advertisements`). */
  tableName?: string;
}

export class SqliteWorkflowAdvertisementRegistry implements WorkflowAdvertisementRegistry {
  private readonly _t: string;

  private constructor(
    private readonly db: SqliteDatabase,
    table: string,
  ) {
    this._t = table;
    this._setup();
  }

  static make(
    opts: SqliteWorkflowAdvertisementRegistryOptions,
  ): SqliteWorkflowAdvertisementRegistry {
    return new SqliteWorkflowAdvertisementRegistry(
      opts.db,
      opts.tableName ?? "promin_workflow_advertisements",
    );
  }

  private _setup(): void {
    const t = this._t;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t} (
        worker_id     TEXT NOT NULL,
        workflow_name TEXT NOT NULL,
        version       TEXT,
        steps         TEXT NOT NULL,
        sample_input  TEXT,
        advertised_at INTEGER NOT NULL,
        PRIMARY KEY (worker_id, workflow_name, version)
      )
    `);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_workflow ON ${t} (workflow_name, version)`);
  }

  async upsert(workerId: string, workflows: AdvertisedWorkflow[]): Promise<void> {
    // Replace all rows for this worker atomically — a worker that
    // re-advertises with a smaller set should drop the workflows it no
    // longer hosts. Wrap in a transaction so a mid-replace failure
    // leaves the previous advertisement intact rather than partially gone.
    const t = this._t;
    const now = Date.now();
    this.db.run("BEGIN IMMEDIATE");
    try {
      this.db.query(`DELETE FROM ${t} WHERE worker_id = ?`).run(workerId);
      for (const wf of workflows) {
        this.db
          .query(
            `INSERT INTO ${t}
               (worker_id, workflow_name, version, steps, sample_input, advertised_at)
             VALUES (?, ?, ?, ?, ?, ?)`,
          )
          .run(
            workerId,
            wf.name,
            wf.version ?? null,
            JSON.stringify(wf.steps),
            wf.sampleInput !== undefined ? JSON.stringify(wf.sampleInput) : null,
            now,
          );
      }
      this.db.run("COMMIT");
    } catch (err) {
      this.db.run("ROLLBACK");
      throw err;
    }
  }

  async remove(workerId: string): Promise<void> {
    this.db.query(`DELETE FROM ${this._t} WHERE worker_id = ?`).run(workerId);
  }

  async list(): Promise<AdvertisementEntry[]> {
    const rows = this.db
      .query<Row, []>(`SELECT * FROM ${this._t} ORDER BY worker_id, workflow_name`)
      .all();
    const byWorker = new Map<string, AdvertisementEntry>();
    for (const r of rows) {
      let entry = byWorker.get(r.worker_id);
      if (!entry) {
        entry = {
          workerId: r.worker_id,
          workflows: [],
          advertisedAt: new Date(r.advertised_at),
        };
        byWorker.set(r.worker_id, entry);
      }
      entry.workflows.push(rowToAdvertisedWorkflow(r));
      // Latest advertised_at across this worker's rows
      if (r.advertised_at > entry.advertisedAt.getTime()) {
        entry.advertisedAt = new Date(r.advertised_at);
      }
    }
    return [...byWorker.values()];
  }

  async distinct(): Promise<AdvertisedWorkflow[]> {
    // Dedupe on (workflow_name, version), keeping whichever row was
    // advertised most recently. Mirrors InMemoryWorkflowAdvertisementRegistry.
    const rows = this.db
      .query<Row, []>(
        `SELECT * FROM ${this._t}
         ORDER BY workflow_name ASC, COALESCE(version, '') ASC, advertised_at DESC`,
      )
      .all();
    const byKey = new Map<string, AdvertisedWorkflow>();
    for (const r of rows) {
      const key = `${r.workflow_name}@${r.version ?? ""}`;
      // First row per key wins (we ordered by advertised_at DESC within the key).
      if (!byKey.has(key)) byKey.set(key, rowToAdvertisedWorkflow(r));
    }
    return [...byKey.values()];
  }
}

function rowToAdvertisedWorkflow(r: Row): AdvertisedWorkflow {
  const out: AdvertisedWorkflow = {
    name: r.workflow_name,
    steps: JSON.parse(r.steps) as AdvertisedWorkflow["steps"],
  };
  if (r.version !== null) out.version = r.version;
  if (r.sample_input !== null) out.sampleInput = JSON.parse(r.sample_input);
  return out;
}
