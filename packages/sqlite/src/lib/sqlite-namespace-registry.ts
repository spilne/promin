import {
  NamespaceExistsError,
  NamespaceNotFoundError,
  SystemWallClock,
  isValidNamespaceId,
  type CreateNamespaceInput,
  type NamespaceCapabilities,
  type NamespacePatch,
  type NamespaceRecord,
  type NamespaceRegistry,
  type NamespaceStatus,
  type WallClock,
} from "@promin/workflow";
import {
  normalizeNamespaceCreate,
  normalizeNamespacePatch,
  parseNamespaceStatus,
} from "@promin/workflow/storage-kit";
import type { SqliteDatabase } from "./sqlite-database.ts";

interface Row {
  id: string;
  display_name: string;
  description: string | null;
  status: string;
  capabilities: string;
  metadata: string;
  created_at: number;
  updated_at: number;
}

/**
 * Persistent `NamespaceRegistry` backed by SQLite. The table
 * (`<prefix>_namespaces`) is created on first use. Capabilities and
 * metadata are stored as JSON text, timestamps as epoch-ms integers stamped
 * from the injected clock. `update` is one `UPDATE … RETURNING` of the
 * patched columns, so concurrent updates of different fields both land.
 *
 * @example
 * ```ts
 * import { Database } from "bun:sqlite";
 * const namespaces = SqliteNamespaceRegistry.make({ db: new Database("state.db") });
 * await namespaces.create({ id: "acme", displayName: "Acme" });
 * ```
 */
export class SqliteNamespaceRegistry implements NamespaceRegistry {
  private readonly table: string;

  private constructor(
    private readonly db: SqliteDatabase,
    tablePrefix: string,
    private readonly clock: WallClock,
  ) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(tablePrefix)) {
      throw new Error(`SqliteNamespaceRegistry: invalid table prefix "${tablePrefix}"`);
    }
    this.table = `${tablePrefix}_namespaces`;
    this.setup();
  }

  static make(params: {
    db: SqliteDatabase;
    /** Table prefix (default: `promin_wf`). The table is `<prefix>_namespaces`. */
    tablePrefix?: string;
    /** Time source for `createdAt` / `updatedAt`. Default: `SystemWallClock`. */
    clock?: WallClock;
  }): SqliteNamespaceRegistry {
    return new SqliteNamespaceRegistry(
      params.db,
      params.tablePrefix ?? "promin_wf",
      params.clock ?? SystemWallClock,
    );
  }

  private setup(): void {
    const t = this.table;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t} (
        id           TEXT    NOT NULL PRIMARY KEY,
        display_name TEXT    NOT NULL,
        description  TEXT,
        status       TEXT    NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived')),
        capabilities TEXT    NOT NULL DEFAULT '{}',
        metadata     TEXT    NOT NULL DEFAULT '{}',
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL
      )
    `);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_status ON ${t} (status, id)`);
  }

  async create(input: CreateNamespaceInput): Promise<NamespaceRecord> {
    const fields = normalizeNamespaceCreate(input);
    const now = this.clock.currentTimeMs();
    const row = this.db
      .query<Row>(
        `INSERT INTO ${this.table}
           (id, display_name, description, status, capabilities, metadata, created_at, updated_at)
         VALUES (?, ?, ?, 'active', ?, ?, ?, ?)
         ON CONFLICT(id) DO NOTHING
         RETURNING *`,
      )
      .get(
        fields.id,
        fields.displayName,
        fields.description,
        JSON.stringify(fields.capabilities),
        JSON.stringify(fields.metadata),
        now,
        now,
      );
    if (!row) throw NamespaceExistsError.of(fields.id);
    return toRecord(row);
  }

  async get(id: string): Promise<NamespaceRecord | null> {
    if (!isValidNamespaceId(id)) return null;
    const row = this.db.query<Row>(`SELECT * FROM ${this.table} WHERE id = ?`).get(id);
    return row ? toRecord(row) : null;
  }

  async list(params: { readonly status?: NamespaceStatus } = {}): Promise<NamespaceRecord[]> {
    const rows =
      params.status !== undefined
        ? this.db
            .query<Row>(`SELECT * FROM ${this.table} WHERE status = ? ORDER BY id`)
            .all(parseNamespaceStatus(params.status))
        : this.db.query<Row>(`SELECT * FROM ${this.table} ORDER BY id`).all();
    return rows.map(toRecord);
  }

  async update(params: {
    readonly id: string;
    readonly patch: NamespacePatch;
  }): Promise<NamespaceRecord> {
    const patch = normalizeNamespacePatch(params);
    const sets: string[] = ["updated_at = ?"];
    const values: Array<string | number | null> = [this.clock.currentTimeMs()];
    if (patch.displayName !== undefined) {
      sets.push("display_name = ?");
      values.push(patch.displayName);
    }
    if (patch.description !== undefined) {
      sets.push("description = ?");
      values.push(patch.description);
    }
    if (patch.status !== undefined) {
      sets.push("status = ?");
      values.push(patch.status);
    }
    if (patch.capabilities !== undefined) {
      sets.push("capabilities = ?");
      values.push(JSON.stringify(patch.capabilities));
    }
    if (patch.metadata !== undefined) {
      sets.push("metadata = ?");
      values.push(JSON.stringify(patch.metadata));
    }
    const row = this.db
      .query<Row>(`UPDATE ${this.table} SET ${sets.join(", ")} WHERE id = ? RETURNING *`)
      .get(...values, params.id);
    if (!row) throw NamespaceNotFoundError.of(params.id);
    return toRecord(row);
  }

  async delete(id: string): Promise<boolean> {
    const row = this.db
      .query<{ id: string }>(`DELETE FROM ${this.table} WHERE id = ? RETURNING id`)
      .get(id);
    return row != null;
  }
}

function toRecord(row: Row): NamespaceRecord {
  return {
    id: row.id,
    displayName: row.display_name,
    description: row.description,
    status: parseNamespaceStatus(row.status),
    capabilities: JSON.parse(row.capabilities) as NamespaceCapabilities,
    metadata: JSON.parse(row.metadata) as Record<string, unknown>,
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
  };
}
