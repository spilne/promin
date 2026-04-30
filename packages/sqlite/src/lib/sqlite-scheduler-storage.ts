import type { DurableScheduleConfig, SchedulerStorage, ScheduleTick } from "@promin/workflow";
import { flattenLeafPaths } from "@promin/workflow";
import type { SqliteDatabase } from "./sqlite-database.ts";

/**
 * Persistent `SchedulerStorage` backed by SQLite.
 *
 * State that needs to survive restarts (`lastFiredAt`, `tickCount`,
 * `nextRun`, leader locks) lives on disk so a `bun --hot` reload or a
 * cold restart picks the schedules up where the previous instance left
 * off, instead of resetting to "first poll" every boot.
 *
 * Schema (auto-created on first use):
 *   promin_wf_schedules        — config + state in one row per schedule
 *   promin_wf_schedule_leaders — TTL'd leader locks per namespace
 *
 * Single-file safety: bun:sqlite serializes writes per file via WAL, so
 * two processes opening the same DB get correct CAS on `tryAcquireLeader`.
 * The session-scoped `pg_try_advisory_lock` story doesn't apply — we lean
 * on row-level CAS via `INSERT … ON CONFLICT DO UPDATE WHERE …`.
 */
export class SqliteSchedulerStorage implements SchedulerStorage {
  private readonly _t: string;

  private constructor(
    private readonly db: SqliteDatabase,
    table: string,
  ) {
    this._t = table;
    this._setup();
  }

  static make(params: { db: SqliteDatabase; tablePrefix?: string }): SqliteSchedulerStorage {
    return new SqliteSchedulerStorage(params.db, params.tablePrefix ?? "promin_wf_schedules");
  }

  private _setup(): void {
    const t = this._t;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t} (
        id              TEXT    NOT NULL PRIMARY KEY,
        namespace       TEXT,
        name            TEXT,
        cron            TEXT,
        rrule           TEXT,
        interval_ms     INTEGER,
        timezone        TEXT    NOT NULL DEFAULT 'UTC',
        enabled         INTEGER NOT NULL DEFAULT 1,
        start_at        INTEGER,
        end_at          INTEGER,
        jitter_ms       INTEGER NOT NULL DEFAULT 0,
        metadata        TEXT,
        overlap_policy  TEXT    NOT NULL DEFAULT 'allow',
        max_catch_up    INTEGER NOT NULL DEFAULT 0,
        next_run        INTEGER,
        last_fired_at   INTEGER,
        tick_count      INTEGER NOT NULL DEFAULT 0,
        created_at      INTEGER NOT NULL,
        updated_at      INTEGER NOT NULL
      )
    `);
    // (next_run, namespace) covers both findDue (single-namespace) and
    // findDueAcross (cross-namespace) — both want next_run as the leading
    // sortable key.
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_due ON ${t} (next_run, namespace)`);
    this.db.run(`CREATE INDEX IF NOT EXISTS ${t}_namespace ON ${t} (namespace)`);

    // Leader locks are keyed by namespace string; '' is the sentinel for
    // the global (undefined) namespace so SQLite's PRIMARY KEY uniqueness
    // covers it (NULLs would compare unequal).
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t}_leaders (
        namespace_key TEXT    NOT NULL PRIMARY KEY,
        instance_id   TEXT    NOT NULL,
        expires_at    INTEGER NOT NULL
      )
    `);

    // Past-fire log. Written inside the same transaction as `commitPoll`'s
    // state advance, so `tickCount` and the count of rows here can never
    // disagree. PK is (schedule_id, tick_number) — duplicate inserts from
    // a retried commitPoll are rejected, not silently double-counted. Index
    // on (schedule_id, fired_at DESC) so the history endpoint can paginate
    // by recency without re-scanning.
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${t}_ticks (
        schedule_id   TEXT    NOT NULL,
        tick_number   INTEGER NOT NULL,
        scheduled_at  INTEGER NOT NULL,
        fired_at      INTEGER NOT NULL,
        schedule_name TEXT,
        metadata      TEXT,
        PRIMARY KEY (schedule_id, tick_number)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${t}_ticks_recent ON ${t}_ticks (schedule_id, fired_at DESC)`,
    );
  }

  // ---------------------------------------------------------------------------
  // Hot path
  // ---------------------------------------------------------------------------

  async findDue(params: { now: Date; limit: number; namespace?: string }): Promise<string[]> {
    const nowMs = params.now.getTime();
    const namespaceClause = params.namespace !== undefined ? `namespace = ?` : `namespace IS NULL`;
    const sql = `
      SELECT id FROM ${this._t}
       WHERE enabled = 1
         AND next_run IS NOT NULL
         AND next_run <= ?
         AND ${namespaceClause}
       ORDER BY next_run ASC
       LIMIT ?
    `;
    const args: unknown[] =
      params.namespace !== undefined
        ? [nowMs, params.namespace, params.limit]
        : [nowMs, params.limit];
    const rows = this.db.query<{ id: string }>(sql).all(...args);
    return rows.map((r) => r.id);
  }

  async findDueAcross(params: {
    now: Date;
    limit: number;
    namespaces?: readonly (string | undefined)[];
  }): Promise<readonly { id: string; namespace?: string }[]> {
    const nowMs = params.now.getTime();
    const args: unknown[] = [nowMs];
    let namespaceClause = "";
    if (params.namespaces) {
      const named = params.namespaces.filter((n): n is string => n !== undefined);
      const includeGlobal = params.namespaces.some((n) => n === undefined);
      const branches: string[] = [];
      if (named.length > 0) {
        const placeholders = named.map(() => "?").join(", ");
        branches.push(`namespace IN (${placeholders})`);
        args.push(...named);
      }
      if (includeGlobal) branches.push(`namespace IS NULL`);
      // Empty filter list — match nothing.
      if (branches.length === 0) return [];
      namespaceClause = `AND (${branches.join(" OR ")})`;
    }
    args.push(params.limit);
    const sql = `
      SELECT id, namespace FROM ${this._t}
       WHERE enabled = 1
         AND next_run IS NOT NULL
         AND next_run <= ?
         ${namespaceClause}
       ORDER BY next_run ASC
       LIMIT ?
    `;
    const rows = this.db.query<{ id: string; namespace: string | null }>(sql).all(...args);
    return rows.map((r) => ({ id: r.id, namespace: r.namespace ?? undefined }));
  }

  async loadSchedule(id: string): Promise<DurableScheduleConfig | null> {
    const row = this.db.query<ScheduleRow>(`SELECT * FROM ${this._t} WHERE id = ?`).get(id);
    return row ? rowToConfig(row) : null;
  }

  async loadSchedules(ids: string[]): Promise<Map<string, DurableScheduleConfig>> {
    if (ids.length === 0) return new Map();
    const placeholders = ids.map(() => "?").join(", ");
    const rows = this.db
      .query<ScheduleRow>(`SELECT * FROM ${this._t} WHERE id IN (${placeholders})`)
      .all(...ids);
    const out = new Map<string, DurableScheduleConfig>();
    for (const row of rows) out.set(row.id, rowToConfig(row));
    return out;
  }

  async loadScheduleState(
    id: string,
  ): Promise<{ lastFired: Date | null; tickCount: number } | null> {
    const row = this.db
      .query<{ last_fired_at: number | null; tick_count: number }>(
        `SELECT last_fired_at, tick_count FROM ${this._t} WHERE id = ?`,
      )
      .get(id);
    if (!row) return null;
    return {
      lastFired: row.last_fired_at == null ? null : new Date(row.last_fired_at),
      tickCount: Number(row.tick_count),
    };
  }

  async loadScheduleStates(
    ids: string[],
  ): Promise<Map<string, { lastFired: Date | null; tickCount: number }>> {
    if (ids.length === 0) return new Map();
    const placeholders = ids.map(() => "?").join(", ");
    const rows = this.db
      .query<{ id: string; last_fired_at: number | null; tick_count: number }>(
        `SELECT id, last_fired_at, tick_count FROM ${this._t} WHERE id IN (${placeholders})`,
      )
      .all(...ids);
    const out = new Map<string, { lastFired: Date | null; tickCount: number }>();
    for (const row of rows) {
      out.set(row.id, {
        lastFired: row.last_fired_at == null ? null : new Date(row.last_fired_at),
        tickCount: Number(row.tick_count),
      });
    }
    return out;
  }

  async recordFire(id: string, firedAt: Date, count: number = 1): Promise<void> {
    this.db
      .query(
        `UPDATE ${this._t}
            SET last_fired_at = ?, tick_count = tick_count + ?, updated_at = ?
          WHERE id = ?`,
      )
      .run(firedAt.getTime(), count, Date.now(), id);
  }

  async setNextRun(id: string, nextRun: Date | null): Promise<void> {
    this.db
      .query(`UPDATE ${this._t} SET next_run = ?, updated_at = ? WHERE id = ?`)
      .run(nextRun == null ? null : nextRun.getTime(), Date.now(), id);
  }

  async commitPoll(
    updates: Array<{
      id: string;
      firedAt?: Date;
      tickIncrement?: number;
      nextRun: Date | null;
      ticks?: readonly ScheduleTick[];
    }>,
  ): Promise<void> {
    if (updates.length === 0) return;
    const now = Date.now();
    // Per-row UPDATEs + tick log inserts in a single transaction. SQLite's
    // VALUES + UPDATE FROM syntax is supported on 3.33+, but a tight
    // prepared-statement loop inside a tx is just as fast for the per-poll
    // batch sizes the scheduler produces (up to `batchSize`, default 100)
    // and avoids the dialect version dependency.
    const updateStmt = this.db.query(`
      UPDATE ${this._t}
         SET last_fired_at = COALESCE(?, last_fired_at),
             tick_count    = tick_count + ?,
             next_run      = ?,
             updated_at    = ?
       WHERE id = ?
    `);
    // INSERT OR IGNORE so a retried commitPoll (rare leader-transition
    // edge case) doesn't fail the whole transaction on a duplicate
    // (schedule_id, tick_number) — the row is already there from the
    // first commit, no need to overwrite it.
    const insertTickStmt = this.db.query(`
      INSERT OR IGNORE INTO ${this._t}_ticks
        (schedule_id, tick_number, scheduled_at, fired_at, schedule_name, metadata)
      VALUES (?, ?, ?, ?, ?, ?)
    `);
    this.db.transaction(() => {
      for (const u of updates) {
        updateStmt.run(
          u.firedAt == null ? null : u.firedAt.getTime(),
          u.tickIncrement ?? 0,
          u.nextRun == null ? null : u.nextRun.getTime(),
          now,
          u.id,
        );
        if (u.ticks) {
          for (const t of u.ticks) {
            insertTickStmt.run(
              t.scheduleId,
              t.tickNumber,
              t.scheduledAt.getTime(),
              t.firedAt.getTime(),
              t.scheduleName ?? null,
              t.metadata ? JSON.stringify(t.metadata) : null,
            );
          }
        }
      }
    })();
  }

  async listTicks(params: {
    scheduleId: string;
    limit?: number;
    offset?: number;
  }): Promise<readonly ScheduleTick[]> {
    const limit = params.limit ?? 100;
    const offset = params.offset ?? 0;
    const rows = this.db
      .query<TickRow>(
        `SELECT schedule_id, tick_number, scheduled_at, fired_at, schedule_name, metadata
           FROM ${this._t}_ticks
          WHERE schedule_id = ?
          ORDER BY fired_at DESC, tick_number DESC
          LIMIT ? OFFSET ?`,
      )
      .all(params.scheduleId, limit, offset);
    return rows.map(rowToTick);
  }

  async countTicks(params: { scheduleId: string }): Promise<number> {
    const row = this.db
      .query<{ c: number }>(`SELECT COUNT(*) AS c FROM ${this._t}_ticks WHERE schedule_id = ?`)
      .get(params.scheduleId);
    return Number(row?.c ?? 0);
  }

  // ---------------------------------------------------------------------------
  // Admin / CRUD
  // ---------------------------------------------------------------------------

  async upsertSchedule(config: DurableScheduleConfig): Promise<void> {
    const now = Date.now();
    this.db
      .query(
        `
      INSERT INTO ${this._t}
        (id, namespace, name, cron, rrule, interval_ms, timezone, enabled,
         start_at, end_at, jitter_ms, metadata, overlap_policy, max_catch_up,
         next_run, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (id) DO UPDATE SET
        namespace      = excluded.namespace,
        name           = excluded.name,
        cron           = excluded.cron,
        rrule          = excluded.rrule,
        interval_ms    = excluded.interval_ms,
        timezone       = excluded.timezone,
        enabled        = excluded.enabled,
        start_at       = excluded.start_at,
        end_at         = excluded.end_at,
        jitter_ms      = excluded.jitter_ms,
        metadata       = excluded.metadata,
        overlap_policy = excluded.overlap_policy,
        max_catch_up   = excluded.max_catch_up,
        updated_at     = excluded.updated_at
      `,
      )
      .run(
        config.id,
        config.namespace ?? null,
        config.name ?? null,
        config.cron ?? null,
        config.rrule ?? null,
        config.intervalMs ?? null,
        config.timezone ?? "UTC",
        config.enabled === false ? 0 : 1,
        config.startAt?.getTime() ?? null,
        config.endAt?.getTime() ?? null,
        config.jitterMs ?? 0,
        config.metadata ? JSON.stringify(config.metadata) : null,
        config.overlapPolicy ?? "allow",
        config.maxCatchUp ?? 0,
        config.enabled === false ? null : now, // seed next_run on enabled INSERT only
        now,
        now,
      );
  }

  async deleteSchedule(id: string): Promise<void> {
    // One transaction so the schedule row + its tick log disappear atomically.
    this.db.transaction(() => {
      this.db.query(`DELETE FROM ${this._t}_ticks WHERE schedule_id = ?`).run(id);
      this.db.query(`DELETE FROM ${this._t} WHERE id = ?`).run(id);
    })();
  }

  async setEnabled(id: string, enabled: boolean): Promise<void> {
    this.db
      .query(`UPDATE ${this._t} SET enabled = ?, updated_at = ? WHERE id = ?`)
      .run(enabled ? 1 : 0, Date.now(), id);
  }

  async listSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
    limit?: number;
    offset?: number;
  }): Promise<DurableScheduleConfig[]> {
    const { where, args } = this.buildScheduleFilters(params);
    const limit = params?.limit ?? 100;
    const offset = params?.offset ?? 0;
    const rows = this.db
      .query<ScheduleRow>(`SELECT * FROM ${this._t} ${where} LIMIT ? OFFSET ?`)
      .all(...args, limit, offset);
    return rows.map(rowToConfig);
  }

  async countSchedules(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
  }): Promise<number> {
    const { where, args } = this.buildScheduleFilters(params);
    const row = this.db
      .query<{ c: number }>(`SELECT COUNT(*) AS c FROM ${this._t} ${where}`)
      .get(...args);
    return Number(row?.c ?? 0);
  }

  /**
   * Shared WHERE-clause builder for list / count so they can't drift on
   * predicate semantics. Translates the metadata containment filter into
   * `json_extract(metadata, '$.<path>') = ?` clauses — Postgres-`@>`-style
   * containment, with each leaf path becoming one indexable predicate.
   *
   * No JSON indexes are created by default. Hot deployments that filter
   * heavily on `metadata.target.type` etc. can add a functional index:
   *     CREATE INDEX idx_schedules_target_type
   *       ON schedules (json_extract(metadata, '$.target.type'));
   */
  private buildScheduleFilters(params?: {
    enabled?: boolean;
    namespace?: string;
    metadata?: Record<string, unknown>;
  }): { where: string; args: unknown[] } {
    const filters: string[] = [];
    const args: unknown[] = [];
    if (params?.enabled !== undefined) {
      filters.push("enabled = ?");
      args.push(params.enabled ? 1 : 0);
    }
    if (params?.namespace !== undefined) {
      filters.push("namespace = ?");
      args.push(params.namespace);
    }
    if (params?.metadata) {
      for (const { path, value } of flattenLeafPaths(params.metadata)) {
        const jsonPath = jsonPathExpr(path);
        if (value === null) {
          filters.push(`json_extract(metadata, ?) IS NULL`);
          args.push(jsonPath);
        } else if (typeof value === "boolean") {
          // SQLite's JSON1 extracts booleans as 0/1 INTEGERs.
          filters.push(`json_extract(metadata, ?) = ?`);
          args.push(jsonPath, value ? 1 : 0);
        } else if (Array.isArray(value)) {
          // Compare the subtree's serialised shape. Stable enough for the
          // small array filters our callers actually use.
          filters.push(`json_extract(metadata, ?) = json(?)`);
          args.push(jsonPath, JSON.stringify(value));
        } else {
          // string / number — direct compare; SQLite typing handles both.
          filters.push(`json_extract(metadata, ?) = ?`);
          args.push(jsonPath, value);
        }
      }
    }
    const where = filters.length > 0 ? `WHERE ${filters.join(" AND ")}` : "";
    return { where, args };
  }

  // ---------------------------------------------------------------------------
  // Leader election — TTL'd row CAS keyed by namespace
  // ---------------------------------------------------------------------------

  async tryAcquireLeader(params: {
    instanceId: string;
    namespace?: string;
    ttlMs: number;
  }): Promise<boolean> {
    const key = params.namespace ?? "";
    const now = Date.now();
    const expiresAt = now + params.ttlMs;

    // CAS in one transaction: take the lock when nobody holds it, the prior
    // holder's TTL has expired, OR the same instance is refreshing. Returns
    // the row that ends up winning, so the caller can compare instance ids.
    return this.db.transaction((): boolean => {
      const existing = this.db
        .query<{ instance_id: string; expires_at: number }>(
          `SELECT instance_id, expires_at FROM ${this._t}_leaders WHERE namespace_key = ?`,
        )
        .get(key);
      if (existing && existing.expires_at > now && existing.instance_id !== params.instanceId) {
        return false;
      }
      this.db
        .query(
          `
        INSERT INTO ${this._t}_leaders (namespace_key, instance_id, expires_at)
        VALUES (?, ?, ?)
        ON CONFLICT (namespace_key) DO UPDATE SET
          instance_id = excluded.instance_id,
          expires_at  = excluded.expires_at
        `,
        )
        .run(key, params.instanceId, expiresAt);
      return true;
    })();
  }
}

interface ScheduleRow {
  id: string;
  namespace: string | null;
  name: string | null;
  cron: string | null;
  rrule: string | null;
  interval_ms: number | null;
  timezone: string;
  enabled: number;
  start_at: number | null;
  end_at: number | null;
  jitter_ms: number;
  metadata: string | null;
  overlap_policy: string;
  max_catch_up: number;
  next_run: number | null;
  last_fired_at: number | null;
  tick_count: number;
  created_at: number;
  updated_at: number;
}

function rowToConfig(row: ScheduleRow): DurableScheduleConfig {
  return {
    id: row.id,
    namespace: row.namespace ?? undefined,
    name: row.name ?? undefined,
    cron: row.cron ?? undefined,
    rrule: row.rrule ?? undefined,
    intervalMs: row.interval_ms ?? undefined,
    timezone: row.timezone,
    enabled: row.enabled === 1,
    startAt: row.start_at == null ? undefined : new Date(row.start_at),
    endAt: row.end_at == null ? undefined : new Date(row.end_at),
    jitterMs: row.jitter_ms,
    metadata: row.metadata ? (JSON.parse(row.metadata) as Record<string, unknown>) : undefined,
    overlapPolicy: row.overlap_policy as DurableScheduleConfig["overlapPolicy"],
    maxCatchUp: row.max_catch_up,
  };
}

interface TickRow {
  schedule_id: string;
  tick_number: number;
  scheduled_at: number;
  fired_at: number;
  schedule_name: string | null;
  metadata: string | null;
}

function rowToTick(row: TickRow): ScheduleTick {
  return {
    scheduleId: row.schedule_id,
    tickNumber: row.tick_number,
    scheduledAt: new Date(row.scheduled_at),
    firedAt: new Date(row.fired_at),
    scheduleName: row.schedule_name ?? undefined,
    metadata: row.metadata ? (JSON.parse(row.metadata) as Record<string, unknown>) : undefined,
  };
}

/**
 * Build a SQLite `json_extract` path string from a key array.
 * SQLite's JSON path grammar accepts the simple dotted form for
 * identifier-shaped keys (`$.foo.bar`); we throw on keys outside that
 * shape rather than silently mis-querying. The convention metadata
 * stamped by our writers (durable scheduler tool, default trigger)
 * always uses identifier-shaped keys, so this is a real precondition.
 */
function jsonPathExpr(path: string[]): string {
  for (const k of path) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(k)) {
      throw new Error(
        `SqliteSchedulerStorage: metadata filter path part "${k}" must be an identifier-shaped key (alphanumeric + underscore, leading non-digit). Got: ${JSON.stringify(path)}`,
      );
    }
  }
  return `$.${path.join(".")}`;
}
