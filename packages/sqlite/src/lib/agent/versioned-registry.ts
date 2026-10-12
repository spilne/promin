// ---------------------------------------------------------------------------
// `SqliteVersionedRegistry` — `VersionedRegistry` over SQLite, one table pair
// shared by every kind (agents, roles, skills, DAGs, fragments), keyed by
// the kind's name.
//
//   promin_registry_head     (kind, id) → active version (NULL while the id
//                            has no live version), activation history (JSON),
//                            kill switch, next seq. Never deleted.
//   promin_registry_version  (kind, id, version) → immutable body + hash,
//                            facets (JSON array), trust, source, created_at,
//                            removed (a tombstone keeps the label bound).
//
// Every write runs in one `BEGIN IMMEDIATE` transaction, which takes the
// database write lock before the first read: writers in this process and
// in other processes on the same file take turns, so a register never
// mints a sequence number twice or loses a compare-and-set, and no write
// fails with SQLITE_BUSY_SNAPSHOT. A writer that finds the lock held waits
// for the connection's `busy_timeout` (set it when several processes share
// the file) and fails with SQLITE_BUSY after it. `list` filters on facets
// in SQL before LIMIT and pages by id.
// ---------------------------------------------------------------------------

import {
  assertListLimit,
  autoVersionLabel,
  facetQuery,
  nextActiveVersion,
  prepareRegister,
  pushHistory,
  RegistryConflictError,
  RegistryNotFoundError,
  type ListVersionedParams,
  type RegisterVersionInput,
  type RegistryKind,
  type Versioned,
  type VersionedPage,
  type VersionedRegistry,
  type VersionTrust,
} from "@promin/agent";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { SqliteDatabase } from "../sqlite-database.ts";

const HEAD = "promin_registry_head";
const VERSION = "promin_registry_version";

export interface SqliteVersionedRegistryConfig<TBody, TFilter, TInput> {
  readonly db: SqliteDatabase;
  readonly kind: RegistryKind<TBody, TFilter, TInput>;
  /** Time source for `created_at`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

interface HeadRow {
  active_version: string | null;
  history: string;
  enabled: number;
  next_seq: number;
}

interface VersionRow {
  id: string;
  version: string;
  seq: number;
  body: string;
  body_hash: string;
  trust: string;
  source: string | null;
  created_at: number;
  removed: number;
  active_version: string | null;
  enabled: number;
}

/** Create the shared registry tables. Idempotent. */
function ensureSchema(db: SqliteDatabase): void {
  db.run(`
    CREATE TABLE IF NOT EXISTS ${HEAD} (
      kind           TEXT NOT NULL,
      id             TEXT NOT NULL,
      active_version TEXT,
      history        TEXT NOT NULL,
      enabled        INTEGER NOT NULL,
      next_seq       INTEGER NOT NULL,
      PRIMARY KEY (kind, id)
    )
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS ${VERSION} (
      kind       TEXT NOT NULL,
      id         TEXT NOT NULL,
      version    TEXT NOT NULL,
      seq        INTEGER NOT NULL,
      body       TEXT NOT NULL,
      body_hash  TEXT NOT NULL,
      facets     TEXT NOT NULL,
      trust      TEXT NOT NULL,
      source     TEXT,
      created_at INTEGER NOT NULL,
      removed    INTEGER NOT NULL,
      PRIMARY KEY (kind, id, version),
      UNIQUE (kind, id, seq)
    )
  `);
  db.run(`CREATE INDEX IF NOT EXISTS ${VERSION}_hash_idx ON ${VERSION} (kind, id, body_hash)`);
}

const SELECT_JOINED = `
  SELECT v.id, v.version, v.seq, v.body, v.body_hash, v.trust, v.source, v.created_at,
         v.removed, h.active_version, h.enabled
  FROM ${VERSION} v
  JOIN ${HEAD} h ON h.kind = v.kind AND h.id = v.id`;

export class SqliteVersionedRegistry<TBody, TFilter, TInput = TBody> implements VersionedRegistry<
  TBody,
  TFilter,
  TInput
> {
  readonly kind: RegistryKind<TBody, TFilter, TInput>;
  private readonly db: SqliteDatabase;
  private readonly clock: WallClock;

  constructor(config: SqliteVersionedRegistryConfig<TBody, TFilter, TInput>) {
    this.db = config.db;
    this.kind = config.kind;
    this.clock = config.clock ?? SystemWallClock;
    ensureSchema(this.db);
  }

  async register(input: RegisterVersionInput<TInput>): Promise<Versioned<TBody>> {
    const prepared = prepareRegister({ kind: this.kind, input });
    const { options } = prepared;
    const kind = this.kind.name;
    const id = options.id;
    return this.write((): Versioned<TBody> => {
      const head = this.head(id);
      this.assertActive({ id, head, expected: options.expectedActive });

      let existing: { version: string; body_hash: string } | null = null;
      if (options.version !== undefined) {
        existing = this.db
          .query<{ version: string; body_hash: string }>(
            `SELECT version, body_hash FROM ${VERSION} WHERE kind = ? AND id = ? AND version = ?`,
          )
          .get(kind, id, options.version);
        if (existing && existing.body_hash !== prepared.hash) {
          throw new RegistryConflictError({
            code: "version_conflict",
            kind,
            id,
            version: options.version,
          });
        }
      } else if (head) {
        existing = this.db
          .query<{ version: string; body_hash: string }>(
            `SELECT version, body_hash FROM ${VERSION}
             WHERE kind = ? AND id = ? AND body_hash = ? ORDER BY removed, seq DESC LIMIT 1`,
          )
          .get(kind, id, prepared.hash);
      }

      let version: string;
      let nextSeq = head?.next_seq ?? 1;
      if (existing) {
        version = existing.version;
        // A tombstone with the same body is restored.
        this.db
          .query(`UPDATE ${VERSION} SET removed = 0 WHERE kind = ? AND id = ? AND version = ?`)
          .run(kind, id, version);
      } else {
        const seq = nextSeq;
        version =
          options.version ??
          autoVersionLabel({
            seq,
            taken: (v) =>
              this.db
                .query(`SELECT 1 FROM ${VERSION} WHERE kind = ? AND id = ? AND version = ?`)
                .get(kind, id, v) !== null,
          });
        this.db
          .query(
            `INSERT INTO ${VERSION}
               (kind, id, version, seq, body, body_hash, facets, trust, source, created_at, removed)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
          )
          .run(
            kind,
            id,
            version,
            seq,
            JSON.stringify(prepared.body),
            prepared.hash,
            JSON.stringify(prepared.facets),
            options.trust ?? "trusted",
            options.source ?? null,
            this.clock.currentTimeMs(),
          );
        nextSeq = seq + 1;
      }

      let active = head?.active_version ?? null;
      let history: string[] = head ? (JSON.parse(head.history) as string[]) : [];
      let enabled = head?.enabled === 0 ? false : true;
      if (active === null) {
        // The id's first live version: active, switch reset.
        active = version;
        history = [];
        enabled = true;
      } else if (options.promote && active !== version) {
        history = pushHistory({ history, previous: active, next: version });
        active = version;
      }
      if (options.enabled !== undefined) enabled = options.enabled;
      this.db
        .query(
          `INSERT INTO ${HEAD} (kind, id, active_version, history, enabled, next_seq)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT (kind, id) DO UPDATE SET
             active_version = excluded.active_version, history = excluded.history,
             enabled = excluded.enabled, next_seq = excluded.next_seq`,
        )
        .run(kind, id, active, JSON.stringify(history), enabled ? 1 : 0, nextSeq);
      return this.read({ id, version })!;
    });
  }

  async get(params: { id: string; version?: string }): Promise<Versioned<TBody> | null> {
    if (params.version !== undefined) return this.read({ id: params.id, version: params.version });
    const row = this.db
      .query<VersionRow>(
        `${SELECT_JOINED} WHERE v.kind = ? AND v.id = ? AND v.version = h.active_version`,
      )
      .get(this.kind.name, params.id);
    return row ? this.decode(row) : null;
  }

  async list(params: ListVersionedParams<TFilter> = {}): Promise<VersionedPage<Versioned<TBody>>> {
    assertListLimit(params.limit);
    const query = facetQuery({ kind: this.kind, filter: params.filter });
    const where = ["v.kind = ?", "v.version = h.active_version"];
    const args: unknown[] = [this.kind.name];
    if (params.cursor !== undefined) {
      where.push("v.id > ?");
      args.push(params.cursor);
    }
    if (params.enabled !== undefined) {
      where.push("h.enabled = ?");
      args.push(params.enabled ? 1 : 0);
    }
    if (params.source !== undefined) {
      where.push("v.source = ?");
      args.push(params.source);
    }
    for (const facet of query.include) {
      where.push(`EXISTS (SELECT 1 FROM json_each(v.facets) WHERE json_each.value = ?)`);
      args.push(facet);
    }
    for (const facet of query.exclude) {
      where.push(`NOT EXISTS (SELECT 1 FROM json_each(v.facets) WHERE json_each.value = ?)`);
      args.push(facet);
    }
    let sql = `${SELECT_JOINED} WHERE ${where.join(" AND ")} ORDER BY v.id`;
    if (params.limit !== undefined) {
      sql += " LIMIT ?";
      args.push(params.limit + 1);
    }
    const rows = this.db.query<VersionRow>(sql).all(...args);
    const hasMore = params.limit !== undefined && rows.length > params.limit;
    const items = (hasMore ? rows.slice(0, params.limit) : rows).map((r) => this.decode(r));
    return { items, nextCursor: hasMore ? items[items.length - 1]!.id : null };
  }

  async versions(params: { id: string }): Promise<Versioned<TBody>[]> {
    return this.db
      .query<VersionRow>(
        `${SELECT_JOINED} WHERE v.kind = ? AND v.id = ? AND v.removed = 0 ORDER BY v.seq`,
      )
      .all(this.kind.name, params.id)
      .map((r) => this.decode(r));
  }

  async promote(params: {
    id: string;
    version: string;
    expectedActive?: string | null;
  }): Promise<Versioned<TBody>> {
    return this.write((): Versioned<TBody> => {
      const head = this.head(params.id);
      this.assertActive({ id: params.id, head, expected: params.expectedActive });
      if (!head || !this.read({ id: params.id, version: params.version })) {
        throw new RegistryNotFoundError({
          kind: this.kind.name,
          id: params.id,
          version: params.version,
        });
      }
      this.setActive({
        id: params.id,
        active: params.version,
        history: pushHistory({
          history: JSON.parse(head.history) as string[],
          previous: head.active_version,
          next: params.version,
        }),
      });
      return this.read({ id: params.id, version: params.version })!;
    });
  }

  async setEnabled(params: {
    id: string;
    enabled: boolean;
    expectedActive?: string | null;
  }): Promise<Versioned<TBody>> {
    return this.write((): Versioned<TBody> => {
      const head = this.head(params.id);
      this.assertActive({ id: params.id, head, expected: params.expectedActive });
      if (!head || head.active_version === null) {
        throw new RegistryNotFoundError({ kind: this.kind.name, id: params.id });
      }
      this.db
        .query(`UPDATE ${HEAD} SET enabled = ? WHERE kind = ? AND id = ?`)
        .run(params.enabled ? 1 : 0, this.kind.name, params.id);
      return this.read({ id: params.id, version: head.active_version })!;
    });
  }

  async setTrust(params: {
    id: string;
    version: string;
    trust: VersionTrust;
  }): Promise<Versioned<TBody>> {
    return this.write((): Versioned<TBody> => {
      if (!this.read({ id: params.id, version: params.version })) {
        throw new RegistryNotFoundError({
          kind: this.kind.name,
          id: params.id,
          version: params.version,
        });
      }
      this.db
        .query(`UPDATE ${VERSION} SET trust = ? WHERE kind = ? AND id = ? AND version = ?`)
        .run(params.trust, this.kind.name, params.id, params.version);
      return this.read({ id: params.id, version: params.version })!;
    });
  }

  async unregister(params: {
    id: string;
    version?: string;
    expectedActive?: string | null;
    promote?: string;
  }): Promise<boolean> {
    const kind = this.kind.name;
    const id = params.id;
    return this.write((): boolean => {
      const head = this.head(id);
      this.assertActive({ id, head, expected: params.expectedActive });
      if (!head || head.active_version === null) return false;
      if (params.version === undefined) {
        this.db.query(`UPDATE ${VERSION} SET removed = 1 WHERE kind = ? AND id = ?`).run(kind, id);
        this.setActive({ id, active: null, history: [] });
        return true;
      }
      if (!this.read({ id, version: params.version })) return false;
      const removingActive = head.active_version === params.version;
      if (
        removingActive &&
        params.promote !== undefined &&
        (params.promote === params.version || !this.read({ id, version: params.promote }))
      ) {
        throw new RegistryNotFoundError({ kind, id, version: params.promote });
      }
      this.db
        .query(`UPDATE ${VERSION} SET removed = 1 WHERE kind = ? AND id = ? AND version = ?`)
        .run(kind, id, params.version);
      const live = this.db
        .query<{ version: string }>(
          `SELECT version FROM ${VERSION} WHERE kind = ? AND id = ? AND removed = 0 ORDER BY seq`,
        )
        .all(kind, id)
        .map((r) => r.version);
      const active = removingActive
        ? nextActiveVersion({
            ...(params.promote !== undefined && { promote: params.promote }),
            history: JSON.parse(head.history) as string[],
            live,
          })
        : head.active_version;
      const history = (JSON.parse(head.history) as string[]).filter(
        (v) => live.includes(v) && v !== active,
      );
      this.setActive({ id, active, history });
      return true;
    });
  }

  /** Run `fn` in one `BEGIN IMMEDIATE` transaction. */
  private write<T>(fn: () => T): T {
    this.db.run("BEGIN IMMEDIATE");
    try {
      const out = fn();
      this.db.run("COMMIT");
      return out;
    } catch (err) {
      this.db.run("ROLLBACK");
      throw err;
    }
  }

  private head(id: string): HeadRow | null {
    return this.db
      .query<HeadRow>(
        `SELECT active_version, history, enabled, next_seq FROM ${HEAD} WHERE kind = ? AND id = ?`,
      )
      .get(this.kind.name, id);
  }

  private setActive(params: { id: string; active: string | null; history: string[] }): void {
    this.db
      .query(`UPDATE ${HEAD} SET active_version = ?, history = ? WHERE kind = ? AND id = ?`)
      .run(params.active, JSON.stringify(params.history), this.kind.name, params.id);
  }

  /** A live version. */
  private read(params: { id: string; version: string }): Versioned<TBody> | null {
    const row = this.db
      .query<VersionRow>(
        `${SELECT_JOINED} WHERE v.kind = ? AND v.id = ? AND v.version = ? AND v.removed = 0`,
      )
      .get(this.kind.name, params.id, params.version);
    return row ? this.decode(row) : null;
  }

  private assertActive(params: {
    id: string;
    head: HeadRow | null;
    expected: string | null | undefined;
  }): void {
    if (params.expected === undefined) return;
    const actual = params.head?.active_version ?? null;
    if (actual !== params.expected) {
      throw new RegistryConflictError({
        code: "active_mismatch",
        kind: this.kind.name,
        id: params.id,
        version: params.expected,
        actual,
      });
    }
  }

  private decode(row: VersionRow): Versioned<TBody> {
    const body = JSON.parse(row.body) as TBody;
    return {
      ...body,
      id: row.id,
      version: row.version,
      createdAt: row.created_at,
      active: row.active_version === row.version,
      enabled: row.enabled === 1,
      trust: row.trust as VersionTrust,
      ...(row.source !== null && { source: row.source }),
    };
  }
}
