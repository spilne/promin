// ---------------------------------------------------------------------------
// `PostgresVersionedRegistry` — `VersionedRegistry` over Postgres, one table
// pair shared by every kind (agents, roles, skills, DAGs, fragments), keyed
// by the kind's name:
//
//   registry_head     (kind, id) → active version (NULL while the id has no
//                     live version), activation history, kill switch, next
//                     seq. Never deleted, so sequence numbers never restart.
//   registry_version  (kind, id, version) → immutable body + hash, facets,
//                     trust, source, created_at, removed (a tombstone keeps
//                     the label bound to its body).
//
// Every write runs in one transaction under a transaction-scoped advisory
// lock on (kind, id), so concurrent writers from any replica take turns:
// sequence numbers never collide and a compare-and-set reads the active
// version it then writes against. `created_at` is stamped with the injected
// `WallClock` and only ever shown, never compared across processes; order
// comes from the per-id sequence. `id` columns use `COLLATE "C"`, so the
// primary key serves the id-ordered, cursor-paged `list`; facets are
// filtered in SQL (GIN-indexed) before LIMIT.
// ---------------------------------------------------------------------------

import { sql, type SQL } from "drizzle-orm";
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
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { execRaw } from "../exec-raw.ts";

export interface PostgresVersionedRegistryConfig<TBody, TFilter, TInput> {
  readonly db: DrizzleDb;
  readonly kind: RegistryKind<TBody, TFilter, TInput>;
  /** Time source for `created_at`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

interface HeadRow {
  active_version: string | null;
  history: string[];
  enabled: boolean;
  next_seq: number;
}

interface VersionRow {
  id: string;
  version: string;
  body: unknown;
  trust: string;
  source: string | null;
  created_at: string | number;
  active_version: string | null;
  enabled: boolean;
}

const SELECT_JOINED = sql`
  SELECT v.id, v.version, v.body, v.trust, v.source, v.created_at, h.active_version, h.enabled
  FROM registry_version v
  JOIN registry_head h ON h.kind = v.kind AND h.id = v.id`;

export class PostgresVersionedRegistry<TBody, TFilter, TInput = TBody> implements VersionedRegistry<
  TBody,
  TFilter,
  TInput
> {
  readonly kind: RegistryKind<TBody, TFilter, TInput>;
  private readonly db: DrizzleDb;
  private readonly clock: WallClock;

  constructor(config: PostgresVersionedRegistryConfig<TBody, TFilter, TInput>) {
    this.db = config.db;
    this.kind = config.kind;
    this.clock = config.clock ?? SystemWallClock;
  }

  async register(input: RegisterVersionInput<TInput>): Promise<Versioned<TBody>> {
    const prepared = prepareRegister({ kind: this.kind, input });
    const { options } = prepared;
    const kind = this.kind.name;
    const id = options.id;
    const createdAt = this.clock.currentTimeMs();
    return this.locked({
      id,
      fn: async (db) => {
        const head = await this.head({ db, id });
        this.assertActive({ id, head, expected: options.expectedActive });

        let existing: { version: string; body_hash: string } | undefined;
        if (options.version !== undefined) {
          [existing] = (await execRaw(
            db,
            sql`SELECT version, body_hash FROM registry_version
                WHERE kind = ${kind} AND id = ${id} AND version = ${options.version}`,
          )) as Array<{ version: string; body_hash: string }>;
          if (existing && existing.body_hash !== prepared.hash) {
            throw new RegistryConflictError({
              code: "version_conflict",
              kind,
              id,
              version: options.version,
            });
          }
        } else if (head) {
          [existing] = (await execRaw(
            db,
            sql`SELECT version, body_hash FROM registry_version
                WHERE kind = ${kind} AND id = ${id} AND body_hash = ${prepared.hash}
                ORDER BY removed, seq DESC LIMIT 1`,
          )) as Array<{ version: string; body_hash: string }>;
        }

        let version: string;
        let nextSeq = head?.next_seq ?? 1;
        if (existing) {
          version = existing.version;
          // A tombstone with the same body is restored.
          await execRaw(
            db,
            sql`UPDATE registry_version SET removed = false
                WHERE kind = ${kind} AND id = ${id} AND version = ${version}`,
          );
        } else {
          const seq = nextSeq;
          const taken = new Set(
            (
              (await execRaw(
                db,
                sql`SELECT version FROM registry_version WHERE kind = ${kind} AND id = ${id}`,
              )) as Array<{ version: string }>
            ).map((r) => r.version),
          );
          version = options.version ?? autoVersionLabel({ seq, taken: (v) => taken.has(v) });
          await execRaw(
            db,
            sql`INSERT INTO registry_version
                  (kind, id, version, seq, body, body_hash, facets, trust, source, created_at, removed)
                VALUES (${kind}, ${id}, ${version}, ${seq},
                        ${JSON.stringify(prepared.body)}::jsonb, ${prepared.hash},
                        ${JSON.stringify(prepared.facets)}::jsonb, ${options.trust ?? "trusted"},
                        ${options.source ?? null}, ${createdAt}, false)`,
          );
          nextSeq = seq + 1;
        }

        let active = head?.active_version ?? null;
        let history = head?.history ?? [];
        let enabled = head?.enabled ?? true;
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
        await execRaw(
          db,
          sql`INSERT INTO registry_head (kind, id, active_version, history, enabled, next_seq)
              VALUES (${kind}, ${id}, ${active}, ${JSON.stringify(history)}::jsonb, ${enabled},
                      ${nextSeq})
              ON CONFLICT (kind, id) DO UPDATE SET
                active_version = excluded.active_version, history = excluded.history,
                enabled = excluded.enabled, next_seq = excluded.next_seq`,
        );
        return (await this.read({ db, id, version }))!;
      },
    });
  }

  async get(params: { id: string; version?: string }): Promise<Versioned<TBody> | null> {
    if (params.version !== undefined) {
      return this.read({ db: this.db, id: params.id, version: params.version });
    }
    const [row] = (await execRaw(
      this.db,
      sql`${SELECT_JOINED}
          WHERE v.kind = ${this.kind.name} AND v.id = ${params.id}
            AND v.version = h.active_version`,
    )) as unknown as VersionRow[];
    return row ? this.decode(row) : null;
  }

  async list(params: ListVersionedParams<TFilter> = {}): Promise<VersionedPage<Versioned<TBody>>> {
    assertListLimit(params.limit);
    const query = facetQuery({ kind: this.kind, filter: params.filter });
    const where: SQL[] = [sql`v.kind = ${this.kind.name}`, sql`v.version = h.active_version`];
    if (params.cursor !== undefined) where.push(sql`v.id > ${params.cursor}`);
    if (params.enabled !== undefined) where.push(sql`h.enabled = ${params.enabled}`);
    if (params.source !== undefined) where.push(sql`v.source = ${params.source}`);
    if (query.include.length > 0) {
      where.push(sql`v.facets @> ${JSON.stringify(query.include)}::jsonb`);
    }
    for (const facet of query.exclude) {
      where.push(sql`NOT (v.facets @> ${JSON.stringify([facet])}::jsonb)`);
    }
    const limit = params.limit !== undefined ? sql`LIMIT ${params.limit + 1}` : sql``;
    const rows = (await execRaw(
      this.db,
      sql`${SELECT_JOINED} WHERE ${sql.join(where, sql` AND `)} ORDER BY v.id ${limit}`,
    )) as unknown as VersionRow[];
    const hasMore = params.limit !== undefined && rows.length > params.limit;
    const items = (hasMore ? rows.slice(0, params.limit) : rows).map((r) => this.decode(r));
    return { items, nextCursor: hasMore ? items[items.length - 1]!.id : null };
  }

  async versions(params: { id: string }): Promise<Versioned<TBody>[]> {
    const rows = (await execRaw(
      this.db,
      sql`${SELECT_JOINED} WHERE v.kind = ${this.kind.name} AND v.id = ${params.id}
          AND NOT v.removed ORDER BY v.seq`,
    )) as unknown as VersionRow[];
    return rows.map((r) => this.decode(r));
  }

  async promote(params: {
    id: string;
    version: string;
    expectedActive?: string | null;
  }): Promise<Versioned<TBody>> {
    return this.locked({
      id: params.id,
      fn: async (db) => {
        const head = await this.head({ db, id: params.id });
        this.assertActive({ id: params.id, head, expected: params.expectedActive });
        if (!head || !(await this.read({ db, id: params.id, version: params.version }))) {
          throw new RegistryNotFoundError({
            kind: this.kind.name,
            id: params.id,
            version: params.version,
          });
        }
        await this.setActive({
          db,
          id: params.id,
          active: params.version,
          history: pushHistory({
            history: head.history,
            previous: head.active_version,
            next: params.version,
          }),
        });
        return (await this.read({ db, id: params.id, version: params.version }))!;
      },
    });
  }

  async setEnabled(params: {
    id: string;
    enabled: boolean;
    expectedActive?: string | null;
  }): Promise<Versioned<TBody>> {
    return this.locked({
      id: params.id,
      fn: async (db) => {
        const head = await this.head({ db, id: params.id });
        this.assertActive({ id: params.id, head, expected: params.expectedActive });
        if (!head || head.active_version === null) {
          throw new RegistryNotFoundError({ kind: this.kind.name, id: params.id });
        }
        await execRaw(
          db,
          sql`UPDATE registry_head SET enabled = ${params.enabled}
              WHERE kind = ${this.kind.name} AND id = ${params.id}`,
        );
        return (await this.read({ db, id: params.id, version: head.active_version }))!;
      },
    });
  }

  async setTrust(params: {
    id: string;
    version: string;
    trust: VersionTrust;
  }): Promise<Versioned<TBody>> {
    const updated = await execRaw(
      this.db,
      sql`UPDATE registry_version SET trust = ${params.trust}
          WHERE kind = ${this.kind.name} AND id = ${params.id} AND version = ${params.version}
            AND NOT removed
          RETURNING version`,
    );
    if (updated.length === 0) {
      throw new RegistryNotFoundError({
        kind: this.kind.name,
        id: params.id,
        version: params.version,
      });
    }
    return (await this.read({ db: this.db, id: params.id, version: params.version }))!;
  }

  async unregister(params: {
    id: string;
    version?: string;
    expectedActive?: string | null;
    promote?: string;
  }): Promise<boolean> {
    const kind = this.kind.name;
    const id = params.id;
    return this.locked({
      id,
      fn: async (db) => {
        const head = await this.head({ db, id });
        this.assertActive({ id, head, expected: params.expectedActive });
        if (!head || head.active_version === null) return false;
        if (params.version === undefined) {
          await execRaw(
            db,
            sql`UPDATE registry_version SET removed = true WHERE kind = ${kind} AND id = ${id}`,
          );
          await this.setActive({ db, id, active: null, history: [] });
          return true;
        }
        if (!(await this.read({ db, id, version: params.version }))) return false;
        const removingActive = head.active_version === params.version;
        if (
          removingActive &&
          params.promote !== undefined &&
          (params.promote === params.version ||
            !(await this.read({ db, id, version: params.promote })))
        ) {
          throw new RegistryNotFoundError({ kind, id, version: params.promote });
        }
        await execRaw(
          db,
          sql`UPDATE registry_version SET removed = true
              WHERE kind = ${kind} AND id = ${id} AND version = ${params.version}`,
        );
        const live = (
          (await execRaw(
            db,
            sql`SELECT version FROM registry_version
                WHERE kind = ${kind} AND id = ${id} AND NOT removed ORDER BY seq`,
          )) as Array<{ version: string }>
        ).map((r) => r.version);
        const active = removingActive
          ? nextActiveVersion({
              ...(params.promote !== undefined && { promote: params.promote }),
              history: head.history,
              live,
            })
          : head.active_version;
        const history = head.history.filter((v) => live.includes(v) && v !== active);
        await this.setActive({ db, id, active, history });
        return true;
      },
    });
  }

  /** Run `fn` in a transaction holding the advisory lock of (kind, id). */
  private locked<T>(params: { id: string; fn: (db: DrizzleDb) => Promise<T> }): Promise<T> {
    return this.db.transaction(async (tx) => {
      const db = tx as unknown as DrizzleDb;
      await execRaw(
        db,
        sql`SELECT pg_advisory_xact_lock(hashtextextended(${JSON.stringify([this.kind.name, params.id])}, 0))`,
      );
      return params.fn(db);
    });
  }

  private async head(params: { db: DrizzleDb; id: string }): Promise<HeadRow | null> {
    const [row] = (await execRaw(
      params.db,
      sql`SELECT active_version, history, enabled, next_seq FROM registry_head
          WHERE kind = ${this.kind.name} AND id = ${params.id}`,
    )) as unknown as HeadRow[];
    return row ?? null;
  }

  private async setActive(params: {
    db: DrizzleDb;
    id: string;
    active: string | null;
    history: string[];
  }): Promise<void> {
    await execRaw(
      params.db,
      sql`UPDATE registry_head
          SET active_version = ${params.active}, history = ${JSON.stringify(params.history)}::jsonb
          WHERE kind = ${this.kind.name} AND id = ${params.id}`,
    );
  }

  /** A live version. */
  private async read(params: {
    db: DrizzleDb;
    id: string;
    version: string;
  }): Promise<Versioned<TBody> | null> {
    const [row] = (await execRaw(
      params.db,
      sql`${SELECT_JOINED}
          WHERE v.kind = ${this.kind.name} AND v.id = ${params.id} AND v.version = ${params.version}
            AND NOT v.removed`,
    )) as unknown as VersionRow[];
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
    const body = row.body as TBody;
    return {
      ...body,
      id: row.id,
      version: row.version,
      createdAt: Number(row.created_at),
      active: row.active_version === row.version,
      enabled: row.enabled,
      trust: row.trust as VersionTrust,
      ...(row.source !== null && { source: row.source }),
    };
  }
}
