// ---------------------------------------------------------------------------
// `SqliteMemoryStore` — `MemoryStore` (three-scope cascade × four tiers)
// backed by SQLite. Driver-agnostic via the `SqliteDatabase` interface;
// works directly with `bun:sqlite` and `better-sqlite3` (via the trivial
// shim documented in `sqlite-database.ts`).
//
// Schema (auto-created on first use). Booleans encoded as INTEGER 0/1.
//
//   promin_memory_namespace (namespace_id PK)
//   promin_memory_resource  ((namespace_id, resource_id) PK)
//   promin_memory_thread    ((namespace_id, thread_id) PK)
//   promin_memory_fact      (id PK, scope-tagged; one table for all scopes)
//   promin_memory_episode   (id PK, scope-tagged; one table for all scopes)
//   promin_memory_message   ((namespace_id, thread_id, seq) PK; payload as JSON)
//
// Single-table-per-tier with a `scope` column keeps queries simple while
// still supporting per-scope indexes. Index naming: `<table>_<purpose>`.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import { resolveContext } from "@promin/agent";
import type {
  EpisodeInput,
  EpisodeListParams,
  EpisodicRecord,
  Fact,
  ListThreadsParams,
  MemoryStore,
  Message,
  MessageRange,
  NamespacePatch,
  NamespaceRow,
  ResolvedContext,
  ResourcePatch,
  ResourceRow,
  ScopedKey,
  StoredMessage,
  ThreadInit,
  ThreadKey,
  ThreadRow,
  ThreadSummary,
  TokenBudget,
} from "@promin/agent";
import type { SqliteDatabase } from "./sqlite-database.ts";

const SCOPE_NS = "namespace";
const SCOPE_RES = "resource";
const SCOPE_THR = "thread";

export interface SqliteMemoryStoreConfig {
  readonly db: SqliteDatabase;
  /**
   * Override the table prefix (default: `promin_memory`). All tables are
   * named `<prefix>_<tier>`; useful for sharing a database with other
   * promin storage primitives.
   */
  readonly tablePrefix?: string;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class SqliteMemoryStore implements MemoryStore {
  private readonly db: SqliteDatabase;
  private readonly prefix: string;
  private readonly clock: () => number;

  private constructor(config: SqliteMemoryStoreConfig) {
    this.db = config.db;
    this.prefix = config.tablePrefix ?? "promin_memory";
    this.clock = config.now ?? (() => Date.now());
    this._setup();
  }

  static make(config: SqliteMemoryStoreConfig): SqliteMemoryStore {
    return new SqliteMemoryStore(config);
  }

  // --- Schema ----------------------------------------------------------

  private _setup(): void {
    const p = this.prefix;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${p}_namespace (
        namespace_id        TEXT NOT NULL PRIMARY KEY,
        static_rules        TEXT,
        working_memory      TEXT,
        inherit_from_parent INTEGER NOT NULL DEFAULT 1,
        metadata            TEXT,
        created_at          INTEGER NOT NULL,
        updated_at          INTEGER NOT NULL
      )
    `);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${p}_resource (
        namespace_id        TEXT NOT NULL,
        resource_id         TEXT NOT NULL,
        static_rules        TEXT,
        working_memory      TEXT,
        inherit_from_parent INTEGER NOT NULL DEFAULT 1,
        metadata            TEXT,
        created_at          INTEGER NOT NULL,
        updated_at          INTEGER NOT NULL,
        PRIMARY KEY (namespace_id, resource_id)
      )
    `);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${p}_thread (
        namespace_id        TEXT NOT NULL,
        thread_id           TEXT NOT NULL,
        resource_id         TEXT,
        working_memory      TEXT,
        inherit_from_parent INTEGER NOT NULL DEFAULT 1,
        metadata            TEXT,
        created_at          INTEGER NOT NULL,
        updated_at          INTEGER NOT NULL,
        PRIMARY KEY (namespace_id, thread_id)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${p}_thread_resource ON ${p}_thread (namespace_id, resource_id)`,
    );
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${p}_fact (
        id           TEXT NOT NULL PRIMARY KEY,
        scope        TEXT NOT NULL,
        namespace_id TEXT NOT NULL,
        resource_id  TEXT,
        thread_id    TEXT,
        text         TEXT NOT NULL,
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${p}_fact_ns ON ${p}_fact (scope, namespace_id, created_at)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${p}_fact_res ON ${p}_fact (scope, namespace_id, resource_id, created_at)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${p}_fact_thr ON ${p}_fact (scope, namespace_id, thread_id, created_at)`,
    );
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${p}_episode (
        id                  TEXT NOT NULL PRIMARY KEY,
        scope               TEXT NOT NULL,
        namespace_id        TEXT NOT NULL,
        resource_id         TEXT,
        thread_id           TEXT,
        summary             TEXT NOT NULL,
        outcome             TEXT,
        salience            REAL NOT NULL DEFAULT 0.5,
        embedding           TEXT,
        source_thread_id    TEXT,
        source_msg_from_seq INTEGER,
        source_msg_to_seq   INTEGER,
        occurred_at         INTEGER NOT NULL,
        created_at          INTEGER NOT NULL,
        metadata            TEXT
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${p}_episode_ns ON ${p}_episode (scope, namespace_id, salience)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${p}_episode_res ON ${p}_episode (scope, namespace_id, resource_id, salience)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${p}_episode_thr ON ${p}_episode (scope, namespace_id, thread_id, created_at)`,
    );
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${p}_message (
        namespace_id TEXT NOT NULL,
        thread_id    TEXT NOT NULL,
        seq          INTEGER NOT NULL,
        payload      TEXT NOT NULL,
        created_at   INTEGER NOT NULL,
        PRIMARY KEY (namespace_id, thread_id, seq)
      )
    `);
  }

  // --- Namespace -------------------------------------------------------

  async getNamespace(namespaceId: string): Promise<NamespaceRow | null> {
    const row = this.db
      .query<DbNamespaceRow>(`SELECT * FROM ${this.prefix}_namespace WHERE namespace_id = ?`)
      .get(namespaceId);
    return row ? toNamespaceRow(row) : null;
  }

  async upsertNamespace(namespaceId: string, patch: NamespacePatch): Promise<NamespaceRow> {
    const existing = await this.getNamespace(namespaceId);
    const now = this.clock();
    const next: NamespaceRow = existing
      ? {
          ...existing,
          staticRules: patch.staticRules !== undefined ? patch.staticRules : existing.staticRules,
          workingMemory:
            patch.workingMemory !== undefined ? patch.workingMemory : existing.workingMemory,
          inheritFromParent: patch.inheritFromParent ?? existing.inheritFromParent,
          metadata: patch.metadata ?? existing.metadata,
          updatedAt: now,
        }
      : {
          namespaceId,
          staticRules: patch.staticRules ?? null,
          workingMemory: patch.workingMemory ?? null,
          inheritFromParent: patch.inheritFromParent ?? true,
          metadata: patch.metadata ?? {},
          createdAt: now,
          updatedAt: now,
        };
    this.db
      .query(
        `INSERT INTO ${this.prefix}_namespace
           (namespace_id, static_rules, working_memory, inherit_from_parent, metadata, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(namespace_id) DO UPDATE SET
           static_rules = excluded.static_rules,
           working_memory = excluded.working_memory,
           inherit_from_parent = excluded.inherit_from_parent,
           metadata = excluded.metadata,
           updated_at = excluded.updated_at`,
      )
      .run(
        namespaceId,
        next.staticRules,
        next.workingMemory,
        next.inheritFromParent ? 1 : 0,
        JSON.stringify(next.metadata),
        next.createdAt,
        next.updatedAt,
      );
    return next;
  }

  async appendNamespaceFact(namespaceId: string, text: string): Promise<Fact> {
    if (!(await this.getNamespace(namespaceId))) {
      await this.upsertNamespace(namespaceId, {});
    }
    const f = this.makeFact(text);
    this.db
      .query(
        `INSERT INTO ${this.prefix}_fact (id, scope, namespace_id, resource_id, thread_id, text, created_at, updated_at)
         VALUES (?, ?, ?, NULL, NULL, ?, ?, ?)`,
      )
      .run(f.id, SCOPE_NS, namespaceId, f.text, f.createdAt, f.updatedAt);
    return f;
  }

  async listNamespaceFacts(namespaceId: string): Promise<Fact[]> {
    const rows = this.db
      .query<DbFactRow>(
        `SELECT id, text, created_at, updated_at FROM ${this.prefix}_fact
         WHERE scope = ? AND namespace_id = ?
         ORDER BY created_at ASC, rowid ASC`,
      )
      .all(SCOPE_NS, namespaceId);
    return rows.map(toFact);
  }

  async deleteNamespaceFact(namespaceId: string, factId: string): Promise<void> {
    this.db
      .query(
        `DELETE FROM ${this.prefix}_fact
         WHERE scope = ? AND namespace_id = ? AND id = ?`,
      )
      .run(SCOPE_NS, namespaceId, factId);
  }

  async appendNamespaceEpisode(namespaceId: string, input: EpisodeInput): Promise<EpisodicRecord> {
    if (!(await this.getNamespace(namespaceId))) {
      await this.upsertNamespace(namespaceId, {});
    }
    const ep = this.makeEpisode(input);
    this.insertEpisode(SCOPE_NS, namespaceId, null, null, ep);
    return ep;
  }

  async listNamespaceEpisodes(
    namespaceId: string,
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    const rows = this.db
      .query<DbEpisodeRow>(
        `SELECT * FROM ${this.prefix}_episode
         WHERE scope = ? AND namespace_id = ?`,
      )
      .all(SCOPE_NS, namespaceId);
    return this.queryEpisodesPostFilter(rows, params);
  }

  async deleteNamespaceEpisode(namespaceId: string, episodeId: string): Promise<void> {
    this.db
      .query(
        `DELETE FROM ${this.prefix}_episode
         WHERE scope = ? AND namespace_id = ? AND id = ?`,
      )
      .run(SCOPE_NS, namespaceId, episodeId);
  }

  // --- Resource --------------------------------------------------------

  async getResource(key: ScopedKey): Promise<ResourceRow | null> {
    const row = this.db
      .query<DbResourceRow>(
        `SELECT * FROM ${this.prefix}_resource WHERE namespace_id = ? AND resource_id = ?`,
      )
      .get(key.namespaceId, key.resourceId);
    return row ? toResourceRow(row) : null;
  }

  async upsertResource(key: ScopedKey, patch: ResourcePatch): Promise<ResourceRow> {
    const existing = await this.getResource(key);
    const now = this.clock();
    const next: ResourceRow = existing
      ? {
          ...existing,
          staticRules: patch.staticRules !== undefined ? patch.staticRules : existing.staticRules,
          workingMemory:
            patch.workingMemory !== undefined ? patch.workingMemory : existing.workingMemory,
          inheritFromParent: patch.inheritFromParent ?? existing.inheritFromParent,
          metadata: patch.metadata ?? existing.metadata,
          updatedAt: now,
        }
      : {
          namespaceId: key.namespaceId,
          resourceId: key.resourceId,
          staticRules: patch.staticRules ?? null,
          workingMemory: patch.workingMemory ?? null,
          inheritFromParent: patch.inheritFromParent ?? true,
          metadata: patch.metadata ?? {},
          createdAt: now,
          updatedAt: now,
        };
    this.db
      .query(
        `INSERT INTO ${this.prefix}_resource
           (namespace_id, resource_id, static_rules, working_memory, inherit_from_parent, metadata, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(namespace_id, resource_id) DO UPDATE SET
           static_rules = excluded.static_rules,
           working_memory = excluded.working_memory,
           inherit_from_parent = excluded.inherit_from_parent,
           metadata = excluded.metadata,
           updated_at = excluded.updated_at`,
      )
      .run(
        key.namespaceId,
        key.resourceId,
        next.staticRules,
        next.workingMemory,
        next.inheritFromParent ? 1 : 0,
        JSON.stringify(next.metadata),
        next.createdAt,
        next.updatedAt,
      );
    return next;
  }

  async appendResourceFact(key: ScopedKey, text: string): Promise<Fact> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const f = this.makeFact(text);
    this.db
      .query(
        `INSERT INTO ${this.prefix}_fact (id, scope, namespace_id, resource_id, thread_id, text, created_at, updated_at)
         VALUES (?, ?, ?, ?, NULL, ?, ?, ?)`,
      )
      .run(f.id, SCOPE_RES, key.namespaceId, key.resourceId, f.text, f.createdAt, f.updatedAt);
    return f;
  }

  async listResourceFacts(key: ScopedKey): Promise<Fact[]> {
    const rows = this.db
      .query<DbFactRow>(
        `SELECT id, text, created_at, updated_at FROM ${this.prefix}_fact
         WHERE scope = ? AND namespace_id = ? AND resource_id = ?
         ORDER BY created_at ASC, rowid ASC`,
      )
      .all(SCOPE_RES, key.namespaceId, key.resourceId);
    return rows.map(toFact);
  }

  async deleteResourceFact(key: ScopedKey, factId: string): Promise<void> {
    this.db
      .query(
        `DELETE FROM ${this.prefix}_fact
         WHERE scope = ? AND namespace_id = ? AND resource_id = ? AND id = ?`,
      )
      .run(SCOPE_RES, key.namespaceId, key.resourceId, factId);
  }

  async appendResourceEpisode(key: ScopedKey, input: EpisodeInput): Promise<EpisodicRecord> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const ep = this.makeEpisode(input);
    this.insertEpisode(SCOPE_RES, key.namespaceId, key.resourceId, null, ep);
    return ep;
  }

  async listResourceEpisodes(
    key: ScopedKey,
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    const rows = this.db
      .query<DbEpisodeRow>(
        `SELECT * FROM ${this.prefix}_episode
         WHERE scope = ? AND namespace_id = ? AND resource_id = ?`,
      )
      .all(SCOPE_RES, key.namespaceId, key.resourceId);
    return this.queryEpisodesPostFilter(rows, params);
  }

  async deleteResourceEpisode(key: ScopedKey, episodeId: string): Promise<void> {
    this.db
      .query(
        `DELETE FROM ${this.prefix}_episode
         WHERE scope = ? AND namespace_id = ? AND resource_id = ? AND id = ?`,
      )
      .run(SCOPE_RES, key.namespaceId, key.resourceId, episodeId);
  }

  // --- Thread ----------------------------------------------------------

  async createThread(key: ThreadKey, init: ThreadInit = {}): Promise<ThreadRow> {
    const existing = await this.getThread(key);
    if (existing) {
      throw new Error(`Thread already exists: ${key.namespaceId}/${key.threadId}`);
    }
    const now = this.clock();
    const row: ThreadRow = {
      namespaceId: key.namespaceId,
      resourceId: key.resourceId ?? init.resourceId ?? null,
      threadId: key.threadId,
      workingMemory: init.workingMemory ?? null,
      inheritFromParent: init.inheritFromParent ?? true,
      metadata: init.metadata ?? {},
      createdAt: now,
      updatedAt: now,
    };
    this.db
      .query(
        `INSERT INTO ${this.prefix}_thread
           (namespace_id, thread_id, resource_id, working_memory, inherit_from_parent, metadata, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        row.namespaceId,
        row.threadId,
        row.resourceId,
        row.workingMemory,
        row.inheritFromParent ? 1 : 0,
        JSON.stringify(row.metadata),
        row.createdAt,
        row.updatedAt,
      );
    return row;
  }

  async getThread(key: ThreadKey): Promise<ThreadRow | null> {
    const row = this.db
      .query<DbThreadRow>(
        `SELECT * FROM ${this.prefix}_thread WHERE namespace_id = ? AND thread_id = ?`,
      )
      .get(key.namespaceId, key.threadId);
    return row ? toThreadRow(row) : null;
  }

  async listThreads(params: ListThreadsParams): Promise<ThreadSummary[]> {
    const where: string[] = ["namespace_id = ?"];
    const args: unknown[] = [params.namespaceId];
    if (params.resourceId !== undefined) {
      where.push("resource_id = ?");
      args.push(params.resourceId);
    }
    if (params.q && params.q.trim().length > 0) {
      // Case-insensitive substring on thread_id. SQLite's LIKE is
      // case-insensitive on ASCII by default; lower() the pattern + the
      // column for safety against non-ASCII threadIds.
      where.push("LOWER(thread_id) LIKE ?");
      args.push(`%${params.q.trim().toLowerCase()}%`);
    }
    const rows = this.db
      .query<DbThreadRow>(`SELECT * FROM ${this.prefix}_thread WHERE ${where.join(" AND ")}`)
      .all(...args);

    // Per-thread message-count + lastActiveAt computed via subqueries.
    const summaries: ThreadSummary[] = rows.map((r) => {
      const lastMsg = this.db
        .query<{ created_at: number }>(
          `SELECT created_at FROM ${this.prefix}_message
           WHERE namespace_id = ? AND thread_id = ?
           ORDER BY seq DESC LIMIT 1`,
        )
        .get(r.namespace_id, r.thread_id);
      const count = this.db
        .query<{ c: number }>(
          `SELECT COUNT(*) AS c FROM ${this.prefix}_message
           WHERE namespace_id = ? AND thread_id = ?`,
        )
        .get(r.namespace_id, r.thread_id);
      return {
        namespaceId: r.namespace_id,
        resourceId: r.resource_id,
        threadId: r.thread_id,
        metadata: r.metadata ? (JSON.parse(r.metadata) as Record<string, unknown>) : {},
        messageCount: count?.c ?? 0,
        lastActiveAt: lastMsg?.created_at ?? r.updated_at,
        createdAt: r.created_at,
      };
    });

    // Apply metadata filter in JS (subset match).
    let filtered = summaries;
    if (params.metadataFilter) {
      filtered = summaries.filter((s) => {
        for (const [k, v] of Object.entries(params.metadataFilter!)) {
          if (s.metadata[k] !== v) return false;
        }
        return true;
      });
    }

    const order = params.order ?? "lastActiveDesc";
    filtered.sort((a, b) => {
      switch (order) {
        case "lastActiveDesc":
          return b.lastActiveAt - a.lastActiveAt;
        case "createdAsc":
          return a.createdAt - b.createdAt;
        case "createdDesc":
          return b.createdAt - a.createdAt;
      }
    });

    const offset = params.cursor ? parseInt(params.cursor, 10) || 0 : 0;
    const limit = params.limit ?? filtered.length;
    return filtered.slice(offset, offset + limit);
  }

  async setThreadWorking(key: ThreadKey, content: string | null): Promise<void> {
    await this.requireThread(key);
    this.db
      .query(
        `UPDATE ${this.prefix}_thread SET working_memory = ?, updated_at = ?
         WHERE namespace_id = ? AND thread_id = ?`,
      )
      .run(content, this.clock(), key.namespaceId, key.threadId);
  }

  async setThreadMetadata(
    key: ThreadKey,
    metadata: Readonly<Record<string, unknown>>,
  ): Promise<void> {
    await this.requireThread(key);
    this.db
      .query(
        `UPDATE ${this.prefix}_thread SET metadata = ?, updated_at = ?
         WHERE namespace_id = ? AND thread_id = ?`,
      )
      .run(JSON.stringify(metadata), this.clock(), key.namespaceId, key.threadId);
  }

  async setThreadInheritFromParent(key: ThreadKey, inherit: boolean): Promise<void> {
    await this.requireThread(key);
    this.db
      .query(
        `UPDATE ${this.prefix}_thread SET inherit_from_parent = ?, updated_at = ?
         WHERE namespace_id = ? AND thread_id = ?`,
      )
      .run(inherit ? 1 : 0, this.clock(), key.namespaceId, key.threadId);
  }

  async appendThreadFact(key: ThreadKey, text: string): Promise<Fact> {
    if (!(await this.getThread(key))) {
      await this.createThread(key);
    }
    const f = this.makeFact(text);
    this.db
      .query(
        `INSERT INTO ${this.prefix}_fact (id, scope, namespace_id, resource_id, thread_id, text, created_at, updated_at)
         VALUES (?, ?, ?, NULL, ?, ?, ?, ?)`,
      )
      .run(f.id, SCOPE_THR, key.namespaceId, key.threadId, f.text, f.createdAt, f.updatedAt);
    return f;
  }

  async listThreadFacts(key: ThreadKey): Promise<Fact[]> {
    const rows = this.db
      .query<DbFactRow>(
        `SELECT id, text, created_at, updated_at FROM ${this.prefix}_fact
         WHERE scope = ? AND namespace_id = ? AND thread_id = ?
         ORDER BY created_at ASC, rowid ASC`,
      )
      .all(SCOPE_THR, key.namespaceId, key.threadId);
    return rows.map(toFact);
  }

  async deleteThreadFact(key: ThreadKey, factId: string): Promise<void> {
    this.db
      .query(
        `DELETE FROM ${this.prefix}_fact
         WHERE scope = ? AND namespace_id = ? AND thread_id = ? AND id = ?`,
      )
      .run(SCOPE_THR, key.namespaceId, key.threadId, factId);
  }

  async appendMessages(key: ThreadKey, msgs: ReadonlyArray<Message>): Promise<StoredMessage[]> {
    if (!(await this.getThread(key))) {
      await this.createThread(key);
    }
    const now = this.clock();
    const lastSeq =
      this.db
        .query<{ s: number | null }>(
          `SELECT MAX(seq) AS s FROM ${this.prefix}_message
           WHERE namespace_id = ? AND thread_id = ?`,
        )
        .get(key.namespaceId, key.threadId)?.s ?? 0;

    const stored: StoredMessage[] = [];
    const insert = this.db.query(
      `INSERT INTO ${this.prefix}_message (namespace_id, thread_id, seq, payload, created_at)
       VALUES (?, ?, ?, ?, ?)`,
    );

    const tx = this.db.transaction(() => {
      let seq = lastSeq;
      for (const m of msgs) {
        seq += 1;
        const sm: StoredMessage = { ...m, seq, createdAt: now } as StoredMessage;
        insert.run(key.namespaceId, key.threadId, seq, JSON.stringify(m), now);
        stored.push(sm);
      }
    });
    tx();

    // Touch updated_at on the thread row so listThreads ranks by activity.
    this.db
      .query(
        `UPDATE ${this.prefix}_thread SET updated_at = ?
         WHERE namespace_id = ? AND thread_id = ?`,
      )
      .run(now, key.namespaceId, key.threadId);

    return stored;
  }

  async getMessages(key: ThreadKey, range?: MessageRange): Promise<StoredMessage[]> {
    const where: string[] = ["namespace_id = ?", "thread_id = ?"];
    const args: unknown[] = [key.namespaceId, key.threadId];
    if (range?.fromSeq !== undefined) {
      where.push("seq >= ?");
      args.push(range.fromSeq);
    }
    if (range?.toSeq !== undefined) {
      where.push("seq <= ?");
      args.push(range.toSeq);
    }
    const order = range?.order === "desc" ? "DESC" : "ASC";
    const limit = range?.limit !== undefined ? `LIMIT ${range.limit}` : "";
    const rows = this.db
      .query<DbMessageRow>(
        `SELECT seq, payload, created_at FROM ${this.prefix}_message
         WHERE ${where.join(" AND ")}
         ORDER BY seq ${order} ${limit}`,
      )
      .all(...args);
    return rows.map((r) => {
      const m = JSON.parse(r.payload) as Message;
      return { ...m, seq: r.seq, createdAt: r.created_at } as StoredMessage;
    });
  }

  async appendThreadEpisode(key: ThreadKey, input: EpisodeInput): Promise<EpisodicRecord> {
    if (!(await this.getThread(key))) {
      await this.createThread(key);
    }
    const ep = this.makeEpisode(input);
    this.insertEpisode(SCOPE_THR, key.namespaceId, null, key.threadId, ep);
    return ep;
  }

  async listThreadEpisodes(key: ThreadKey, params?: EpisodeListParams): Promise<EpisodicRecord[]> {
    const rows = this.db
      .query<DbEpisodeRow>(
        `SELECT * FROM ${this.prefix}_episode
         WHERE scope = ? AND namespace_id = ? AND thread_id = ?`,
      )
      .all(SCOPE_THR, key.namespaceId, key.threadId);
    return this.queryEpisodesPostFilter(rows, params);
  }

  async deleteThreadEpisode(key: ThreadKey, episodeId: string): Promise<void> {
    this.db
      .query(
        `DELETE FROM ${this.prefix}_episode
         WHERE scope = ? AND namespace_id = ? AND thread_id = ? AND id = ?`,
      )
      .run(SCOPE_THR, key.namespaceId, key.threadId, episodeId);
  }

  async deleteThread(key: ThreadKey): Promise<void> {
    const tx = this.db.transaction(() => {
      this.db
        .query(`DELETE FROM ${this.prefix}_message WHERE namespace_id = ? AND thread_id = ?`)
        .run(key.namespaceId, key.threadId);
      this.db
        .query(
          `DELETE FROM ${this.prefix}_fact WHERE scope = ? AND namespace_id = ? AND thread_id = ?`,
        )
        .run(SCOPE_THR, key.namespaceId, key.threadId);
      this.db
        .query(
          `DELETE FROM ${this.prefix}_episode WHERE scope = ? AND namespace_id = ? AND thread_id = ?`,
        )
        .run(SCOPE_THR, key.namespaceId, key.threadId);
      this.db
        .query(`DELETE FROM ${this.prefix}_thread WHERE namespace_id = ? AND thread_id = ?`)
        .run(key.namespaceId, key.threadId);
    });
    tx();
  }

  // --- Cascade ---------------------------------------------------------

  async resolveContext(key: ThreadKey, budget: TokenBudget): Promise<ResolvedContext> {
    const thread = await this.getThread(key);
    if (!thread) {
      throw new Error(`Thread not found: ${key.namespaceId}/${key.threadId}`);
    }
    const namespace = await this.getNamespace(key.namespaceId);
    const namespaceFacts = await this.listNamespaceFacts(key.namespaceId);

    const resourceId = thread.resourceId ?? key.resourceId;
    const resource = resourceId
      ? await this.getResource({ namespaceId: key.namespaceId, resourceId })
      : null;
    const resourceFacts = resourceId
      ? await this.listResourceFacts({ namespaceId: key.namespaceId, resourceId })
      : [];
    const resourceEpisodes =
      resourceId && (budget.maxEpisodeTokens ?? 0) > 0
        ? await this.listResourceEpisodes(
            { namespaceId: key.namespaceId, resourceId },
            { order: "salienceDesc" },
          )
        : [];

    const threadFacts = await this.listThreadFacts(key);
    const messages = await this.getMessages(key);

    return resolveContext({
      namespace,
      namespaceFacts,
      resource,
      resourceFacts,
      resourceEpisodes,
      thread,
      threadFacts,
      messages,
      budget,
    });
  }

  // --- helpers ---------------------------------------------------------

  private async requireThread(key: ThreadKey): Promise<ThreadRow> {
    const row = await this.getThread(key);
    if (!row) {
      throw new Error(`Thread not found: ${key.namespaceId}/${key.threadId}`);
    }
    return row;
  }

  private makeFact(text: string): Fact {
    const now = this.clock();
    return { id: randomUUID(), text, createdAt: now, updatedAt: now };
  }

  private makeEpisode(input: EpisodeInput): EpisodicRecord {
    const now = this.clock();
    const salience = clamp01(input.salience ?? 0.5);
    return {
      id: randomUUID(),
      summary: input.summary,
      outcome: input.outcome ?? null,
      salience,
      embedding: input.embedding ?? null,
      sourceThreadId: input.sourceThreadId ?? null,
      sourceMessageRange: input.sourceMessageRange ?? null,
      occurredAt: input.occurredAt ?? now,
      createdAt: now,
      metadata: input.metadata ?? {},
    };
  }

  private insertEpisode(
    scope: string,
    namespaceId: string,
    resourceId: string | null,
    threadId: string | null,
    ep: EpisodicRecord,
  ): void {
    this.db
      .query(
        `INSERT INTO ${this.prefix}_episode
           (id, scope, namespace_id, resource_id, thread_id,
            summary, outcome, salience, embedding,
            source_thread_id, source_msg_from_seq, source_msg_to_seq,
            occurred_at, created_at, metadata)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        ep.id,
        scope,
        namespaceId,
        resourceId,
        threadId,
        ep.summary,
        ep.outcome,
        ep.salience,
        ep.embedding ? JSON.stringify(ep.embedding) : null,
        ep.sourceThreadId,
        ep.sourceMessageRange?.fromSeq ?? null,
        ep.sourceMessageRange?.toSeq ?? null,
        ep.occurredAt,
        ep.createdAt,
        JSON.stringify(ep.metadata),
      );
  }

  private queryEpisodesPostFilter(
    rows: DbEpisodeRow[],
    params?: EpisodeListParams,
  ): EpisodicRecord[] {
    let out = rows.map(toEpisode);
    if (params?.minSalience !== undefined) {
      const threshold = params.minSalience;
      out = out.filter((e) => e.salience >= threshold);
    }
    const order = params?.order ?? "salienceDesc";
    out.sort((a, b) => {
      switch (order) {
        case "salienceDesc":
          return b.salience - a.salience;
        case "createdDesc":
          return b.createdAt - a.createdAt;
        case "occurredDesc":
          return b.occurredAt - a.occurredAt;
      }
    });
    if (params?.limit !== undefined) out = out.slice(0, params.limit);
    return out;
  }
}

// ---------------------------------------------------------------------------
// DB row types + converters
// ---------------------------------------------------------------------------

interface DbNamespaceRow {
  namespace_id: string;
  static_rules: string | null;
  working_memory: string | null;
  inherit_from_parent: number;
  metadata: string | null;
  created_at: number;
  updated_at: number;
}

interface DbResourceRow {
  namespace_id: string;
  resource_id: string;
  static_rules: string | null;
  working_memory: string | null;
  inherit_from_parent: number;
  metadata: string | null;
  created_at: number;
  updated_at: number;
}

interface DbThreadRow {
  namespace_id: string;
  thread_id: string;
  resource_id: string | null;
  working_memory: string | null;
  inherit_from_parent: number;
  metadata: string | null;
  created_at: number;
  updated_at: number;
}

interface DbFactRow {
  id: string;
  text: string;
  created_at: number;
  updated_at: number;
}

interface DbEpisodeRow {
  id: string;
  scope: string;
  namespace_id: string;
  resource_id: string | null;
  thread_id: string | null;
  summary: string;
  outcome: string | null;
  salience: number;
  embedding: string | null;
  source_thread_id: string | null;
  source_msg_from_seq: number | null;
  source_msg_to_seq: number | null;
  occurred_at: number;
  created_at: number;
  metadata: string | null;
}

interface DbMessageRow {
  seq: number;
  payload: string;
  created_at: number;
}

function toNamespaceRow(r: DbNamespaceRow): NamespaceRow {
  return {
    namespaceId: r.namespace_id,
    staticRules: r.static_rules,
    workingMemory: r.working_memory,
    inheritFromParent: r.inherit_from_parent === 1,
    metadata: r.metadata ? (JSON.parse(r.metadata) as Record<string, unknown>) : {},
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

function toResourceRow(r: DbResourceRow): ResourceRow {
  return {
    namespaceId: r.namespace_id,
    resourceId: r.resource_id,
    staticRules: r.static_rules,
    workingMemory: r.working_memory,
    inheritFromParent: r.inherit_from_parent === 1,
    metadata: r.metadata ? (JSON.parse(r.metadata) as Record<string, unknown>) : {},
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

function toThreadRow(r: DbThreadRow): ThreadRow {
  return {
    namespaceId: r.namespace_id,
    resourceId: r.resource_id,
    threadId: r.thread_id,
    workingMemory: r.working_memory,
    inheritFromParent: r.inherit_from_parent === 1,
    metadata: r.metadata ? (JSON.parse(r.metadata) as Record<string, unknown>) : {},
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

function toFact(r: DbFactRow): Fact {
  return { id: r.id, text: r.text, createdAt: r.created_at, updatedAt: r.updated_at };
}

function toEpisode(r: DbEpisodeRow): EpisodicRecord {
  const range =
    r.source_msg_from_seq !== null && r.source_msg_to_seq !== null
      ? { fromSeq: r.source_msg_from_seq, toSeq: r.source_msg_to_seq }
      : null;
  return {
    id: r.id,
    summary: r.summary,
    outcome: r.outcome,
    salience: r.salience,
    embedding: r.embedding ? (JSON.parse(r.embedding) as number[]) : null,
    sourceThreadId: r.source_thread_id,
    sourceMessageRange: range,
    occurredAt: r.occurred_at,
    createdAt: r.created_at,
    metadata: r.metadata ? (JSON.parse(r.metadata) as Record<string, unknown>) : {},
  };
}

function clamp01(n: number): number {
  if (Number.isNaN(n)) return 0.5;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}
