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
//   promin_memory_reservation (id PK; consolidation rate-limit window)
//
// Reads that grow with the data are bounded in SQL: `listThreads` pages
// over the thread rows (message count and last activity are kept on the
// row by `appendMessages`), episode lists filter / order / limit in the
// query, and `resolveContext` reads only the message tail that fits.
//
// Single-table-per-tier with a `scope` column keeps queries simple while
// still supporting per-scope indexes. Index naming: `<table>_<purpose>`.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import {
  appendStaticRule,
  DEFAULT_THREAD_PAGE_SIZE,
  loadResolvedContext,
  MemoryLimitError,
  newEpisode,
  newFact,
  MIN_SALIENCE_RETENTION,
  normalizeRuleText,
  parseThreadCursor,
  relationKey,
  resolveFactConfidence,
  ThreadExistsError,
} from "@promin/agent";
import type {
  AppendMessagesOptions,
  ConsolidationReservation,
  ConsolidationReservationRequest,
  EpisodeInput,
  EpisodeListParams,
  Relation,
  RelationInput,
  RuleProposal,
  RuleProposalListParams,
  RuleProposalStatus,
  EpisodicRecord,
  Fact,
  FactInput,
  FactListParams,
  FactSource,
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
import type { SqliteDatabase } from "../sqlite-database.ts";

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
        title               TEXT,
        working_memory      TEXT,
        inherit_from_parent INTEGER NOT NULL DEFAULT 1,
        metadata            TEXT,
        archived_at         INTEGER,
        message_count       INTEGER NOT NULL DEFAULT 0,
        last_active_at      INTEGER NOT NULL,
        created_at          INTEGER NOT NULL,
        updated_at          INTEGER NOT NULL,
        PRIMARY KEY (namespace_id, thread_id)
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${p}_thread_resource ON ${p}_thread (namespace_id, resource_id)`,
    );
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${p}_thread_active ON ${p}_thread (namespace_id, last_active_at)`,
    );
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${p}_reservation (
        id           TEXT NOT NULL PRIMARY KEY,
        namespace_id TEXT NOT NULL,
        resource_id  TEXT NOT NULL,
        kind         TEXT NOT NULL,
        reserved_at  INTEGER NOT NULL
      )
    `);
    this.db.run(
      `CREATE INDEX IF NOT EXISTS ${p}_reservation_res ON ${p}_reservation (namespace_id, resource_id, kind, reserved_at)`,
    );
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${p}_fact (
        id           TEXT NOT NULL PRIMARY KEY,
        scope        TEXT NOT NULL,
        namespace_id TEXT NOT NULL,
        resource_id  TEXT,
        thread_id    TEXT,
        text         TEXT NOT NULL,
        source       TEXT NOT NULL,
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL,
        retired_at   INTEGER,
        superseded_by TEXT,
        confidence   REAL NOT NULL DEFAULT 1
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
      CREATE TABLE IF NOT EXISTS ${p}_relation (
        id           TEXT NOT NULL PRIMARY KEY,
        namespace_id TEXT NOT NULL,
        resource_id  TEXT NOT NULL,
        subject      TEXT NOT NULL,
        predicate    TEXT NOT NULL,
        object       TEXT NOT NULL,
        relation_key TEXT NOT NULL,
        source       TEXT NOT NULL,
        confidence   REAL NOT NULL DEFAULT 1,
        created_at   INTEGER NOT NULL
      )
    `);
    this.db.run(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${p}_relation_key ON ${p}_relation (namespace_id, resource_id, relation_key)`,
    );
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${p}_rule_proposal (
        id                  TEXT NOT NULL PRIMARY KEY,
        namespace_id        TEXT NOT NULL,
        resource_id         TEXT NOT NULL,
        text                TEXT NOT NULL,
        text_key            TEXT NOT NULL,
        status              TEXT NOT NULL DEFAULT 'pending',
        evidence_thread_ids TEXT NOT NULL DEFAULT '[]',
        first_seen_at       INTEGER NOT NULL,
        last_seen_at        INTEGER NOT NULL
      )
    `);
    this.db.run(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${p}_rule_proposal_key ON ${p}_rule_proposal (namespace_id, resource_id, text_key)`,
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
        append_key   TEXT,
        append_index INTEGER,
        PRIMARY KEY (namespace_id, thread_id, seq)
      )
    `);
    this.db.run(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${p}_message_append ON ${p}_message
         (namespace_id, thread_id, append_key, append_index) WHERE append_key IS NOT NULL`,
    );
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

  async appendNamespaceFact(namespaceId: string, input: FactInput): Promise<Fact> {
    if (!(await this.getNamespace(namespaceId))) {
      await this.upsertNamespace(namespaceId, {});
    }
    const f = newFact({ input, now: this.clock() });
    this.insertFact({
      fact: f,
      supersedes: input.supersedes,
      scopeCondition: "scope = ? AND namespace_id = ?",
      scopeBindings: [SCOPE_NS, namespaceId],
      insert: (db) =>
        db
          .query(
            `INSERT INTO ${this.prefix}_fact (id, scope, namespace_id, resource_id, thread_id, text, source, created_at, updated_at, confidence)
             VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?)`,
          )
          .run(
            f.id,
            SCOPE_NS,
            namespaceId,
            f.text,
            f.source,
            f.createdAt,
            f.updatedAt,
            f.confidence,
          ),
    });
    return f;
  }

  async listNamespaceFacts(namespaceId: string, params?: FactListParams): Promise<Fact[]> {
    const rows = this.db
      .query<DbFactRow>(
        `SELECT id, text, source, created_at, updated_at, retired_at, superseded_by, confidence FROM ${this.prefix}_fact
         WHERE scope = ? AND namespace_id = ?${retiredFilter(params)}
         ORDER BY retired_at IS NOT NULL, created_at ASC, rowid ASC`,
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
    const ep = newEpisode({ input, now: this.clock() });
    this.insertEpisode(SCOPE_NS, namespaceId, null, null, ep);
    return ep;
  }

  async listNamespaceEpisodes(
    namespaceId: string,
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    return this.queryEpisodes({
      where: "scope = ? AND namespace_id = ?",
      args: [SCOPE_NS, namespaceId],
      ...(params && { params }),
    });
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

  async appendResourceFact(key: ScopedKey, input: FactInput): Promise<Fact> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const f = newFact({ input, now: this.clock() });
    this.insertFact({
      fact: f,
      supersedes: input.supersedes,
      scopeCondition: "scope = ? AND namespace_id = ? AND resource_id = ?",
      scopeBindings: [SCOPE_RES, key.namespaceId, key.resourceId],
      insert: (db) =>
        db
          .query(
            `INSERT INTO ${this.prefix}_fact (id, scope, namespace_id, resource_id, thread_id, text, source, created_at, updated_at, confidence)
             VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)`,
          )
          .run(
            f.id,
            SCOPE_RES,
            key.namespaceId,
            key.resourceId,
            f.text,
            f.source,
            f.createdAt,
            f.updatedAt,
            f.confidence,
          ),
    });
    return f;
  }

  async listResourceFacts(key: ScopedKey, params?: FactListParams): Promise<Fact[]> {
    const rows = this.db
      .query<DbFactRow>(
        `SELECT id, text, source, created_at, updated_at, retired_at, superseded_by, confidence FROM ${this.prefix}_fact
         WHERE scope = ? AND namespace_id = ? AND resource_id = ?${retiredFilter(params)}
         ORDER BY retired_at IS NOT NULL, created_at ASC, rowid ASC`,
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

  async appendResourceRelation(
    key: ScopedKey,
    input: RelationInput,
    options?: { readonly maxRelations?: number },
  ): Promise<Relation> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const identity = relationKey(input);
    const table = `${this.prefix}_relation`;
    const tx = this.db.transaction(() => {
      const find = () =>
        this.db
          .query<DbRelationRow>(
            `SELECT * FROM ${table} WHERE namespace_id = ? AND resource_id = ? AND relation_key = ?`,
          )
          .get(key.namespaceId, key.resourceId, identity);
      const existing = find();
      if (existing) return existing;
      if (options?.maxRelations !== undefined) {
        const count = this.db
          .query<{ n: number }>(
            `SELECT COUNT(*) AS n FROM ${table} WHERE namespace_id = ? AND resource_id = ?`,
          )
          .get(key.namespaceId, key.resourceId);
        if ((count?.n ?? 0) >= options.maxRelations) {
          throw new MemoryLimitError(`Resource already has ${options.maxRelations} relations.`);
        }
      }
      this.db
        .query(
          `INSERT INTO ${table}
             (id, namespace_id, resource_id, subject, predicate, object, relation_key, source, confidence, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          randomUUID(),
          key.namespaceId,
          key.resourceId,
          input.subject.trim(),
          input.predicate.trim(),
          input.object.trim(),
          identity,
          input.source,
          resolveFactConfidence(input),
          this.clock(),
        );
      return find();
    });
    const row = tx();
    if (!row) throw new Error(`Relation vanished after insert: ${identity}`);
    return toRelation(row);
  }

  async appendResourceStaticRule(
    key: ScopedKey,
    params: { readonly text: string; readonly maxChars: number },
  ): Promise<ResourceRow> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const tx = this.db.transaction(() => {
      const row = this.db
        .query<DbResourceRow>(
          `SELECT * FROM ${this.prefix}_resource WHERE namespace_id = ? AND resource_id = ?`,
        )
        .get(key.namespaceId, key.resourceId);
      if (!row) throw new Error("Resource vanished while appending a rule");
      const rules = appendStaticRule({
        existing: row.static_rules,
        text: params.text,
        maxChars: params.maxChars,
      });
      if (rules !== (row.static_rules?.trim() ?? "")) {
        this.db
          .query(
            `UPDATE ${this.prefix}_resource SET static_rules = ?, updated_at = ? WHERE namespace_id = ? AND resource_id = ?`,
          )
          .run(rules, this.clock(), key.namespaceId, key.resourceId);
      }
    });
    tx();
    const updated = await this.getResource(key);
    if (!updated) throw new Error("Resource vanished while appending a rule");
    return updated;
  }

  async recordResourceRuleProposal(
    key: ScopedKey,
    input: { readonly text: string; readonly threadId: string },
    options?: { readonly maxPending?: number },
  ): Promise<RuleProposal> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const identity = normalizeRuleText(input.text);
    const table = `${this.prefix}_rule_proposal`;
    const tx = this.db.transaction(() => {
      const now = this.clock();
      const row = this.db
        .query<DbRuleProposalRow>(
          `SELECT * FROM ${table} WHERE namespace_id = ? AND resource_id = ? AND text_key = ?`,
        )
        .get(key.namespaceId, key.resourceId, identity);
      if (!row) {
        if (options?.maxPending !== undefined) {
          const pending = this.db
            .query<{ n: number }>(
              `SELECT COUNT(*) AS n FROM ${table} WHERE namespace_id = ? AND resource_id = ? AND status = 'pending'`,
            )
            .get(key.namespaceId, key.resourceId);
          if ((pending?.n ?? 0) >= options.maxPending) {
            throw new MemoryLimitError(
              `Resource already has ${options.maxPending} pending proposals.`,
            );
          }
        }
        this.db
          .query(
            `INSERT INTO ${table}
               (id, namespace_id, resource_id, text, text_key, status, evidence_thread_ids, first_seen_at, last_seen_at)
             VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)`,
          )
          .run(
            randomUUID(),
            key.namespaceId,
            key.resourceId,
            input.text.trim(),
            identity,
            JSON.stringify([input.threadId]),
            now,
            now,
          );
      } else {
        const evidence = parseThreadIds(row.evidence_thread_ids);
        if (!evidence.includes(input.threadId)) evidence.push(input.threadId);
        this.db
          .query(`UPDATE ${table} SET evidence_thread_ids = ?, last_seen_at = ? WHERE id = ?`)
          .run(JSON.stringify(evidence), now, row.id);
      }
      return this.db
        .query<DbRuleProposalRow>(
          `SELECT * FROM ${table} WHERE namespace_id = ? AND resource_id = ? AND text_key = ?`,
        )
        .get(key.namespaceId, key.resourceId, identity);
    });
    const recorded = tx();
    if (!recorded) throw new Error(`Rule proposal vanished after insert: ${identity}`);
    return toRuleProposal(recorded);
  }

  async listResourceRuleProposals(
    key: ScopedKey,
    params?: RuleProposalListParams,
  ): Promise<RuleProposal[]> {
    const where = ["namespace_id = ?", "resource_id = ?"];
    const args: string[] = [key.namespaceId, key.resourceId];
    if (params?.status !== undefined) {
      where.push("status = ?");
      args.push(params.status);
    }
    return this.db
      .query<DbRuleProposalRow>(
        `SELECT * FROM ${this.prefix}_rule_proposal WHERE ${where.join(" AND ")}
         ORDER BY first_seen_at ASC, rowid ASC`,
      )
      .all(...args)
      .map(toRuleProposal)
      .filter(
        (p) => params?.minOccurrences === undefined || p.occurrences >= params.minOccurrences,
      );
  }

  async decideResourceRuleProposal(
    key: ScopedKey,
    input: { readonly id: string; readonly status: "approved" | "rejected" },
  ): Promise<RuleProposal | null> {
    const table = `${this.prefix}_rule_proposal`;
    const tx = this.db.transaction(() => {
      const row = this.db
        .query<DbRuleProposalRow>(
          `SELECT * FROM ${table}
           WHERE namespace_id = ? AND resource_id = ? AND id = ? AND status = 'pending'`,
        )
        .get(key.namespaceId, key.resourceId, input.id);
      if (!row) return null;
      this.db.query(`UPDATE ${table} SET status = ? WHERE id = ?`).run(input.status, row.id);
      return { ...row, status: input.status };
    });
    const decided = tx();
    return decided ? toRuleProposal(decided) : null;
  }

  async approveResourceRuleProposal(
    key: ScopedKey,
    params: { readonly id: string; readonly maxChars: number },
  ): Promise<RuleProposal | null> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const table = `${this.prefix}_rule_proposal`;
    const tx = this.db.transaction(() => {
      const proposal = this.db
        .query<DbRuleProposalRow>(
          `SELECT * FROM ${table}
           WHERE namespace_id = ? AND resource_id = ? AND id = ? AND status = 'pending'`,
        )
        .get(key.namespaceId, key.resourceId, params.id);
      if (!proposal) return null;
      const resource = this.db
        .query<DbResourceRow>(
          `SELECT * FROM ${this.prefix}_resource WHERE namespace_id = ? AND resource_id = ?`,
        )
        .get(key.namespaceId, key.resourceId);
      if (!resource) throw new Error("Resource vanished while approving a rule");
      const rules = appendStaticRule({
        existing: resource.static_rules,
        text: proposal.text,
        maxChars: params.maxChars,
      });
      if (rules !== (resource.static_rules?.trim() ?? "")) {
        this.db
          .query(
            `UPDATE ${this.prefix}_resource SET static_rules = ?, updated_at = ? WHERE namespace_id = ? AND resource_id = ?`,
          )
          .run(rules, this.clock(), key.namespaceId, key.resourceId);
      }
      this.db.query(`UPDATE ${table} SET status = 'approved' WHERE id = ?`).run(proposal.id);
      return { ...proposal, status: "approved" };
    });
    const approved = tx();
    return approved ? toRuleProposal(approved) : null;
  }

  async listResourceRelations(key: ScopedKey): Promise<Relation[]> {
    return this.db
      .query<DbRelationRow>(
        `SELECT * FROM ${this.prefix}_relation
         WHERE namespace_id = ? AND resource_id = ?
         ORDER BY created_at ASC, rowid ASC`,
      )
      .all(key.namespaceId, key.resourceId)
      .map(toRelation);
  }

  async deleteResourceRelation(key: ScopedKey, relationId: string): Promise<void> {
    this.db
      .query(
        `DELETE FROM ${this.prefix}_relation
         WHERE namespace_id = ? AND resource_id = ? AND id = ?`,
      )
      .run(key.namespaceId, key.resourceId, relationId);
  }

  async appendResourceEpisode(key: ScopedKey, input: EpisodeInput): Promise<EpisodicRecord> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const ep = newEpisode({ input, now: this.clock() });
    this.insertEpisode(SCOPE_RES, key.namespaceId, key.resourceId, null, ep);
    return ep;
  }

  async listResourceEpisodes(
    key: ScopedKey,
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    return this.queryEpisodes({
      where: "scope = ? AND namespace_id = ? AND resource_id = ?",
      args: [SCOPE_RES, key.namespaceId, key.resourceId],
      ...(params && { params }),
    });
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
      throw new ThreadExistsError(key);
    }
    const now = this.clock();
    const row: ThreadRow = {
      namespaceId: key.namespaceId,
      resourceId: key.resourceId ?? init.resourceId ?? null,
      threadId: key.threadId,
      title: init.title ?? null,
      workingMemory: init.workingMemory ?? null,
      inheritFromParent: init.inheritFromParent ?? true,
      metadata: init.metadata ?? {},
      archivedAt: init.archivedAt ?? null,
      createdAt: now,
      updatedAt: now,
    };
    try {
      this.db
        .query(
          `INSERT INTO ${this.prefix}_thread
             (namespace_id, thread_id, resource_id, title, working_memory, inherit_from_parent, metadata, archived_at, message_count, last_active_at, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)`,
        )
        .run(
          row.namespaceId,
          row.threadId,
          row.resourceId,
          row.title,
          row.workingMemory,
          row.inheritFromParent ? 1 : 0,
          JSON.stringify(row.metadata),
          row.archivedAt,
          row.createdAt,
          row.createdAt,
          row.updatedAt,
        );
    } catch (error) {
      // A concurrent create past the getThread check above hits the primary key.
      if (
        error instanceof Error &&
        /UNIQUE constraint failed|SQLITE_CONSTRAINT/.test(error.message)
      ) {
        throw new ThreadExistsError(key);
      }
      throw error;
    }
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
    const args: Array<string | number | null> = [params.namespaceId];
    if (params.resourceId !== undefined) {
      where.push("resource_id = ?");
      args.push(params.resourceId);
    }
    const q = params.q?.trim().toLowerCase();
    if (q) {
      // Case-insensitive substring on thread_id; LIKE wildcards in the
      // search text match literally.
      where.push("LOWER(thread_id) LIKE ? ESCAPE '\\'");
      args.push(`%${q.replace(/[\\%_]/g, (c) => `\\${c}`)}%`);
    }
    for (const [k, v] of Object.entries(params.metadataFilter ?? {})) {
      if (v === undefined) continue;
      // Present, of the same JSON type (`true` is not `1`), and equal:
      // json_quote of a scalar / of a container's text gives the same
      // canonical text on both sides.
      const path = `$.${JSON.stringify(k)}`;
      const value = JSON.stringify(v);
      where.push(
        "json_type(metadata, ?) = json_type(?) AND json_quote(json_extract(metadata, ?)) = json_quote(json_extract(?, '$'))",
      );
      args.push(path, value, path, value);
    }
    if (params.archived === true) where.push("archived_at IS NOT NULL");
    if (params.archived === false) where.push("archived_at IS NULL");

    const order = params.order ?? "lastActiveDesc";
    const keyColumn = order === "lastActiveDesc" ? "last_active_at" : "created_at";
    const direction = order === "createdAsc" ? "ASC" : "DESC";
    const position = parseThreadCursor(params.cursor);
    if (position) {
      // Keyset: strictly after the cursor's (key, thread id).
      where.push(
        `(${keyColumn} ${order === "createdAsc" ? ">" : "<"} ? OR (${keyColumn} = ? AND thread_id > ?))`,
      );
      args.push(position.key, position.key, position.threadId);
    }
    args.push(params.limit ?? DEFAULT_THREAD_PAGE_SIZE);
    const rows = this.db
      .query<DbThreadRow>(
        // Thread id breaks ties, so pages never overlap or skip a thread.
        `SELECT * FROM ${this.prefix}_thread WHERE ${where.join(" AND ")}
         ORDER BY ${keyColumn} ${direction}, thread_id ASC LIMIT ?`,
      )
      .all(...args);

    return rows.map((r) => ({
      namespaceId: r.namespace_id,
      resourceId: r.resource_id,
      threadId: r.thread_id,
      title: r.title ?? null,
      metadata: r.metadata ? (JSON.parse(r.metadata) as Record<string, unknown>) : {},
      archivedAt: r.archived_at ?? null,
      messageCount: r.message_count,
      lastActiveAt: r.last_active_at,
      createdAt: r.created_at,
    }));
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

  async setThreadTitle(key: ThreadKey, title: string | null): Promise<void> {
    await this.requireThread(key);
    this.db
      .query(
        `UPDATE ${this.prefix}_thread SET title = ?, updated_at = ?
         WHERE namespace_id = ? AND thread_id = ?`,
      )
      .run(title, this.clock(), key.namespaceId, key.threadId);
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

  async setThreadArchived(key: ThreadKey, archivedAt: number | null): Promise<void> {
    await this.requireThread(key);
    this.db
      .query(
        `UPDATE ${this.prefix}_thread SET archived_at = ?, updated_at = ?
         WHERE namespace_id = ? AND thread_id = ?`,
      )
      .run(archivedAt, this.clock(), key.namespaceId, key.threadId);
  }

  async appendThreadFact(key: ThreadKey, input: FactInput): Promise<Fact> {
    if (!(await this.getThread(key))) {
      await this.createThread(key);
    }
    const f = newFact({ input, now: this.clock() });
    this.insertFact({
      fact: f,
      supersedes: input.supersedes,
      scopeCondition: "scope = ? AND namespace_id = ? AND thread_id = ?",
      scopeBindings: [SCOPE_THR, key.namespaceId, key.threadId],
      insert: (db) =>
        db
          .query(
            `INSERT INTO ${this.prefix}_fact (id, scope, namespace_id, resource_id, thread_id, text, source, created_at, updated_at, confidence)
             VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)`,
          )
          .run(
            f.id,
            SCOPE_THR,
            key.namespaceId,
            key.threadId,
            f.text,
            f.source,
            f.createdAt,
            f.updatedAt,
            f.confidence,
          ),
    });
    return f;
  }

  async listThreadFacts(key: ThreadKey, params?: FactListParams): Promise<Fact[]> {
    const rows = this.db
      .query<DbFactRow>(
        `SELECT id, text, source, created_at, updated_at, retired_at, superseded_by, confidence FROM ${this.prefix}_fact
         WHERE scope = ? AND namespace_id = ? AND thread_id = ?${retiredFilter(params)}
         ORDER BY retired_at IS NOT NULL, created_at ASC, rowid ASC`,
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

  async appendMessages(
    key: ThreadKey,
    msgs: ReadonlyArray<Message>,
    opts?: AppendMessagesOptions,
  ): Promise<StoredMessage[]> {
    if (!(await this.getThread(key))) {
      await this.createThread(key);
    }
    const appendKey = opts?.idempotencyKey ?? null;
    const now = this.clock();
    const insert = this.db.query(
      `INSERT INTO ${this.prefix}_message
         (namespace_id, thread_id, seq, payload, created_at, append_key, append_index)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
    );

    // Read-check-insert in one synchronous transaction: an earlier append
    // with the same key wins and is returned instead.
    const tx = this.db.transaction((): { stored: StoredMessage[]; appended: boolean } => {
      if (appendKey !== null) {
        const existing = this.messagesByAppendKey({ key, appendKey });
        if (existing.length > 0) return { stored: existing, appended: false };
      }
      const lastSeq =
        this.db
          .query<{ s: number | null }>(
            `SELECT MAX(seq) AS s FROM ${this.prefix}_message
             WHERE namespace_id = ? AND thread_id = ?`,
          )
          .get(key.namespaceId, key.threadId)?.s ?? 0;
      const stored: StoredMessage[] = [];
      let seq = lastSeq;
      msgs.forEach((m, index) => {
        seq += 1;
        insert.run(
          key.namespaceId,
          key.threadId,
          seq,
          JSON.stringify(m),
          now,
          appendKey,
          appendKey !== null ? index : null,
        );
        stored.push({ ...m, seq, createdAt: now } as StoredMessage);
      });
      // Keep the thread row's activity bookkeeping that listThreads reads.
      this.db
        .query(
          `UPDATE ${this.prefix}_thread
           SET updated_at = ?, last_active_at = ?, message_count = message_count + ?
           WHERE namespace_id = ? AND thread_id = ?`,
        )
        .run(now, now, stored.length, key.namespaceId, key.threadId);
      return { stored, appended: true };
    });
    const { stored } = tx();
    return stored;
  }

  private messagesByAppendKey(params: { key: ThreadKey; appendKey: string }): StoredMessage[] {
    return this.db
      .query<DbMessageRow>(
        `SELECT seq, payload, created_at FROM ${this.prefix}_message
         WHERE namespace_id = ? AND thread_id = ? AND append_key = ?
         ORDER BY seq ASC`,
      )
      .all(params.key.namespaceId, params.key.threadId, params.appendKey)
      .map((r) => {
        const m = JSON.parse(r.payload) as Message;
        return { ...m, seq: r.seq, createdAt: r.created_at } as StoredMessage;
      });
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
    const ep = newEpisode({ input, now: this.clock() });
    this.insertEpisode(SCOPE_THR, key.namespaceId, null, key.threadId, ep);
    return ep;
  }

  async listThreadEpisodes(key: ThreadKey, params?: EpisodeListParams): Promise<EpisodicRecord[]> {
    return this.queryEpisodes({
      where: "scope = ? AND namespace_id = ? AND thread_id = ?",
      args: [SCOPE_THR, key.namespaceId, key.threadId],
      ...(params && { params }),
    });
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

  // --- Consolidation rate limit ---------------------------------------

  async reserveConsolidation(
    request: ConsolidationReservationRequest,
  ): Promise<ConsolidationReservation> {
    const t = `${this.prefix}_reservation`;
    // One synchronous transaction: nothing interleaves between the counts
    // and the insert.
    const tx = this.db.transaction((): ConsolidationReservation => {
      const now = this.clock();
      this.db
        .query(`DELETE FROM ${t} WHERE namespace_id = ? AND kind = ? AND reserved_at <= ?`)
        .run(request.namespaceId, request.kind, now - request.windowMs);
      const window = (resourceId?: string) =>
        this.db
          .query<{ seen: number; oldest: number | null }>(
            `SELECT COUNT(*) AS seen, MIN(reserved_at) AS oldest FROM ${t}
             WHERE namespace_id = ? AND kind = ?${resourceId !== undefined ? " AND resource_id = ?" : ""}`,
          )
          .get(
            request.namespaceId,
            request.kind,
            ...(resourceId !== undefined ? [resourceId] : []),
          ) ?? { seen: 0, oldest: null };
      const denied = (params: {
        scope: "resource" | "namespace";
        row: { seen: number; oldest: number | null };
        max: number;
      }): ConsolidationReservation => ({
        granted: false,
        scope: params.scope,
        seen: params.row.seen,
        max: params.max,
        retryAfterMs: Math.max(0, (params.row.oldest ?? now) + request.windowMs - now),
      });
      const perResource = window(request.resourceId);
      if (perResource.seen >= request.maxPerResource) {
        return denied({ scope: "resource", row: perResource, max: request.maxPerResource });
      }
      if (request.maxPerNamespace !== undefined) {
        const perNamespace = window();
        if (perNamespace.seen >= request.maxPerNamespace) {
          return denied({ scope: "namespace", row: perNamespace, max: request.maxPerNamespace });
        }
      }
      const id = randomUUID();
      this.db
        .query(
          `INSERT INTO ${t} (id, namespace_id, resource_id, kind, reserved_at) VALUES (?, ?, ?, ?, ?)`,
        )
        .run(id, request.namespaceId, request.resourceId, request.kind, now);
      return { granted: true, reservationId: id };
    });
    return tx();
  }

  async releaseConsolidation(params: {
    namespaceId: string;
    reservationId: string;
  }): Promise<void> {
    this.db
      .query(`DELETE FROM ${this.prefix}_reservation WHERE namespace_id = ? AND id = ?`)
      .run(params.namespaceId, params.reservationId);
  }

  // --- Cascade ---------------------------------------------------------

  async resolveContext(key: ThreadKey, budget: TokenBudget): Promise<ResolvedContext> {
    return loadResolvedContext({ store: this, key, budget });
  }

  // --- helpers ---------------------------------------------------------

  private async requireThread(key: ThreadKey): Promise<ThreadRow> {
    const row = await this.getThread(key);
    if (!row) {
      throw new Error(`Thread not found: ${key.namespaceId}/${key.threadId}`);
    }
    return row;
  }

  /**
   * Insert `fact` and retire the current fact `supersedes` names inside
   * `scopeCondition`, in one transaction.
   */
  private insertFact(params: {
    readonly fact: Fact;
    readonly supersedes: string | undefined;
    readonly scopeCondition: string;
    readonly scopeBindings: ReadonlyArray<string>;
    readonly insert: (db: SqliteDatabase) => void;
  }): void {
    const { fact, supersedes, scopeCondition, scopeBindings, insert } = params;
    const tx = this.db.transaction(() => {
      if (supersedes !== undefined) {
        this.db
          .query(
            `UPDATE ${this.prefix}_fact SET retired_at = ?, superseded_by = ?
             WHERE id = ? AND ${scopeCondition} AND retired_at IS NULL`,
          )
          .run(fact.createdAt, fact.id, supersedes, ...scopeBindings);
      }
      insert(this.db);
    });
    tx();
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
            summary, outcome, salience,
            source_thread_id, source_msg_from_seq, source_msg_to_seq,
            occurred_at, created_at, metadata)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
        ep.sourceThreadId,
        ep.sourceMessageRange?.fromSeq ?? null,
        ep.sourceMessageRange?.toSeq ?? null,
        ep.occurredAt,
        ep.createdAt,
        JSON.stringify(ep.metadata),
      );
  }

  /** Episodes matching `where` and `params`, filtered, ordered and limited in SQL. */
  private queryEpisodes(query: {
    where: string;
    args: ReadonlyArray<string | number | null>;
    params?: EpisodeListParams;
  }): EpisodicRecord[] {
    const { params } = query;
    const where = [query.where];
    const args: Array<string | number | null> = [...query.args];
    if (params?.kind !== undefined) {
      where.push("json_extract(metadata, '$.kind') = ?");
      args.push(params.kind);
    }
    if (params?.sourceThreadId !== undefined) {
      where.push("source_thread_id = ?");
      args.push(params.sourceThreadId);
    }
    if (params?.minSalience !== undefined) {
      where.push("salience >= ?");
      args.push(params.minSalience);
    }
    if (params?.anyTerms && params.anyTerms.length > 0) {
      const terms = params.anyTerms.map((t) => t.toLowerCase());
      where.push(
        `(${terms
          .map(() => "instr(lower(summary), ?) > 0 OR instr(lower(COALESCE(outcome, '')), ?) > 0")
          .join(" OR ")})`,
      );
      for (const term of terms) args.push(term, term);
    }
    const order = params?.order ?? "salienceDesc";
    const decays = order === "decayedSalienceDesc" && (params?.halfLifeMs ?? 0) > 0;
    const orderArgs: number[] = [];
    let primary: string;
    if (decays) {
      // Same formula as `decayedSalience`: salience * max(0.5^(age/halfLife), floor).
      primary = "salience * max(pow(0.5, max(? - occurred_at, 0) * 1.0 / ?), ?) DESC";
      orderArgs.push(params?.now ?? this.clock(), params?.halfLifeMs ?? 1, MIN_SALIENCE_RETENTION);
    } else {
      primary =
        order === "createdDesc"
          ? "created_at DESC"
          : order === "occurredDesc"
            ? "occurred_at DESC"
            : order === "sourceRangeDesc"
              ? "source_msg_to_seq IS NULL, source_msg_to_seq DESC"
              : "salience DESC";
    }
    args.push(...orderArgs);
    let limit = "";
    if (params?.limit !== undefined) {
      limit = " LIMIT ?";
      args.push(params.limit);
    }
    return this.db
      .query<DbEpisodeRow>(
        // Ties go to the later insert.
        `SELECT * FROM ${this.prefix}_episode WHERE ${where.join(" AND ")}
         ORDER BY ${primary}, rowid DESC${limit}`,
      )
      .all(...args)
      .map(toEpisode);
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
  title: string | null;
  working_memory: string | null;
  inherit_from_parent: number;
  metadata: string | null;
  archived_at: number | null;
  message_count: number;
  last_active_at: number;
  created_at: number;
  updated_at: number;
}

interface DbFactRow {
  id: string;
  text: string;
  source: string;
  created_at: number;
  updated_at: number;
  retired_at: number | null;
  superseded_by: string | null;
  confidence: number;
}

interface DbRelationRow {
  id: string;
  subject: string;
  predicate: string;
  object: string;
  source: string;
  confidence: number;
  created_at: number;
}

function toRelation(r: DbRelationRow): Relation {
  return {
    id: r.id,
    subject: r.subject,
    predicate: r.predicate,
    object: r.object,
    source: r.source as FactSource,
    confidence: r.confidence,
    createdAt: r.created_at,
  };
}

interface DbRuleProposalRow {
  id: string;
  text: string;
  status: string;
  evidence_thread_ids: string;
  first_seen_at: number;
  last_seen_at: number;
}

/** The thread ids of a stored evidence list; anything but a JSON array of strings reads as empty. */
function parseThreadIds(raw: string): string[] {
  const parsed: unknown = JSON.parse(raw);
  return Array.isArray(parsed) ? parsed.filter((id): id is string => typeof id === "string") : [];
}

function toRuleProposal(r: DbRuleProposalRow): RuleProposal {
  const evidenceThreadIds = parseThreadIds(r.evidence_thread_ids);
  return {
    id: r.id,
    text: r.text,
    status: r.status as RuleProposalStatus,
    evidenceThreadIds,
    occurrences: evidenceThreadIds.length,
    firstSeenAt: r.first_seen_at,
    lastSeenAt: r.last_seen_at,
  };
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
  const metadata = r.metadata ? (JSON.parse(r.metadata) as Record<string, unknown>) : {};
  const title = r.title ?? null;
  return {
    namespaceId: r.namespace_id,
    resourceId: r.resource_id,
    threadId: r.thread_id,
    title,
    workingMemory: r.working_memory,
    inheritFromParent: r.inherit_from_parent === 1,
    metadata,
    archivedAt: r.archived_at ?? null,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

function toFact(r: DbFactRow): Fact {
  return {
    id: r.id,
    text: r.text,
    source: r.source as FactSource,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
    retiredAt: r.retired_at,
    supersededBy: r.superseded_by,
    confidence: r.confidence,
  };
}

/** SQL suffix keeping current facts; with `includeRetired`, every fact. */
function retiredFilter(params: FactListParams | undefined): string {
  return params?.includeRetired ? "" : " AND retired_at IS NULL";
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
    sourceThreadId: r.source_thread_id,
    sourceMessageRange: range,
    occurredAt: r.occurred_at,
    createdAt: r.created_at,
    metadata: r.metadata ? (JSON.parse(r.metadata) as Record<string, unknown>) : {},
  };
}
