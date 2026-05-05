// ---------------------------------------------------------------------------
// PostgresMemoryStore — `MemoryStore` over Postgres. Mirrors
// SqliteMemoryStore so the same conformance suite passes against both,
// and so multiple Zorya replicas can share one memory store.
//
// Six tables: agent_namespace / agent_resource / agent_thread for the
// scope rows, agent_fact / agent_episode for tier rows (single-table-
// per-tier, scope-tagged), and agent_message for the per-thread
// transcript. See `schema.ts` for the Drizzle definitions and
// `drizzle/0033_agent_memory.sql` for the raw DDL.
//
// Timestamps are millisecond unix epochs (BIGINT) for clock-parity with
// the SQLite implementation. Booleans are real BOOLEANs in PG; metadata
// and embedding are JSONB so containment queries land cheaply when we
// need them.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import { and, asc, desc, eq, isNotNull, isNull, sql } from "drizzle-orm";
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
import type { DrizzleDb } from "./drizzle-db.ts";
import {
  agentEpisode,
  agentFact,
  agentMessage,
  agentNamespace,
  agentResource,
  agentThread,
} from "./schema.ts";

const SCOPE_NS = "namespace";
const SCOPE_RES = "resource";
const SCOPE_THR = "thread";

export interface PostgresMemoryStoreConfig {
  readonly db: DrizzleDb;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class PostgresMemoryStore implements MemoryStore {
  private readonly db: DrizzleDb;
  private readonly clock: () => number;

  constructor(config: PostgresMemoryStoreConfig) {
    this.db = config.db;
    this.clock = config.now ?? (() => Date.now());
  }

  // --- Namespace -------------------------------------------------------

  async getNamespace(namespaceId: string): Promise<NamespaceRow | null> {
    const rows = await this.db
      .select()
      .from(agentNamespace)
      .where(eq(agentNamespace.namespaceId, namespaceId))
      .limit(1);
    const row = rows[0];
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
    await this.db
      .insert(agentNamespace)
      .values({
        namespaceId,
        staticRules: next.staticRules,
        workingMemory: next.workingMemory,
        inheritFromParent: next.inheritFromParent,
        metadata: next.metadata as Record<string, unknown>,
        createdAt: next.createdAt,
        updatedAt: next.updatedAt,
      })
      .onConflictDoUpdate({
        target: agentNamespace.namespaceId,
        set: {
          staticRules: next.staticRules,
          workingMemory: next.workingMemory,
          inheritFromParent: next.inheritFromParent,
          metadata: next.metadata as Record<string, unknown>,
          updatedAt: next.updatedAt,
        },
      });
    return next;
  }

  async appendNamespaceFact(namespaceId: string, text: string): Promise<Fact> {
    if (!(await this.getNamespace(namespaceId))) {
      await this.upsertNamespace(namespaceId, {});
    }
    const f = this.makeFact(text);
    await this.db.insert(agentFact).values({
      id: f.id,
      scope: SCOPE_NS,
      namespaceId,
      resourceId: null,
      threadId: null,
      factText: f.text,
      createdAt: f.createdAt,
      updatedAt: f.updatedAt,
    });
    return f;
  }

  async listNamespaceFacts(namespaceId: string): Promise<Fact[]> {
    const rows = await this.db
      .select()
      .from(agentFact)
      .where(and(eq(agentFact.scope, SCOPE_NS), eq(agentFact.namespaceId, namespaceId)))
      .orderBy(asc(agentFact.createdAt), asc(agentFact.id));
    return rows.map(toFact);
  }

  async deleteNamespaceFact(namespaceId: string, factId: string): Promise<void> {
    await this.db
      .delete(agentFact)
      .where(
        and(
          eq(agentFact.scope, SCOPE_NS),
          eq(agentFact.namespaceId, namespaceId),
          eq(agentFact.id, factId),
        ),
      );
  }

  async appendNamespaceEpisode(namespaceId: string, input: EpisodeInput): Promise<EpisodicRecord> {
    if (!(await this.getNamespace(namespaceId))) {
      await this.upsertNamespace(namespaceId, {});
    }
    const ep = this.makeEpisode(input);
    await this.insertEpisode(SCOPE_NS, namespaceId, null, null, ep);
    return ep;
  }

  async listNamespaceEpisodes(
    namespaceId: string,
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    const rows = await this.db
      .select()
      .from(agentEpisode)
      .where(and(eq(agentEpisode.scope, SCOPE_NS), eq(agentEpisode.namespaceId, namespaceId)));
    return queryEpisodesPostFilter(rows, params);
  }

  async deleteNamespaceEpisode(namespaceId: string, episodeId: string): Promise<void> {
    await this.db
      .delete(agentEpisode)
      .where(
        and(
          eq(agentEpisode.scope, SCOPE_NS),
          eq(agentEpisode.namespaceId, namespaceId),
          eq(agentEpisode.id, episodeId),
        ),
      );
  }

  // --- Resource --------------------------------------------------------

  async getResource(key: ScopedKey): Promise<ResourceRow | null> {
    const rows = await this.db
      .select()
      .from(agentResource)
      .where(
        and(
          eq(agentResource.namespaceId, key.namespaceId),
          eq(agentResource.resourceId, key.resourceId),
        ),
      )
      .limit(1);
    const row = rows[0];
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
    await this.db
      .insert(agentResource)
      .values({
        namespaceId: key.namespaceId,
        resourceId: key.resourceId,
        staticRules: next.staticRules,
        workingMemory: next.workingMemory,
        inheritFromParent: next.inheritFromParent,
        metadata: next.metadata as Record<string, unknown>,
        createdAt: next.createdAt,
        updatedAt: next.updatedAt,
      })
      .onConflictDoUpdate({
        target: [agentResource.namespaceId, agentResource.resourceId],
        set: {
          staticRules: next.staticRules,
          workingMemory: next.workingMemory,
          inheritFromParent: next.inheritFromParent,
          metadata: next.metadata as Record<string, unknown>,
          updatedAt: next.updatedAt,
        },
      });
    return next;
  }

  async appendResourceFact(key: ScopedKey, text: string): Promise<Fact> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const f = this.makeFact(text);
    await this.db.insert(agentFact).values({
      id: f.id,
      scope: SCOPE_RES,
      namespaceId: key.namespaceId,
      resourceId: key.resourceId,
      threadId: null,
      factText: f.text,
      createdAt: f.createdAt,
      updatedAt: f.updatedAt,
    });
    return f;
  }

  async listResourceFacts(key: ScopedKey): Promise<Fact[]> {
    const rows = await this.db
      .select()
      .from(agentFact)
      .where(
        and(
          eq(agentFact.scope, SCOPE_RES),
          eq(agentFact.namespaceId, key.namespaceId),
          eq(agentFact.resourceId, key.resourceId),
        ),
      )
      .orderBy(asc(agentFact.createdAt), asc(agentFact.id));
    return rows.map(toFact);
  }

  async deleteResourceFact(key: ScopedKey, factId: string): Promise<void> {
    await this.db
      .delete(agentFact)
      .where(
        and(
          eq(agentFact.scope, SCOPE_RES),
          eq(agentFact.namespaceId, key.namespaceId),
          eq(agentFact.resourceId, key.resourceId),
          eq(agentFact.id, factId),
        ),
      );
  }

  async appendResourceEpisode(key: ScopedKey, input: EpisodeInput): Promise<EpisodicRecord> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const ep = this.makeEpisode(input);
    await this.insertEpisode(SCOPE_RES, key.namespaceId, key.resourceId, null, ep);
    return ep;
  }

  async listResourceEpisodes(
    key: ScopedKey,
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    const rows = await this.db
      .select()
      .from(agentEpisode)
      .where(
        and(
          eq(agentEpisode.scope, SCOPE_RES),
          eq(agentEpisode.namespaceId, key.namespaceId),
          eq(agentEpisode.resourceId, key.resourceId),
        ),
      );
    return queryEpisodesPostFilter(rows, params);
  }

  async deleteResourceEpisode(key: ScopedKey, episodeId: string): Promise<void> {
    await this.db
      .delete(agentEpisode)
      .where(
        and(
          eq(agentEpisode.scope, SCOPE_RES),
          eq(agentEpisode.namespaceId, key.namespaceId),
          eq(agentEpisode.resourceId, key.resourceId),
          eq(agentEpisode.id, episodeId),
        ),
      );
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
      title: init.title ?? null,
      workingMemory: init.workingMemory ?? null,
      inheritFromParent: init.inheritFromParent ?? true,
      metadata: init.metadata ?? {},
      archivedAt: init.archivedAt ?? null,
      createdAt: now,
      updatedAt: now,
    };
    await this.db.insert(agentThread).values({
      namespaceId: row.namespaceId,
      threadId: row.threadId,
      resourceId: row.resourceId,
      title: row.title,
      workingMemory: row.workingMemory,
      inheritFromParent: row.inheritFromParent,
      metadata: row.metadata as Record<string, unknown>,
      archivedAt: row.archivedAt,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    });
    return row;
  }

  async getThread(key: ThreadKey): Promise<ThreadRow | null> {
    const rows = await this.db
      .select()
      .from(agentThread)
      .where(
        and(eq(agentThread.namespaceId, key.namespaceId), eq(agentThread.threadId, key.threadId)),
      )
      .limit(1);
    const row = rows[0];
    return row ? toThreadRow(row) : null;
  }

  async listThreads(params: ListThreadsParams): Promise<ThreadSummary[]> {
    const conditions = [eq(agentThread.namespaceId, params.namespaceId)];
    if (params.resourceId !== undefined) {
      conditions.push(eq(agentThread.resourceId, params.resourceId));
    }
    if (params.q && params.q.trim().length > 0) {
      const needle = `%${params.q.trim().toLowerCase()}%`;
      conditions.push(sql`LOWER(${agentThread.threadId}) LIKE ${needle}`);
    }
    if (params.archived === true) {
      conditions.push(isNotNull(agentThread.archivedAt));
    } else {
      conditions.push(isNull(agentThread.archivedAt));
    }

    const rows = await this.db
      .select()
      .from(agentThread)
      .where(and(...conditions));

    // Per-thread message-count + lastActiveAt. Mirrors the SQLite N+1
    // pattern; can be folded into a single CTE later if it shows up hot.
    const summaries: ThreadSummary[] = await Promise.all(
      rows.map(async (r) => {
        const lastMsgRows = await this.db
          .select({ createdAt: agentMessage.createdAt })
          .from(agentMessage)
          .where(
            and(eq(agentMessage.namespaceId, r.namespaceId), eq(agentMessage.threadId, r.threadId)),
          )
          .orderBy(desc(agentMessage.seq))
          .limit(1);
        const countRows = await this.db
          .select({ c: sql<number>`COUNT(*)::int` })
          .from(agentMessage)
          .where(
            and(eq(agentMessage.namespaceId, r.namespaceId), eq(agentMessage.threadId, r.threadId)),
          );
        const metadata = (r.metadata as Record<string, unknown> | null) ?? {};
        const title =
          r.title ?? (typeof metadata.title === "string" ? (metadata.title as string) : null);
        return {
          namespaceId: r.namespaceId,
          resourceId: r.resourceId,
          threadId: r.threadId,
          title,
          metadata,
          archivedAt: r.archivedAt,
          messageCount: countRows[0]?.c ?? 0,
          lastActiveAt: lastMsgRows[0]?.createdAt ?? r.updatedAt,
          createdAt: r.createdAt,
        };
      }),
    );

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
    await this.db
      .update(agentThread)
      .set({ workingMemory: content, updatedAt: this.clock() })
      .where(
        and(eq(agentThread.namespaceId, key.namespaceId), eq(agentThread.threadId, key.threadId)),
      );
  }

  async setThreadTitle(key: ThreadKey, title: string | null): Promise<void> {
    await this.requireThread(key);
    await this.db
      .update(agentThread)
      .set({ title, updatedAt: this.clock() })
      .where(
        and(eq(agentThread.namespaceId, key.namespaceId), eq(agentThread.threadId, key.threadId)),
      );
  }

  async setThreadMetadata(
    key: ThreadKey,
    metadata: Readonly<Record<string, unknown>>,
  ): Promise<void> {
    await this.requireThread(key);
    await this.db
      .update(agentThread)
      .set({ metadata: metadata as Record<string, unknown>, updatedAt: this.clock() })
      .where(
        and(eq(agentThread.namespaceId, key.namespaceId), eq(agentThread.threadId, key.threadId)),
      );
  }

  async setThreadInheritFromParent(key: ThreadKey, inherit: boolean): Promise<void> {
    await this.requireThread(key);
    await this.db
      .update(agentThread)
      .set({ inheritFromParent: inherit, updatedAt: this.clock() })
      .where(
        and(eq(agentThread.namespaceId, key.namespaceId), eq(agentThread.threadId, key.threadId)),
      );
  }

  async setThreadArchived(key: ThreadKey, archivedAt: number | null): Promise<void> {
    await this.requireThread(key);
    await this.db
      .update(agentThread)
      .set({ archivedAt, updatedAt: this.clock() })
      .where(
        and(eq(agentThread.namespaceId, key.namespaceId), eq(agentThread.threadId, key.threadId)),
      );
  }

  async appendThreadFact(key: ThreadKey, text: string): Promise<Fact> {
    if (!(await this.getThread(key))) {
      await this.createThread(key);
    }
    const f = this.makeFact(text);
    await this.db.insert(agentFact).values({
      id: f.id,
      scope: SCOPE_THR,
      namespaceId: key.namespaceId,
      resourceId: null,
      threadId: key.threadId,
      factText: f.text,
      createdAt: f.createdAt,
      updatedAt: f.updatedAt,
    });
    return f;
  }

  async listThreadFacts(key: ThreadKey): Promise<Fact[]> {
    const rows = await this.db
      .select()
      .from(agentFact)
      .where(
        and(
          eq(agentFact.scope, SCOPE_THR),
          eq(agentFact.namespaceId, key.namespaceId),
          eq(agentFact.threadId, key.threadId),
        ),
      )
      .orderBy(asc(agentFact.createdAt), asc(agentFact.id));
    return rows.map(toFact);
  }

  async deleteThreadFact(key: ThreadKey, factId: string): Promise<void> {
    await this.db
      .delete(agentFact)
      .where(
        and(
          eq(agentFact.scope, SCOPE_THR),
          eq(agentFact.namespaceId, key.namespaceId),
          eq(agentFact.threadId, key.threadId),
          eq(agentFact.id, factId),
        ),
      );
  }

  async appendMessages(key: ThreadKey, msgs: ReadonlyArray<Message>): Promise<StoredMessage[]> {
    if (!(await this.getThread(key))) {
      await this.createThread(key);
    }
    const now = this.clock();
    const stored: StoredMessage[] = [];

    await this.db.transaction(async (tx) => {
      const lastSeqRows = await tx
        .select({ s: sql<number | null>`MAX(${agentMessage.seq})` })
        .from(agentMessage)
        .where(
          and(
            eq(agentMessage.namespaceId, key.namespaceId),
            eq(agentMessage.threadId, key.threadId),
          ),
        );
      let seq = lastSeqRows[0]?.s ?? 0;

      const values = msgs.map((m) => {
        seq += 1;
        const sm: StoredMessage = { ...m, seq, createdAt: now } as StoredMessage;
        stored.push(sm);
        return {
          namespaceId: key.namespaceId,
          threadId: key.threadId,
          seq,
          payload: m as unknown as Record<string, unknown>,
          createdAt: now,
        };
      });
      if (values.length > 0) {
        await tx.insert(agentMessage).values(values);
      }

      // Touch updated_at on the thread row so listThreads ranks by activity.
      await tx
        .update(agentThread)
        .set({ updatedAt: now })
        .where(
          and(eq(agentThread.namespaceId, key.namespaceId), eq(agentThread.threadId, key.threadId)),
        );
    });

    return stored;
  }

  async getMessages(key: ThreadKey, range?: MessageRange): Promise<StoredMessage[]> {
    const conditions = [
      eq(agentMessage.namespaceId, key.namespaceId),
      eq(agentMessage.threadId, key.threadId),
    ];
    if (range?.fromSeq !== undefined) {
      conditions.push(sql`${agentMessage.seq} >= ${range.fromSeq}`);
    }
    if (range?.toSeq !== undefined) {
      conditions.push(sql`${agentMessage.seq} <= ${range.toSeq}`);
    }
    const orderFn = range?.order === "desc" ? desc : asc;
    let q = this.db
      .select()
      .from(agentMessage)
      .where(and(...conditions))
      .orderBy(orderFn(agentMessage.seq));
    if (range?.limit !== undefined) {
      q = q.limit(range.limit) as typeof q;
    }
    const rows = await q;
    return rows.map((r) => {
      const m = r.payload as unknown as Message;
      return { ...m, seq: r.seq, createdAt: r.createdAt } as StoredMessage;
    });
  }

  async appendThreadEpisode(key: ThreadKey, input: EpisodeInput): Promise<EpisodicRecord> {
    if (!(await this.getThread(key))) {
      await this.createThread(key);
    }
    const ep = this.makeEpisode(input);
    await this.insertEpisode(SCOPE_THR, key.namespaceId, null, key.threadId, ep);
    return ep;
  }

  async listThreadEpisodes(key: ThreadKey, params?: EpisodeListParams): Promise<EpisodicRecord[]> {
    const rows = await this.db
      .select()
      .from(agentEpisode)
      .where(
        and(
          eq(agentEpisode.scope, SCOPE_THR),
          eq(agentEpisode.namespaceId, key.namespaceId),
          eq(agentEpisode.threadId, key.threadId),
        ),
      );
    return queryEpisodesPostFilter(rows, params);
  }

  async deleteThreadEpisode(key: ThreadKey, episodeId: string): Promise<void> {
    await this.db
      .delete(agentEpisode)
      .where(
        and(
          eq(agentEpisode.scope, SCOPE_THR),
          eq(agentEpisode.namespaceId, key.namespaceId),
          eq(agentEpisode.threadId, key.threadId),
          eq(agentEpisode.id, episodeId),
        ),
      );
  }

  async deleteThread(key: ThreadKey): Promise<void> {
    await this.db.transaction(async (tx) => {
      await tx
        .delete(agentMessage)
        .where(
          and(
            eq(agentMessage.namespaceId, key.namespaceId),
            eq(agentMessage.threadId, key.threadId),
          ),
        );
      await tx
        .delete(agentFact)
        .where(
          and(
            eq(agentFact.scope, SCOPE_THR),
            eq(agentFact.namespaceId, key.namespaceId),
            eq(agentFact.threadId, key.threadId),
          ),
        );
      await tx
        .delete(agentEpisode)
        .where(
          and(
            eq(agentEpisode.scope, SCOPE_THR),
            eq(agentEpisode.namespaceId, key.namespaceId),
            eq(agentEpisode.threadId, key.threadId),
          ),
        );
      await tx
        .delete(agentThread)
        .where(
          and(eq(agentThread.namespaceId, key.namespaceId), eq(agentThread.threadId, key.threadId)),
        );
    });
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

  private async insertEpisode(
    scope: string,
    namespaceId: string,
    resourceId: string | null,
    threadId: string | null,
    ep: EpisodicRecord,
  ): Promise<void> {
    await this.db.insert(agentEpisode).values({
      id: ep.id,
      scope,
      namespaceId,
      resourceId,
      threadId,
      summary: ep.summary,
      outcome: ep.outcome,
      salience: ep.salience,
      embedding: ep.embedding ? (ep.embedding as unknown as Record<string, unknown>) : null,
      sourceThreadId: ep.sourceThreadId,
      sourceMsgFromSeq: ep.sourceMessageRange?.fromSeq ?? null,
      sourceMsgToSeq: ep.sourceMessageRange?.toSeq ?? null,
      occurredAt: ep.occurredAt,
      createdAt: ep.createdAt,
      metadata: ep.metadata as Record<string, unknown>,
    });
  }
}

// ---------------------------------------------------------------------------
// Row converters
// ---------------------------------------------------------------------------

interface DbNamespaceRow {
  namespaceId: string;
  staticRules: string | null;
  workingMemory: string | null;
  inheritFromParent: boolean;
  metadata: unknown;
  createdAt: number;
  updatedAt: number;
}

interface DbResourceRow {
  namespaceId: string;
  resourceId: string;
  staticRules: string | null;
  workingMemory: string | null;
  inheritFromParent: boolean;
  metadata: unknown;
  createdAt: number;
  updatedAt: number;
}

interface DbThreadRow {
  namespaceId: string;
  threadId: string;
  resourceId: string | null;
  title: string | null;
  workingMemory: string | null;
  inheritFromParent: boolean;
  metadata: unknown;
  archivedAt: number | null;
  createdAt: number;
  updatedAt: number;
}

interface DbFactRow {
  id: string;
  factText: string;
  createdAt: number;
  updatedAt: number;
}

interface DbEpisodeRow {
  id: string;
  scope: string;
  namespaceId: string;
  resourceId: string | null;
  threadId: string | null;
  summary: string;
  outcome: string | null;
  salience: number;
  embedding: unknown;
  sourceThreadId: string | null;
  sourceMsgFromSeq: number | null;
  sourceMsgToSeq: number | null;
  occurredAt: number;
  createdAt: number;
  metadata: unknown;
}

function toNamespaceRow(r: DbNamespaceRow): NamespaceRow {
  return {
    namespaceId: r.namespaceId,
    staticRules: r.staticRules,
    workingMemory: r.workingMemory,
    inheritFromParent: r.inheritFromParent,
    metadata: (r.metadata as Record<string, unknown>) ?? {},
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  };
}

function toResourceRow(r: DbResourceRow): ResourceRow {
  return {
    namespaceId: r.namespaceId,
    resourceId: r.resourceId,
    staticRules: r.staticRules,
    workingMemory: r.workingMemory,
    inheritFromParent: r.inheritFromParent,
    metadata: (r.metadata as Record<string, unknown>) ?? {},
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  };
}

function toThreadRow(r: DbThreadRow): ThreadRow {
  const metadata = (r.metadata as Record<string, unknown>) ?? {};
  const title = r.title ?? (typeof metadata.title === "string" ? (metadata.title as string) : null);
  return {
    namespaceId: r.namespaceId,
    resourceId: r.resourceId,
    threadId: r.threadId,
    title,
    workingMemory: r.workingMemory,
    inheritFromParent: r.inheritFromParent,
    metadata,
    archivedAt: r.archivedAt ?? null,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  };
}

function toFact(r: DbFactRow): Fact {
  return { id: r.id, text: r.factText, createdAt: r.createdAt, updatedAt: r.updatedAt };
}

function toEpisode(r: DbEpisodeRow): EpisodicRecord {
  const range =
    r.sourceMsgFromSeq !== null && r.sourceMsgToSeq !== null
      ? { fromSeq: r.sourceMsgFromSeq, toSeq: r.sourceMsgToSeq }
      : null;
  const embedding = r.embedding as number[] | null;
  return {
    id: r.id,
    summary: r.summary,
    outcome: r.outcome,
    salience: r.salience,
    embedding,
    sourceThreadId: r.sourceThreadId,
    sourceMessageRange: range,
    occurredAt: r.occurredAt,
    createdAt: r.createdAt,
    metadata: (r.metadata as Record<string, unknown>) ?? {},
  };
}

function queryEpisodesPostFilter(
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

function clamp01(n: number): number {
  if (Number.isNaN(n)) return 0.5;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}
