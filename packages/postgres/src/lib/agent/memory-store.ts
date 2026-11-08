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
// is JSONB so filters on it run in SQL.
//
// Reads that grow with the data are bounded in SQL: `listThreads` pages
// over the thread rows (message count and last activity are kept on the
// row by `appendMessages`), episode lists filter / order / limit in the
// query, and `resolveContext` reads only the message tail that fits.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import { and, asc, desc, eq, gte, isNotNull, isNull, lt, sql, type SQL } from "drizzle-orm";
import {
  appendStaticRule,
  DEFAULT_THREAD_PAGE_SIZE,
  loadResolvedContext,
  MAX_DECAY_HALF_LIVES,
  MemoryLimitError,
  newEpisode,
  newFact,
  MIN_SALIENCE_RETENTION,
  normalizeRuleText,
  parseThreadCursor,
  relationKey,
  resolveFactConfidence,
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
import type { DrizzleDb } from "@spilne/perfect-postgres";
import {
  agentConsolidationReservation,
  agentEpisode,
  agentFact,
  agentRelation,
  agentRuleProposal,
  agentMessage,
  agentNamespace,
  agentResource,
  agentThread,
} from "../schema.ts";

/** The transaction handle `DrizzleDb.transaction` passes to its callback. */
type PgTransaction = Parameters<Parameters<DrizzleDb["transaction"]>[0]>[0];

/**
 * Serialize writers of one kind of record (`topic`) for one resource until the
 * transaction ends, so a count taken before an insert stays true through it.
 */
async function lockResourceTopic(params: {
  readonly tx: PgTransaction;
  readonly key: ScopedKey;
  readonly topic: string;
}): Promise<void> {
  const { tx, key, topic } = params;
  await tx.execute(
    sql`SELECT pg_advisory_xact_lock(hashtextextended(${JSON.stringify([topic, key.namespaceId, key.resourceId])}, 0))`,
  );
}

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

  async appendNamespaceFact(namespaceId: string, input: FactInput): Promise<Fact> {
    if (!(await this.getNamespace(namespaceId))) {
      await this.upsertNamespace(namespaceId, {});
    }
    const f = newFact({ input, now: this.clock() });
    await this.db.transaction(async (tx) => {
      if (input.supersedes !== undefined) {
        await tx
          .update(agentFact)
          .set({ retiredAt: f.createdAt, supersededBy: f.id })
          .where(
            and(
              eq(agentFact.id, input.supersedes),
              eq(agentFact.scope, SCOPE_NS),
              eq(agentFact.namespaceId, namespaceId),
              isNull(agentFact.retiredAt),
            ),
          );
      }
      await tx.insert(agentFact).values({
        id: f.id,
        scope: SCOPE_NS,
        namespaceId,
        resourceId: null,
        threadId: null,
        factText: f.text,
        source: f.source,
        createdAt: f.createdAt,
        updatedAt: f.updatedAt,
        confidence: f.confidence,
      });
    });
    return f;
  }

  async listNamespaceFacts(namespaceId: string, params?: FactListParams): Promise<Fact[]> {
    const rows = await this.db
      .select()
      .from(agentFact)
      .where(
        and(
          eq(agentFact.scope, SCOPE_NS),
          eq(agentFact.namespaceId, namespaceId),
          activeFactsOnly(params),
        ),
      )
      .orderBy(...factOrder());
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
    const ep = newEpisode({ input, now: this.clock() });
    await this.insertEpisode(SCOPE_NS, namespaceId, null, null, ep);
    return ep;
  }

  async listNamespaceEpisodes(
    namespaceId: string,
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    return this.queryEpisodes(
      [eq(agentEpisode.scope, SCOPE_NS), eq(agentEpisode.namespaceId, namespaceId)],
      params,
    );
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

  async appendResourceFact(key: ScopedKey, input: FactInput): Promise<Fact> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const f = newFact({ input, now: this.clock() });
    await this.db.transaction(async (tx) => {
      if (input.supersedes !== undefined) {
        await tx
          .update(agentFact)
          .set({ retiredAt: f.createdAt, supersededBy: f.id })
          .where(
            and(
              eq(agentFact.id, input.supersedes),
              eq(agentFact.scope, SCOPE_RES),
              eq(agentFact.namespaceId, key.namespaceId),
              eq(agentFact.resourceId, key.resourceId),
              isNull(agentFact.retiredAt),
            ),
          );
      }
      await tx.insert(agentFact).values({
        id: f.id,
        scope: SCOPE_RES,
        namespaceId: key.namespaceId,
        resourceId: key.resourceId,
        threadId: null,
        factText: f.text,
        source: f.source,
        createdAt: f.createdAt,
        updatedAt: f.updatedAt,
        confidence: f.confidence,
      });
    });
    return f;
  }

  async listResourceFacts(key: ScopedKey, params?: FactListParams): Promise<Fact[]> {
    const rows = await this.db
      .select()
      .from(agentFact)
      .where(
        and(
          eq(agentFact.scope, SCOPE_RES),
          eq(agentFact.namespaceId, key.namespaceId),
          eq(agentFact.resourceId, key.resourceId),
          activeFactsOnly(params),
        ),
      )
      .orderBy(...factOrder());
    return rows.map(toFact);
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
    const row = await this.db.transaction(async (tx) => {
      await lockResourceTopic({ tx, key, topic: "relation" });
      const find = async () =>
        (
          await tx
            .select()
            .from(agentRelation)
            .where(
              and(
                eq(agentRelation.namespaceId, key.namespaceId),
                eq(agentRelation.resourceId, key.resourceId),
                eq(agentRelation.relationKey, identity),
              ),
            )
        )[0];
      const existing = await find();
      if (existing) return existing;
      if (options?.maxRelations !== undefined) {
        const [count] = await tx
          .select({ n: sql<number>`count(*)::int` })
          .from(agentRelation)
          .where(
            and(
              eq(agentRelation.namespaceId, key.namespaceId),
              eq(agentRelation.resourceId, key.resourceId),
            ),
          );
        if ((count?.n ?? 0) >= options.maxRelations) {
          throw new MemoryLimitError(`Resource already has ${options.maxRelations} relations.`);
        }
      }
      await tx.insert(agentRelation).values({
        id: randomUUID(),
        namespaceId: key.namespaceId,
        resourceId: key.resourceId,
        subject: input.subject.trim(),
        predicate: input.predicate.trim(),
        object: input.object.trim(),
        relationKey: identity,
        source: input.source,
        confidence: resolveFactConfidence(input),
        createdAt: this.clock(),
      });
      return find();
    });
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
    await this.db.transaction(async (tx) => {
      const [row] = await tx
        .select()
        .from(agentResource)
        .where(
          and(
            eq(agentResource.namespaceId, key.namespaceId),
            eq(agentResource.resourceId, key.resourceId),
          ),
        )
        .for("update");
      if (!row) throw new Error("Resource vanished while appending a rule");
      const rules = appendStaticRule({
        existing: row.staticRules,
        text: params.text,
        maxChars: params.maxChars,
      });
      if (rules !== (row.staticRules?.trim() ?? "")) {
        await tx
          .update(agentResource)
          .set({ staticRules: rules, updatedAt: this.clock() })
          .where(
            and(
              eq(agentResource.namespaceId, key.namespaceId),
              eq(agentResource.resourceId, key.resourceId),
            ),
          );
      }
    });
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
    const now = this.clock();
    const textKey = normalizeRuleText(input.text);
    const [row] = await this.db.transaction(async (tx) => {
      await lockResourceTopic({ tx, key, topic: "rule-proposal" });
      if (options?.maxPending !== undefined) {
        await this.assertRoomForProposal({ tx, key, textKey, maxPending: options.maxPending });
      }
      return this.upsertProposal({ tx, key, input, textKey, now });
    });
    if (!row) throw new Error("Rule proposal upsert returned no row");
    return toRuleProposal(row);
  }

  /** Throws `MemoryLimitError` when `textKey` is new and `maxPending` proposals already wait. */
  private async assertRoomForProposal(params: {
    readonly tx: PgTransaction;
    readonly key: ScopedKey;
    readonly textKey: string;
    readonly maxPending: number;
  }): Promise<void> {
    const { tx, key, textKey, maxPending } = params;
    const owned = and(
      eq(agentRuleProposal.namespaceId, key.namespaceId),
      eq(agentRuleProposal.resourceId, key.resourceId),
    );
    const [known] = await tx
      .select({ id: agentRuleProposal.id })
      .from(agentRuleProposal)
      .where(and(owned, eq(agentRuleProposal.textKey, textKey)));
    if (known) return;
    const [pending] = await tx
      .select({ n: sql<number>`count(*)::int` })
      .from(agentRuleProposal)
      .where(and(owned, eq(agentRuleProposal.status, "pending")));
    if ((pending?.n ?? 0) >= maxPending) {
      throw new MemoryLimitError(`Resource already has ${maxPending} pending proposals.`);
    }
  }

  private upsertProposal(params: {
    readonly tx: PgTransaction;
    readonly key: ScopedKey;
    readonly input: { readonly text: string; readonly threadId: string };
    readonly textKey: string;
    readonly now: number;
  }) {
    const { tx, key, input, textKey, now } = params;
    const evidence = agentRuleProposal.evidenceThreadIds;
    return tx
      .insert(agentRuleProposal)
      .values({
        id: randomUUID(),
        namespaceId: key.namespaceId,
        resourceId: key.resourceId,
        proposalText: input.text.trim(),
        textKey,
        evidenceThreadIds: [input.threadId],
        firstSeenAt: now,
        lastSeenAt: now,
      })
      .onConflictDoUpdate({
        target: [
          agentRuleProposal.namespaceId,
          agentRuleProposal.resourceId,
          agentRuleProposal.textKey,
        ],
        set: {
          // Add the thread to the evidence unless it is already there.
          evidenceThreadIds: sql`CASE WHEN jsonb_exists(${evidence}, ${input.threadId}) THEN ${evidence} ELSE ${evidence} || to_jsonb(${input.threadId}::text) END`,
          lastSeenAt: now,
        },
      })
      .returning();
  }

  async listResourceRuleProposals(
    key: ScopedKey,
    params?: RuleProposalListParams,
  ): Promise<RuleProposal[]> {
    const rows = await this.db
      .select()
      .from(agentRuleProposal)
      .where(
        and(
          eq(agentRuleProposal.namespaceId, key.namespaceId),
          eq(agentRuleProposal.resourceId, key.resourceId),
          params?.status !== undefined ? eq(agentRuleProposal.status, params.status) : undefined,
        ),
      )
      .orderBy(asc(agentRuleProposal.firstSeenAt), asc(agentRuleProposal.seq));
    return rows
      .map(toRuleProposal)
      .filter(
        (p) => params?.minOccurrences === undefined || p.occurrences >= params.minOccurrences,
      );
  }

  async decideResourceRuleProposal(
    key: ScopedKey,
    input: { readonly id: string; readonly status: "approved" | "rejected" },
  ): Promise<RuleProposal | null> {
    const [row] = await this.db
      .update(agentRuleProposal)
      .set({ status: input.status })
      .where(
        and(
          eq(agentRuleProposal.namespaceId, key.namespaceId),
          eq(agentRuleProposal.resourceId, key.resourceId),
          eq(agentRuleProposal.id, input.id),
          eq(agentRuleProposal.status, "pending"),
        ),
      )
      .returning();
    return row ? toRuleProposal(row) : null;
  }

  async approveResourceRuleProposal(
    key: ScopedKey,
    params: { readonly id: string; readonly maxChars: number },
  ): Promise<RuleProposal | null> {
    if (!(await this.getResource(key))) {
      await this.upsertResource(key, {});
    }
    const approved = await this.db.transaction(async (tx) => {
      // Lock the proposal first, then the resource; every other writer takes
      // at most one of them, so the order cannot deadlock.
      const [proposal] = await tx
        .select()
        .from(agentRuleProposal)
        .where(
          and(
            eq(agentRuleProposal.namespaceId, key.namespaceId),
            eq(agentRuleProposal.resourceId, key.resourceId),
            eq(agentRuleProposal.id, params.id),
            eq(agentRuleProposal.status, "pending"),
          ),
        )
        .for("update");
      if (!proposal) return null;
      const [resource] = await tx
        .select()
        .from(agentResource)
        .where(
          and(
            eq(agentResource.namespaceId, key.namespaceId),
            eq(agentResource.resourceId, key.resourceId),
          ),
        )
        .for("update");
      if (!resource) throw new Error("Resource vanished while approving a rule");
      const rules = appendStaticRule({
        existing: resource.staticRules,
        text: proposal.proposalText,
        maxChars: params.maxChars,
      });
      if (rules !== (resource.staticRules?.trim() ?? "")) {
        await tx
          .update(agentResource)
          .set({ staticRules: rules, updatedAt: this.clock() })
          .where(
            and(
              eq(agentResource.namespaceId, key.namespaceId),
              eq(agentResource.resourceId, key.resourceId),
            ),
          );
      }
      const [updated] = await tx
        .update(agentRuleProposal)
        .set({ status: "approved" })
        .where(eq(agentRuleProposal.id, proposal.id))
        .returning();
      return updated ?? null;
    });
    return approved ? toRuleProposal(approved) : null;
  }

  async listResourceRelations(key: ScopedKey): Promise<Relation[]> {
    const rows = await this.db
      .select()
      .from(agentRelation)
      .where(
        and(
          eq(agentRelation.namespaceId, key.namespaceId),
          eq(agentRelation.resourceId, key.resourceId),
        ),
      )
      .orderBy(asc(agentRelation.createdAt), asc(agentRelation.seq));
    return rows.map(toRelation);
  }

  async deleteResourceRelation(key: ScopedKey, relationId: string): Promise<void> {
    await this.db
      .delete(agentRelation)
      .where(
        and(
          eq(agentRelation.namespaceId, key.namespaceId),
          eq(agentRelation.resourceId, key.resourceId),
          eq(agentRelation.id, relationId),
        ),
      );
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
    const ep = newEpisode({ input, now: this.clock() });
    await this.insertEpisode(SCOPE_RES, key.namespaceId, key.resourceId, null, ep);
    return ep;
  }

  async listResourceEpisodes(
    key: ScopedKey,
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    return this.queryEpisodes(
      [
        eq(agentEpisode.scope, SCOPE_RES),
        eq(agentEpisode.namespaceId, key.namespaceId),
        eq(agentEpisode.resourceId, key.resourceId),
      ],
      params,
    );
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
      messageCount: 0,
      lastActiveAt: row.createdAt,
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
    const conditions: SQL[] = [eq(agentThread.namespaceId, params.namespaceId)];
    if (params.resourceId !== undefined) {
      conditions.push(eq(agentThread.resourceId, params.resourceId));
    }
    const q = params.q?.trim().toLowerCase();
    if (q) {
      const needle = `%${q.replace(/[\\%_]/g, (c) => `\\${c}`)}%`;
      conditions.push(sql`LOWER(${agentThread.threadId}) LIKE ${needle}`);
    }
    for (const [k, v] of Object.entries(params.metadataFilter ?? {})) {
      if (v === undefined) continue;
      conditions.push(sql`(${agentThread.metadata} -> ${k}) = ${JSON.stringify(v)}::jsonb`);
    }
    if (params.archived === true) conditions.push(isNotNull(agentThread.archivedAt));
    if (params.archived === false) conditions.push(isNull(agentThread.archivedAt));

    const order = params.order ?? "lastActiveDesc";
    const keyColumn = order === "lastActiveDesc" ? agentThread.lastActiveAt : agentThread.createdAt;
    const position = parseThreadCursor(params.cursor);
    if (position) {
      // Keyset: strictly after the cursor's (key, thread id).
      const beyond =
        order === "createdAsc"
          ? sql`${keyColumn} > ${position.key}`
          : sql`${keyColumn} < ${position.key}`;
      conditions.push(
        sql`(${beyond} OR (${keyColumn} = ${position.key} AND ${agentThread.threadId} > ${position.threadId}))`,
      );
    }
    const rows = await this.db
      .select()
      .from(agentThread)
      .where(and(...conditions))
      // Thread id breaks ties, so pages never overlap or skip a thread.
      .orderBy(order === "createdAsc" ? asc(keyColumn) : desc(keyColumn), asc(agentThread.threadId))
      .limit(params.limit ?? DEFAULT_THREAD_PAGE_SIZE);

    return rows.map((r) => ({
      namespaceId: r.namespaceId,
      resourceId: r.resourceId,
      threadId: r.threadId,
      title: r.title ?? null,
      metadata: (r.metadata as Record<string, unknown> | null) ?? {},
      archivedAt: r.archivedAt,
      messageCount: r.messageCount,
      lastActiveAt: r.lastActiveAt,
      createdAt: r.createdAt,
    }));
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

  async appendThreadFact(key: ThreadKey, input: FactInput): Promise<Fact> {
    if (!(await this.getThread(key))) {
      await this.createThread(key);
    }
    const f = newFact({ input, now: this.clock() });
    await this.db.transaction(async (tx) => {
      if (input.supersedes !== undefined) {
        await tx
          .update(agentFact)
          .set({ retiredAt: f.createdAt, supersededBy: f.id })
          .where(
            and(
              eq(agentFact.id, input.supersedes),
              eq(agentFact.scope, SCOPE_THR),
              eq(agentFact.namespaceId, key.namespaceId),
              eq(agentFact.threadId, key.threadId),
              isNull(agentFact.retiredAt),
            ),
          );
      }
      await tx.insert(agentFact).values({
        id: f.id,
        scope: SCOPE_THR,
        namespaceId: key.namespaceId,
        resourceId: null,
        threadId: key.threadId,
        factText: f.text,
        source: f.source,
        createdAt: f.createdAt,
        updatedAt: f.updatedAt,
        confidence: f.confidence,
      });
    });
    return f;
  }

  async listThreadFacts(key: ThreadKey, params?: FactListParams): Promise<Fact[]> {
    const rows = await this.db
      .select()
      .from(agentFact)
      .where(
        and(
          eq(agentFact.scope, SCOPE_THR),
          eq(agentFact.namespaceId, key.namespaceId),
          eq(agentFact.threadId, key.threadId),
          activeFactsOnly(params),
        ),
      )
      .orderBy(...factOrder());
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

  async appendMessages(
    key: ThreadKey,
    msgs: ReadonlyArray<Message>,
    opts?: AppendMessagesOptions,
  ): Promise<StoredMessage[]> {
    if (!(await this.getThread(key))) {
      await this.createThread(key);
    }
    const appendKey = opts?.idempotencyKey;
    const now = this.clock();
    const stored: StoredMessage[] = [];
    let existing: StoredMessage[] = [];

    // Appends to one thread are serialized by a transaction-scoped advisory
    // lock: concurrent appends (any keys) take the next seq in turn instead
    // of colliding on it, and the idempotency check below is atomic.
    await this.db.transaction(async (tx) => {
      await tx.execute(
        sql`SELECT pg_advisory_xact_lock(hashtextextended(${JSON.stringify([key.namespaceId, key.threadId])}, 0))`,
      );
      if (appendKey !== undefined) {
        existing = await this.messagesByAppendKey({ key, appendKey, db: tx });
        if (existing.length > 0) return;
      }
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

      const values = msgs.map((m, index) => {
        seq += 1;
        const sm: StoredMessage = { ...m, seq, createdAt: now } as StoredMessage;
        stored.push(sm);
        return {
          namespaceId: key.namespaceId,
          threadId: key.threadId,
          seq,
          payload: m as unknown as Record<string, unknown>,
          createdAt: now,
          appendKey: appendKey ?? null,
          appendIndex: appendKey !== undefined ? index : null,
        };
      });
      if (values.length > 0) {
        await tx.insert(agentMessage).values(values);
      }

      // Keep the thread row's activity bookkeeping that listThreads reads.
      await tx
        .update(agentThread)
        .set({
          updatedAt: now,
          lastActiveAt: now,
          messageCount: sql`${agentThread.messageCount} + ${values.length}`,
        })
        .where(
          and(eq(agentThread.namespaceId, key.namespaceId), eq(agentThread.threadId, key.threadId)),
        );
    });

    return existing.length > 0 ? existing : stored;
  }

  private async messagesByAppendKey(params: {
    key: ThreadKey;
    appendKey: string;
    db: Pick<DrizzleDb, "select">;
  }): Promise<StoredMessage[]> {
    const rows = await params.db
      .select()
      .from(agentMessage)
      .where(
        and(
          eq(agentMessage.namespaceId, params.key.namespaceId),
          eq(agentMessage.threadId, params.key.threadId),
          eq(agentMessage.appendKey, params.appendKey),
        ),
      )
      .orderBy(asc(agentMessage.seq));
    return rows.map(
      (r) =>
        ({
          ...(r.payload as unknown as Message),
          seq: r.seq,
          createdAt: Number(r.createdAt),
        }) as StoredMessage,
    );
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
    const ep = newEpisode({ input, now: this.clock() });
    await this.insertEpisode(SCOPE_THR, key.namespaceId, null, key.threadId, ep);
    return ep;
  }

  async listThreadEpisodes(key: ThreadKey, params?: EpisodeListParams): Promise<EpisodicRecord[]> {
    return this.queryEpisodes(
      [
        eq(agentEpisode.scope, SCOPE_THR),
        eq(agentEpisode.namespaceId, key.namespaceId),
        eq(agentEpisode.threadId, key.threadId),
      ],
      params,
    );
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

  // --- Consolidation rate limit ---------------------------------------

  async reserveConsolidation(
    request: ConsolidationReservationRequest,
  ): Promise<ConsolidationReservation> {
    const r = agentConsolidationReservation;
    // The window is measured on the database clock: every replica counts
    // the same reservations. A transaction-scoped advisory lock per
    // (namespace, kind) makes count-then-insert atomic.
    return this.db.transaction(async (tx) => {
      await tx.execute(
        sql`SELECT pg_advisory_xact_lock(hashtextextended(${JSON.stringify(["consolidation", request.namespaceId, request.kind])}, 0))`,
      );
      const nowRows = await tx.execute(
        sql`SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint AS now`,
      );
      const now = Number((nowRows as unknown as Array<{ now: string | number }>)[0]!.now);
      const since = now - request.windowMs;
      await tx
        .delete(r)
        .where(
          and(
            eq(r.namespaceId, request.namespaceId),
            eq(r.kind, request.kind),
            lt(r.reservedAt, since + 1),
          ),
        );
      const window = (resourceId?: string) =>
        tx
          .select({
            seen: sql<number>`COUNT(*)::int`,
            oldest: sql<string | null>`MIN(${r.reservedAt})`,
          })
          .from(r)
          .where(
            and(
              eq(r.namespaceId, request.namespaceId),
              eq(r.kind, request.kind),
              ...(resourceId !== undefined ? [eq(r.resourceId, resourceId)] : []),
            ),
          );
      const denied = (params: {
        scope: "resource" | "namespace";
        row: { seen: number; oldest: string | null };
        max: number;
      }): ConsolidationReservation => ({
        granted: false,
        scope: params.scope,
        seen: params.row.seen,
        max: params.max,
        retryAfterMs: Math.max(0, Number(params.row.oldest ?? now) + request.windowMs - now),
      });
      const [perResource] = await window(request.resourceId);
      if (perResource!.seen >= request.maxPerResource) {
        return denied({ scope: "resource", row: perResource!, max: request.maxPerResource });
      }
      if (request.maxPerNamespace !== undefined) {
        const [perNamespace] = await window();
        if (perNamespace!.seen >= request.maxPerNamespace) {
          return denied({ scope: "namespace", row: perNamespace!, max: request.maxPerNamespace });
        }
      }
      const id = randomUUID();
      await tx.insert(r).values({
        id,
        namespaceId: request.namespaceId,
        resourceId: request.resourceId,
        kind: request.kind,
        reservedAt: now,
      });
      return { granted: true, reservationId: id };
    });
  }

  async releaseConsolidation(params: {
    namespaceId: string;
    reservationId: string;
  }): Promise<void> {
    await this.db
      .delete(agentConsolidationReservation)
      .where(
        and(
          eq(agentConsolidationReservation.namespaceId, params.namespaceId),
          eq(agentConsolidationReservation.id, params.reservationId),
        ),
      );
  }

  // --- Cascade ---------------------------------------------------------

  async resolveContext(key: ThreadKey, budget: TokenBudget): Promise<ResolvedContext> {
    return loadResolvedContext({ store: this, key, budget });
  }

  // --- helpers ---------------------------------------------------------

  /** Episodes matching `scoped` and `params`, filtered, ordered and limited in SQL. */
  private async queryEpisodes(
    scoped: SQL[],
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    const conditions = [...scoped];
    if (params?.kind !== undefined) {
      conditions.push(sql`${agentEpisode.metadata} ->> 'kind' = ${params.kind}`);
    }
    if (params?.sourceThreadId !== undefined) {
      conditions.push(eq(agentEpisode.sourceThreadId, params.sourceThreadId));
    }
    if (params?.minSalience !== undefined) {
      conditions.push(gte(agentEpisode.salience, params.minSalience));
    }
    if (params?.anyTerms && params.anyTerms.length > 0) {
      const matches = params.anyTerms.map((term) => {
        const t = term.toLowerCase();
        return sql`(strpos(lower(${agentEpisode.summary}), ${t}) > 0 OR strpos(lower(COALESCE(${agentEpisode.outcome}, '')), ${t}) > 0)`;
      });
      conditions.push(sql`(${sql.join(matches, sql` OR `)})`);
    }
    const order = params?.order ?? "salienceDesc";
    const halfLifeMs = params?.halfLifeMs ?? 0;
    const now = params?.now ?? this.clock();
    // Same formula as `decayedSalience`: salience * max(0.5^(age/halfLife), floor).
    const decayed = sql`${agentEpisode.salience} * GREATEST(power(0.5, LEAST(GREATEST(${now}::double precision - ${agentEpisode.occurredAt}, 0) / ${halfLifeMs}::double precision, ${MAX_DECAY_HALF_LIVES}::double precision)), ${MIN_SALIENCE_RETENTION}::double precision) DESC`;
    const primary =
      order === "decayedSalienceDesc" && halfLifeMs > 0
        ? decayed
        : order === "createdDesc"
          ? desc(agentEpisode.createdAt)
          : order === "occurredDesc"
            ? desc(agentEpisode.occurredAt)
            : order === "sourceRangeDesc"
              ? sql`${agentEpisode.sourceMsgToSeq} DESC NULLS LAST`
              : desc(agentEpisode.salience);
    let q = this.db
      .select()
      .from(agentEpisode)
      .where(and(...conditions))
      // Ties go to the later insert.
      .orderBy(primary, desc(agentEpisode.seq));
    if (params?.limit !== undefined) q = q.limit(params.limit) as typeof q;
    return (await q).map(toEpisode);
  }

  private async requireThread(key: ThreadKey): Promise<ThreadRow> {
    const row = await this.getThread(key);
    if (!row) {
      throw new Error(`Thread not found: ${key.namespaceId}/${key.threadId}`);
    }
    return row;
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
  source: string;
  createdAt: number;
  updatedAt: number;
  retiredAt: number | null;
  supersededBy: string | null;
  confidence: number;
}

interface DbRelationRow {
  id: string;
  subject: string;
  predicate: string;
  object: string;
  source: string;
  confidence: number;
  createdAt: number;
}

function toRelation(r: DbRelationRow): Relation {
  return {
    id: r.id,
    subject: r.subject,
    predicate: r.predicate,
    object: r.object,
    source: r.source as FactSource,
    confidence: r.confidence,
    createdAt: r.createdAt,
  };
}

interface DbRuleProposalRow {
  id: string;
  proposalText: string;
  status: string;
  evidenceThreadIds: string[];
  firstSeenAt: number;
  lastSeenAt: number;
}

function toRuleProposal(r: DbRuleProposalRow): RuleProposal {
  return {
    id: r.id,
    text: r.proposalText,
    status: r.status as RuleProposalStatus,
    evidenceThreadIds: r.evidenceThreadIds,
    occurrences: r.evidenceThreadIds.length,
    firstSeenAt: r.firstSeenAt,
    lastSeenAt: r.lastSeenAt,
  };
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
  const title = r.title ?? null;
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
  return {
    id: r.id,
    text: r.factText,
    source: r.source as FactSource,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
    retiredAt: r.retiredAt,
    supersededBy: r.supersededBy,
    confidence: r.confidence,
  };
}

/** Condition keeping current facts; with `includeRetired`, every fact. */
function activeFactsOnly(params: FactListParams | undefined): SQL | undefined {
  return params?.includeRetired ? undefined : isNull(agentFact.retiredAt);
}

/** Current facts first, then retired ones; insertion order within each. */
function factOrder(): SQL[] {
  return [sql`(${agentFact.retiredAt} IS NOT NULL)`, asc(agentFact.createdAt), asc(agentFact.seq)];
}

function toEpisode(r: DbEpisodeRow): EpisodicRecord {
  const range =
    r.sourceMsgFromSeq !== null && r.sourceMsgToSeq !== null
      ? { fromSeq: r.sourceMsgFromSeq, toSeq: r.sourceMsgToSeq }
      : null;
  return {
    id: r.id,
    summary: r.summary,
    outcome: r.outcome,
    salience: r.salience,
    sourceThreadId: r.sourceThreadId,
    sourceMessageRange: range,
    occurredAt: r.occurredAt,
    createdAt: r.createdAt,
    metadata: (r.metadata as Record<string, unknown>) ?? {},
  };
}
