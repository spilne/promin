// ---------------------------------------------------------------------------
// PostgresKnowledgeBaseStore — `KnowledgeBaseStore` over the `knowledge_base`
// and `knowledge_base_source` tables. Mirrors `SqliteKnowledgeBaseStore`.
//
// Ingestion claims are stamped and aged on the database server's clock
// (`clock_timestamp()`), so replicas with skewed clocks agree on which claim
// is stale. A claim takes the oldest claimable row with
// `FOR UPDATE SKIP LOCKED`, so concurrent workers never share one.
// ---------------------------------------------------------------------------

import { and, asc, eq, sql } from "drizzle-orm";
import type {
  ClaimedKnowledgeSource,
  KnowledgeBaseKey,
  KnowledgeBaseRecord,
  KnowledgeBaseStore,
  KnowledgeSourceKey,
  KnowledgeSourceRecord,
  KnowledgeSourceSummary,
  StoredKnowledgeBase,
} from "@promin/agent";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { knowledgeBase, knowledgeBaseSource } from "../schema.ts";

export interface PostgresKnowledgeBaseStoreConfig {
  readonly db: DrizzleDb;
}

type BaseRow = typeof knowledgeBase.$inferSelect;
type SourceRow = typeof knowledgeBaseSource.$inferSelect;

/** Database-server time in epoch ms. */
const DB_NOW_MS = sql<number>`(extract(epoch from clock_timestamp()) * 1000)::bigint`;

/** Every source column but its text, for listings. */
const SUMMARY_COLUMNS = {
  namespaceId: knowledgeBaseSource.namespaceId,
  baseId: knowledgeBaseSource.baseId,
  sourceId: knowledgeBaseSource.sourceId,
  title: knowledgeBaseSource.title,
  uri: knowledgeBaseSource.uri,
  mimeType: knowledgeBaseSource.mimeType,
  tags: knowledgeBaseSource.tags,
  metadata: knowledgeBaseSource.metadata,
  status: knowledgeBaseSource.status,
  error: knowledgeBaseSource.error,
  chunkCount: knowledgeBaseSource.chunkCount,
  createdAt: knowledgeBaseSource.createdAt,
  updatedAt: knowledgeBaseSource.updatedAt,
};

type SummaryRow = Omit<SourceRow, "text" | "claimedBy" | "claimedAt">;

export class PostgresKnowledgeBaseStore implements KnowledgeBaseStore {
  private readonly db: DrizzleDb;

  constructor(config: PostgresKnowledgeBaseStoreConfig) {
    this.db = config.db;
  }

  async listBases(): Promise<ReadonlyArray<StoredKnowledgeBase>> {
    const bases = await this.db
      .select()
      .from(knowledgeBase)
      .orderBy(asc(knowledgeBase.namespaceId), asc(knowledgeBase.id));
    const sources = await this.db
      .select(SUMMARY_COLUMNS)
      .from(knowledgeBaseSource)
      .orderBy(
        asc(knowledgeBaseSource.namespaceId),
        asc(knowledgeBaseSource.baseId),
        asc(knowledgeBaseSource.sourceId),
      );
    const byBase = new Map<string, KnowledgeSourceSummary[]>();
    for (const row of sources) {
      const key = JSON.stringify([row.namespaceId, row.baseId]);
      const list = byBase.get(key) ?? [];
      list.push(toSummary(row));
      byBase.set(key, list);
    }
    return bases.map((row) => ({
      base: toBase(row),
      sources: byBase.get(JSON.stringify([row.namespaceId, row.id])) ?? [],
    }));
  }

  async getBase(key: KnowledgeBaseKey): Promise<StoredKnowledgeBase | null> {
    const [row] = await this.db
      .select()
      .from(knowledgeBase)
      .where(and(eq(knowledgeBase.namespaceId, key.namespaceId), eq(knowledgeBase.id, key.id)));
    if (!row) return null;
    const sources = await this.db
      .select(SUMMARY_COLUMNS)
      .from(knowledgeBaseSource)
      .where(
        and(
          eq(knowledgeBaseSource.namespaceId, key.namespaceId),
          eq(knowledgeBaseSource.baseId, key.id),
        ),
      )
      .orderBy(asc(knowledgeBaseSource.sourceId));
    return { base: toBase(row), sources: sources.map(toSummary) };
  }

  async getSource(key: KnowledgeSourceKey): Promise<KnowledgeSourceRecord | null> {
    const [row] = await this.db.select().from(knowledgeBaseSource).where(sourceWhere(key));
    return row ? toSource(row) : null;
  }

  async saveBase(base: KnowledgeBaseRecord): Promise<void> {
    const values = {
      namespaceId: base.namespaceId,
      id: base.id,
      description: base.description ?? null,
      tags: [...base.tags],
      metadata: { ...base.metadata },
      createdAt: base.createdAt,
      updatedAt: base.updatedAt,
    };
    await this.db
      .insert(knowledgeBase)
      .values(values)
      .onConflictDoUpdate({
        target: [knowledgeBase.namespaceId, knowledgeBase.id],
        set: {
          description: values.description,
          tags: values.tags,
          metadata: values.metadata,
          createdAt: values.createdAt,
          updatedAt: values.updatedAt,
        },
      });
  }

  async deleteBase(key: KnowledgeBaseKey): Promise<void> {
    await this.db.transaction(async (tx) => {
      await tx
        .delete(knowledgeBaseSource)
        .where(
          and(
            eq(knowledgeBaseSource.namespaceId, key.namespaceId),
            eq(knowledgeBaseSource.baseId, key.id),
          ),
        );
      await tx
        .delete(knowledgeBase)
        .where(and(eq(knowledgeBase.namespaceId, key.namespaceId), eq(knowledgeBase.id, key.id)));
    });
  }

  async saveSource(params: {
    namespaceId: string;
    baseId: string;
    source: KnowledgeSourceRecord;
  }): Promise<void> {
    const values = sourceValues(params);
    const { namespaceId: _ns, baseId: _baseId, sourceId: _sourceId, ...set } = values;
    await this.db
      .insert(knowledgeBaseSource)
      .values(values)
      .onConflictDoUpdate({
        target: [
          knowledgeBaseSource.namespaceId,
          knowledgeBaseSource.baseId,
          knowledgeBaseSource.sourceId,
        ],
        set,
      });
  }

  async deleteSource(key: KnowledgeSourceKey): Promise<void> {
    await this.db.delete(knowledgeBaseSource).where(sourceWhere(key));
  }

  async claimSource(params: {
    workerId: string;
    staleAfterMs: number;
  }): Promise<ClaimedKnowledgeSource | null> {
    const t = knowledgeBaseSource;
    const [row] = await this.db
      .update(t)
      .set({ status: "ingesting", claimedBy: params.workerId, claimedAt: DB_NOW_MS })
      .where(
        sql`(${t.namespaceId}, ${t.baseId}, ${t.sourceId}) = (
          SELECT namespace_id, base_id, source_id FROM knowledge_base_source
          WHERE status = 'pending'
             OR (status = 'ingesting' AND claimed_at < ${DB_NOW_MS} - ${params.staleAfterMs})
          ORDER BY updated_at, namespace_id, base_id, source_id
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        )`,
      )
      .returning();
    if (!row) return null;
    return { namespaceId: row.namespaceId, baseId: row.baseId, source: toSource(row) };
  }

  async completeSource(params: {
    namespaceId: string;
    baseId: string;
    workerId: string;
    source: KnowledgeSourceRecord;
  }): Promise<boolean> {
    const values = sourceValues(params);
    const { namespaceId: _ns, baseId: _baseId, sourceId: _sourceId, ...set } = values;
    const rows = await this.db
      .update(knowledgeBaseSource)
      .set(set)
      .where(
        and(
          sourceWhere({
            namespaceId: params.namespaceId,
            baseId: params.baseId,
            sourceId: params.source.id,
          }),
          eq(knowledgeBaseSource.status, "ingesting"),
          eq(knowledgeBaseSource.claimedBy, params.workerId),
        ),
      )
      .returning({ sourceId: knowledgeBaseSource.sourceId });
    return rows.length > 0;
  }

  async heartbeatClaim(params: KnowledgeSourceKey & { workerId: string }): Promise<boolean> {
    // The claim age is measured on the database clock, like the claim itself.
    const rows = await this.db
      .update(knowledgeBaseSource)
      .set({ claimedAt: DB_NOW_MS })
      .where(
        and(
          sourceWhere(params),
          eq(knowledgeBaseSource.status, "ingesting"),
          eq(knowledgeBaseSource.claimedBy, params.workerId),
        ),
      )
      .returning({ sourceId: knowledgeBaseSource.sourceId });
    return rows.length > 0;
  }

  async releaseClaims(params: { workerId: string }): Promise<number> {
    const rows = await this.db
      .update(knowledgeBaseSource)
      .set({ status: "pending", claimedBy: null, claimedAt: null })
      .where(
        and(
          eq(knowledgeBaseSource.status, "ingesting"),
          eq(knowledgeBaseSource.claimedBy, params.workerId),
        ),
      )
      .returning({ sourceId: knowledgeBaseSource.sourceId });
    return rows.length;
  }
}

function sourceWhere(key: KnowledgeSourceKey) {
  return and(
    eq(knowledgeBaseSource.namespaceId, key.namespaceId),
    eq(knowledgeBaseSource.baseId, key.baseId),
    eq(knowledgeBaseSource.sourceId, key.sourceId),
  );
}

/** Row values of a source write; any claim is dropped. */
function sourceValues(params: {
  namespaceId: string;
  baseId: string;
  source: KnowledgeSourceRecord;
}): typeof knowledgeBaseSource.$inferInsert & {
  namespaceId: string;
  baseId: string;
  sourceId: string;
} {
  const { namespaceId, baseId, source } = params;
  return {
    namespaceId,
    baseId,
    sourceId: source.id,
    title: source.title ?? null,
    uri: source.uri ?? null,
    mimeType: source.mimeType ?? null,
    tags: [...source.tags],
    metadata: { ...source.metadata },
    text: source.text,
    status: source.status,
    error: source.error ?? null,
    chunkCount: source.chunkCount,
    createdAt: source.createdAt,
    updatedAt: source.updatedAt,
    claimedBy: null,
    claimedAt: null,
  };
}

function toBase(row: BaseRow): KnowledgeBaseRecord {
  return {
    namespaceId: row.namespaceId,
    id: row.id,
    ...(row.description !== null && { description: row.description }),
    tags: row.tags,
    metadata: row.metadata,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function toSummary(row: SummaryRow): KnowledgeSourceSummary {
  return {
    id: row.sourceId,
    ...(row.title !== null && { title: row.title }),
    ...(row.uri !== null && { uri: row.uri }),
    ...(row.mimeType !== null && { mimeType: row.mimeType }),
    tags: row.tags,
    metadata: row.metadata,
    status: row.status,
    ...(row.error !== null && { error: row.error }),
    chunkCount: row.chunkCount,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function toSource(row: SourceRow): KnowledgeSourceRecord {
  return { ...toSummary(row), text: row.text };
}
