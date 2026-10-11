// ---------------------------------------------------------------------------
// PostgresKnowledgeBaseStore — `KnowledgeBaseStore` over the `knowledge_base`
// and `knowledge_base_source` tables. Mirrors `SqliteKnowledgeBaseStore`.
// ---------------------------------------------------------------------------

import { and, asc, eq } from "drizzle-orm";
import type {
  KnowledgeBaseRecord,
  KnowledgeBaseStore,
  KnowledgeSourceRecord,
  StoredKnowledgeBase,
} from "@promin/agent";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { knowledgeBase, knowledgeBaseSource } from "../schema.ts";

export interface PostgresKnowledgeBaseStoreConfig {
  readonly db: DrizzleDb;
}

type BaseRow = typeof knowledgeBase.$inferSelect;
type SourceRow = typeof knowledgeBaseSource.$inferSelect;

export class PostgresKnowledgeBaseStore implements KnowledgeBaseStore {
  private readonly db: DrizzleDb;

  constructor(config: PostgresKnowledgeBaseStoreConfig) {
    this.db = config.db;
  }

  async loadAll(): Promise<ReadonlyArray<StoredKnowledgeBase>> {
    const bases = await this.db.select().from(knowledgeBase).orderBy(asc(knowledgeBase.id));
    const sources = await this.db
      .select()
      .from(knowledgeBaseSource)
      .orderBy(asc(knowledgeBaseSource.baseId), asc(knowledgeBaseSource.sourceId));
    const byBase = new Map<string, KnowledgeSourceRecord[]>();
    for (const row of sources) {
      const list = byBase.get(row.baseId) ?? [];
      list.push(toSource(row));
      byBase.set(row.baseId, list);
    }
    return bases.map((row) => ({ base: toBase(row), sources: byBase.get(row.id) ?? [] }));
  }

  async saveBase(base: KnowledgeBaseRecord): Promise<void> {
    const values = {
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
        target: knowledgeBase.id,
        set: {
          description: values.description,
          tags: values.tags,
          metadata: values.metadata,
          createdAt: values.createdAt,
          updatedAt: values.updatedAt,
        },
      });
  }

  async deleteBase(id: string): Promise<void> {
    await this.db.transaction(async (tx) => {
      await tx.delete(knowledgeBaseSource).where(eq(knowledgeBaseSource.baseId, id));
      await tx.delete(knowledgeBase).where(eq(knowledgeBase.id, id));
    });
  }

  async saveSource(params: { baseId: string; source: KnowledgeSourceRecord }): Promise<void> {
    const { baseId, source } = params;
    const values = {
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
    };
    const { baseId: _baseId, sourceId: _sourceId, ...set } = values;
    await this.db
      .insert(knowledgeBaseSource)
      .values(values)
      .onConflictDoUpdate({
        target: [knowledgeBaseSource.baseId, knowledgeBaseSource.sourceId],
        set,
      });
  }

  async deleteSource(params: { baseId: string; sourceId: string }): Promise<void> {
    await this.db
      .delete(knowledgeBaseSource)
      .where(
        and(
          eq(knowledgeBaseSource.baseId, params.baseId),
          eq(knowledgeBaseSource.sourceId, params.sourceId),
        ),
      );
  }
}

function toBase(row: BaseRow): KnowledgeBaseRecord {
  return {
    id: row.id,
    ...(row.description !== null && { description: row.description }),
    tags: row.tags,
    metadata: row.metadata,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function toSource(row: SourceRow): KnowledgeSourceRecord {
  return {
    id: row.sourceId,
    ...(row.title !== null && { title: row.title }),
    ...(row.uri !== null && { uri: row.uri }),
    ...(row.mimeType !== null && { mimeType: row.mimeType }),
    tags: row.tags,
    metadata: row.metadata,
    text: row.text,
    status: row.status,
    ...(row.error !== null && { error: row.error }),
    chunkCount: row.chunkCount,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}
