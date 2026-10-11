// ---------------------------------------------------------------------------
// KnowledgeBaseStore — durable rows behind managed knowledge bases.
//
// A managed knowledge base is a named retriever that operators fill with
// sources (pasted text, uploaded files, imported URLs). The store keeps the
// base's metadata and every source's text and ingestion outcome, so a host
// can rebuild an in-memory index at boot and recipes that bind the base by
// id keep resolving across restarts. The live `Retriever` is not stored.
//
// Implementations: `InMemoryKnowledgeBaseStore` (here), `SqliteKnowledgeBaseStore`
// (@promin/sqlite) and `PostgresKnowledgeBaseStore` (@promin/postgres). All
// pass `knowledgeBaseStoreTestSuite`.
// ---------------------------------------------------------------------------

/** A knowledge base's own row. Source counts and status derive from its sources. */
export interface KnowledgeBaseRecord {
  readonly id: string;
  readonly description?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
  /** Epoch ms. */
  readonly createdAt: number;
  /** Epoch ms. */
  readonly updatedAt: number;
}

/** `ready`: indexed. `failed`: the last ingestion of this source threw; see `error`. */
export type KnowledgeSourceStatus = "ready" | "failed";

/** One source of a knowledge base: its text and how its last ingestion went. */
export interface KnowledgeSourceRecord {
  readonly id: string;
  readonly title?: string;
  readonly uri?: string;
  readonly mimeType?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
  /** Full source text, kept so an index can be rebuilt and chunks inspected. */
  readonly text: string;
  readonly status: KnowledgeSourceStatus;
  /** Why the last ingestion failed. Present only when `status` is `failed`. */
  readonly error?: string;
  /** Chunks the retriever stored for this source. 0 when it failed. */
  readonly chunkCount: number;
  /** Epoch ms. */
  readonly createdAt: number;
  /** Epoch ms. */
  readonly updatedAt: number;
}

export interface StoredKnowledgeBase {
  readonly base: KnowledgeBaseRecord;
  /** Ordered by source id. */
  readonly sources: ReadonlyArray<KnowledgeSourceRecord>;
}

export interface KnowledgeBaseStore {
  /** Every base with its sources, ordered by base id. Called once at boot. */
  loadAll(): Promise<ReadonlyArray<StoredKnowledgeBase>>;
  /** Create or replace a base row. Its sources are untouched. */
  saveBase(base: KnowledgeBaseRecord): Promise<void>;
  /** Remove a base and all its sources. No-op when absent. */
  deleteBase(id: string): Promise<void>;
  /** Create or replace a source of `baseId`. The base row is saved first. */
  saveSource(params: { baseId: string; source: KnowledgeSourceRecord }): Promise<void>;
  /** Remove one source. No-op when absent. */
  deleteSource(params: { baseId: string; sourceId: string }): Promise<void>;
}

/** Reference implementation for tests and hosts without durable storage. */
export class InMemoryKnowledgeBaseStore implements KnowledgeBaseStore {
  private readonly bases = new Map<string, KnowledgeBaseRecord>();
  private readonly sources = new Map<string, Map<string, KnowledgeSourceRecord>>();

  async loadAll(): Promise<ReadonlyArray<StoredKnowledgeBase>> {
    return [...this.bases.values()]
      .sort((a, b) => a.id.localeCompare(b.id))
      .map((base) => ({
        base: structuredClone(base),
        sources: [...(this.sources.get(base.id)?.values() ?? [])]
          .sort((a, b) => a.id.localeCompare(b.id))
          .map((source) => structuredClone(source)),
      }));
  }

  async saveBase(base: KnowledgeBaseRecord): Promise<void> {
    this.bases.set(base.id, structuredClone(base));
  }

  async deleteBase(id: string): Promise<void> {
    this.bases.delete(id);
    this.sources.delete(id);
  }

  async saveSource(params: { baseId: string; source: KnowledgeSourceRecord }): Promise<void> {
    let sources = this.sources.get(params.baseId);
    if (!sources) {
      sources = new Map();
      this.sources.set(params.baseId, sources);
    }
    sources.set(params.source.id, structuredClone(params.source));
  }

  async deleteSource(params: { baseId: string; sourceId: string }): Promise<void> {
    this.sources.get(params.baseId)?.delete(params.sourceId);
  }
}
