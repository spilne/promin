// ---------------------------------------------------------------------------
// KnowledgeBaseStore — durable rows behind managed knowledge bases.
//
// A managed knowledge base is a named retriever that operators fill with
// sources (pasted text, uploaded files, imported URLs). Bases live in a
// namespace: ids are unique per namespace, and an agent binds the bases of
// the namespace it runs in. The store keeps each base's metadata and every
// source's text and ingestion state, so every process sharing it can build
// its index from the rows. The live `Retriever` is not stored.
//
// Ingestion is a queue over the source rows: writing a source with status
// `pending` enqueues it, `claimSource` hands it to one worker (`ingesting`),
// and `completeSource` records the outcome while that worker still holds
// the claim. A claim older than `staleAfterMs` (its worker died) is claimed
// again; the claim age is measured on the store's own clock (the database
// server's for shared stores), never compared with a caller's clock.
//
// Implementations: `InMemoryKnowledgeBaseStore` (here), `SqliteKnowledgeBaseStore`
// (@promin/sqlite) and `PostgresKnowledgeBaseStore` (@promin/postgres). All
// pass `knowledgeBaseStoreTestSuite`.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";

/** Identity of a knowledge base: its id within a namespace. */
export interface KnowledgeBaseKey {
  readonly namespaceId: string;
  readonly id: string;
}

/** Identity of one source of a knowledge base. */
export interface KnowledgeSourceKey {
  readonly namespaceId: string;
  readonly baseId: string;
  readonly sourceId: string;
}

/** A knowledge base's own row. Source counts and status derive from its sources. */
export interface KnowledgeBaseRecord {
  readonly namespaceId: string;
  readonly id: string;
  readonly description?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
  /** Epoch ms. */
  readonly createdAt: number;
  /** Epoch ms. */
  readonly updatedAt: number;
}

/**
 * `pending`: queued for ingestion. `ingesting`: a worker holds it.
 * `ready`: indexed. `failed`: the last ingestion threw; see `error`.
 */
export type KnowledgeSourceStatus = "pending" | "ingesting" | "ready" | "failed";

/** One source of a knowledge base: its text and its ingestion state. */
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
  /** Chunks the retriever stored for this source at its last ingestion. 0 when it failed. */
  readonly chunkCount: number;
  /** Epoch ms. */
  readonly createdAt: number;
  /** Epoch ms; changes on every write of the source (not on a claim). */
  readonly updatedAt: number;
}

/** A source without its text, as listings return it. */
export type KnowledgeSourceSummary = Omit<KnowledgeSourceRecord, "text">;

export interface StoredKnowledgeBase {
  readonly base: KnowledgeBaseRecord;
  /** Ordered by source id; without their text (see `getSource`). */
  readonly sources: ReadonlyArray<KnowledgeSourceSummary>;
}

/** A source a worker claimed for ingestion. */
export interface ClaimedKnowledgeSource {
  readonly namespaceId: string;
  readonly baseId: string;
  /** The claimed row, status `ingesting`. */
  readonly source: KnowledgeSourceRecord;
}

export interface KnowledgeBaseStore {
  /** Every base with its sources, ordered by namespace then base id. */
  listBases(): Promise<ReadonlyArray<StoredKnowledgeBase>>;
  getBase(key: KnowledgeBaseKey): Promise<StoredKnowledgeBase | null>;
  /** One source with its text. */
  getSource(key: KnowledgeSourceKey): Promise<KnowledgeSourceRecord | null>;
  /** Create or replace a base row. Its sources are untouched. */
  saveBase(base: KnowledgeBaseRecord): Promise<void>;
  /** Remove a base and all its sources. No-op when absent. */
  deleteBase(key: KnowledgeBaseKey): Promise<void>;
  /**
   * Create or replace a source of a base, dropping any ingestion claim on
   * it. Writing status `pending` enqueues it.
   */
  saveSource(params: {
    namespaceId: string;
    baseId: string;
    source: KnowledgeSourceRecord;
  }): Promise<void>;
  /** Remove one source. No-op when absent. */
  deleteSource(key: KnowledgeSourceKey): Promise<void>;
  /**
   * Claim one source for `workerId`: the least recently written `pending`
   * source, or an `ingesting` one whose claim is older than `staleAfterMs`.
   * Atomic: two callers never claim the same source. The claimed row turns
   * `ingesting`. `null` when nothing is claimable.
   */
  claimSource(params: {
    workerId: string;
    staleAfterMs: number;
  }): Promise<ClaimedKnowledgeSource | null>;
  /**
   * Write an ingestion outcome (`ready` or `failed`) while `workerId` still
   * holds the claim on the source. `false`, and nothing written, when the
   * source was rewritten, deleted or claimed by another worker meanwhile.
   */
  completeSource(params: {
    namespaceId: string;
    baseId: string;
    workerId: string;
    source: KnowledgeSourceRecord;
  }): Promise<boolean>;
  /**
   * Renew `workerId`'s claim on a source it is still ingesting: its claim
   * age restarts (on the store's clock), so a long ingestion is not taken
   * over as stale. `false` when the claim is gone (rewritten, deleted,
   * reclaimed).
   */
  heartbeatClaim(params: KnowledgeSourceKey & { workerId: string }): Promise<boolean>;
  /**
   * Re-queue (`pending`) every source `workerId` holds a claim on. A worker
   * restarting under the same id calls it so its interrupted ingestions
   * resume at once. Returns how many were released.
   */
  releaseClaims(params: { workerId: string }): Promise<number>;
}

export interface InMemoryKnowledgeBaseStoreConfig {
  /** Time source of claim ages. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

interface InMemorySource {
  record: KnowledgeSourceRecord;
  claim?: { workerId: string; at: number };
}

/** Reference implementation for tests and hosts without durable storage. */
export class InMemoryKnowledgeBaseStore implements KnowledgeBaseStore {
  private readonly clock: WallClock;
  private readonly bases = new Map<string, KnowledgeBaseRecord>();
  private readonly sources = new Map<string, Map<string, InMemorySource>>();

  constructor(config: InMemoryKnowledgeBaseStoreConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
  }

  async listBases(): Promise<ReadonlyArray<StoredKnowledgeBase>> {
    return [...this.bases.values()]
      .sort((a, b) => compareBaseKeys(a, b))
      .map((base) => this.stored(base));
  }

  async getBase(key: KnowledgeBaseKey): Promise<StoredKnowledgeBase | null> {
    const base = this.bases.get(baseKey(key));
    return base ? this.stored(base) : null;
  }

  async getSource(key: KnowledgeSourceKey): Promise<KnowledgeSourceRecord | null> {
    const entry = this.sources.get(baseKey({ namespaceId: key.namespaceId, id: key.baseId }));
    const source = entry?.get(key.sourceId);
    return source ? structuredClone(source.record) : null;
  }

  async saveBase(base: KnowledgeBaseRecord): Promise<void> {
    this.bases.set(baseKey(base), structuredClone(base));
  }

  async deleteBase(key: KnowledgeBaseKey): Promise<void> {
    this.bases.delete(baseKey(key));
    this.sources.delete(baseKey(key));
  }

  async saveSource(params: {
    namespaceId: string;
    baseId: string;
    source: KnowledgeSourceRecord;
  }): Promise<void> {
    const key = baseKey({ namespaceId: params.namespaceId, id: params.baseId });
    let sources = this.sources.get(key);
    if (!sources) {
      sources = new Map();
      this.sources.set(key, sources);
    }
    sources.set(params.source.id, { record: structuredClone(params.source) });
  }

  async deleteSource(key: KnowledgeSourceKey): Promise<void> {
    this.sources
      .get(baseKey({ namespaceId: key.namespaceId, id: key.baseId }))
      ?.delete(key.sourceId);
  }

  async claimSource(params: {
    workerId: string;
    staleAfterMs: number;
  }): Promise<ClaimedKnowledgeSource | null> {
    const now = this.clock.currentTimeMs();
    let best: { key: string; entry: InMemorySource } | undefined;
    for (const [key, sources] of this.sources) {
      for (const entry of sources.values()) {
        const claimable =
          entry.record.status === "pending" ||
          (entry.record.status === "ingesting" &&
            (entry.claim === undefined || entry.claim.at < now - params.staleAfterMs));
        if (!claimable) continue;
        if (!best || entry.record.updatedAt < best.entry.record.updatedAt) best = { key, entry };
      }
    }
    if (!best) return null;
    best.entry.record = { ...best.entry.record, status: "ingesting" };
    best.entry.claim = { workerId: params.workerId, at: now };
    const { namespaceId, id } = parseBaseKey(best.key);
    return { namespaceId, baseId: id, source: structuredClone(best.entry.record) };
  }

  async completeSource(params: {
    namespaceId: string;
    baseId: string;
    workerId: string;
    source: KnowledgeSourceRecord;
  }): Promise<boolean> {
    const sources = this.sources.get(
      baseKey({ namespaceId: params.namespaceId, id: params.baseId }),
    );
    const entry = sources?.get(params.source.id);
    if (
      !entry ||
      entry.record.status !== "ingesting" ||
      entry.claim?.workerId !== params.workerId
    ) {
      return false;
    }
    sources!.set(params.source.id, { record: structuredClone(params.source) });
    return true;
  }

  async heartbeatClaim(params: KnowledgeSourceKey & { workerId: string }): Promise<boolean> {
    const entry = this.sources
      .get(baseKey({ namespaceId: params.namespaceId, id: params.baseId }))
      ?.get(params.sourceId);
    if (
      !entry ||
      entry.record.status !== "ingesting" ||
      entry.claim?.workerId !== params.workerId
    ) {
      return false;
    }
    entry.claim = { workerId: params.workerId, at: this.clock.currentTimeMs() };
    return true;
  }

  async releaseClaims(params: { workerId: string }): Promise<number> {
    let released = 0;
    for (const sources of this.sources.values()) {
      for (const entry of sources.values()) {
        if (entry.record.status !== "ingesting" || entry.claim?.workerId !== params.workerId) {
          continue;
        }
        entry.record = { ...entry.record, status: "pending" };
        delete entry.claim;
        released += 1;
      }
    }
    return released;
  }

  private stored(base: KnowledgeBaseRecord): StoredKnowledgeBase {
    return {
      base: structuredClone(base),
      sources: [...(this.sources.get(baseKey(base))?.values() ?? [])]
        .map((entry) => entry.record)
        .sort((a, b) => compareIds(a.id, b.id))
        .map(({ text: _text, ...summary }) => structuredClone(summary)),
    };
  }
}

function baseKey(key: KnowledgeBaseKey): string {
  return JSON.stringify([key.namespaceId, key.id]);
}

function parseBaseKey(key: string): KnowledgeBaseKey {
  const [namespaceId, id] = JSON.parse(key) as [string, string];
  return { namespaceId, id };
}

function compareBaseKeys(a: KnowledgeBaseKey, b: KnowledgeBaseKey): number {
  return compareIds(a.namespaceId, b.namespaceId) || compareIds(a.id, b.id);
}

/** Code-unit order, matching `ORDER BY` under the C collation of the SQL stores. */
function compareIds(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}
