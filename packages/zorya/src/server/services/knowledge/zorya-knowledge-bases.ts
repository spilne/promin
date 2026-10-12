// ---------------------------------------------------------------------------
// ZoryaKnowledgeBases — managed knowledge bases: named retrievers operators
// create and fill with sources from the dashboard, which agents then bind by
// id in `backend.knowledge`.
//
// Bases live in a namespace: ids are unique per namespace, and an agent
// running in a namespace binds that namespace's bases (plus any global,
// host-wired retriever). The service owns three things per base:
//   - its durable rows in a `KnowledgeBaseStore` (metadata plus every
//     source's text and ingestion state),
//   - a live `IngestibleRetriever` built by the retriever factory, and
//   - that retriever's row in a `RetrieverRegistry`, under the base's
//     namespace, which is the registry `ZoryaAgents.retrievers` and
//     `resolveLocalAgent({ retrievers })` read.
//
// Ingestion runs in the background. Adding or re-indexing a source writes it
// as `pending` and returns; a worker loop claims pending sources from the
// store (at most `ingestion.concurrency` at a time per process) and records
// each outcome, `ready` or `failed` with the reason. The queue is the store's
// rows, so it survives a restart: pending sources are picked up again, and a
// source whose worker died is claimed again once its claim is stale.
//
// Several processes may share one store. The store is the source of truth:
// a base missing locally is re-read from the store before answering
// "not found", reads re-read the base they show, and every
// `refreshIntervalMs` the whole store is re-read so bases, sources and
// deletions made elsewhere reach this process's registry. The default
// factory builds an `InMemoryRetriever`, which each process fills from the
// stored text of ready sources; a durable factory (e.g. one
// `PgVectorRetriever` table per base) shares its chunks and is not
// replayed. Writes to one base run one at a time in a process; different
// bases proceed in parallel. An ingestion holds that lock only to record its
// outcome, never while it embeds, and renews its claim while it works.
//
// Chunks carry their source version (`KnowledgeSource.version`): a new
// version is indexed next to the old one, which stays searchable until the
// outcome is recorded; an ingestion whose outcome the store refuses (the
// source was rewritten or removed meanwhile, maybe on another replica)
// deletes exactly the chunks it wrote.
// ---------------------------------------------------------------------------

import { createHash, randomUUID } from "node:crypto";
import {
  FlatTextChunker,
  InMemoryKnowledgeBaseStore,
  InMemoryRetriever,
  InMemoryRetrieverRegistry,
  type ClaimedKnowledgeSource,
  type EmbeddingProvider,
  type IngestibleRetriever,
  type KnowledgeBaseKey,
  type KnowledgeBaseRecord,
  type KnowledgeBaseStore,
  type KnowledgeChunk,
  type KnowledgeChunker,
  type KnowledgeIngestDocument,
  type KnowledgeSourceRecord,
  type KnowledgeSourceSummary,
  type RetrieveRequest,
  type RetrieveResult,
  type RetrieverRegistry,
  type StoredKnowledgeBase,
} from "@promin/agent";
import { SystemWallClock, type TimerHandle, type WallClock } from "@promin/workflow";
import {
  KnowledgeBaseExistsError,
  KnowledgeBaseNotFoundError,
  KnowledgeInputError,
  KnowledgeSourceNotFoundError,
  errorMessage,
} from "./errors.ts";

export type { KnowledgeSourceSummary } from "@promin/agent";

/** Knowledge base ids: what recipes put in `backend.knowledge[].id`. */
export const KNOWLEDGE_BASE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

/** Longest namespace id accepted. */
export const MAX_KNOWLEDGE_NAMESPACE_LENGTH = 256;

/** Longest source id accepted (file paths and URLs make long ids). */
export const MAX_KNOWLEDGE_SOURCE_ID_LENGTH = 512;

/** Default cap on one source's text, in characters. */
export const DEFAULT_MAX_SOURCE_CHARACTERS = 2_000_000;

/** Defaults of `ZoryaKnowledgeBasesConfig.ingestion` and `refreshIntervalMs`. */
export const KNOWLEDGE_INGESTION_DEFAULTS = {
  concurrency: 2,
  pollIntervalMs: 2_000,
  staleAfterMs: 10 * 60_000,
  refreshIntervalMs: 15_000,
} as const;

/** A knowledge base as the API returns it: its row plus counts derived from its sources. */
export interface KnowledgeBase extends KnowledgeBaseRecord {
  /**
   * `degraded` while any source's last ingestion failed, else `ingesting`
   * while any source waits for or is in ingestion, else `ready`.
   */
  readonly status: "ready" | "ingesting" | "degraded";
  readonly sourceCount: number;
  readonly chunkCount: number;
}

export interface KnowledgeBaseCreateInput {
  readonly namespaceId: string;
  readonly id: string;
  readonly description?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface KnowledgeBaseUpdateInput {
  /** `null` clears it. */
  readonly description?: string | null;
  readonly tags?: ReadonlyArray<string>;
  /** Replaces the whole metadata object. */
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/** One document to ingest. Re-ingesting an id replaces that source and its chunks. */
export interface KnowledgeSourceInput {
  readonly id: string;
  readonly text: string;
  readonly title?: string;
  readonly uri?: string;
  readonly mimeType?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/** A base created at `start()` when the store does not have it yet. */
export interface KnowledgeBaseSeed extends KnowledgeBaseCreateInput {
  /** Queued for ingestion when the base is created. */
  readonly documents?: ReadonlyArray<KnowledgeSourceInput>;
}

/**
 * Loads documents from somewhere other than the request body: a URL, a file
 * under a configured root, a CMS. `kind` names it in
 * `POST /api/knowledge-bases/:id/sources/import`; `config` is the request's
 * untrusted `config` field, which the adapter validates.
 */
export interface KnowledgeSourceAdapter {
  readonly kind: string;
  load(params: { config: unknown }): Promise<ReadonlyArray<KnowledgeSourceInput>>;
}

/** Builds the live retriever behind each base. */
export interface KnowledgeRetrieverFactory {
  create(params: {
    base: KnowledgeBaseRecord;
    /** The service's chunker; chunk inspection uses it, so retrievers should too. */
    chunker: KnowledgeChunker;
  }): IngestibleRetriever | Promise<IngestibleRetriever>;
  /**
   * True when the retriever keeps its chunks across restarts and processes
   * (a pgvector table). False: each process indexes every ready source's
   * stored text itself.
   */
  readonly durable: boolean;
}

/** The background ingestion worker of one process. */
export interface KnowledgeIngestionConfig {
  /** Sources this process ingests at once. Default 2. */
  readonly concurrency?: number;
  /**
   * How often the worker looks for queued sources it was not told about
   * (queued by another process, or before a restart), in ms. Default 2,000.
   */
  readonly pollIntervalMs?: number;
  /**
   * A source claimed longer ago than this, still `ingesting`, is assumed
   * orphaned by a dead worker and claimed again. Default 10 minutes; keep
   * it above the longest ingestion.
   */
  readonly staleAfterMs?: number;
  /**
   * This worker's id. A process restarting under the same id resumes its
   * interrupted ingestions at once instead of after `staleAfterMs`.
   * Default: a random id per instance.
   */
  readonly workerId?: string;
}

export interface ZoryaKnowledgeBasesConfig {
  /**
   * Registry each base's retriever is registered in, under the base's
   * namespace and id. Pass the same registry as `ZoryaAgents.retrievers`
   * and to `resolveLocalAgent({ retrievers })`. Default: a new in-memory
   * registry (read it back from `.registry`).
   */
  registry?: RetrieverRegistry;
  /** Durable rows. Default: `InMemoryKnowledgeBaseStore` (lost on restart). */
  store?: KnowledgeBaseStore;
  /** Default: `FlatTextChunker`. */
  chunker?: KnowledgeChunker;
  /**
   * Embeddings for the default in-memory retriever (cosine ranking).
   * Without it, ranking is keyword overlap. Ignored with `retrievers`.
   */
  embeddings?: EmbeddingProvider;
  /** Build each base's retriever. Default: a non-durable `InMemoryRetriever`. */
  retrievers?: KnowledgeRetrieverFactory;
  /** Loaders behind the import route, by `kind`. Default: none. */
  sourceAdapters?: ReadonlyArray<KnowledgeSourceAdapter>;
  /** Bases created at `start()` unless the store already has them. */
  seed?: ReadonlyArray<KnowledgeBaseSeed>;
  /** Cap on one source's text, in characters. Default 2,000,000. */
  maxSourceCharacters?: number;
  /** Background ingestion worker. */
  ingestion?: KnowledgeIngestionConfig;
  /**
   * How often the whole store is re-read so bases and sources written by
   * other processes reach this one, in ms. Default 15,000; 0 turns the
   * periodic re-read off (a base missing locally is still re-read on use).
   */
  refreshIntervalMs?: number;
  /** Time source for timestamps, polling and refreshes. Default: `SystemWallClock`. */
  clock?: WallClock;
}

interface Runtime {
  base: KnowledgeBaseRecord;
  sources: Map<string, KnowledgeSourceSummary>;
  retriever: IngestibleRetriever;
  /** Non-durable retrievers: `updatedAt` of the source version indexed here. */
  indexed: Map<string, number>;
  /** Non-durable retrievers: source versions that failed to index here. */
  localFailures: Map<string, { updatedAt: number; error: string }>;
}

export class ZoryaKnowledgeBases {
  /**
   * The retriever registry to pass to `ZoryaAgents.retrievers` and
   * `resolveLocalAgent({ retrievers })`. Its `resolve` re-reads the store
   * when a base of the asked namespace is missing here, so a base created
   * on another replica binds without waiting for a refresh.
   */
  readonly registry: RetrieverRegistry;
  /** The registry rows live in (the configured registry). */
  private readonly rows: RetrieverRegistry;
  readonly store: KnowledgeBaseStore;
  readonly chunker: KnowledgeChunker;
  /** This process's ingestion worker id. */
  readonly workerId: string;

  private readonly retrievers: KnowledgeRetrieverFactory;
  private readonly adapters = new Map<string, KnowledgeSourceAdapter>();
  private readonly seed: ReadonlyArray<KnowledgeBaseSeed>;
  private readonly maxSourceCharacters: number;
  private readonly concurrency: number;
  private readonly pollIntervalMs: number;
  private readonly staleAfterMs: number;
  private readonly refreshIntervalMs: number;
  private readonly clock: WallClock;
  private readonly bases = new Map<string, Runtime>();
  /** Tail of each base's write queue. */
  private readonly locks = new Map<string, Promise<unknown>>();
  private readonly inFlight = new Set<Promise<void>>();
  private loading?: Promise<void>;
  private filling?: Promise<void>;
  private refill = false;
  private stopped = false;
  private pollTimer?: TimerHandle;
  private refreshTimer?: TimerHandle;

  constructor(config: ZoryaKnowledgeBasesConfig = {}) {
    this.rows = config.registry ?? new InMemoryRetrieverRegistry();
    this.registry = {
      register: (input) => this.rows.register(input),
      get: (key) => this.rows.get(key),
      list: (params) => this.rows.list(params),
      unregister: (key) => this.rows.unregister(key),
      resolve: async (key) => {
        const local = this.rows.get(key);
        if (local && (key.namespaceId === undefined || local.namespaceId === key.namespaceId)) {
          return local;
        }
        if (key.namespaceId !== undefined) {
          await this.ensure({ namespaceId: key.namespaceId, ids: [key.id] });
        }
        return this.rows.get(key);
      },
    };
    this.clock = config.clock ?? SystemWallClock;
    this.store = config.store ?? new InMemoryKnowledgeBaseStore({ clock: this.clock });
    this.chunker = config.chunker ?? new FlatTextChunker();
    const embeddings = config.embeddings;
    this.retrievers = config.retrievers ?? {
      durable: false,
      create: ({ chunker }) =>
        new InMemoryRetriever({ chunker, ...(embeddings !== undefined && { embeddings }) }),
    };
    for (const adapter of config.sourceAdapters ?? []) {
      if (this.adapters.has(adapter.kind)) {
        throw new Error(`ZoryaKnowledgeBases: duplicate source adapter kind "${adapter.kind}"`);
      }
      this.adapters.set(adapter.kind, adapter);
    }
    this.seed = config.seed ?? [];
    this.maxSourceCharacters = config.maxSourceCharacters ?? DEFAULT_MAX_SOURCE_CHARACTERS;
    const ingestion = config.ingestion ?? {};
    this.concurrency = positiveInteger({
      name: "ingestion.concurrency",
      value: ingestion.concurrency ?? KNOWLEDGE_INGESTION_DEFAULTS.concurrency,
    });
    this.pollIntervalMs = ingestion.pollIntervalMs ?? KNOWLEDGE_INGESTION_DEFAULTS.pollIntervalMs;
    this.staleAfterMs = ingestion.staleAfterMs ?? KNOWLEDGE_INGESTION_DEFAULTS.staleAfterMs;
    this.workerId = ingestion.workerId ?? `kb-worker-${randomUUID()}`;
    this.refreshIntervalMs =
      config.refreshIntervalMs ?? KNOWLEDGE_INGESTION_DEFAULTS.refreshIntervalMs;
  }

  /** Kinds the import route accepts, sorted. */
  sourceKinds(): string[] {
    return [...this.adapters.keys()].sort();
  }

  /**
   * Load every base from the store, build and register its retriever
   * (indexing stored text into non-durable ones), create missing seed
   * bases, resume this worker's interrupted ingestions, and start the
   * ingestion worker and the periodic re-read. Idempotent; a failed load is
   * retried by the next call. Every other method awaits it, so embedders
   * that skip `ZoryaServer` need not call it.
   */
  start(): Promise<void> {
    this.loading ??= this.load().catch((err: unknown) => {
      this.loading = undefined;
      throw err;
    });
    return this.loading;
  }

  /** Stop polling and re-reading, and wait for the ingestions in flight. */
  async stop(): Promise<void> {
    this.stopped = true;
    this.pollTimer?.clear();
    this.refreshTimer?.clear();
    await this.filling?.catch(() => {});
    await Promise.all(this.inFlight);
  }

  /**
   * Ingest every queued source this process can claim and wait until none
   * is left in flight. For tests and scripts that need sources indexed
   * before they go on.
   */
  async drain(): Promise<void> {
    await this.start();
    for (;;) {
      await this.fill();
      if (this.inFlight.size === 0) return;
      await Promise.all(this.inFlight);
    }
  }

  /**
   * Re-read the whole store and bring this process's bases, retrievers and
   * registry rows in line with it. Runs every `refreshIntervalMs`.
   */
  async refresh(): Promise<void> {
    await this.readAll({ awaitIndexing: true });
  }

  /**
   * Make sure the given bases of a namespace are loaded from the store, so
   * `registry.get` finds them. `registry.resolve` (what `resolveLocalAgent`
   * calls) does this itself on a miss; ids the store does not have are
   * skipped.
   */
  async ensure(params: { namespaceId: string; ids: ReadonlyArray<string> }): Promise<void> {
    await this.start();
    for (const id of params.ids) {
      if (!KNOWLEDGE_BASE_ID_PATTERN.test(id)) continue;
      const key = { namespaceId: params.namespaceId, id };
      await this.withLock(lockKey(key), () => this.find(key));
    }
  }

  /** Every base of a namespace, as the store has them now. */
  async list(params: { namespaceId: string }): Promise<KnowledgeBase[]> {
    await this.start();
    await this.readAll({ awaitIndexing: false });
    return [...this.bases.values()]
      .filter((runtime) => runtime.base.namespaceId === params.namespaceId)
      .sort((a, b) => a.base.id.localeCompare(b.base.id))
      .map((runtime) => view(runtime));
  }

  async get(key: KnowledgeBaseKey): Promise<KnowledgeBase | null> {
    await this.start();
    const runtime = await this.reread(key);
    return runtime ? view(runtime) : null;
  }

  async create(input: KnowledgeBaseCreateInput): Promise<KnowledgeBase> {
    await this.start();
    return this.withLock(lockKey(input), () => this.createLocked(input));
  }

  async update(params: {
    namespaceId: string;
    id: string;
    patch: KnowledgeBaseUpdateInput;
  }): Promise<KnowledgeBase> {
    await this.start();
    return this.withLock(lockKey(params), async () => {
      const runtime = await this.require(params);
      const { patch } = params;
      const { description: _previous, ...rest } = runtime.base;
      const description =
        patch.description === undefined ? runtime.base.description : patch.description;
      const base: KnowledgeBaseRecord = {
        ...rest,
        ...(description !== null && description !== undefined && description !== ""
          ? { description }
          : {}),
        tags: patch.tags !== undefined ? normalizeTags(patch.tags) : runtime.base.tags,
        metadata: patch.metadata !== undefined ? { ...patch.metadata } : runtime.base.metadata,
        updatedAt: this.clock.currentTimeMs(),
      };
      await this.store.saveBase(base);
      runtime.base = base;
      this.register(runtime);
      return view(runtime);
    });
  }

  /** Drop a base: its chunks, its rows and its registry entry. */
  async remove(key: KnowledgeBaseKey): Promise<void> {
    await this.start();
    await this.withLock(lockKey(key), async () => {
      const runtime = await this.require(key);
      for (const sourceId of runtime.sources.keys()) {
        await runtime.retriever.deleteSource({ sourceId });
      }
      await this.store.deleteBase(key);
      this.drop(runtime);
    });
  }

  async listSources(params: {
    namespaceId: string;
    baseId: string;
  }): Promise<KnowledgeSourceSummary[]> {
    await this.start();
    const key = { namespaceId: params.namespaceId, id: params.baseId };
    const runtime = await this.reread(key);
    if (!runtime) throw new KnowledgeBaseNotFoundError(key);
    return [...runtime.sources.values()].sort((a, b) => a.id.localeCompare(b.id));
  }

  /**
   * Queue one source for ingestion, replacing any source with the same id
   * once it is indexed (until then the previous version stays searchable).
   * Returns the queued source, status `pending`; its outcome shows in
   * `listSources`.
   */
  async ingest(params: {
    namespaceId: string;
    baseId: string;
    source: KnowledgeSourceInput;
  }): Promise<KnowledgeSourceSummary> {
    await this.start();
    const source = this.checkSource(params.source);
    const key = { namespaceId: params.namespaceId, id: params.baseId };
    const queued = await this.withLock(lockKey(key), async () =>
      this.enqueueLocked({ runtime: await this.require(key), source }),
    );
    this.kick();
    return queued;
  }

  /** Queue a stored source for ingestion again from its stored text (e.g. after a failure). */
  async reindexSource(params: {
    namespaceId: string;
    baseId: string;
    sourceId: string;
  }): Promise<KnowledgeSourceSummary> {
    await this.start();
    const key = { namespaceId: params.namespaceId, id: params.baseId };
    const queued = await this.withLock(lockKey(key), async () => {
      const runtime = await this.require(key);
      const stored = await this.store.getSource(params);
      if (!stored) throw new KnowledgeSourceNotFoundError(params);
      return this.enqueueLocked({ runtime, source: toDocument(stored) });
    });
    this.kick();
    return queued;
  }

  /**
   * Load documents through the adapter registered for `kind` and queue them
   * for ingestion. Loading happens here; a document the adapter returns
   * that fails validation refuses the whole import before anything is
   * queued.
   */
  async importSources(params: {
    namespaceId: string;
    baseId: string;
    kind: string;
    config: unknown;
  }): Promise<KnowledgeSourceSummary[]> {
    await this.start();
    const key = { namespaceId: params.namespaceId, id: params.baseId };
    await this.withLock(lockKey(key), () => this.require(key));
    const adapter = this.adapters.get(params.kind);
    if (!adapter) {
      throw new KnowledgeInputError({
        code: "unknown_source_kind",
        message: `No source adapter for kind "${params.kind}". Known kinds: ${this.sourceKinds().join(", ") || "none"}.`,
      });
    }
    const documents = (await adapter.load({ config: params.config })).map((d) =>
      this.checkSource(d),
    );
    const queued = await this.withLock(lockKey(key), async () => {
      const runtime = await this.require(key);
      const out: KnowledgeSourceSummary[] = [];
      for (const source of documents) out.push(await this.enqueueLocked({ runtime, source }));
      return out;
    });
    this.kick();
    return queued;
  }

  async removeSource(params: {
    namespaceId: string;
    baseId: string;
    sourceId: string;
  }): Promise<void> {
    await this.start();
    const key = { namespaceId: params.namespaceId, id: params.baseId };
    await this.withLock(lockKey(key), async () => {
      const runtime = await this.require(key);
      if (!runtime.sources.has(params.sourceId)) {
        const stored = await this.store.getSource(params);
        if (!stored) throw new KnowledgeSourceNotFoundError(params);
      }
      await runtime.retriever.deleteSource({ sourceId: params.sourceId });
      await this.store.deleteSource(params);
      runtime.sources.delete(params.sourceId);
      runtime.indexed.delete(params.sourceId);
      runtime.localFailures.delete(params.sourceId);
      await this.touch(runtime);
    });
  }

  /**
   * The chunks of ready sources (or of one source), as the service's chunker
   * splits them, ordered by source id then chunk index.
   */
  async chunks(params: {
    namespaceId: string;
    baseId: string;
    sourceId?: string;
    limit?: number;
    offset?: number;
  }): Promise<{ chunks: KnowledgeChunk[]; total: number }> {
    await this.start();
    const key = { namespaceId: params.namespaceId, id: params.baseId };
    const runtime = await this.reread(key);
    if (!runtime) throw new KnowledgeBaseNotFoundError(key);
    let sources = [...runtime.sources.values()].sort((a, b) => a.id.localeCompare(b.id));
    if (params.sourceId !== undefined) {
      sources = sources.filter((s) => s.id === params.sourceId);
      if (sources.length === 0) {
        throw new KnowledgeSourceNotFoundError({
          baseId: params.baseId,
          sourceId: params.sourceId,
        });
      }
    }
    const all: KnowledgeChunk[] = [];
    for (const summary of sources) {
      if (summary.status !== "ready") continue;
      const stored = await this.store.getSource({
        namespaceId: params.namespaceId,
        baseId: params.baseId,
        sourceId: summary.id,
      });
      if (stored?.status === "ready") all.push(...this.chunker.chunk(toDocument(stored)));
    }
    const offset = params.offset ?? 0;
    return {
      chunks: all.slice(offset, offset + (params.limit ?? all.length)),
      total: all.length,
    };
  }

  /** Query one base's retriever, as an agent's search tool would. */
  async search(params: {
    namespaceId: string;
    baseId: string;
    request: RetrieveRequest;
  }): Promise<RetrieveResult[]> {
    await this.start();
    const key = { namespaceId: params.namespaceId, id: params.baseId };
    const runtime = this.bases.get(lockKey(key)) ?? (await this.reread(key));
    if (!runtime) throw new KnowledgeBaseNotFoundError(key);
    return runtime.retriever.retrieve(params.request);
  }

  // -------------------------------------------------------------------------
  // Loading and reconciling with the store

  private async load(): Promise<void> {
    this.stopped = false;
    await this.store.releaseClaims({ workerId: this.workerId });
    await this.refresh();
    for (const seed of this.seed) {
      await this.withLock(lockKey(seed), async () => {
        if (await this.find(seed)) return;
        await this.createLocked(seed);
        const runtime = this.bases.get(lockKey(seed))!;
        for (const document of seed.documents ?? []) {
          await this.enqueueLocked({ runtime, source: this.checkSource(document) });
        }
      });
    }
    this.schedulePoll();
    this.scheduleRefresh();
    this.kick();
  }

  /** The local runtime of a base, re-read from the store when missing here. */
  private async find(key: KnowledgeBaseKey): Promise<Runtime | null> {
    const local = this.bases.get(lockKey(key));
    if (local) return local;
    const stored = await this.store.getBase(key);
    if (!stored) return null;
    return this.reconcile(stored);
  }

  /** `find`, throwing `KnowledgeBaseNotFoundError` when the store has no such base. */
  private async require(key: KnowledgeBaseKey): Promise<Runtime> {
    const runtime = await this.find(key);
    if (!runtime) throw new KnowledgeBaseNotFoundError(key);
    return runtime;
  }

  /**
   * Re-read every base from the store: build bases new here (indexing
   * them), apply source changes to known ones and drop bases gone from the
   * store. Indexing changed sources of known bases runs under each base's
   * lock; `awaitIndexing: false` leaves it running in the background so a
   * read is not held up by an ingestion in flight.
   */
  private async readAll(params: { awaitIndexing: boolean }): Promise<void> {
    const rows = await this.store.listBases();
    const seen = new Set<string>();
    const indexing: Promise<void>[] = [];
    for (const row of rows) {
      seen.add(lockKey(row.base));
      const applied = await this.apply(row);
      if (applied.indexing) indexing.push(applied.indexing);
    }
    for (const [key, runtime] of Array.from(this.bases)) {
      if (seen.has(key)) continue;
      const stored = await this.store.getBase(runtime.base);
      if (stored) {
        const applied = await this.apply(stored);
        if (applied.indexing) indexing.push(applied.indexing);
      } else {
        this.drop(runtime);
      }
    }
    if (params.awaitIndexing) await Promise.all(indexing);
  }

  /** Re-read one base from the store and return its runtime (null: gone). */
  private async reread(key: KnowledgeBaseKey): Promise<Runtime | null> {
    const stored = await this.store.getBase(key);
    if (!stored) {
      const local = this.bases.get(lockKey(key));
      if (local) this.drop(local);
      return null;
    }
    return (await this.apply(stored)).runtime;
  }

  /**
   * Apply stored rows to this process: a base new here is built and
   * indexed under its lock; a known base takes the stored metadata and
   * source states at once, and its index catches up under its lock
   * (`indexing`, unset when nothing changed).
   */
  private async apply(
    stored: StoredKnowledgeBase,
  ): Promise<{ runtime: Runtime; indexing?: Promise<void> }> {
    const key = lockKey(stored.base);
    const local = this.bases.get(key);
    if (!local) return { runtime: await this.withLock(key, () => this.reconcile(stored)) };
    this.applyRows({ runtime: local, stored });
    if (!this.needsIndexing(local)) return { runtime: local };
    const indexing = this.withLock(key, () => this.syncIndex(local)).catch((err: unknown) => {
      console.warn(`[knowledge] indexing ${key} failed: ${errorMessage(err)}`);
    });
    return { runtime: local, indexing };
  }

  /**
   * Bring the local runtime of `stored.base` in line with the stored rows,
   * building it when new and indexing a non-durable retriever. Call under
   * the base's lock.
   */
  private async reconcile(stored: StoredKnowledgeBase): Promise<Runtime> {
    const key = lockKey(stored.base);
    let runtime = this.bases.get(key);
    if (!runtime) {
      runtime = {
        base: stored.base,
        sources: new Map(),
        retriever: await this.retrievers.create({ base: stored.base, chunker: this.chunker }),
        indexed: new Map(),
        localFailures: new Map(),
      };
      this.bases.set(key, runtime);
    }
    this.applyRows({ runtime, stored });
    await this.syncIndex(runtime);
    return runtime;
  }

  /** Take the stored base row and source states; no retriever work. */
  private applyRows(params: { runtime: Runtime; stored: StoredKnowledgeBase }): void {
    const { runtime, stored } = params;
    runtime.base = stored.base;
    this.register(runtime);
    const sources = new Map<string, KnowledgeSourceSummary>();
    for (const source of stored.sources) {
      const failure = runtime.localFailures.get(source.id);
      sources.set(
        source.id,
        source.status === "ready" && failure?.updatedAt === source.updatedAt
          ? failedSummary({ summary: source, error: failure.error })
          : source,
      );
    }
    runtime.sources = sources;
  }

  /** Whether a non-durable retriever is behind its base's source states. */
  private needsIndexing(runtime: Runtime): boolean {
    if (this.retrievers.durable) return false;
    for (const source of runtime.sources.values()) {
      if (source.status === "ready" && runtime.indexed.get(source.id) !== source.updatedAt) {
        return true;
      }
    }
    for (const sourceId of runtime.indexed.keys()) {
      const source = runtime.sources.get(sourceId);
      if (!source || source.status === "failed") return true;
    }
    return false;
  }

  /**
   * Index every ready source version a non-durable retriever lacks, and
   * drop the chunks of sources that are gone or failed. Call under the
   * base's lock.
   */
  private async syncIndex(runtime: Runtime): Promise<void> {
    if (this.retrievers.durable) return;
    for (const sourceId of runtime.indexed.keys()) {
      const source = runtime.sources.get(sourceId);
      if (source && source.status !== "failed") continue;
      await runtime.retriever.deleteSource({ sourceId }).catch(() => {});
      runtime.indexed.delete(sourceId);
    }
    for (const sourceId of runtime.localFailures.keys()) {
      if (!runtime.sources.has(sourceId)) runtime.localFailures.delete(sourceId);
    }
    for (const source of runtime.sources.values()) {
      if (source.status === "ready") await this.indexLocally({ runtime, summary: source });
    }
  }
  /**
   * Index a ready source version into this process's non-durable retriever
   * unless it is already there. A failure is kept local: the source shows
   * as failed here, and the same version is not retried until it changes.
   */
  private async indexLocally(params: {
    runtime: Runtime;
    summary: KnowledgeSourceSummary;
  }): Promise<void> {
    const { runtime, summary } = params;
    if (runtime.indexed.get(summary.id) === summary.updatedAt) return;
    const failure = runtime.localFailures.get(summary.id);
    if (failure?.updatedAt === summary.updatedAt) {
      runtime.sources.set(summary.id, failedSummary({ summary, error: failure.error }));
      return;
    }
    const full = await this.store.getSource({
      namespaceId: runtime.base.namespaceId,
      baseId: runtime.base.id,
      sourceId: summary.id,
    });
    if (!full || full.status !== "ready") return;
    const version = sourceVersion(full);
    try {
      // The new version goes in before the old one is dropped.
      await runtime.retriever.addDocument({ ...toDocument(full), version });
      await runtime.retriever.deleteSource({ sourceId: full.id, keepVersion: version });
      runtime.indexed.set(full.id, full.updatedAt);
      runtime.localFailures.delete(full.id);
    } catch (err) {
      await runtime.retriever.deleteSource({ sourceId: full.id }).catch(() => {});
      runtime.indexed.delete(full.id);
      const error = errorMessage(err);
      runtime.localFailures.set(full.id, { updatedAt: full.updatedAt, error });
      runtime.sources.set(full.id, failedSummary({ summary, error }));
      console.warn(
        `[knowledge] indexing ${runtime.base.namespaceId}/${runtime.base.id}/${full.id} failed: ${error}`,
      );
    }
  }

  private drop(runtime: Runtime): void {
    const key = lockKey(runtime.base);
    if (this.bases.get(key) === runtime) this.bases.delete(key);
    this.rows.unregister({ id: runtime.base.id, namespaceId: runtime.base.namespaceId });
  }

  // -------------------------------------------------------------------------
  // Writes

  private async createLocked(input: KnowledgeBaseCreateInput): Promise<KnowledgeBase> {
    checkNamespace(input.namespaceId);
    if (!KNOWLEDGE_BASE_ID_PATTERN.test(input.id)) {
      throw new KnowledgeInputError({
        code: "invalid_id",
        message:
          "Knowledge base ids are 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit.",
      });
    }
    const key = { namespaceId: input.namespaceId, id: input.id };
    if (await this.find(key)) throw new KnowledgeBaseExistsError(key);
    const hostRow = this.rows.get(key);
    if (hostRow) {
      throw new KnowledgeBaseExistsError({
        ...key,
        message: `A retriever "${input.id}" is already registered by the host.`,
      });
    }
    const now = this.clock.currentTimeMs();
    const base: KnowledgeBaseRecord = {
      namespaceId: input.namespaceId,
      id: input.id,
      ...(input.description ? { description: input.description } : {}),
      tags: normalizeTags(input.tags),
      metadata: { ...input.metadata },
      createdAt: now,
      updatedAt: now,
    };
    const retriever = await this.retrievers.create({ base, chunker: this.chunker });
    await this.store.saveBase(base);
    const runtime: Runtime = {
      base,
      sources: new Map(),
      retriever,
      indexed: new Map(),
      localFailures: new Map(),
    };
    this.bases.set(lockKey(base), runtime);
    this.register(runtime);
    return view(runtime);
  }

  /** Write `source` as `pending`. Call under the base's lock. */
  private async enqueueLocked(params: {
    runtime: Runtime;
    source: KnowledgeIngestDocument;
  }): Promise<KnowledgeSourceSummary> {
    const { runtime, source } = params;
    const previous = runtime.sources.get(source.id);
    const now = this.clock.currentTimeMs();
    const queued: KnowledgeSourceRecord = {
      ...sourceFields(source),
      status: "pending",
      chunkCount: previous?.chunkCount ?? 0,
      createdAt: previous?.createdAt ?? now,
      updatedAt: now,
    };
    await this.store.saveSource({
      namespaceId: runtime.base.namespaceId,
      baseId: runtime.base.id,
      source: queued,
    });
    const summary = summarize(queued);
    runtime.sources.set(source.id, summary);
    runtime.localFailures.delete(source.id);
    await this.touch(runtime);
    return summary;
  }

  /** Bump the base's `updatedAt` after a source change. */
  private async touch(runtime: Runtime): Promise<void> {
    const base = { ...runtime.base, updatedAt: this.clock.currentTimeMs() };
    await this.store.saveBase(base);
    runtime.base = base;
  }

  // -------------------------------------------------------------------------
  // Ingestion worker

  /** Look for queued sources now. */
  private kick(): void {
    if (this.stopped || this.loading === undefined) return;
    void this.fill().catch((err: unknown) => {
      console.warn(`[knowledge] claiming queued sources failed: ${errorMessage(err)}`);
    });
  }

  /** Claim queued sources until every ingestion slot is busy or none is left. */
  private fill(): Promise<void> {
    if (this.filling) {
      this.refill = true;
      return this.filling;
    }
    this.filling = (async () => {
      do {
        this.refill = false;
        while (!this.stopped && this.inFlight.size < this.concurrency) {
          const claimed = await this.store.claimSource({
            workerId: this.workerId,
            staleAfterMs: this.staleAfterMs,
          });
          if (!claimed) break;
          const job: Promise<void> = this.process(claimed)
            .catch((err: unknown) => {
              console.warn(
                `[knowledge] ingesting ${claimed.namespaceId}/${claimed.baseId}/${claimed.source.id} failed: ${errorMessage(err)}`,
              );
            })
            .finally(() => {
              this.inFlight.delete(job);
              this.kick();
            });
          this.inFlight.add(job);
        }
      } while (this.refill && !this.stopped);
    })().finally(() => {
      this.filling = undefined;
    });
    return this.filling;
  }

  /**
   * Index one claimed source and record the outcome. The embedding runs
   * outside the base's lock (other writes and `ensure` on the base go on)
   * while a heartbeat renews the claim; the lock is taken only to record
   * the outcome. An outcome the store refuses (the source was rewritten,
   * removed or reclaimed meanwhile) is dropped together with the chunks
   * this ingestion wrote; a removed source's chunks are dropped with it.
   */
  private async process(claimed: ClaimedKnowledgeSource): Promise<void> {
    const key = { namespaceId: claimed.namespaceId, id: claimed.baseId };
    const { source } = claimed;
    const version = sourceVersion(source);
    const heartbeat = this.heartbeat(claimed);
    try {
      const runtime = await this.withLock(lockKey(key), () => this.find(key));
      if (!runtime) return;
      let outcome: KnowledgeSourceRecord;
      try {
        const chunks = await runtime.retriever.addDocument({ ...toDocument(source), version });
        const { error: _error, ...rest } = source;
        outcome = {
          ...rest,
          status: "ready",
          chunkCount: chunks.length,
          updatedAt: this.clock.currentTimeMs(),
        };
      } catch (err) {
        await runtime.retriever.deleteSource({ sourceId: source.id, version }).catch(() => {});
        outcome = {
          ...source,
          status: "failed",
          error: errorMessage(err),
          chunkCount: 0,
          updatedAt: this.clock.currentTimeMs(),
        };
      }
      heartbeat.stop();
      await this.withLock(lockKey(key), async () => {
        const written = await this.store.completeSource({
          namespaceId: claimed.namespaceId,
          baseId: claimed.baseId,
          workerId: this.workerId,
          source: outcome,
        });
        if (written) {
          // The recorded version replaces every other one; a failure leaves none.
          await runtime.retriever.deleteSource(
            outcome.status === "ready"
              ? { sourceId: source.id, keepVersion: version }
              : { sourceId: source.id },
          );
          runtime.sources.set(source.id, summarize(outcome));
          if (outcome.status === "ready") runtime.indexed.set(source.id, outcome.updatedAt);
          else runtime.indexed.delete(source.id);
          runtime.localFailures.delete(source.id);
          await this.touch(runtime);
          return;
        }
        // Superseded: drop exactly what this ingestion wrote; the version the
        // store now holds is indexed by whoever ingests it.
        await runtime.retriever.deleteSource({ sourceId: source.id, version });
        const current = await this.store.getSource({ ...claimed, sourceId: source.id });
        if (!current) {
          await runtime.retriever.deleteSource({ sourceId: source.id }).catch(() => {});
          runtime.sources.delete(source.id);
        }
        runtime.indexed.delete(source.id);
      });
    } finally {
      heartbeat.stop();
    }
  }

  /**
   * Renew this worker's claim on `claimed` every third of `staleAfterMs`
   * until stopped, so a long ingestion is never taken over as stale. Stops
   * by itself once the claim is gone.
   */
  private heartbeat(claimed: ClaimedKnowledgeSource): { stop(): void } {
    const everyMs = Math.max(1, Math.floor(this.staleAfterMs / 3));
    let stopped = false;
    let timer: TimerHandle | undefined;
    const schedule = () => {
      if (stopped) return;
      timer = this.clock.setTimeout(() => {
        void this.store
          .heartbeatClaim({
            namespaceId: claimed.namespaceId,
            baseId: claimed.baseId,
            sourceId: claimed.source.id,
            workerId: this.workerId,
          })
          .then(
            (held) => {
              if (held) schedule();
            },
            (err: unknown) => {
              console.warn(`[knowledge] renewing an ingestion claim failed: ${errorMessage(err)}`);
              schedule();
            },
          );
      }, everyMs);
      timer.unref?.();
    };
    schedule();
    return {
      stop: () => {
        stopped = true;
        timer?.clear();
      },
    };
  }

  private schedulePoll(): void {
    if (this.stopped || this.pollIntervalMs <= 0) return;
    this.pollTimer = this.clock.setTimeout(() => {
      this.kick();
      this.schedulePoll();
    }, this.pollIntervalMs);
    this.pollTimer.unref?.();
  }

  private scheduleRefresh(): void {
    if (this.stopped || this.refreshIntervalMs <= 0) return;
    this.refreshTimer = this.clock.setTimeout(() => {
      void this.refresh()
        .catch((err: unknown) => {
          console.warn(`[knowledge] re-reading the store failed: ${errorMessage(err)}`);
        })
        .finally(() => this.scheduleRefresh());
    }, this.refreshIntervalMs);
    this.refreshTimer.unref?.();
  }

  // -------------------------------------------------------------------------

  private checkSource(input: KnowledgeSourceInput): KnowledgeIngestDocument {
    const id = input.id.trim();
    if (id.length === 0 || id.length > MAX_KNOWLEDGE_SOURCE_ID_LENGTH) {
      throw new KnowledgeInputError({
        code: "invalid_source",
        message: `A source id must be 1-${MAX_KNOWLEDGE_SOURCE_ID_LENGTH} characters.`,
      });
    }
    if (input.text.trim().length === 0) {
      throw new KnowledgeInputError({
        code: "invalid_source",
        message: `Source "${id}" has no text.`,
      });
    }
    if (input.text.length > this.maxSourceCharacters) {
      throw new KnowledgeInputError({
        code: "source_too_large",
        message: `Source "${id}" has ${input.text.length} characters; the limit is ${this.maxSourceCharacters}.`,
      });
    }
    return {
      id,
      text: input.text,
      ...(input.title !== undefined && input.title !== "" && { title: input.title }),
      ...(input.uri !== undefined && input.uri !== "" && { uri: input.uri }),
      ...(input.mimeType !== undefined && input.mimeType !== "" && { mimeType: input.mimeType }),
      tags: normalizeTags(input.tags),
      metadata: { ...input.metadata },
    };
  }

  private register(runtime: Runtime): void {
    const { base } = runtime;
    this.rows.register({
      id: base.id,
      namespaceId: base.namespaceId,
      retriever: runtime.retriever,
      ...(base.description !== undefined && { description: base.description }),
      tags: base.tags,
      metadata: base.metadata,
    });
  }

  /** Run `fn` after every earlier write to `key` settled. */
  private withLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const previous = this.locks.get(key) ?? Promise.resolve();
    const run = previous.then(fn, fn);
    const tail = run.catch(() => {});
    this.locks.set(key, tail);
    void tail.then(() => {
      if (this.locks.get(key) === tail) this.locks.delete(key);
    });
    return run;
  }
}

function lockKey(key: KnowledgeBaseKey): string {
  return JSON.stringify([key.namespaceId, key.id]);
}

function checkNamespace(namespaceId: string): void {
  if (namespaceId.trim().length === 0 || namespaceId.length > MAX_KNOWLEDGE_NAMESPACE_LENGTH) {
    throw new KnowledgeInputError({
      code: "invalid_namespace",
      message: `A namespace id must be 1-${MAX_KNOWLEDGE_NAMESPACE_LENGTH} characters.`,
    });
  }
}

function positiveInteger(params: { name: string; value: number }): number {
  if (!Number.isInteger(params.value) || params.value < 1) {
    throw new Error(`ZoryaKnowledgeBases: ${params.name} must be an integer ≥ 1`);
  }
  return params.value;
}

function view(runtime: Runtime): KnowledgeBase {
  let chunkCount = 0;
  let failed = false;
  let queued = false;
  for (const source of runtime.sources.values()) {
    chunkCount += source.chunkCount;
    if (source.status === "failed") failed = true;
    if (source.status === "pending" || source.status === "ingesting") queued = true;
  }
  return {
    ...runtime.base,
    status: failed ? "degraded" : queued ? "ingesting" : "ready",
    sourceCount: runtime.sources.size,
    chunkCount,
  };
}

function summarize(source: KnowledgeSourceRecord): KnowledgeSourceSummary {
  const { text: _text, ...summary } = source;
  return summary;
}

function failedSummary(params: {
  summary: KnowledgeSourceSummary;
  error: string;
}): KnowledgeSourceSummary {
  return { ...params.summary, status: "failed", error: params.error, chunkCount: 0 };
}

function sourceFields(
  source: KnowledgeIngestDocument,
): Omit<KnowledgeSourceRecord, "status" | "error" | "chunkCount" | "createdAt" | "updatedAt"> {
  return {
    id: source.id,
    ...(source.title !== undefined && { title: source.title }),
    ...(source.uri !== undefined && { uri: source.uri }),
    ...(source.mimeType !== undefined && { mimeType: source.mimeType }),
    tags: [...(source.tags ?? [])],
    metadata: { ...source.metadata },
    text: source.text,
  };
}

/**
 * The version a source's chunks are tagged with: a digest of the written
 * source (content and write stamp), so two writes never share a version
 * unless they are the same write.
 */
function sourceVersion(source: KnowledgeSourceRecord): string {
  const { status: _status, error: _error, chunkCount: _chunkCount, ...written } = source;
  return createHash("sha256").update(JSON.stringify(written)).digest("hex").slice(0, 16);
}

function toDocument(source: KnowledgeSourceRecord): KnowledgeIngestDocument {
  return {
    id: source.id,
    text: source.text,
    ...(source.title !== undefined && { title: source.title }),
    ...(source.uri !== undefined && { uri: source.uri }),
    ...(source.mimeType !== undefined && { mimeType: source.mimeType }),
    tags: source.tags,
    metadata: source.metadata,
  };
}

function normalizeTags(tags: ReadonlyArray<string> | undefined): string[] {
  return [...new Set((tags ?? []).map((tag) => tag.trim()).filter(Boolean))];
}
