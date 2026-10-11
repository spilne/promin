// ---------------------------------------------------------------------------
// ZoryaKnowledgeBases — managed knowledge bases: named retrievers operators
// create and fill with sources from the dashboard, which agents then bind by
// id in `backend.knowledge`.
//
// The service owns three things per base:
//   - its durable rows in a `KnowledgeBaseStore` (metadata plus every
//     source's text and last ingestion outcome),
//   - a live `IngestibleRetriever` built by the retriever factory, and
//   - that retriever's row in a `RetrieverRegistry`, which is the registry
//     `ZoryaAgents.retrievers` and `resolveLocalAgent({ retrievers })` read.
//
// The default factory builds an `InMemoryRetriever`, which is not durable:
// `start()` re-ingests every ready source from the store. A durable factory
// (e.g. one `PgVectorRetriever` table per base) keeps its chunks and is not
// replayed. Writes to one base run one at a time; different bases proceed
// in parallel.
//
// The rows are read once at `start()`, so the metadata view is per process:
// replicas sharing a store see each other's bases after a restart. Serve the
// Knowledge page from one replica, or use a durable retriever so searches
// agree across replicas.
// ---------------------------------------------------------------------------

import {
  FlatTextChunker,
  InMemoryKnowledgeBaseStore,
  InMemoryRetriever,
  InMemoryRetrieverRegistry,
  type EmbeddingProvider,
  type IngestibleRetriever,
  type KnowledgeBaseRecord,
  type KnowledgeBaseStore,
  type KnowledgeChunk,
  type KnowledgeChunker,
  type KnowledgeIngestDocument,
  type KnowledgeSourceRecord,
  type RetrieveRequest,
  type RetrieveResult,
  type RetrieverRegistry,
} from "@promin/agent";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  KnowledgeBaseExistsError,
  KnowledgeBaseNotFoundError,
  KnowledgeIngestError,
  KnowledgeInputError,
  KnowledgeSourceNotFoundError,
  errorMessage,
} from "./errors.ts";

/** Knowledge base ids: what recipes put in `backend.knowledge[].id`. */
export const KNOWLEDGE_BASE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

/** Longest source id accepted (file paths and URLs make long ids). */
export const MAX_KNOWLEDGE_SOURCE_ID_LENGTH = 512;

/** Default cap on one source's text, in characters. */
export const DEFAULT_MAX_SOURCE_CHARACTERS = 2_000_000;

/** A knowledge base as the API returns it: its row plus counts derived from its sources. */
export interface KnowledgeBase extends KnowledgeBaseRecord {
  /** `degraded` while any source's last ingestion failed. */
  readonly status: "ready" | "degraded";
  readonly sourceCount: number;
  readonly chunkCount: number;
}

/** A source without its text, as listings return it. */
export type KnowledgeSourceSummary = Omit<KnowledgeSourceRecord, "text">;

export interface KnowledgeBaseCreateInput {
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
   * True when the retriever keeps its chunks across restarts (a pgvector
   * table). False: `start()` re-ingests every ready source's stored text.
   */
  readonly durable: boolean;
}

export interface ZoryaKnowledgeBasesConfig {
  /**
   * Registry each base's retriever is registered in under the base id. Pass
   * the same registry as `ZoryaAgents.retrievers` and to
   * `resolveLocalAgent({ retrievers })`. Default: a new in-memory registry
   * (read it back from `.registry`).
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
  /** Time source for timestamps. Default: `SystemWallClock`. */
  clock?: WallClock;
}

interface Runtime {
  base: KnowledgeBaseRecord;
  sources: Map<string, KnowledgeSourceRecord>;
  retriever: IngestibleRetriever;
}

export class ZoryaKnowledgeBases {
  readonly registry: RetrieverRegistry;
  readonly store: KnowledgeBaseStore;
  readonly chunker: KnowledgeChunker;

  private readonly retrievers: KnowledgeRetrieverFactory;
  private readonly adapters = new Map<string, KnowledgeSourceAdapter>();
  private readonly seed: ReadonlyArray<KnowledgeBaseSeed>;
  private readonly maxSourceCharacters: number;
  private readonly clock: WallClock;
  private readonly bases = new Map<string, Runtime>();
  /** Tail of each base's write queue. */
  private readonly locks = new Map<string, Promise<unknown>>();
  private loading?: Promise<void>;

  constructor(config: ZoryaKnowledgeBasesConfig = {}) {
    this.registry = config.registry ?? new InMemoryRetrieverRegistry();
    this.store = config.store ?? new InMemoryKnowledgeBaseStore();
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
    this.clock = config.clock ?? SystemWallClock;
  }

  /** Kinds the import route accepts, sorted. */
  sourceKinds(): string[] {
    return [...this.adapters.keys()].sort();
  }

  /**
   * Load every base from the store, build and register its retriever
   * (re-ingesting stored text into non-durable ones), then create missing
   * seed bases. Idempotent; a failed load is retried by the next call. Every
   * other method awaits it, so embedders that skip `ZoryaServer` need not
   * call it.
   */
  start(): Promise<void> {
    this.loading ??= this.load().catch((err: unknown) => {
      this.loading = undefined;
      throw err;
    });
    return this.loading;
  }

  async stop(): Promise<void> {}

  async list(): Promise<KnowledgeBase[]> {
    await this.start();
    return [...this.bases.values()]
      .sort((a, b) => a.base.id.localeCompare(b.base.id))
      .map((runtime) => view(runtime));
  }

  async get(id: string): Promise<KnowledgeBase | null> {
    await this.start();
    const runtime = this.bases.get(id);
    return runtime ? view(runtime) : null;
  }

  async create(input: KnowledgeBaseCreateInput): Promise<KnowledgeBase> {
    await this.start();
    return this.withLock(input.id, () => this.createLocked(input));
  }

  async update(params: { id: string; patch: KnowledgeBaseUpdateInput }): Promise<KnowledgeBase> {
    await this.start();
    return this.withLock(params.id, async () => {
      const runtime = this.require(params.id);
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
  async remove(id: string): Promise<void> {
    await this.start();
    await this.withLock(id, async () => {
      const runtime = this.require(id);
      for (const sourceId of runtime.sources.keys()) {
        await runtime.retriever.deleteSource(sourceId);
      }
      await this.store.deleteBase(id);
      this.bases.delete(id);
      this.registry.unregister(id);
    });
  }

  async listSources(id: string): Promise<KnowledgeSourceSummary[]> {
    await this.start();
    const runtime = this.require(id);
    return [...runtime.sources.values()]
      .sort((a, b) => a.id.localeCompare(b.id))
      .map((source) => summarize(source));
  }

  /**
   * Chunk, index and record one source, replacing any source with the same
   * id. On failure the source is recorded as `failed` with the reason, its
   * partial chunks are dropped, and `KnowledgeIngestError` is thrown.
   */
  async ingest(params: {
    baseId: string;
    source: KnowledgeSourceInput;
  }): Promise<KnowledgeSourceSummary> {
    await this.start();
    const source = this.checkSource(params.source);
    return this.withLock(params.baseId, () =>
      this.ingestLocked({ runtime: this.require(params.baseId), source }),
    );
  }

  /** Re-ingest a stored source from its stored text (e.g. after a failure). */
  async reindexSource(params: {
    baseId: string;
    sourceId: string;
  }): Promise<KnowledgeSourceSummary> {
    await this.start();
    return this.withLock(params.baseId, () => {
      const runtime = this.require(params.baseId);
      const stored = runtime.sources.get(params.sourceId);
      if (!stored) throw new KnowledgeSourceNotFoundError(params);
      return this.ingestLocked({ runtime, source: toDocument(stored) });
    });
  }

  /**
   * Load documents through the adapter registered for `kind` and ingest them
   * in order. Stops at the first document that fails to index; the ones
   * before it stay ingested.
   */
  async importSources(params: {
    baseId: string;
    kind: string;
    config: unknown;
  }): Promise<KnowledgeSourceSummary[]> {
    await this.start();
    this.require(params.baseId);
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
    const out: KnowledgeSourceSummary[] = [];
    for (const source of documents) {
      out.push(await this.ingest({ baseId: params.baseId, source }));
    }
    return out;
  }

  async removeSource(params: { baseId: string; sourceId: string }): Promise<void> {
    await this.start();
    await this.withLock(params.baseId, async () => {
      const runtime = this.require(params.baseId);
      if (!runtime.sources.has(params.sourceId)) throw new KnowledgeSourceNotFoundError(params);
      await runtime.retriever.deleteSource(params.sourceId);
      await this.store.deleteSource(params);
      runtime.sources.delete(params.sourceId);
      await this.touch(runtime);
    });
  }

  /**
   * The chunks of ready sources (or of one source), as the service's chunker
   * splits them, ordered by source id then chunk index.
   */
  async chunks(params: {
    baseId: string;
    sourceId?: string;
    limit?: number;
    offset?: number;
  }): Promise<{ chunks: KnowledgeChunk[]; total: number }> {
    await this.start();
    const runtime = this.require(params.baseId);
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
    const all = sources
      .filter((s) => s.status === "ready")
      .flatMap((s) => this.chunker.chunk(toDocument(s)));
    const offset = params.offset ?? 0;
    return {
      chunks: all.slice(offset, offset + (params.limit ?? all.length)),
      total: all.length,
    };
  }

  /** Query one base's retriever, as an agent's search tool would. */
  async search(params: { baseId: string; request: RetrieveRequest }): Promise<RetrieveResult[]> {
    await this.start();
    return this.require(params.baseId).retriever.retrieve(params.request);
  }

  // -------------------------------------------------------------------------

  private async load(): Promise<void> {
    const rows = await this.store.loadAll();
    const loaded: Runtime[] = [];
    for (const row of rows) {
      const runtime: Runtime = {
        base: row.base,
        sources: new Map(row.sources.map((s) => [s.id, s])),
        retriever: await this.retrievers.create({ base: row.base, chunker: this.chunker }),
      };
      if (!this.retrievers.durable) {
        for (const source of row.sources) {
          if (source.status !== "ready") continue;
          try {
            await runtime.retriever.addDocument(toDocument(source));
          } catch (err) {
            // Keep booting: the source shows as failed with the reason and
            // can be re-indexed from the dashboard.
            await runtime.retriever.deleteSource(source.id).catch(() => {});
            const failed = this.failedRecord({ source: toDocument(source), previous: source, err });
            await this.store.saveSource({ baseId: row.base.id, source: failed });
            runtime.sources.set(source.id, failed);
          }
        }
      }
      loaded.push(runtime);
    }
    for (const runtime of loaded) {
      this.bases.set(runtime.base.id, runtime);
      this.register(runtime);
    }
    for (const seed of this.seed) {
      if (this.bases.has(seed.id)) continue;
      const created = await this.createLocked(seed);
      const runtime = this.require(created.id);
      for (const document of seed.documents ?? []) {
        try {
          await this.ingestLocked({ runtime, source: this.checkSource(document) });
        } catch (err) {
          // Recorded as a failed source; one bad seed document must not
          // keep the service from starting.
          if (!(err instanceof KnowledgeIngestError)) throw err;
        }
      }
    }
  }

  private async createLocked(input: KnowledgeBaseCreateInput): Promise<KnowledgeBase> {
    if (!KNOWLEDGE_BASE_ID_PATTERN.test(input.id)) {
      throw new KnowledgeInputError({
        code: "invalid_id",
        message:
          "Knowledge base ids are 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit.",
      });
    }
    if (this.bases.has(input.id)) throw new KnowledgeBaseExistsError({ id: input.id });
    if (this.registry.get(input.id)) {
      throw new KnowledgeBaseExistsError({
        id: input.id,
        message: `A retriever "${input.id}" is already registered by the host.`,
      });
    }
    const now = this.clock.currentTimeMs();
    const base: KnowledgeBaseRecord = {
      id: input.id,
      ...(input.description ? { description: input.description } : {}),
      tags: normalizeTags(input.tags),
      metadata: { ...input.metadata },
      createdAt: now,
      updatedAt: now,
    };
    const retriever = await this.retrievers.create({ base, chunker: this.chunker });
    await this.store.saveBase(base);
    const runtime: Runtime = { base, sources: new Map(), retriever };
    this.bases.set(base.id, runtime);
    this.register(runtime);
    return view(runtime);
  }

  private async ingestLocked(params: {
    runtime: Runtime;
    source: KnowledgeIngestDocument;
  }): Promise<KnowledgeSourceSummary> {
    const { runtime, source } = params;
    const previous = runtime.sources.get(source.id);
    let chunkCount: number;
    try {
      if (previous) await runtime.retriever.deleteSource(source.id);
      chunkCount = (await runtime.retriever.addDocument(source)).length;
    } catch (err) {
      await runtime.retriever.deleteSource(source.id).catch(() => {});
      const failed = this.failedRecord({ source, previous, err });
      runtime.sources.set(source.id, failed);
      try {
        await this.store.saveSource({ baseId: runtime.base.id, source: failed });
        await this.touch(runtime);
      } catch {
        // The ingestion failure is the error to report.
      }
      throw new KnowledgeIngestError({ sourceId: source.id, cause: err });
    }
    const now = this.clock.currentTimeMs();
    const ready: KnowledgeSourceRecord = {
      ...sourceFields(source),
      status: "ready",
      chunkCount,
      createdAt: previous?.createdAt ?? now,
      updatedAt: now,
    };
    await this.store.saveSource({ baseId: runtime.base.id, source: ready });
    runtime.sources.set(source.id, ready);
    await this.touch(runtime);
    return summarize(ready);
  }

  private failedRecord(params: {
    source: KnowledgeIngestDocument;
    previous: KnowledgeSourceRecord | undefined;
    err: unknown;
  }): KnowledgeSourceRecord {
    const now = this.clock.currentTimeMs();
    return {
      ...sourceFields(params.source),
      status: "failed",
      error: errorMessage(params.err),
      chunkCount: 0,
      createdAt: params.previous?.createdAt ?? now,
      updatedAt: now,
    };
  }

  /** Bump the base's `updatedAt` after a source change. */
  private async touch(runtime: Runtime): Promise<void> {
    const base = { ...runtime.base, updatedAt: this.clock.currentTimeMs() };
    await this.store.saveBase(base);
    runtime.base = base;
  }

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

  private require(id: string): Runtime {
    const runtime = this.bases.get(id);
    if (!runtime) throw new KnowledgeBaseNotFoundError(id);
    return runtime;
  }

  private register(runtime: Runtime): void {
    const { base } = runtime;
    this.registry.register({
      id: base.id,
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

function view(runtime: Runtime): KnowledgeBase {
  let chunkCount = 0;
  let failed = false;
  for (const source of runtime.sources.values()) {
    chunkCount += source.chunkCount;
    if (source.status === "failed") failed = true;
  }
  return {
    ...runtime.base,
    status: failed ? "degraded" : "ready",
    sourceCount: runtime.sources.size,
    chunkCount,
  };
}

function summarize(source: KnowledgeSourceRecord): KnowledgeSourceSummary {
  const { text: _text, ...summary } = source;
  return summary;
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
