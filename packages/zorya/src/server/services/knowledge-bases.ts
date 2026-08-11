import {
  FlatTextChunker,
  InMemoryRetriever,
  InMemoryRetrieverRegistry,
  type KnowledgeChunk,
  type KnowledgeIngestDocument,
  type KnowledgeSource,
  type RegisterRetrieverInput,
  type RetrieveResult,
  type Retriever,
  type RetrieverRegistry,
} from "@promin/agent";
import { InMemoryResourceRegistry } from "@promin/core";
import {
  fileKnowledgeSourceAdapter,
  urlKnowledgeSourceAdapter,
} from "./knowledge-source-adapters.ts";

export type KnowledgeBaseProvider = "memory" | "external";
export type KnowledgeSourceStatus = "ready" | "failed";

export interface KnowledgeBaseDefinition {
  readonly id: string;
  readonly namespace: string;
  readonly description?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
  readonly provider: KnowledgeBaseProvider;
  readonly status: "ready" | "degraded";
  readonly sourceCount: number;
  readonly chunkCount: number;
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface KnowledgeBaseSourceRecord {
  readonly id: string;
  readonly title?: string;
  readonly uri?: string;
  readonly mimeType?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
  readonly text: string;
  readonly status: KnowledgeSourceStatus;
  readonly error?: string;
  readonly chunkCount: number;
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface StoredKnowledgeBase {
  readonly definition: KnowledgeBaseDefinition;
  readonly sources: ReadonlyArray<KnowledgeBaseSourceRecord>;
}

/** Durable persistence boundary for managed knowledge-base metadata/content. */
export interface KnowledgeBaseStore {
  load(): Promise<ReadonlyArray<StoredKnowledgeBase>>;
  save(record: StoredKnowledgeBase): Promise<void>;
  delete(namespace: string, id: string): Promise<void>;
}

export class InMemoryKnowledgeBaseStore implements KnowledgeBaseStore {
  private readonly records = new Map<string, StoredKnowledgeBase>();

  constructor(records: ReadonlyArray<StoredKnowledgeBase> = []) {
    for (const record of records)
      this.records.set(key(record.definition.namespace, record.definition.id), record);
  }

  async load(): Promise<ReadonlyArray<StoredKnowledgeBase>> {
    return [...this.records.values()].map(cloneRecord);
  }

  async save(record: StoredKnowledgeBase): Promise<void> {
    this.records.set(key(record.definition.namespace, record.definition.id), cloneRecord(record));
  }

  async delete(namespace: string, id: string): Promise<void> {
    this.records.delete(key(namespace, id));
  }
}

export interface KnowledgeBaseCreateInput {
  readonly id: string;
  readonly namespace: string;
  readonly description?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
  readonly provider?: KnowledgeBaseProvider;
}

export interface KnowledgeBaseUpdateInput {
  readonly description?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface KnowledgeSourceInput {
  readonly id: string;
  readonly text: string;
  readonly title?: string;
  readonly uri?: string;
  readonly mimeType?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export type KnowledgeSourceKind = "file" | "url" | (string & {});

export interface KnowledgeSourceAdapter {
  readonly kind: KnowledgeSourceKind;
  load(config: unknown): Promise<ReadonlyArray<KnowledgeSourceInput>>;
}

export class KnowledgeSourceAdapterRegistry extends InMemoryResourceRegistry<KnowledgeSourceAdapter> {
  constructor(adapters: ReadonlyArray<KnowledgeSourceAdapter> = []) {
    super({ keyOf: (adapter) => adapter.kind, compare: (a, b) => a.kind.localeCompare(b.kind) });
    for (const adapter of adapters) this.set(adapter);
  }

  register(adapter: KnowledgeSourceAdapter): KnowledgeSourceAdapter {
    return this.set(adapter);
  }
}

export interface KnowledgeBaseChunk {
  readonly id: string;
  readonly text: string;
  readonly index: number;
  readonly parentId?: string;
  readonly metadata: Readonly<Record<string, unknown>>;
  readonly source: KnowledgeSource;
}

export interface ZoryaKnowledgeBasesConfig {
  readonly registry?: RetrieverRegistry;
  readonly store?: KnowledgeBaseStore;
  readonly now?: () => number;
  /** Build a live retriever for each managed definition (for example pgvector). */
  readonly retrieverFactory?: (definition: KnowledgeBaseDefinition) => Retriever;
  /**
   * Optional async setup hook for retrievers that need schema/index/bootstrap
   * work before documents are ingested or queries are served.
   */
  readonly retrieverSetup?: (
    definition: KnowledgeBaseDefinition,
    retriever: Retriever,
  ) => Promise<void> | void;
  readonly sourceAdapters?: KnowledgeSourceAdapterRegistry;
  readonly initial?: ReadonlyArray<
    KnowledgeBaseCreateInput & { documents?: ReadonlyArray<KnowledgeSourceInput> }
  >;
}

interface RuntimeBase {
  definition: KnowledgeBaseDefinition;
  sources: Map<string, KnowledgeBaseSourceRecord>;
  retriever: Retriever;
}

/**
 * Namespace-aware managed knowledge-base service.
 *
 * It owns durable metadata and the runtime registry entry, while keeping the
 * actual retriever implementation behind the existing Retriever contract.
 * The default runtime is an in-memory retriever; hosts can provide a registry
 * and durable store for Postgres, SQLite, or another vector backend.
 */
export class ZoryaKnowledgeBases {
  readonly registry: RetrieverRegistry;
  readonly store: KnowledgeBaseStore;
  readonly sourceAdapters: KnowledgeSourceAdapterRegistry;

  private readonly now: () => number;
  private readonly retrieverFactory?: ZoryaKnowledgeBasesConfig["retrieverFactory"];
  private readonly retrieverSetup?: ZoryaKnowledgeBasesConfig["retrieverSetup"];
  private readonly bases = new Map<string, RuntimeBase>();
  private readonly readyPromise: Promise<void>;

  constructor(config: ZoryaKnowledgeBasesConfig = {}) {
    this.registry = config.registry ?? new InMemoryRetrieverRegistry();
    this.store = config.store ?? new InMemoryKnowledgeBaseStore();
    this.sourceAdapters =
      config.sourceAdapters ??
      new KnowledgeSourceAdapterRegistry([fileKnowledgeSourceAdapter, urlKnowledgeSourceAdapter]);
    this.now = config.now ?? (() => Date.now());
    this.retrieverFactory = config.retrieverFactory;
    this.retrieverSetup = config.retrieverSetup;
    this.readyPromise = this.load(config.initial ?? []);
  }

  async ready(): Promise<void> {
    await this.readyPromise;
  }

  async list(namespace: string): Promise<KnowledgeBaseDefinition[]> {
    await this.ready();
    return [...this.bases.values()]
      .filter((base) => base.definition.namespace === namespace)
      .map((base) => ({ ...base.definition }));
  }

  async get(namespace: string, id: string): Promise<KnowledgeBaseDefinition | null> {
    await this.ready();
    return this.bases.get(key(namespace, id))?.definition ?? null;
  }

  async create(input: KnowledgeBaseCreateInput): Promise<KnowledgeBaseDefinition> {
    await this.ready();
    validateId(input.id);
    const id = key(input.namespace, input.id);
    if ([...this.bases.values()].some((base) => base.definition.id === input.id)) {
      throw new Error("knowledge_base_exists");
    }
    if (this.bases.has(id)) throw new Error("knowledge_base_exists");
    const now = this.now();
    const definition: KnowledgeBaseDefinition = {
      id: input.id,
      namespace: input.namespace,
      ...(input.description ? { description: input.description } : {}),
      tags: normalizeTags(input.tags),
      metadata: { ...(input.metadata ?? {}) },
      provider: input.provider ?? "memory",
      status: "ready",
      sourceCount: 0,
      chunkCount: 0,
      createdAt: now,
      updatedAt: now,
    };
    const runtime: RuntimeBase = {
      definition,
      sources: new Map(),
      retriever: this.createRetriever(definition),
    };
    await this.setupRetriever(definition, runtime.retriever);
    this.bases.set(id, runtime);
    this.registry.register(toRegistryInput(definition, runtime.retriever));
    await this.persist(runtime);
    return { ...definition };
  }

  async update(
    namespace: string,
    id: string,
    patch: KnowledgeBaseUpdateInput,
  ): Promise<KnowledgeBaseDefinition> {
    await this.ready();
    const runtime = this.require(namespace, id);
    const definition: KnowledgeBaseDefinition = {
      ...runtime.definition,
      ...(patch.description !== undefined ? { description: patch.description || undefined } : {}),
      ...(patch.tags !== undefined ? { tags: normalizeTags(patch.tags) } : {}),
      ...(patch.metadata !== undefined ? { metadata: { ...patch.metadata } } : {}),
      updatedAt: this.now(),
    };
    runtime.definition = definition;
    this.registry.register(toRegistryInput(definition, runtime.retriever));
    await this.persist(runtime);
    return { ...definition };
  }

  async remove(namespace: string, id: string): Promise<void> {
    await this.ready();
    this.require(namespace, id);
    this.bases.delete(key(namespace, id));
    this.registry.unregister(id);
    await this.store.delete(namespace, id);
  }

  async listSources(namespace: string, id: string): Promise<KnowledgeBaseSourceRecord[]> {
    await this.ready();
    return [...this.require(namespace, id).sources.values()].map((source) => ({ ...source }));
  }

  async ingest(
    namespace: string,
    id: string,
    input: KnowledgeSourceInput,
  ): Promise<KnowledgeBaseSourceRecord> {
    await this.ready();
    if (!input.id.trim()) throw new Error("source_id_required");
    if (!input.text.trim()) throw new Error("source_text_required");
    const runtime = this.require(namespace, id);
    const now = this.now();
    const document: KnowledgeIngestDocument = {
      id: input.id,
      text: input.text,
      ...(input.title !== undefined && { title: input.title }),
      ...(input.uri !== undefined && { uri: input.uri }),
      ...(input.mimeType !== undefined && { mimeType: input.mimeType }),
      tags: normalizeTags(input.tags),
      metadata: { ...(input.metadata ?? {}) },
    };
    const sourceBase = toSourceRecord(document, now, "ready", 0);
    try {
      const existing = runtime.sources.get(input.id);
      if (existing) {
        const retriever = runtime.retriever as Retriever & {
          deleteSource?: (sourceId: string) => Promise<void>;
        };
        if (!retriever.deleteSource) throw new Error("retriever_source_replace_unsupported");
        await retriever.deleteSource(input.id);
      }
      runtime.sources.set(input.id, sourceBase);
      const chunks = await addDocument(runtime.retriever, document);
      const source = { ...sourceBase, chunkCount: chunks.length, updatedAt: this.now() };
      runtime.sources.set(input.id, source);
      runtime.definition = updateCounts(runtime.definition, runtime.sources, this.now());
      await this.persist(runtime);
      return { ...source };
    } catch (error) {
      const source = {
        ...sourceBase,
        status: "failed" as const,
        error: error instanceof Error ? error.message : String(error),
        updatedAt: this.now(),
      };
      runtime.sources.set(input.id, source);
      runtime.definition = { ...runtime.definition, status: "degraded", updatedAt: this.now() };
      await this.persist(runtime);
      throw error;
    }
  }

  async ingestFrom(
    namespace: string,
    id: string,
    kind: KnowledgeSourceKind,
    config: unknown,
  ): Promise<KnowledgeBaseSourceRecord[]> {
    await this.ready();
    const adapter = this.sourceAdapters.get(kind);
    if (!adapter) throw new Error("source_adapter_not_found");
    const documents = await adapter.load(config);
    const sources: KnowledgeBaseSourceRecord[] = [];
    for (const document of documents) sources.push(await this.ingest(namespace, id, document));
    return sources;
  }

  async removeSource(namespace: string, id: string, sourceId: string): Promise<void> {
    await this.ready();
    const runtime = this.require(namespace, id);
    if (!runtime.sources.has(sourceId)) throw new Error("source_not_found");
    const retriever = runtime.retriever as Retriever & {
      deleteSource?: (sourceId: string) => Promise<void>;
    };
    if (!retriever.deleteSource) throw new Error("retriever_source_delete_unsupported");
    await retriever.deleteSource(sourceId);
    runtime.sources.delete(sourceId);
    runtime.definition = updateCounts(runtime.definition, runtime.sources, this.now());
    await this.persist(runtime);
  }

  async chunks(namespace: string, id: string, sourceId?: string): Promise<KnowledgeBaseChunk[]> {
    await this.ready();
    const runtime = this.require(namespace, id);
    const sources = [...runtime.sources.values()].filter(
      (source) => !sourceId || source.id === sourceId,
    );
    const chunker = new FlatTextChunker();
    return sources.flatMap((source) =>
      chunker.chunk(toDocument(source)).map((chunk) => ({
        ...chunk,
        metadata: { ...(chunk.metadata ?? {}) },
      })),
    );
  }

  async search(
    namespace: string,
    id: string,
    request: Parameters<Retriever["retrieve"]>[0],
  ): Promise<RetrieveResult[]> {
    await this.ready();
    return this.require(namespace, id).retriever.retrieve(request);
  }

  private async load(
    initial: ReadonlyArray<
      KnowledgeBaseCreateInput & { documents?: ReadonlyArray<KnowledgeSourceInput> }
    >,
  ): Promise<void> {
    const stored = await this.store.load();
    for (const record of stored) await this.install(record);
    for (const input of initial) {
      if (this.bases.has(key(input.namespace, input.id))) continue;
      const now = this.now();
      const definition: KnowledgeBaseDefinition = {
        id: input.id,
        namespace: input.namespace,
        ...(input.description ? { description: input.description } : {}),
        tags: normalizeTags(input.tags),
        metadata: { ...(input.metadata ?? {}) },
        provider: input.provider ?? "memory",
        status: "ready",
        sourceCount: 0,
        chunkCount: 0,
        createdAt: now,
        updatedAt: now,
      };
      const runtime: RuntimeBase = {
        definition,
        sources: new Map(),
        retriever: this.createRetriever(definition),
      };
      await this.setupRetriever(definition, runtime.retriever);
      this.bases.set(key(input.namespace, input.id), runtime);
      this.registry.register(toRegistryInput(definition, runtime.retriever));
      for (const document of input.documents ?? []) {
        await this.ingestInitial(runtime, document);
      }
      await this.persist(runtime);
    }
  }

  private async install(record: StoredKnowledgeBase): Promise<void> {
    const retriever = this.createRetriever(
      record.definition,
      record.sources.filter((source) => source.status === "ready").map(toDocument),
    );
    await this.setupRetriever(record.definition, retriever);
    const runtime: RuntimeBase = {
      definition: record.definition,
      sources: new Map(record.sources.map((source) => [source.id, source])),
      retriever,
    };
    this.bases.set(key(record.definition.namespace, record.definition.id), runtime);
    this.registry.register(toRegistryInput(record.definition, retriever));
  }

  private createRetriever(
    definition: KnowledgeBaseDefinition,
    documents: ReadonlyArray<KnowledgeIngestDocument> = [],
  ): Retriever {
    if (this.retrieverFactory) return this.retrieverFactory(definition);
    if (definition.provider === "external") throw new Error("external_retriever_required");
    return new InMemoryRetriever({ documents });
  }

  private async setupRetriever(
    definition: KnowledgeBaseDefinition,
    retriever: Retriever,
  ): Promise<void> {
    await this.retrieverSetup?.(definition, retriever);
  }

  private async ingestInitial(runtime: RuntimeBase, input: KnowledgeSourceInput): Promise<void> {
    const now = this.now();
    const document: KnowledgeIngestDocument = {
      id: input.id,
      text: input.text,
      ...(input.title !== undefined && { title: input.title }),
      ...(input.uri !== undefined && { uri: input.uri }),
      ...(input.mimeType !== undefined && { mimeType: input.mimeType }),
      tags: normalizeTags(input.tags),
      metadata: { ...(input.metadata ?? {}) },
    };
    const chunks = await addDocument(runtime.retriever, document);
    runtime.sources.set(input.id, {
      ...toSourceRecord(document, now, "ready", chunks.length),
      updatedAt: this.now(),
    });
    runtime.definition = updateCounts(runtime.definition, runtime.sources, this.now());
  }

  private require(namespace: string, id: string): RuntimeBase {
    const runtime = this.bases.get(key(namespace, id));
    if (!runtime) throw new Error("knowledge_base_not_found");
    return runtime;
  }

  private async persist(runtime: RuntimeBase): Promise<void> {
    await this.store.save({
      definition: runtime.definition,
      sources: [...runtime.sources.values()],
    });
  }
}

/**
 * Fluent setup for managed KBs. Use it to keep the durable store, live
 * retriever registry, seed data, and retriever bootstrap hooks in one place.
 *
 * @example
 * ```ts
 * const knowledgeBases = createZoryaKnowledgeBasesBuilder()
 *   .store(stack.knowledgeBaseStore)
 *   .registry(sharedRetrievers)
 *   .retrieverFactory((definition) =>
 *     createPgVectorRetriever({ db, embeddings, dimensions: 1536, tableName: `kb_${definition.id}` }),
 *   )
 *   .retrieverSetup((_definition, retriever) =>
 *     (retriever as PgVectorRetriever).ensureSchema({ createExtension: true }),
 *   )
 *   .build();
 *
 * const agents = new ZoryaAgents({ retrievers: knowledgeBases.registry, ... });
 * ```
 */
export class ZoryaKnowledgeBasesBuilder {
  private config: ZoryaKnowledgeBasesConfig = {};
  private sourceAdapterRows: KnowledgeSourceAdapter[] = [];
  private initialRows: Array<
    KnowledgeBaseCreateInput & { documents?: ReadonlyArray<KnowledgeSourceInput> }
  > = [];

  registry(registry: RetrieverRegistry): this {
    this.config = { ...this.config, registry };
    return this;
  }

  store(store: KnowledgeBaseStore): this {
    this.config = { ...this.config, store };
    return this;
  }

  now(now: () => number): this {
    this.config = { ...this.config, now };
    return this;
  }

  retrieverFactory(factory: ZoryaKnowledgeBasesConfig["retrieverFactory"]): this {
    this.config = { ...this.config, retrieverFactory: factory };
    return this;
  }

  retrieverSetup(setup: ZoryaKnowledgeBasesConfig["retrieverSetup"]): this {
    this.config = { ...this.config, retrieverSetup: setup };
    return this;
  }

  sourceAdapters(sourceAdapters: KnowledgeSourceAdapterRegistry): this {
    this.config = { ...this.config, sourceAdapters };
    this.sourceAdapterRows = [];
    return this;
  }

  sourceAdapter(adapter: KnowledgeSourceAdapter): this {
    this.sourceAdapterRows = [...this.sourceAdapterRows, adapter];
    return this;
  }

  initial(
    input: KnowledgeBaseCreateInput & { documents?: ReadonlyArray<KnowledgeSourceInput> },
  ): this {
    this.initialRows = [...this.initialRows, input];
    return this;
  }

  initialMany(
    inputs: ReadonlyArray<
      KnowledgeBaseCreateInput & { documents?: ReadonlyArray<KnowledgeSourceInput> }
    >,
  ): this {
    this.initialRows = [...this.initialRows, ...inputs];
    return this;
  }

  build(): ZoryaKnowledgeBases {
    const sourceAdapters =
      this.config.sourceAdapters ??
      (this.sourceAdapterRows.length > 0
        ? new KnowledgeSourceAdapterRegistry([
            fileKnowledgeSourceAdapter,
            urlKnowledgeSourceAdapter,
            ...this.sourceAdapterRows,
          ])
        : undefined);
    return new ZoryaKnowledgeBases({
      ...this.config,
      ...(sourceAdapters !== undefined && { sourceAdapters }),
      ...(this.initialRows.length > 0 && { initial: this.initialRows }),
    });
  }
}

export function createZoryaKnowledgeBasesBuilder(): ZoryaKnowledgeBasesBuilder {
  return new ZoryaKnowledgeBasesBuilder();
}

function addDocument(
  retriever: Retriever,
  document: KnowledgeIngestDocument,
): Promise<KnowledgeChunk[]> {
  const ingestible = retriever as Retriever & {
    addDocument?: (document: KnowledgeIngestDocument) => Promise<KnowledgeChunk[]>;
  };
  if (!ingestible.addDocument) throw new Error("retriever_ingest_unsupported");
  return ingestible.addDocument(document);
}

function toRegistryInput(
  definition: KnowledgeBaseDefinition,
  retriever: Retriever,
): RegisterRetrieverInput {
  return {
    id: definition.id,
    retriever,
    ...(definition.description !== undefined && { description: definition.description }),
    tags: definition.tags,
    metadata: { ...definition.metadata, namespace: definition.namespace },
  };
}

function toSourceRecord(
  document: KnowledgeIngestDocument,
  now: number,
  status: KnowledgeSourceStatus,
  chunkCount: number,
): KnowledgeBaseSourceRecord {
  return {
    id: document.id,
    ...(document.title !== undefined && { title: document.title }),
    ...(document.uri !== undefined && { uri: document.uri }),
    ...(document.mimeType !== undefined && { mimeType: document.mimeType }),
    tags: normalizeTags(document.tags),
    metadata: { ...(document.metadata ?? {}) },
    text: document.text,
    status,
    chunkCount,
    createdAt: now,
    updatedAt: now,
  };
}

function toDocument(source: KnowledgeBaseSourceRecord): KnowledgeIngestDocument {
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

function updateCounts(
  definition: KnowledgeBaseDefinition,
  sources: ReadonlyMap<string, KnowledgeBaseSourceRecord>,
  updatedAt: number,
): KnowledgeBaseDefinition {
  const sourceCount = sources.size;
  const chunkCount = [...sources.values()].reduce((sum, source) => sum + source.chunkCount, 0);
  return {
    ...definition,
    sourceCount,
    chunkCount,
    status: [...sources.values()].some((source) => source.status === "failed")
      ? "degraded"
      : "ready",
    updatedAt,
  };
}

function normalizeTags(tags: ReadonlyArray<string> | undefined): string[] {
  return [...new Set((tags ?? []).map((tag) => tag.trim()).filter(Boolean))];
}

function validateId(id: string): void {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(id)) throw new Error("invalid_knowledge_base_id");
}

function key(namespace: string, id: string): string {
  return `${namespace}\u0000${id}`;
}

function cloneRecord(record: StoredKnowledgeBase): StoredKnowledgeBase {
  return structuredClone(record);
}
