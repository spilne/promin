// ---------------------------------------------------------------------------
// RAG contracts — the source-bearing chunk shape every retriever returns,
// the request a retriever answers, and the chunker contract ingestion uses.
// Concrete stores (in-memory, pgvector, ...) implement `Retriever`; the
// agent side (`createRetrieverTool`, `LocalAgentConfig.retrievers`) only
// ever sees this interface.
// ---------------------------------------------------------------------------

export interface KnowledgeSource {
  /** Stable source id, such as a document id, URL, or file path. */
  readonly id: string;
  /** Human-readable source title shown in citations and traces. */
  readonly title?: string;
  /** Optional URL/path backing this source. */
  readonly uri?: string;
  /** Optional MIME/content type for ingestion and UI display. */
  readonly mimeType?: string;
  /** Tags used for filtering, tenancy, ownership, or category routing. */
  readonly tags?: ReadonlyArray<string>;
  /** Extra host-defined metadata. */
  readonly metadata?: Readonly<Record<string, unknown>>;
  /**
   * Which ingestion of the source the chunk came from. Chunks of different
   * versions live side by side until the old ones are deleted, so a
   * re-ingestion never leaves the source unsearchable and a superseded one
   * can be cleaned up exactly (`IngestibleRetriever.deleteSource`).
   */
  readonly version?: string;
}

export interface KnowledgeChunk {
  /** Stable chunk id. */
  readonly id: string;
  /** Chunk text passed to embedding models and returned to agents. */
  readonly text: string;
  /** Original source this chunk came from. */
  readonly source: KnowledgeSource;
  /** Zero-based chunk index within the source. */
  readonly index: number;
  /** Optional parent chunk id for hierarchical/parent-child retrieval. */
  readonly parentId?: string;
  /** Extra chunk-level metadata. */
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface RetrieverFilter {
  /** Require all listed tags to exist on the chunk source. */
  readonly tags?: ReadonlyArray<string>;
  /** Exact-match metadata filters against source metadata and chunk metadata. */
  readonly metadata?: Readonly<Record<string, string | number | boolean>>;
}

export interface RetrieveRequest {
  readonly query: string;
  /** Number of chunks to return. Default is retriever-specific. */
  readonly topK?: number;
  /** Optional metadata/tag restriction. */
  readonly filter?: RetrieverFilter;
  /** Truncate returned chunk text for prompt/tool output. */
  readonly maxChunkCharacters?: number;
  /**
   * Cancels the search (a timeout, a cancelled turn). Retrievers stop at
   * their next step and reject with the signal's reason.
   */
  readonly signal?: AbortSignal;
}

export interface RetrieveResult {
  readonly chunk: KnowledgeChunk;
  /** Higher means more relevant. Exact scoring scale is retriever-specific. */
  readonly score: number;
}

export interface Retriever {
  retrieve(request: RetrieveRequest): Promise<RetrieveResult[]>;
}

export interface KnowledgeIngestDocument {
  readonly id: string;
  readonly text: string;
  readonly title?: string;
  readonly uri?: string;
  readonly mimeType?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
  /** Version stamped on the document's chunks (`KnowledgeSource.version`). */
  readonly version?: string;
}

/** Which chunks of a source `IngestibleRetriever.deleteSource` drops. */
export interface DeleteSourceRequest {
  readonly sourceId: string;
  /** Only the chunks of this version. */
  readonly version?: string;
  /** Every chunk except those of this version (older and unversioned ones). */
  readonly keepVersion?: string;
}

/**
 * A retriever that also takes documents in and drops them by source: the
 * contract managed knowledge bases ingest through. `InMemoryRetriever` and
 * `PgVectorRetriever` implement it.
 */
export interface IngestibleRetriever extends Retriever {
  /**
   * Chunk, index and store a document; returns the stored chunks. Chunks of
   * a versioned document get version-scoped ids, so they never overwrite
   * another version's.
   */
  addDocument(document: KnowledgeIngestDocument): Promise<KnowledgeChunk[]>;
  /**
   * Drop chunks of a source: all of them, one version, or all but one
   * version (see `DeleteSourceRequest`). Dropping what is absent is a no-op.
   */
  deleteSource(request: DeleteSourceRequest): Promise<void>;
}

/** Whether a chunk of `version` is dropped by `request` (its source id already matched). */
export function deletesVersion(params: {
  readonly request: DeleteSourceRequest;
  readonly version: string | undefined;
}): boolean {
  const { request, version } = params;
  if (request.version !== undefined && version !== request.version) return false;
  if (request.keepVersion !== undefined && version === request.keepVersion) return false;
  return true;
}

export interface KnowledgeChunker {
  chunk(document: KnowledgeIngestDocument): KnowledgeChunk[];
}
