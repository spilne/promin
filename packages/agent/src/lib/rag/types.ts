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
}

export interface KnowledgeChunker {
  chunk(document: KnowledgeIngestDocument): KnowledgeChunk[];
}
