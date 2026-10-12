import type { EmbeddingProvider } from "../embedding-provider.ts";
import { FlatTextChunker } from "./chunking.ts";
import {
  deletesVersion,
  type DeleteSourceRequest,
  type IngestibleRetriever,
  type KnowledgeChunk,
  type KnowledgeChunker,
  type KnowledgeIngestDocument,
  type RetrieveRequest,
  type RetrieveResult,
  type RetrieverFilter,
} from "./types.ts";

export interface InMemoryRetrieverConfig {
  /** Pre-chunked content loaded at construction. */
  readonly chunks?: ReadonlyArray<KnowledgeChunk>;
  /** Documents chunked with `chunker` at construction. */
  readonly documents?: ReadonlyArray<KnowledgeIngestDocument>;
  /** Chunker for `documents` and `addDocument`. Default `FlatTextChunker`. */
  readonly chunker?: KnowledgeChunker;
  /** Switches scoring from keyword overlap to cosine similarity. */
  readonly embeddings?: EmbeddingProvider;
  /** Result count when a request sets no `topK`. Default 8. */
  readonly defaultTopK?: number;
}

interface StoredChunk {
  chunk: KnowledgeChunk;
  embedding?: number[];
}

/**
 * In-process retriever for tests, demos, and small knowledge bases.
 *
 * With an embedding provider, search ranks by cosine similarity; chunks
 * loaded through the constructor are embedded lazily on first search.
 * Without one it falls back to keyword overlap. Results scoring 0 are
 * dropped.
 */
export class InMemoryRetriever implements IngestibleRetriever {
  private readonly chunks: StoredChunk[] = [];
  private readonly chunker: KnowledgeChunker;
  private readonly embeddings?: EmbeddingProvider;
  private readonly defaultTopK: number;

  constructor(config: InMemoryRetrieverConfig = {}) {
    this.chunker = config.chunker ?? new FlatTextChunker();
    if (config.embeddings) this.embeddings = config.embeddings;
    this.defaultTopK = config.defaultTopK ?? 8;
    for (const chunk of config.chunks ?? []) {
      this.chunks.push({ chunk });
    }
    for (const document of config.documents ?? []) {
      for (const chunk of this.chunker.chunk(document)) {
        this.chunks.push({ chunk });
      }
    }
  }

  /** Chunk a document, embed (when configured) and store the chunks. */
  async addDocument(document: KnowledgeIngestDocument): Promise<KnowledgeChunk[]> {
    const chunks = this.chunker.chunk(document);
    await this.addChunks(chunks);
    return chunks;
  }

  async addChunks(chunks: ReadonlyArray<KnowledgeChunk>): Promise<void> {
    for (const chunk of chunks) {
      this.chunks.push({
        chunk,
        ...(this.embeddings && { embedding: await this.embeddings.embed(chunk.text) }),
      });
    }
  }

  /** Drop a source's chunks: all, one version, or all but one (see `DeleteSourceRequest`). */
  async deleteSource(request: DeleteSourceRequest): Promise<void> {
    for (let index = this.chunks.length - 1; index >= 0; index -= 1) {
      const { source } = this.chunks[index]!.chunk;
      if (source.id !== request.sourceId) continue;
      if (deletesVersion({ request, version: source.version })) this.chunks.splice(index, 1);
    }
  }

  async retrieve(request: RetrieveRequest): Promise<RetrieveResult[]> {
    const query = request.query.trim();
    if (!query) return [];
    request.signal?.throwIfAborted();

    const candidates = this.chunks.filter(({ chunk }) =>
      matchesFilter({ chunk, filter: request.filter }),
    );
    const queryEmbedding = this.embeddings ? await this.embeddings.embed(query) : undefined;
    const scored: RetrieveResult[] = [];
    for (const candidate of candidates) {
      request.signal?.throwIfAborted();
      if (this.embeddings && !candidate.embedding) {
        candidate.embedding = await this.embeddings.embed(candidate.chunk.text);
      }
      scored.push({
        chunk: truncateChunk({ chunk: candidate.chunk, max: request.maxChunkCharacters }),
        score:
          queryEmbedding && candidate.embedding
            ? cosineSimilarity({ a: queryEmbedding, b: candidate.embedding })
            : keywordScore({ query, text: candidate.chunk.text }),
      });
    }

    return scored
      .filter((r) => r.score > 0)
      .sort((a, b) => b.score - a.score || a.chunk.id.localeCompare(b.chunk.id))
      .slice(0, request.topK ?? this.defaultTopK);
  }
}

function matchesFilter(params: {
  chunk: KnowledgeChunk;
  filter: RetrieverFilter | undefined;
}): boolean {
  const { chunk, filter } = params;
  if (!filter) return true;
  if (filter.tags) {
    const tags = new Set(chunk.source.tags ?? []);
    for (const tag of filter.tags) {
      if (!tags.has(tag)) return false;
    }
  }
  if (filter.metadata) {
    const metadata = { ...chunk.source.metadata, ...chunk.metadata };
    for (const [key, value] of Object.entries(filter.metadata)) {
      if (metadata[key] !== value) return false;
    }
  }
  return true;
}

function cosineSimilarity(params: { a: readonly number[]; b: readonly number[] }): number {
  const { a, b } = params;
  let dot = 0;
  let magA = 0;
  let magB = 0;
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    dot += a[i]! * b[i]!;
    magA += a[i]! ** 2;
    magB += b[i]! ** 2;
  }
  const denom = Math.sqrt(magA) * Math.sqrt(magB);
  return denom === 0 ? 0 : dot / denom;
}

function keywordScore(params: { query: string; text: string }): number {
  const queryWords = tokenize(params.query);
  if (queryWords.size === 0) return 0;
  const textWords = tokenize(params.text);
  let matches = 0;
  for (const word of queryWords) {
    if (textWords.has(word)) matches += 1;
  }
  return matches / queryWords.size;
}

function tokenize(text: string): Set<string> {
  return new Set(
    text
      .toLowerCase()
      .split(/\W+/)
      .filter((word) => word.length > 2),
  );
}

/** Trim a chunk's text to `max` characters; returns the chunk as-is when it fits. */
function truncateChunk(params: { chunk: KnowledgeChunk; max: number | undefined }): KnowledgeChunk {
  const { chunk, max } = params;
  if (max === undefined || chunk.text.length <= max) return chunk;
  return { ...chunk, text: chunk.text.slice(0, Math.max(0, max)).trimEnd() };
}
