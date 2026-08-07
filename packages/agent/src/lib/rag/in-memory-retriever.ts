import type { EmbeddingProvider } from "../memory-index.ts";
import { FlatTextChunker } from "./chunking.ts";
import type {
  KnowledgeChunk,
  KnowledgeChunker,
  KnowledgeIngestDocument,
  RetrieveRequest,
  RetrieveResult,
  Retriever,
  RetrieverFilter,
} from "./types.ts";

export interface InMemoryRetrieverConfig {
  readonly chunks?: ReadonlyArray<KnowledgeChunk>;
  readonly documents?: ReadonlyArray<KnowledgeIngestDocument>;
  readonly chunker?: KnowledgeChunker;
  readonly embeddings?: EmbeddingProvider;
  readonly defaultTopK?: number;
}

interface StoredChunk {
  chunk: KnowledgeChunk;
  embedding?: number[];
}

/**
 * In-process retriever for tests, demos, and small knowledge bases.
 *
 * When an embedding provider is configured, search uses cosine similarity.
 * Otherwise it falls back to keyword overlap. Production stores can implement
 * `Retriever` directly while keeping the same result/source contract.
 */
export class InMemoryRetriever implements Retriever {
  private readonly chunks: StoredChunk[] = [];
  private readonly chunker: KnowledgeChunker;
  private readonly embeddings?: EmbeddingProvider;
  private readonly defaultTopK: number;

  constructor(config: InMemoryRetrieverConfig = {}) {
    this.chunker = config.chunker ?? new FlatTextChunker();
    this.embeddings = config.embeddings;
    this.defaultTopK = config.defaultTopK ?? 8;
    if (config.chunks) {
      for (const chunk of config.chunks) {
        this.chunks.push({ chunk });
      }
    }
    if (config.documents) {
      for (const document of config.documents) {
        for (const chunk of this.chunker.chunk(document)) {
          this.chunks.push({ chunk });
        }
      }
    }
  }

  async addDocument(document: KnowledgeIngestDocument): Promise<KnowledgeChunk[]> {
    const chunks = this.chunker.chunk(document);
    await this.addChunks(chunks);
    return chunks;
  }

  async addChunks(chunks: ReadonlyArray<KnowledgeChunk>): Promise<void> {
    for (const chunk of chunks) {
      this.chunks.push({
        chunk,
        embedding: this.embeddings ? await this.embeddings.embed(chunk.text) : undefined,
      });
    }
  }

  async retrieve(request: RetrieveRequest): Promise<RetrieveResult[]> {
    const query = request.query.trim();
    if (!query) return [];

    const candidates = this.chunks.filter(({ chunk }) => matchesFilter(chunk, request.filter));
    const queryEmbedding = this.embeddings ? await this.embeddings.embed(query) : undefined;
    const scored: RetrieveResult[] = [];
    for (const candidate of candidates) {
      if (this.embeddings && !candidate.embedding) {
        candidate.embedding = await this.embeddings.embed(candidate.chunk.text);
      }
      scored.push({
        chunk: truncateChunk(candidate.chunk, request.maxChunkCharacters),
        score:
          queryEmbedding && candidate.embedding
            ? cosineSimilarity(queryEmbedding, candidate.embedding)
            : keywordScore(query, candidate.chunk.text),
      });
    }

    return scored
      .filter((r) => r.score > 0)
      .sort((a, b) => b.score - a.score || a.chunk.id.localeCompare(b.chunk.id))
      .slice(0, request.topK ?? this.defaultTopK);
  }
}

export function createInMemoryRetriever(config: InMemoryRetrieverConfig = {}): InMemoryRetriever {
  return new InMemoryRetriever(config);
}

function matchesFilter(chunk: KnowledgeChunk, filter: RetrieverFilter | undefined): boolean {
  if (!filter) return true;
  if (filter.tags) {
    const tags = new Set(chunk.source.tags ?? []);
    for (const tag of filter.tags) {
      if (!tags.has(tag)) return false;
    }
  }
  if (filter.metadata) {
    const metadata = { ...(chunk.source.metadata ?? {}), ...(chunk.metadata ?? {}) };
    for (const [key, value] of Object.entries(filter.metadata)) {
      if (metadata[key] !== value) return false;
    }
  }
  return true;
}

function truncateChunk(chunk: KnowledgeChunk, max: number | undefined): KnowledgeChunk {
  if (max === undefined || chunk.text.length <= max) return chunk;
  return { ...chunk, text: chunk.text.slice(0, Math.max(0, max)).trimEnd() };
}

function cosineSimilarity(a: readonly number[], b: readonly number[]): number {
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

function keywordScore(query: string, text: string): number {
  const queryWords = tokenize(query);
  if (queryWords.size === 0) return 0;
  const textWords = tokenize(text);
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
