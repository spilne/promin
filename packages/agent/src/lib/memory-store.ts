import { randomUUID } from "node:crypto";

export interface MemoryEntry {
  id: string;
  content: string;
  metadata?: Record<string, unknown>;
  createdAt: Date;
}

export interface MemoryScope {
  /** User, org, or entity identifier. */
  resourceId?: string;
  /** Conversation or session identifier. */
  threadId?: string;
}

export interface EmbeddingProvider {
  embed(text: string): Promise<number[]>;
}

export interface MemoryStore {
  save(
    entry: { content: string; metadata?: Record<string, unknown> },
    scope?: MemoryScope,
  ): Promise<string>;
  search(query: string, limit?: number, scope?: MemoryScope): Promise<MemoryEntry[]>;
  list(limit?: number, scope?: MemoryScope): Promise<MemoryEntry[]>;
  delete(id: string): Promise<void>;
}

// ---- scoring helpers ----

function cosineSimilarity(a: number[], b: number[]): number {
  let dot = 0;
  let magA = 0;
  let magB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    magA += a[i] ** 2;
    magB += b[i] ** 2;
  }
  const denom = Math.sqrt(magA) * Math.sqrt(magB);
  return denom === 0 ? 0 : dot / denom;
}

function keywordScore(query: string, content: string): number {
  const words = (s: string) =>
    new Set(
      s
        .toLowerCase()
        .split(/\W+/)
        .filter((w) => w.length > 2),
    );
  const queryWords = words(query);
  const contentWords = words(content);
  if (queryWords.size === 0) return 0;
  let matches = 0;
  for (const w of queryWords) {
    if (contentWords.has(w)) matches++;
  }
  return matches / queryWords.size;
}

// ---- scope helpers ----

function scopeMatches(
  entryScope: MemoryScope | undefined,
  queryScope: MemoryScope | undefined,
): boolean {
  // No query scope → global namespace: only match entries with no scope
  if (!queryScope) return !entryScope;
  // Scoped query, unscoped entry → no match
  if (!entryScope) return false;
  if (queryScope.resourceId !== undefined && entryScope.resourceId !== queryScope.resourceId)
    return false;
  if (queryScope.threadId !== undefined && entryScope.threadId !== queryScope.threadId)
    return false;
  return true;
}

// ---- InMemoryMemoryStore ----

interface StoredEntry {
  entry: MemoryEntry;
  embedding?: number[];
  scope?: MemoryScope;
}

export interface InMemoryMemoryStoreConfig {
  /**
   * Optional embedding provider for semantic search.
   * Without one, falls back to keyword overlap scoring.
   */
  embeddings?: EmbeddingProvider;
}

export class InMemoryMemoryStore implements MemoryStore {
  private readonly entries: StoredEntry[] = [];
  private readonly embeddings?: EmbeddingProvider;

  constructor(config: InMemoryMemoryStoreConfig = {}) {
    this.embeddings = config.embeddings;
  }

  async save(
    input: { content: string; metadata?: Record<string, unknown> },
    scope?: MemoryScope,
  ): Promise<string> {
    const id = randomUUID();
    const entry: MemoryEntry = {
      id,
      content: input.content,
      metadata: input.metadata,
      createdAt: new Date(),
    };

    const embedding = this.embeddings ? await this.embeddings.embed(input.content) : undefined;
    this.entries.push({ entry, embedding, scope });
    return id;
  }

  async search(query: string, limit = 5, scope?: MemoryScope): Promise<MemoryEntry[]> {
    const candidates = this.entries.filter((e) => scopeMatches(e.scope, scope));
    if (candidates.length === 0) return [];

    if (this.embeddings) {
      const queryEmbedding = await this.embeddings.embed(query);
      return candidates
        .map(({ entry, embedding }) => ({
          entry,
          score: embedding ? cosineSimilarity(queryEmbedding, embedding) : 0,
        }))
        .sort((a, b) => b.score - a.score)
        .slice(0, limit)
        .map(({ entry }) => entry);
    }

    return candidates
      .map(({ entry }) => ({ entry, score: keywordScore(query, entry.content) }))
      .filter(({ score }) => score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, limit)
      .map(({ entry }) => entry);
  }

  async list(limit?: number, scope?: MemoryScope): Promise<MemoryEntry[]> {
    const all = this.entries
      .filter((e) => scopeMatches(e.scope, scope))
      .reverse()
      .map(({ entry }) => entry);
    return limit ? all.slice(0, limit) : all;
  }

  async delete(id: string): Promise<void> {
    const idx = this.entries.findIndex(({ entry }) => entry.id === id);
    if (idx !== -1) this.entries.splice(idx, 1);
  }
}
