import type { KnowledgeChunk, RetrieveResult } from "./types.ts";

/** Trim a chunk's text to `max` characters; returns the chunk as-is when it fits. */
export function truncateChunk(params: {
  chunk: KnowledgeChunk;
  max: number | undefined;
}): KnowledgeChunk {
  const { chunk, max } = params;
  if (max === undefined || chunk.text.length <= max) return chunk;
  return { ...chunk, text: chunk.text.slice(0, Math.max(0, max)).trimEnd() };
}

/**
 * Merge result sets from several retrievers: keep the best-scoring result
 * per chunk id, sort by score (ties broken by chunk id for determinism),
 * and cap at `topK`.
 */
export function mergeResults(params: {
  results: ReadonlyArray<RetrieveResult>;
  topK: number | undefined;
}): RetrieveResult[] {
  const bestByChunk = new Map<string, RetrieveResult>();
  for (const result of params.results) {
    const existing = bestByChunk.get(result.chunk.id);
    if (!existing || result.score > existing.score) {
      bestByChunk.set(result.chunk.id, result);
    }
  }
  return [...bestByChunk.values()]
    .sort((a, b) => b.score - a.score || a.chunk.id.localeCompare(b.chunk.id))
    .slice(0, params.topK ?? bestByChunk.size);
}
