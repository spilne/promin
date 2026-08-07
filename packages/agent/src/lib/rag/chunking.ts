import type { KnowledgeChunk, KnowledgeChunker, KnowledgeIngestDocument } from "./types.ts";

export interface FlatTextChunkerConfig {
  /** Preferred split delimiter. Defaults to blank-line paragraphs. */
  readonly delimiter?: string | RegExp;
  /** Target max chunk size in characters. Default 1000. */
  readonly maxSize?: number;
  /** Minimum non-whitespace chunk size in characters. Default 1. */
  readonly minSize?: number;
  /** Characters copied from the prior chunk into the next chunk. Default 100. */
  readonly overlap?: number;
}

/**
 * Simple paragraph-oriented chunker for RAG ingestion.
 *
 * This intentionally starts small: it is deterministic, dependency-free, and
 * good enough for docs/FAQ/markdown content. More advanced parent-child and
 * parser-specific chunkers can implement the same `KnowledgeChunker` contract.
 */
export class FlatTextChunker implements KnowledgeChunker {
  private readonly delimiter: string | RegExp;
  private readonly maxSize: number;
  private readonly minSize: number;
  private readonly overlap: number;

  constructor(config: FlatTextChunkerConfig = {}) {
    this.delimiter = config.delimiter ?? /\n\s*\n/g;
    this.maxSize = config.maxSize ?? 1000;
    this.minSize = config.minSize ?? 1;
    this.overlap = config.overlap ?? 100;
    if (this.maxSize < 1) throw new Error("FlatTextChunker: maxSize must be >= 1");
    if (this.minSize < 0) throw new Error("FlatTextChunker: minSize must be >= 0");
    if (this.overlap < 0) throw new Error("FlatTextChunker: overlap must be >= 0");
    if (this.overlap >= this.maxSize) {
      throw new Error("FlatTextChunker: overlap must be smaller than maxSize");
    }
  }

  chunk(document: KnowledgeIngestDocument): KnowledgeChunk[] {
    const parts = document.text
      .split(this.delimiter)
      .map((p) => p.trim())
      .filter((p) => p.length >= this.minSize);

    const chunks: string[] = [];
    let current = "";
    for (const part of parts.length > 0 ? parts : [document.text.trim()]) {
      if (!part) continue;
      if (part.length > this.maxSize) {
        if (current) {
          chunks.push(current);
          current = "";
        }
        chunks.push(...splitLongText(part, this.maxSize, this.overlap, this.minSize));
        continue;
      }
      const next = current ? `${current}\n\n${part}` : part;
      if (next.length <= this.maxSize) {
        current = next;
      } else {
        chunks.push(current);
        current = withOverlap(current, this.overlap, part);
      }
    }
    if (current) chunks.push(current);

    return chunks
      .map((text) => text.trim())
      .filter((text) => text.length >= this.minSize)
      .map((text, index) => ({
        id: `${document.id}#${index}`,
        text,
        index,
        source: {
          id: document.id,
          title: document.title,
          uri: document.uri,
          mimeType: document.mimeType,
          tags: document.tags,
          metadata: document.metadata,
        },
      }));
  }
}

export function createFlatTextChunker(config: FlatTextChunkerConfig = {}): FlatTextChunker {
  return new FlatTextChunker(config);
}

function splitLongText(text: string, maxSize: number, overlap: number, minSize: number): string[] {
  const chunks: string[] = [];
  let start = 0;
  while (start < text.length) {
    const end = Math.min(text.length, start + maxSize);
    const chunk = text.slice(start, end).trim();
    if (chunk.length >= minSize) chunks.push(chunk);
    if (end === text.length) break;
    start = Math.max(end - overlap, start + 1);
  }
  return chunks;
}

function withOverlap(previous: string, overlap: number, next: string): string {
  if (overlap === 0) return next;
  const prefix = previous.slice(Math.max(0, previous.length - overlap)).trim();
  return prefix ? `${prefix}\n\n${next}` : next;
}
