import type { RetrieveRequest, RetrieveResult, Retriever } from "./types.ts";

export interface CombinedRetrieverSource {
  readonly id?: string;
  readonly retriever: Retriever;
  /** Score multiplier applied before merge. Default 1. */
  readonly weight?: number;
}

export interface CombinedRetrieverConfig {
  readonly sources: ReadonlyArray<CombinedRetrieverSource>;
}

export interface FallbackRetrieverSource {
  readonly id?: string;
  readonly retriever: Retriever;
  /** Minimum accepted result count before trying the next source. Default 1. */
  readonly minResults?: number;
  /** Minimum accepted best score before trying the next source. */
  readonly minScore?: number;
}

export interface FallbackRetrieverConfig {
  readonly sources: ReadonlyArray<FallbackRetrieverSource>;
}

interface NormalizedFallbackRetrieverSource {
  readonly id: string;
  readonly retriever: Retriever;
  readonly minResults: number;
  readonly minScore?: number;
}

/**
 * Retriever that queries every source and returns one deduped, score-sorted
 * result set. Use it when sources are complementary and should all contribute.
 */
export class CombinedRetriever implements Retriever {
  private readonly sources: ReadonlyArray<Required<CombinedRetrieverSource>>;

  constructor(config: CombinedRetrieverConfig) {
    if (config.sources.length === 0) {
      throw new Error("CombinedRetriever: at least one source is required");
    }
    this.sources = config.sources.map((source, index) => ({
      id: source.id ?? `source-${index}`,
      retriever: source.retriever,
      weight: source.weight ?? 1,
    }));
    for (const source of this.sources) {
      if (!Number.isFinite(source.weight) || source.weight < 0) {
        throw new Error("CombinedRetriever: source weight must be a non-negative number");
      }
    }
  }

  async retrieve(request: RetrieveRequest): Promise<RetrieveResult[]> {
    const batches = await Promise.all(
      this.sources.map(async (source) =>
        (await source.retriever.retrieve(request)).map((result) => ({
          ...result,
          score: result.score * source.weight,
        })),
      ),
    );
    return mergeResults(batches.flat(), request.topK);
  }
}

/**
 * Retriever that tries sources in priority order and returns the first result
 * set that satisfies its configured quality gate.
 */
export class FallbackRetriever implements Retriever {
  private readonly sources: ReadonlyArray<NormalizedFallbackRetrieverSource>;

  constructor(config: FallbackRetrieverConfig) {
    if (config.sources.length === 0) {
      throw new Error("FallbackRetriever: at least one source is required");
    }
    this.sources = config.sources.map((source, index) => ({
      id: source.id ?? `source-${index}`,
      retriever: source.retriever,
      minResults: source.minResults ?? 1,
      minScore: source.minScore,
    }));
    for (const source of this.sources) {
      if (!Number.isInteger(source.minResults) || source.minResults < 1) {
        throw new Error("FallbackRetriever: minResults must be a positive integer");
      }
    }
  }

  async retrieve(request: RetrieveRequest): Promise<RetrieveResult[]> {
    let lastResults: RetrieveResult[] = [];
    for (const source of this.sources) {
      const results = await source.retriever.retrieve(request);
      lastResults = [...results].slice(0, request.topK ?? results.length);
      if (passesFallbackGate(lastResults, source)) return lastResults;
    }
    return lastResults;
  }
}

export function createCombinedRetriever(config: CombinedRetrieverConfig): CombinedRetriever {
  return new CombinedRetriever(config);
}

export function createFallbackRetriever(config: FallbackRetrieverConfig): FallbackRetriever {
  return new FallbackRetriever(config);
}

function passesFallbackGate(
  results: ReadonlyArray<RetrieveResult>,
  source: Pick<NormalizedFallbackRetrieverSource, "minResults" | "minScore">,
): boolean {
  if (results.length < source.minResults) return false;
  if (source.minScore === undefined) return true;
  return Math.max(...results.map((result) => result.score)) >= source.minScore;
}

function mergeResults(
  results: ReadonlyArray<RetrieveResult>,
  topK: number | undefined,
): RetrieveResult[] {
  const bestByChunk = new Map<string, RetrieveResult>();
  for (const result of results) {
    const existing = bestByChunk.get(result.chunk.id);
    if (!existing || result.score > existing.score) {
      bestByChunk.set(result.chunk.id, result);
    }
  }
  return [...bestByChunk.values()]
    .sort((a, b) => b.score - a.score || a.chunk.id.localeCompare(b.chunk.id))
    .slice(0, topK ?? bestByChunk.size);
}
