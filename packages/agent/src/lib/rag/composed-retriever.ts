import { mergeResults } from "./result-utils.ts";
import type { RetrieveRequest, RetrieveResult, Retriever } from "./types.ts";

export interface CombinedRetrieverSource {
  readonly id?: string;
  readonly retriever: Retriever;
  /** Score multiplier applied before the merge. Default 1. */
  readonly weight?: number;
}

export interface CombinedRetrieverConfig {
  readonly sources: ReadonlyArray<CombinedRetrieverSource>;
}

export interface FallbackRetrieverSource {
  readonly id?: string;
  readonly retriever: Retriever;
  /** Minimum result count to accept this source's answer. Default 1. */
  readonly minResults?: number;
  /** Minimum best score to accept this source's answer. */
  readonly minScore?: number;
}

export interface FallbackRetrieverConfig {
  readonly sources: ReadonlyArray<FallbackRetrieverSource>;
}

/**
 * Retriever that queries every source in parallel and returns one deduped,
 * score-sorted result set, with each source's scores scaled by its `weight`.
 * Use it when sources are complementary and should all contribute.
 */
export class CombinedRetriever implements Retriever {
  private readonly sources: ReadonlyArray<{ retriever: Retriever; weight: number }>;

  constructor(config: CombinedRetrieverConfig) {
    if (config.sources.length === 0) {
      throw new Error("CombinedRetriever: at least one source is required");
    }
    this.sources = config.sources.map((source) => {
      const weight = source.weight ?? 1;
      if (!Number.isFinite(weight) || weight < 0) {
        throw new Error("CombinedRetriever: source weight must be a non-negative number");
      }
      return { retriever: source.retriever, weight };
    });
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
    return mergeResults({ results: batches.flat(), topK: request.topK });
  }
}

/**
 * Retriever that tries sources in priority order and returns the first
 * result set that passes that source's quality gate (`minResults`,
 * `minScore`). When none passes, the last source's results are returned.
 */
export class FallbackRetriever implements Retriever {
  private readonly sources: ReadonlyArray<{
    retriever: Retriever;
    minResults: number;
    minScore: number | undefined;
  }>;

  constructor(config: FallbackRetrieverConfig) {
    if (config.sources.length === 0) {
      throw new Error("FallbackRetriever: at least one source is required");
    }
    this.sources = config.sources.map((source) => {
      const minResults = source.minResults ?? 1;
      if (!Number.isInteger(minResults) || minResults < 1) {
        throw new Error("FallbackRetriever: minResults must be a positive integer");
      }
      return { retriever: source.retriever, minResults, minScore: source.minScore };
    });
  }

  async retrieve(request: RetrieveRequest): Promise<RetrieveResult[]> {
    let lastResults: RetrieveResult[] = [];
    for (const source of this.sources) {
      const results = await source.retriever.retrieve(request);
      lastResults = results.slice(0, request.topK ?? results.length);
      if (passesGate({ results: lastResults, ...source })) return lastResults;
    }
    return lastResults;
  }
}

function passesGate(params: {
  results: ReadonlyArray<RetrieveResult>;
  minResults: number;
  minScore: number | undefined;
}): boolean {
  if (params.results.length < params.minResults) return false;
  if (params.minScore === undefined) return true;
  const best = Math.max(...params.results.map((result) => result.score));
  return best >= params.minScore;
}
