import type { RetrieveRequest, RetrieveResult, Retriever } from "./types.ts";

export type RetrieveReranker = (
  request: RetrieveRequest,
  results: ReadonlyArray<RetrieveResult>,
) => Promise<ReadonlyArray<RetrieveResult>> | ReadonlyArray<RetrieveResult>;

export interface RerankingRetrieverConfig {
  readonly retriever: Retriever;
  readonly rerank: RetrieveReranker;
  /**
   * Fetch more candidates than the caller asked for before reranking.
   * Default 4. Set 1 to preserve the upstream topK.
   */
  readonly candidateMultiplier?: number;
}

/**
 * Retriever decorator that fetches a wider candidate set and lets a caller
 * supplied reranker reorder, filter, or rescore the results.
 */
export class RerankingRetriever implements Retriever {
  private readonly retriever: Retriever;
  private readonly rerank: RetrieveReranker;
  private readonly candidateMultiplier: number;

  constructor(config: RerankingRetrieverConfig) {
    this.retriever = config.retriever;
    this.rerank = config.rerank;
    this.candidateMultiplier = config.candidateMultiplier ?? 4;
    if (this.candidateMultiplier < 1) {
      throw new Error("RerankingRetriever: candidateMultiplier must be >= 1");
    }
  }

  async retrieve(request: RetrieveRequest): Promise<RetrieveResult[]> {
    const requestedTopK = request.topK;
    const candidateTopK =
      requestedTopK === undefined ? undefined : Math.ceil(requestedTopK * this.candidateMultiplier);
    const candidates = await this.retriever.retrieve({ ...request, topK: candidateTopK });
    const reranked = await this.rerank(request, candidates);
    return [...reranked].slice(0, requestedTopK ?? reranked.length);
  }
}

export function createRerankingRetriever(config: RerankingRetrieverConfig): RerankingRetriever {
  return new RerankingRetriever(config);
}
