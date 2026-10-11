import type { RetrieveRequest, RetrieveResult, Retriever } from "./types.ts";

/** Reorder, filter, or rescore a candidate set for a request. */
export type RetrieveReranker = (params: {
  readonly request: RetrieveRequest;
  readonly results: ReadonlyArray<RetrieveResult>;
}) => Promise<ReadonlyArray<RetrieveResult>> | ReadonlyArray<RetrieveResult>;

export interface RerankingRetrieverConfig {
  readonly retriever: Retriever;
  readonly rerank: RetrieveReranker;
  /**
   * Fetch `topK * candidateMultiplier` candidates before reranking.
   * Default 4. Set 1 to keep the caller's topK upstream.
   */
  readonly candidateMultiplier?: number;
}

/**
 * Retriever decorator that fetches a wider candidate set and lets a
 * caller-supplied reranker (a cross-encoder, an LLM judge, a business rule)
 * reorder it before the result is cut back to the requested `topK`.
 */
export class RerankingRetriever implements Retriever {
  private readonly retriever: Retriever;
  private readonly rerank: RetrieveReranker;
  private readonly candidateMultiplier: number;

  constructor(config: RerankingRetrieverConfig) {
    this.retriever = config.retriever;
    this.rerank = config.rerank;
    this.candidateMultiplier = config.candidateMultiplier ?? 4;
    if (!Number.isFinite(this.candidateMultiplier) || this.candidateMultiplier < 1) {
      throw new Error("RerankingRetriever: candidateMultiplier must be >= 1");
    }
  }

  async retrieve(request: RetrieveRequest): Promise<RetrieveResult[]> {
    const requestedTopK = request.topK;
    const candidates = await this.retriever.retrieve(
      requestedTopK === undefined
        ? request
        : { ...request, topK: Math.ceil(requestedTopK * this.candidateMultiplier) },
    );
    const reranked = await this.rerank({ request, results: candidates });
    return reranked.slice(0, requestedTopK ?? reranked.length);
  }
}
