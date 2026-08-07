import type { RetrieveRequest, RetrieveResult, Retriever } from "./types.ts";

export interface RetrieverRoute {
  readonly id: string;
  readonly retriever: Retriever;
  readonly description?: string;
  /** Optional tags describing when this route is useful. */
  readonly tags?: ReadonlyArray<string>;
  /** Predicate hook for custom query/metadata routing. */
  readonly when?: (request: RetrieveRequest) => boolean;
}

export interface RouterRetrieverConfig {
  readonly routes: ReadonlyArray<RetrieverRoute>;
  /**
   * Optional route selector. Defaults to routes whose `when` predicate passes,
   * then routes whose tags intersect `request.filter.tags`, falling back to all.
   */
  readonly selectRoutes?: (
    request: RetrieveRequest,
    routes: ReadonlyArray<RetrieverRoute>,
  ) => ReadonlyArray<RetrieverRoute>;
}

/**
 * Retriever that fans a query out to one or more child retrievers and merges
 * their source-bearing results. Useful for domain, tenant, or storage routing.
 */
export class RouterRetriever implements Retriever {
  private readonly routes: ReadonlyArray<RetrieverRoute>;
  private readonly selectRoutes?: RouterRetrieverConfig["selectRoutes"];

  constructor(config: RouterRetrieverConfig) {
    if (config.routes.length === 0) {
      throw new Error("RouterRetriever: at least one route is required");
    }
    this.routes = config.routes;
    this.selectRoutes = config.selectRoutes;
  }

  async retrieve(request: RetrieveRequest): Promise<RetrieveResult[]> {
    const selected =
      this.selectRoutes?.(request, this.routes) ?? defaultSelectRoutes(request, this.routes);
    const batches = await Promise.all(selected.map((route) => route.retriever.retrieve(request)));
    return mergeResults(batches.flat(), request.topK);
  }
}

export function createRouterRetriever(config: RouterRetrieverConfig): RouterRetriever {
  return new RouterRetriever(config);
}

function defaultSelectRoutes(
  request: RetrieveRequest,
  routes: ReadonlyArray<RetrieverRoute>,
): ReadonlyArray<RetrieverRoute> {
  const predicateMatches = routes.filter((route) => route.when?.(request) === true);
  if (predicateMatches.length > 0) return predicateMatches;

  const requestTags = request.filter?.tags;
  if (requestTags && requestTags.length > 0) {
    const tagSet = new Set(requestTags);
    const tagMatches = routes.filter((route) => route.tags?.some((tag) => tagSet.has(tag)));
    if (tagMatches.length > 0) return tagMatches;
  }

  return routes;
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
