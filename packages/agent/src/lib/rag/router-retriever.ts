import { mergeResults } from "./result-utils.ts";
import type { RetrieveRequest, RetrieveResult, Retriever } from "./types.ts";

export interface RetrieverRoute {
  readonly id: string;
  readonly retriever: Retriever;
  readonly description?: string;
  /** Tags describing when this route is useful; matched against `request.filter.tags`. */
  readonly tags?: ReadonlyArray<string>;
  /** Predicate hook for custom query/metadata routing. */
  readonly when?: (request: RetrieveRequest) => boolean;
}

export interface RouterRetrieverConfig {
  readonly routes: ReadonlyArray<RetrieverRoute>;
  /**
   * Custom route selector. Default: routes whose `when` predicate passes;
   * else routes whose tags intersect `request.filter.tags`; else all routes.
   */
  readonly selectRoutes?: (params: {
    readonly request: RetrieveRequest;
    readonly routes: ReadonlyArray<RetrieverRoute>;
  }) => ReadonlyArray<RetrieverRoute>;
}

/**
 * Retriever that fans a query out to the matching child retrievers and
 * merges their results (best score per chunk id wins). Useful for domain,
 * tenant, or storage routing.
 */
export class RouterRetriever implements Retriever {
  private readonly routes: ReadonlyArray<RetrieverRoute>;
  private readonly selectRoutes: NonNullable<RouterRetrieverConfig["selectRoutes"]>;

  constructor(config: RouterRetrieverConfig) {
    if (config.routes.length === 0) {
      throw new Error("RouterRetriever: at least one route is required");
    }
    this.routes = config.routes;
    this.selectRoutes = config.selectRoutes ?? defaultSelectRoutes;
  }

  async retrieve(request: RetrieveRequest): Promise<RetrieveResult[]> {
    const selected = this.selectRoutes({ request, routes: this.routes });
    const batches = await Promise.all(selected.map((route) => route.retriever.retrieve(request)));
    return mergeResults({ results: batches.flat(), topK: request.topK });
  }
}

function defaultSelectRoutes(params: {
  readonly request: RetrieveRequest;
  readonly routes: ReadonlyArray<RetrieverRoute>;
}): ReadonlyArray<RetrieverRoute> {
  const { request, routes } = params;
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
