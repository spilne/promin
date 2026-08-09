import type { RetrieverRegistry } from "@promin/agent";
import { json, jsonError } from "../router.ts";

export interface KnowledgeBaseCatalogEntry {
  readonly id: string;
  readonly description?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
}

export interface KnowledgeBaseSearchResult {
  readonly id: string;
  readonly score: number;
  readonly text: string;
  readonly source: {
    readonly id: string;
    readonly title?: string;
    readonly uri?: string;
    readonly tags: ReadonlyArray<string>;
  };
  readonly index: number;
}

export function listKnowledgeBases(deps: { retrievers: RetrieverRegistry }) {
  return async (): Promise<Response> => {
    try {
      return json(200, {
        knowledgeBases: deps.retrievers.list().map(
          (r) =>
            ({
              id: r.id,
              ...(r.description ? { description: r.description } : {}),
              tags: r.tags,
              metadata: r.metadata,
            }) satisfies KnowledgeBaseCatalogEntry,
        ),
      });
    } catch (error) {
      return jsonError(500, "list_failed", error instanceof Error ? error.message : String(error));
    }
  };
}

export function searchKnowledgeBase(deps: { retrievers: RetrieverRegistry }) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    let body: { query?: unknown; topK?: unknown; tags?: unknown };
    try {
      body = (await req.json()) as typeof body;
    } catch {
      return jsonError(400, "invalid_json", "Request body must be valid JSON");
    }
    const query = typeof body.query === "string" ? body.query.trim() : "";
    if (!query) return jsonError(400, "invalid_query", "query is required");
    const registered = deps.retrievers.get(params.id);
    if (!registered) return jsonError(404, "knowledge_base_not_found", params.id);
    const topK =
      typeof body.topK === "number" && Number.isInteger(body.topK)
        ? Math.max(1, Math.min(body.topK, 50))
        : 8;
    const tags = Array.isArray(body.tags)
      ? body.tags.filter((tag): tag is string => typeof tag === "string")
      : undefined;
    try {
      const results = await registered.retriever.retrieve({
        query,
        topK,
        ...(tags && tags.length > 0 ? { filter: { tags } } : {}),
      });
      return json(200, {
        results: results.map(
          (result) =>
            ({
              id: result.chunk.id,
              score: result.score,
              text: result.chunk.text,
              source: {
                id: result.chunk.source.id,
                ...(result.chunk.source.title ? { title: result.chunk.source.title } : {}),
                ...(result.chunk.source.uri ? { uri: result.chunk.source.uri } : {}),
                tags: result.chunk.source.tags ?? [],
              },
              index: result.chunk.index,
            }) satisfies KnowledgeBaseSearchResult,
        ),
      });
    } catch (error) {
      return jsonError(
        502,
        "search_failed",
        error instanceof Error ? error.message : String(error),
      );
    }
  };
}
