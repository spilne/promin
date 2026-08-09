import type { RetrieveResult, RetrieverRegistry } from "@promin/agent";
import { json, jsonError, readJson } from "../router.ts";
import type {
  KnowledgeBaseCreateInput,
  KnowledgeBaseUpdateInput,
  KnowledgeSourceInput,
  KnowledgeSourceKind,
  ZoryaKnowledgeBases,
} from "../services/knowledge-bases.ts";
import {
  NamespaceArchivedError,
  NamespaceNotFoundError,
  type NamespaceService,
} from "../services/namespaces.ts";

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

export interface ManagedKnowledgeBaseGatewayDeps {
  readonly knowledgeBases: ZoryaKnowledgeBases;
  readonly namespaces: NamespaceService;
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

export function listManagedKnowledgeBases(deps: ManagedKnowledgeBaseGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    try {
      const url = new URL(req.url);
      const namespace = url.searchParams.get("namespace");
      const knowledgeBases = namespace
        ? await deps.knowledgeBases.list((await deps.namespaces.resolve(namespace)).id)
        : (
            await Promise.all(
              (
                await deps.namespaces.listActive()
              ).map((entry) => deps.knowledgeBases.list(entry.id)),
            )
          ).flat();
      return json(200, { knowledgeBases });
    } catch (error) {
      return managedKnowledgeBaseError(error, "list_failed");
    }
  };
}

export function createManagedKnowledgeBase(deps: ManagedKnowledgeBaseGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<Record<string, unknown>>(req);
    if (!body || typeof body.id !== "string") return jsonError(400, "missing_id");
    try {
      const input: KnowledgeBaseCreateInput = {
        id: body.id,
        namespace: await resolveRequestNamespace(deps, body.namespace),
        ...(typeof body.description === "string" && { description: body.description }),
        ...(Array.isArray(body.tags) && {
          tags: body.tags.filter((tag): tag is string => typeof tag === "string"),
        }),
        ...(isRecord(body.metadata) && { metadata: body.metadata }),
        ...(body.provider === "memory" || body.provider === "external"
          ? { provider: body.provider }
          : {}),
      };
      return json(201, { knowledgeBase: await deps.knowledgeBases.create(input) });
    } catch (error) {
      return managedKnowledgeBaseError(error, "create_failed");
    }
  };
}

export function updateManagedKnowledgeBase(deps: ManagedKnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = await readJson<Record<string, unknown>>(req);
    if (!body) return jsonError(400, "missing_body");
    try {
      const patch: KnowledgeBaseUpdateInput = {
        ...(typeof body.description === "string" && { description: body.description }),
        ...(Array.isArray(body.tags) && {
          tags: body.tags.filter((tag): tag is string => typeof tag === "string"),
        }),
        ...(isRecord(body.metadata) && { metadata: body.metadata }),
      };
      if (Object.keys(patch).length === 0) return jsonError(400, "empty_patch");
      const namespace = await resolveRequestNamespace(deps, body.namespace);
      return json(200, {
        knowledgeBase: await deps.knowledgeBases.update(namespace, params.id!, patch),
      });
    } catch (error) {
      return managedKnowledgeBaseError(error, "update_failed");
    }
  };
}

export function deleteManagedKnowledgeBase(deps: ManagedKnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    try {
      const namespace = await resolveRequestNamespace(
        deps,
        new URL(req.url).searchParams.get("namespace"),
      );
      await deps.knowledgeBases.remove(namespace, params.id!);
      return json(200, { ok: true });
    } catch (error) {
      return managedKnowledgeBaseError(error, "delete_failed");
    }
  };
}

export function listKnowledgeBaseSources(deps: ManagedKnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    try {
      const namespace = await resolveRequestNamespace(
        deps,
        new URL(req.url).searchParams.get("namespace"),
      );
      const sources = await deps.knowledgeBases.listSources(namespace, params.id!);
      return json(200, {
        sources: sources.map(({ text: _text, ...source }) => source),
      });
    } catch (error) {
      return managedKnowledgeBaseError(error, "sources_failed");
    }
  };
}

export function ingestKnowledgeBaseSource(deps: ManagedKnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = await readJson<Record<string, unknown>>(req);
    if (!body || typeof body.id !== "string" || typeof body.text !== "string") {
      return jsonError(400, "invalid_source", "Source id and text are required.");
    }
    try {
      const namespace = await resolveRequestNamespace(deps, body.namespace);
      const input: KnowledgeSourceInput = {
        id: body.id,
        text: body.text,
        ...(typeof body.title === "string" && { title: body.title }),
        ...(typeof body.uri === "string" && { uri: body.uri }),
        ...(typeof body.mimeType === "string" && { mimeType: body.mimeType }),
        ...(Array.isArray(body.tags) && {
          tags: body.tags.filter((tag): tag is string => typeof tag === "string"),
        }),
        ...(isRecord(body.metadata) && { metadata: body.metadata }),
      };
      const source = await deps.knowledgeBases.ingest(namespace, params.id!, input);
      const { text: _text, ...publicSource } = source;
      return json(201, { source: publicSource });
    } catch (error) {
      return managedKnowledgeBaseError(error, "ingest_failed");
    }
  };
}

export function importKnowledgeBaseSource(deps: ManagedKnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const body = await readJson<Record<string, unknown>>(req);
    if (!body || typeof body.kind !== "string") return jsonError(400, "source_kind_required");
    try {
      const namespace = await resolveRequestNamespace(deps, body.namespace);
      const sources = await deps.knowledgeBases.ingestFrom(
        namespace,
        params.id!,
        body.kind as KnowledgeSourceKind,
        body.config,
      );
      return json(201, {
        sources: sources.map(({ text: _text, ...source }) => source),
      });
    } catch (error) {
      return managedKnowledgeBaseError(error, "source_import_failed");
    }
  };
}

export function deleteKnowledgeBaseSource(deps: ManagedKnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    try {
      const namespace = await resolveRequestNamespace(
        deps,
        new URL(req.url).searchParams.get("namespace"),
      );
      await deps.knowledgeBases.removeSource(namespace, params.id!, params.sourceId!);
      return json(200, { ok: true });
    } catch (error) {
      return managedKnowledgeBaseError(error, "source_delete_failed");
    }
  };
}

export function listKnowledgeBaseChunks(deps: ManagedKnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    try {
      const url = new URL(req.url);
      const namespace = await resolveRequestNamespace(deps, url.searchParams.get("namespace"));
      const chunks = await deps.knowledgeBases.chunks(
        namespace,
        params.id!,
        url.searchParams.get("sourceId") ?? undefined,
      );
      return json(200, { chunks });
    } catch (error) {
      return managedKnowledgeBaseError(error, "chunks_failed");
    }
  };
}

export function searchManagedKnowledgeBase(deps: ManagedKnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    let body: Record<string, unknown>;
    try {
      body = (await req.json()) as Record<string, unknown>;
    } catch {
      return jsonError(400, "invalid_json", "Request body must be valid JSON");
    }
    const query = typeof body.query === "string" ? body.query.trim() : "";
    if (!query) return jsonError(400, "invalid_query", "query is required");
    try {
      const namespace = await resolveRequestNamespace(deps, body.namespace);
      const topK =
        typeof body.topK === "number" && Number.isInteger(body.topK)
          ? Math.max(1, Math.min(body.topK, 50))
          : 8;
      const tags = Array.isArray(body.tags)
        ? body.tags.filter((tag): tag is string => typeof tag === "string")
        : undefined;
      const results = await deps.knowledgeBases.search(namespace, params.id!, {
        query,
        topK,
        ...(tags && tags.length > 0 ? { filter: { tags } } : {}),
      });
      return json(200, { results: results.map(toSearchResult) });
    } catch (error) {
      return managedKnowledgeBaseError(error, "search_failed");
    }
  };
}

async function resolveRequestNamespace(
  deps: ManagedKnowledgeBaseGatewayDeps,
  value: unknown,
): Promise<string> {
  return (await deps.namespaces.resolve(typeof value === "string" ? value : undefined)).id;
}

function managedKnowledgeBaseError(error: unknown, fallback: string): Response {
  if (error instanceof NamespaceNotFoundError) return jsonError(404, "namespace_not_found");
  if (error instanceof NamespaceArchivedError) return jsonError(409, "namespace_archived");
  const message = error instanceof Error ? error.message : String(error);
  const status =
    message === "knowledge_base_not_found" || message === "source_not_found"
      ? 404
      : message === "knowledge_base_exists"
        ? 409
        : message.includes("unsupported")
          ? 409
          : message === "invalid_knowledge_base_id" || message.endsWith("_required")
            ? 400
            : 500;
  const code =
    message === "knowledge_base_not_found"
      ? "knowledge_base_not_found"
      : message === "source_not_found"
        ? "source_not_found"
        : message === "knowledge_base_exists"
          ? "knowledge_base_exists"
          : fallback;
  return jsonError(status, code, message);
}

function toSearchResult(result: RetrieveResult): KnowledgeBaseSearchResult {
  return {
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
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}
