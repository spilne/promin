// ---------------------------------------------------------------------------
// Managed knowledge-base routes — the dashboard's Knowledge page. Backed by
// `ZoryaKnowledgeBases`; mounted only when the server is given one.
//
// Routes:
//   GET    /api/knowledge-bases                        — list
//   POST   /api/knowledge-bases                        — create (409 when the id is taken)
//   GET    /api/knowledge-bases/_adapters              — import kinds this server accepts
//   GET    /api/knowledge-bases/:id                    — get one
//   PATCH  /api/knowledge-bases/:id                    — edit description / tags / metadata
//   DELETE /api/knowledge-bases/:id                    — delete; 409 while recipes bind it
//   GET    /api/knowledge-bases/:id/sources            — sources (without their text)
//   POST   /api/knowledge-bases/:id/sources            — ingest one document
//   POST   /api/knowledge-bases/:id/sources/import     — ingest through a source adapter
//   POST   /api/knowledge-bases/:id/sources/:sourceId/reindex — re-ingest stored text
//   DELETE /api/knowledge-bases/:id/sources/:sourceId  — remove a source and its chunks
//   GET    /api/knowledge-bases/:id/chunks             — chunk inspection (paged)
//   POST   /api/knowledge-bases/:id/search             — query the retriever
//
// Ingestion runs inside the request: an indexing failure (embedding provider
// down, store error) answers 502 `ingest_failed` with the reason, and the
// source is listed with status `failed` and that reason until it is
// re-ingested. Bodies are bounded by the server's request body limit; a
// source's text is also bounded by the service's `maxSourceCharacters`.
// ---------------------------------------------------------------------------

import {
  listAllVersioned,
  type AgentRegistry,
  type KnowledgeChunk,
  type RetrieveResult,
} from "@promin/agent";
import { z } from "zod";
import { BooleanQuery, limitQuery, OffsetQuery } from "../api-types.ts";
import { parseBody, parseParams, parseQuery } from "../request-validation.ts";
import { HttpError, created, json, noContent, ok } from "../router.ts";
import {
  KnowledgeBaseExistsError,
  KnowledgeBaseNotFoundError,
  KnowledgeIngestError,
  KnowledgeInputError,
  KnowledgeSourceImportError,
  KnowledgeSourceNotFoundError,
  errorMessage,
} from "../services/knowledge/errors.ts";
import {
  KNOWLEDGE_BASE_ID_PATTERN,
  MAX_KNOWLEDGE_SOURCE_ID_LENGTH,
  type KnowledgeBase,
  type KnowledgeSourceSummary,
  type ZoryaKnowledgeBases,
} from "../services/knowledge/zorya-knowledge-bases.ts";

export type {
  KnowledgeBase,
  KnowledgeSourceSummary,
} from "../services/knowledge/zorya-knowledge-bases.ts";

export interface KnowledgeBaseGatewayDeps {
  readonly knowledgeBases: ZoryaKnowledgeBases;
  /** Agent recipes — ones that bind a base block its delete. */
  readonly agents?: AgentRegistry;
}

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

export interface KnowledgeBasesListResponse {
  knowledgeBases: KnowledgeBase[];
}

export interface KnowledgeSourceAdaptersResponse {
  /** Kinds `POST /sources/import` accepts, e.g. `url`, `file`. */
  kinds: string[];
}

export interface KnowledgeSourcesResponse {
  sources: KnowledgeSourceSummary[];
}

export interface KnowledgeSourcesImportResponse {
  sources: KnowledgeSourceSummary[];
}

export interface KnowledgeChunksResponse {
  chunks: KnowledgeChunk[];
  total: number;
}

export interface KnowledgeSearchResultDto {
  id: string;
  score: number;
  text: string;
  index: number;
  source: { id: string; title?: string; uri?: string; tags: string[] };
}

export interface KnowledgeSearchResponse {
  results: KnowledgeSearchResultDto[];
}

/** 409 body of `DELETE /api/knowledge-bases/:id` while recipes bind the base. */
export interface KnowledgeBaseInUseResponse {
  error: "knowledge_base_in_use";
  message: string;
  agents: string[];
}

// ---------------------------------------------------------------------------
// Request schemas
// ---------------------------------------------------------------------------

const KnowledgeBaseId = z
  .string()
  .regex(
    KNOWLEDGE_BASE_ID_PATTERN,
    "must be 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit",
  );
const SourceId = z.string().trim().min(1).max(MAX_KNOWLEDGE_SOURCE_ID_LENGTH);
const Tags = z.array(z.string().max(128)).max(64);
const Metadata = z.record(z.string(), z.unknown());
const Description = z.string().max(2_000);

export const KnowledgeBaseParamsSchema = z.object({ id: z.string().min(1).max(256) });
export const KnowledgeSourceParamsSchema = KnowledgeBaseParamsSchema.extend({
  sourceId: z.string().min(1).max(MAX_KNOWLEDGE_SOURCE_ID_LENGTH),
});

/** Body of `POST /api/knowledge-bases`. */
export const KnowledgeBaseCreateRequestSchema = z
  .object({
    id: KnowledgeBaseId,
    description: Description.optional(),
    tags: Tags.optional(),
    metadata: Metadata.optional(),
  })
  .strict();
export type KnowledgeBaseCreateRequest = z.infer<typeof KnowledgeBaseCreateRequestSchema>;

/** Body of `PATCH /api/knowledge-bases/:id`; `description: null` clears it. */
export const KnowledgeBasePatchRequestSchema = z
  .object({
    description: Description.nullable().optional(),
    tags: Tags.optional(),
    metadata: Metadata.optional(),
  })
  .strict()
  .refine((b) => Object.keys(b).length > 0, { message: "Send at least one field to change" });
export type KnowledgeBasePatchRequest = z.infer<typeof KnowledgeBasePatchRequestSchema>;

/** Body of `POST /api/knowledge-bases/:id/sources`. Re-sending an id replaces that source. */
export const KnowledgeSourceCreateRequestSchema = z
  .object({
    id: SourceId,
    text: z.string().min(1),
    title: z.string().max(512).optional(),
    uri: z.string().max(4096).optional(),
    mimeType: z.string().max(256).optional(),
    tags: Tags.optional(),
    metadata: Metadata.optional(),
  })
  .strict();
export type KnowledgeSourceCreateRequest = z.infer<typeof KnowledgeSourceCreateRequestSchema>;

/** Body of `POST /api/knowledge-bases/:id/sources/import`; the adapter validates `config`. */
export const KnowledgeSourceImportRequestSchema = z
  .object({ kind: z.string().min(1).max(64), config: z.unknown() })
  .strict();
export type KnowledgeSourceImportRequest = z.infer<typeof KnowledgeSourceImportRequestSchema>;

/** Query of `GET /api/knowledge-bases/:id/chunks`. */
export const KnowledgeChunksQuerySchema = z.object({
  sourceId: z.preprocess(
    (v) => (v === "" ? undefined : v),
    z.string().max(MAX_KNOWLEDGE_SOURCE_ID_LENGTH).optional(),
  ),
  limit: limitQuery({ fallback: 100 }),
  offset: OffsetQuery,
});

/** Query of `DELETE /api/knowledge-bases/:id`. */
export const KnowledgeBaseDeleteQuerySchema = z.object({ force: BooleanQuery });

/** Body of `POST /api/knowledge-bases/:id/search`. */
export const KnowledgeSearchRequestSchema = z
  .object({
    query: z.string().trim().min(1).max(4_096),
    topK: z.number().int().min(1).max(50).optional(),
    tags: z.array(z.string().max(128)).max(32).optional(),
    metadata: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).optional(),
    maxChunkCharacters: z.number().int().positive().max(100_000).optional(),
  })
  .strict();
export type KnowledgeSearchRequest = z.infer<typeof KnowledgeSearchRequestSchema>;

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

export function listKnowledgeBases(deps: KnowledgeBaseGatewayDeps) {
  return async (): Promise<Response> =>
    ok({
      knowledgeBases: await deps.knowledgeBases.list(),
    } satisfies KnowledgeBasesListResponse);
}

export function listKnowledgeSourceAdapters(deps: KnowledgeBaseGatewayDeps) {
  return async (): Promise<Response> =>
    ok({ kinds: deps.knowledgeBases.sourceKinds() } satisfies KnowledgeSourceAdaptersResponse);
}

export function getKnowledgeBase(deps: KnowledgeBaseGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const base = await deps.knowledgeBases.get(id);
    if (!base) throw mapError(new KnowledgeBaseNotFoundError(id));
    return ok(base);
  };
}

export function createKnowledgeBase(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await parseBody({ req, schema: KnowledgeBaseCreateRequestSchema });
    return created(await guard(() => deps.knowledgeBases.create(body)));
  };
}

export function updateKnowledgeBase(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const patch = await parseBody({ req, schema: KnowledgeBasePatchRequestSchema });
    return ok(await guard(() => deps.knowledgeBases.update({ id, patch })));
  };
}

export function deleteKnowledgeBase(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const { force } = parseQuery({ req, schema: KnowledgeBaseDeleteQuerySchema });
    if (!(await deps.knowledgeBases.get(id))) throw mapError(new KnowledgeBaseNotFoundError(id));
    if (!force && deps.agents) {
      const agents = await agentsBinding({ agents: deps.agents, baseId: id });
      if (agents.length > 0) {
        return json(409, {
          error: "knowledge_base_in_use",
          message:
            "Agents still bind this knowledge base in backend.knowledge; remove the binding there or pass ?force=true.",
          agents,
        } satisfies KnowledgeBaseInUseResponse);
      }
    }
    await guard(() => deps.knowledgeBases.remove(id));
    return noContent();
  };
}

export function listKnowledgeSources(deps: KnowledgeBaseGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const sources = await guard(() => deps.knowledgeBases.listSources(id));
    return ok({ sources } satisfies KnowledgeSourcesResponse);
  };
}

export function ingestKnowledgeSource(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const source = await parseBody({ req, schema: KnowledgeSourceCreateRequestSchema });
    return created(await guard(() => deps.knowledgeBases.ingest({ baseId: id, source })));
  };
}

export function importKnowledgeSources(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const body = await parseBody({ req, schema: KnowledgeSourceImportRequestSchema });
    const sources = await guard(() =>
      deps.knowledgeBases.importSources({ baseId: id, kind: body.kind, config: body.config }),
    );
    return created({ sources } satisfies KnowledgeSourcesImportResponse);
  };
}

export function reindexKnowledgeSource(deps: KnowledgeBaseGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id, sourceId } = parseParams({ params, schema: KnowledgeSourceParamsSchema });
    return ok(await guard(() => deps.knowledgeBases.reindexSource({ baseId: id, sourceId })));
  };
}

export function deleteKnowledgeSource(deps: KnowledgeBaseGatewayDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const { id, sourceId } = parseParams({ params, schema: KnowledgeSourceParamsSchema });
    await guard(() => deps.knowledgeBases.removeSource({ baseId: id, sourceId }));
    return noContent();
  };
}

export function listKnowledgeChunks(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const query = parseQuery({ req, schema: KnowledgeChunksQuerySchema });
    const page = await guard(() =>
      deps.knowledgeBases.chunks({
        baseId: id,
        limit: query.limit,
        offset: query.offset,
        ...(query.sourceId !== undefined && { sourceId: query.sourceId }),
      }),
    );
    return ok(page satisfies KnowledgeChunksResponse);
  };
}

export function searchKnowledgeBase(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const body = await parseBody({ req, schema: KnowledgeSearchRequestSchema });
    const filter = {
      ...(body.tags !== undefined && body.tags.length > 0 && { tags: body.tags }),
      ...(body.metadata !== undefined && { metadata: body.metadata }),
    };
    if (!(await deps.knowledgeBases.get(id))) throw mapError(new KnowledgeBaseNotFoundError(id));
    let results: RetrieveResult[];
    try {
      results = await deps.knowledgeBases.search({
        baseId: id,
        request: {
          query: body.query,
          topK: body.topK ?? 8,
          ...(Object.keys(filter).length > 0 && { filter }),
          ...(body.maxChunkCharacters !== undefined && {
            maxChunkCharacters: body.maxChunkCharacters,
          }),
        },
      });
    } catch (err) {
      if (err instanceof KnowledgeBaseNotFoundError) throw mapError(err);
      throw new HttpError({ status: 502, code: "search_failed", message: errorMessage(err) });
    }
    return ok({ results: results.map(toSearchResult) } satisfies KnowledgeSearchResponse);
  };
}

// ---------------------------------------------------------------------------

/** Await `fn`, answering the service's errors with their HTTP status. */
async function guard<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (err) {
    throw mapError(err);
  }
}

function mapError(err: unknown): unknown {
  if (err instanceof KnowledgeBaseNotFoundError) {
    return new HttpError({ status: 404, code: "knowledge_base_not_found", message: err.message });
  }
  if (err instanceof KnowledgeSourceNotFoundError) {
    return new HttpError({ status: 404, code: "source_not_found", message: err.message });
  }
  if (err instanceof KnowledgeBaseExistsError) {
    return new HttpError({ status: 409, code: "knowledge_base_exists", message: err.message });
  }
  if (err instanceof KnowledgeInputError) {
    return new HttpError({ status: 400, code: err.code, message: err.message });
  }
  if (err instanceof KnowledgeSourceImportError) {
    return err.reason === "invalid"
      ? new HttpError({ status: 400, code: "invalid_source_config", message: err.message })
      : new HttpError({ status: 502, code: "source_unavailable", message: err.message });
  }
  if (err instanceof KnowledgeIngestError) {
    return new HttpError({ status: 502, code: "ingest_failed", message: err.message });
  }
  return err;
}

/** Ids of local agents whose active recipe binds `baseId` in `backend.knowledge`. */
async function agentsBinding(params: { agents: AgentRegistry; baseId: string }): Promise<string[]> {
  const recipes = await listAllVersioned({
    registry: params.agents,
    filter: { backendType: "local" },
  });
  return recipes
    .filter(
      (recipe) =>
        recipe.backend.type === "local" &&
        (recipe.backend.knowledge ?? []).some((binding) => binding.id === params.baseId),
    )
    .map((recipe) => recipe.id)
    .sort();
}

function toSearchResult(result: RetrieveResult): KnowledgeSearchResultDto {
  const { chunk } = result;
  return {
    id: chunk.id,
    score: result.score,
    text: chunk.text,
    index: chunk.index,
    source: {
      id: chunk.source.id,
      ...(chunk.source.title !== undefined && { title: chunk.source.title }),
      ...(chunk.source.uri !== undefined && { uri: chunk.source.uri }),
      tags: [...(chunk.source.tags ?? [])],
    },
  };
}
