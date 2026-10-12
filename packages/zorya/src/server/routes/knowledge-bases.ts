// ---------------------------------------------------------------------------
// Managed knowledge-base routes — the dashboard's Knowledge page. Backed by
// `ZoryaKnowledgeBases`; mounted only when the server is given one.
//
// Knowledge bases live in a namespace: every route except `_adapters` takes
// `?namespaceId=` and acts on that namespace's bases only. With an
// `authorizeScope` hook (the agents service's) the caller must be allowed
// the namespace (403 `scope_forbidden`); with a namespace registry, writes
// that put state into a namespace need it registered and active (404
// `namespace_not_found`, 409 `namespace_archived`). Deletes are not checked
// against the registry, so an archived namespace's bases can be cleaned up.
//
// Routes:
//   GET    /api/knowledge-bases                        — list
//   POST   /api/knowledge-bases                        — create (409 when the id is taken)
//   GET    /api/knowledge-bases/_adapters              — import kinds this server accepts
//   GET    /api/knowledge-bases/:id                    — get one
//   PATCH  /api/knowledge-bases/:id                    — edit description / tags / metadata
//   DELETE /api/knowledge-bases/:id                    — delete; 409 while recipes bind it
//   GET    /api/knowledge-bases/:id/sources            — sources (without their text)
//   POST   /api/knowledge-bases/:id/sources            — queue one document (202)
//   POST   /api/knowledge-bases/:id/sources/import     — load through an adapter, queue (202)
//   POST   /api/knowledge-bases/:id/sources/:sourceId/reindex — queue stored text again (202)
//   DELETE /api/knowledge-bases/:id/sources/:sourceId  — remove a source and its chunks
//   GET    /api/knowledge-bases/:id/chunks             — chunk inspection (paged)
//   POST   /api/knowledge-bases/:id/search             — query the retriever
//
// Ingestion runs in the background: the ingest, import and reindex routes
// answer 202 with the queued source(s), status `pending`, and a `Location`
// of the base's source listing, where each source moves to `ingesting`,
// then `ready` or `failed` with the reason (embedding provider down, store
// error). Bodies are bounded by the server's request body limit; a source's
// text is also bounded by the service's `maxSourceCharacters`.
// ---------------------------------------------------------------------------

import {
  listAllVersioned,
  type AgentRegistry,
  type KnowledgeChunk,
  type RetrieveResult,
} from "@promin/agent";
import type { NamespaceRegistry } from "@promin/workflow";
import { z } from "zod";
import { BooleanQuery, limitQuery, OffsetQuery } from "../api-types.ts";
import { requireActiveNamespace } from "../namespace-guard.ts";
import { parseBody, parseParams, parseQuery } from "../request-validation.ts";
import { HttpError, accepted, created, json, noContent, ok } from "../router.ts";
import {
  KnowledgeBaseExistsError,
  KnowledgeBaseNotFoundError,
  KnowledgeInputError,
  KnowledgeSourceImportError,
  KnowledgeSourceNotFoundError,
  errorMessage,
} from "../services/knowledge/errors.ts";
import {
  KNOWLEDGE_BASE_ID_PATTERN,
  MAX_KNOWLEDGE_NAMESPACE_LENGTH,
  MAX_KNOWLEDGE_SOURCE_ID_LENGTH,
  type KnowledgeBase,
  type KnowledgeSourceSummary,
  type ZoryaKnowledgeBases,
} from "../services/knowledge/zorya-knowledge-bases.ts";
import type { ScopeAuthorizer } from "./agent-gateway-kit.ts";

export type {
  KnowledgeBase,
  KnowledgeSourceSummary,
} from "../services/knowledge/zorya-knowledge-bases.ts";

export interface KnowledgeBaseGatewayDeps {
  readonly knowledgeBases: ZoryaKnowledgeBases;
  /** Agent recipes — ones that bind a base block its delete. */
  readonly agents?: AgentRegistry;
  /** Binds the requested namespace to the caller. See `ScopeAuthorizer`. */
  readonly authorizeScope?: ScopeAuthorizer;
  /** When set, writes need a registered, active namespace. */
  readonly namespaces?: NamespaceRegistry;
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

/** `?namespaceId=` of every route but `_adapters`: the namespace the bases live in. */
export const KnowledgeNamespaceQuerySchema = z.object({
  namespaceId: z.string().min(1).max(MAX_KNOWLEDGE_NAMESPACE_LENGTH),
});

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

/**
 * The namespace a request names in `?namespaceId=`, after the caller's
 * `authorizeScope` allowed it (403 `scope_forbidden` otherwise) and — for
 * writes, with a namespace registry — after the namespace was found active.
 */
async function scopedNamespace(params: {
  deps: KnowledgeBaseGatewayDeps;
  req: Request;
  write: boolean;
}): Promise<string> {
  const { deps, req } = params;
  const { namespaceId } = parseQuery({ req, schema: KnowledgeNamespaceQuerySchema });
  if (deps.authorizeScope && !(await deps.authorizeScope({ req, scope: { namespaceId } }))) {
    throw new HttpError({ status: 403, code: "scope_forbidden" });
  }
  if (params.write) {
    await requireActiveNamespace({ registry: deps.namespaces, namespace: namespaceId });
  }
  return namespaceId;
}

/** `Location` of a base's source listing, where queued ingestions report their outcome. */
function sourcesLocation(params: { namespaceId: string; id: string }): string {
  return `/api/knowledge-bases/${encodeURIComponent(params.id)}/sources?namespaceId=${encodeURIComponent(params.namespaceId)}`;
}

export function listKnowledgeBases(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const namespaceId = await scopedNamespace({ deps, req, write: false });
    return ok({
      knowledgeBases: await deps.knowledgeBases.list({ namespaceId }),
    } satisfies KnowledgeBasesListResponse);
  };
}

export function listKnowledgeSourceAdapters(deps: KnowledgeBaseGatewayDeps) {
  return async (): Promise<Response> =>
    ok({ kinds: deps.knowledgeBases.sourceKinds() } satisfies KnowledgeSourceAdaptersResponse);
}

export function getKnowledgeBase(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const namespaceId = await scopedNamespace({ deps, req, write: false });
    const base = await deps.knowledgeBases.get({ namespaceId, id });
    if (!base) throw mapError(new KnowledgeBaseNotFoundError({ namespaceId, id }));
    return ok(base);
  };
}

export function createKnowledgeBase(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await parseBody({ req, schema: KnowledgeBaseCreateRequestSchema });
    const namespaceId = await scopedNamespace({ deps, req, write: true });
    return created(await guard(() => deps.knowledgeBases.create({ ...body, namespaceId })));
  };
}

export function updateKnowledgeBase(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const patch = await parseBody({ req, schema: KnowledgeBasePatchRequestSchema });
    const namespaceId = await scopedNamespace({ deps, req, write: true });
    return ok(await guard(() => deps.knowledgeBases.update({ namespaceId, id, patch })));
  };
}

export function deleteKnowledgeBase(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const { force } = parseQuery({ req, schema: KnowledgeBaseDeleteQuerySchema });
    const namespaceId = await scopedNamespace({ deps, req, write: false });
    const key = { namespaceId, id };
    if (!(await deps.knowledgeBases.get(key))) throw mapError(new KnowledgeBaseNotFoundError(key));
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
    await guard(() => deps.knowledgeBases.remove(key));
    return noContent();
  };
}

export function listKnowledgeSources(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const namespaceId = await scopedNamespace({ deps, req, write: false });
    const sources = await guard(() => deps.knowledgeBases.listSources({ namespaceId, baseId: id }));
    return ok({ sources } satisfies KnowledgeSourcesResponse);
  };
}

export function ingestKnowledgeSource(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const source = await parseBody({ req, schema: KnowledgeSourceCreateRequestSchema });
    const namespaceId = await scopedNamespace({ deps, req, write: true });
    const queued = await guard(() =>
      deps.knowledgeBases.ingest({ namespaceId, baseId: id, source }),
    );
    return accepted(queued, sourcesLocation({ namespaceId, id }));
  };
}

export function importKnowledgeSources(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const body = await parseBody({ req, schema: KnowledgeSourceImportRequestSchema });
    const namespaceId = await scopedNamespace({ deps, req, write: true });
    const sources = await guard(() =>
      deps.knowledgeBases.importSources({
        namespaceId,
        baseId: id,
        kind: body.kind,
        config: body.config,
      }),
    );
    return accepted(
      { sources } satisfies KnowledgeSourcesImportResponse,
      sourcesLocation({ namespaceId, id }),
    );
  };
}

export function reindexKnowledgeSource(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id, sourceId } = parseParams({ params, schema: KnowledgeSourceParamsSchema });
    const namespaceId = await scopedNamespace({ deps, req, write: true });
    const queued = await guard(() =>
      deps.knowledgeBases.reindexSource({ namespaceId, baseId: id, sourceId }),
    );
    return accepted(queued, sourcesLocation({ namespaceId, id }));
  };
}

export function deleteKnowledgeSource(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id, sourceId } = parseParams({ params, schema: KnowledgeSourceParamsSchema });
    const namespaceId = await scopedNamespace({ deps, req, write: false });
    await guard(() => deps.knowledgeBases.removeSource({ namespaceId, baseId: id, sourceId }));
    return noContent();
  };
}

export function listKnowledgeChunks(deps: KnowledgeBaseGatewayDeps) {
  return async (req: Request, params: Record<string, string>): Promise<Response> => {
    const { id } = parseParams({ params, schema: KnowledgeBaseParamsSchema });
    const query = parseQuery({ req, schema: KnowledgeChunksQuerySchema });
    const namespaceId = await scopedNamespace({ deps, req, write: false });
    const page = await guard(() =>
      deps.knowledgeBases.chunks({
        namespaceId,
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
    const namespaceId = await scopedNamespace({ deps, req, write: false });
    const key = { namespaceId, id };
    const filter = {
      ...(body.tags !== undefined && body.tags.length > 0 && { tags: body.tags }),
      ...(body.metadata !== undefined && { metadata: body.metadata }),
    };
    if (!(await deps.knowledgeBases.get(key))) throw mapError(new KnowledgeBaseNotFoundError(key));
    let results: RetrieveResult[];
    try {
      results = await deps.knowledgeBases.search({
        namespaceId,
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
