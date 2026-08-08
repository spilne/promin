// ---------------------------------------------------------------------------
// Read-only metadata endpoints for the agent designer UI.
//
// `GET /api/agents/_catalog/models` returns the host's `ModelCatalog`
// stripped of the runtime `llm` field — JSON-safe wire shape that powers
// the designer's model dropdown. The `_catalog` URL prefix signals that
// these are metadata reads, not agent invocations (the `_` keeps them
// from colliding with a real agent id).
// ---------------------------------------------------------------------------

import type {
  AgentRegistry,
  AgentToolCatalog,
  FragmentRegistry,
  ModelCatalog,
  RetrieverRegistry,
  SerializedModelCatalogItem,
  SkillRegistry,
  ToolCatalogEntry,
  ToolHistoryQuery,
  ToolHistoryRecord,
  ToolHistorySourceKind,
  ToolHistoryStore,
  ToolRefReport,
} from "@promin/agent";
import { reconcileToolReferences } from "@promin/agent";
import { json, jsonError } from "../router.ts";

export interface ModelsCatalogResponse {
  models: SerializedModelCatalogItem[];
}

export interface ToolsCatalogResponse {
  tools: ToolCatalogEntry[];
}

export interface RetrieverCatalogEntry {
  readonly id: string;
  readonly description?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
}

export interface RetrieversCatalogResponse {
  retrievers: RetrieverCatalogEntry[];
}

/**
 * Catalog entry for the agent editor's skill picker — the metadata an
 * operator needs to choose which skills an agent can load. Deliberately
 * omits `body` (the picker only needs description + trigger); the full body
 * is fetched via `GET /api/skills/:id` when editing the skill itself.
 */
export interface SkillCatalogEntry {
  readonly id: string;
  readonly version: string;
  readonly description: string;
  readonly whenToUse: string;
  readonly tags: ReadonlyArray<string>;
  readonly capabilities: ReadonlyArray<string>;
  readonly enabled: boolean;
}

export interface SkillsCatalogResponse {
  skills: SkillCatalogEntry[];
}

export interface AgentSkillCatalogDeps {
  readonly skills: SkillRegistry;
}

export interface AgentCatalogDeps {
  readonly models: ModelCatalog;
}

export interface AgentToolCatalogDeps {
  readonly tools: AgentToolCatalog;
}

export interface AgentRetrieverCatalogDeps {
  readonly retrievers: RetrieverRegistry;
}

export function listCatalogModels(deps: AgentCatalogDeps) {
  return async (): Promise<Response> => {
    const body: ModelsCatalogResponse = { models: deps.models.serialize() };
    return json(200, body);
  };
}

export function listCatalogTools(deps: AgentToolCatalogDeps) {
  return async (): Promise<Response> => {
    try {
      const tools = await deps.tools.listAll();
      const body: ToolsCatalogResponse = { tools };
      return json(200, body);
    } catch (err) {
      return jsonError(500, "list_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

/**
 * List host-wired retrievers for the agent editor's RAG picker. The live
 * `Retriever` implementation is intentionally omitted; recipes persist only
 * the retriever id plus per-binding options.
 */
export function listCatalogRetrievers(deps: AgentRetrieverCatalogDeps) {
  return async (): Promise<Response> => {
    try {
      const retrievers = deps.retrievers.list().map((r) => ({
        id: r.id,
        ...(r.description ? { description: r.description } : {}),
        tags: r.tags,
        metadata: r.metadata,
      }));
      return json(200, { retrievers } satisfies RetrieversCatalogResponse);
    } catch (err) {
      return jsonError(500, "list_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

/**
 * Catalog entry for the agent editor's layered-prompt editor — a fragment
 * key + its full content (so the picker can show a preview without a
 * follow-up fetch). Fragments are small markdown layers; sending them
 * inline keeps the picker snappy and the wire shape simple.
 */
export interface FragmentCatalogEntry {
  readonly key: string;
  readonly content: string;
}

export interface FragmentsCatalogResponse {
  fragments: FragmentCatalogEntry[];
}

export interface AgentFragmentCatalogDeps {
  readonly fragments: FragmentRegistry;
}

/**
 * List the registered prompt fragments for the layered-prompt editor.
 * Mirrors `listCatalogTools` / `listCatalogSkills`.
 */
export function listCatalogFragments(deps: AgentFragmentCatalogDeps) {
  return async (): Promise<Response> => {
    try {
      const fragments = deps.fragments.list().map((f) => ({ key: f.key, content: f.content }));
      return json(200, { fragments } satisfies FragmentsCatalogResponse);
    } catch (err) {
      return jsonError(500, "list_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

/**
 * List available skills (sans body) for the agent editor's skill picker.
 * Mirrors `listCatalogTools`; backed by the host's `SkillRegistry`.
 */
export function listCatalogSkills(deps: AgentSkillCatalogDeps) {
  return async (): Promise<Response> => {
    try {
      const rows = await deps.skills.list();
      // Hide needs-review skills from the agent editor's picker — they
      // can't actually be loaded (resolveSkillCatalog drops them), so
      // showing them would only confuse the operator. Review happens in
      // the skills manager via /api/skills, which surfaces all of them.
      const skills: SkillCatalogEntry[] = rows
        .filter((s) => s.metadata.trust !== "needs-review")
        .map((s) => ({
          id: s.id,
          version: s.version,
          description: s.description,
          whenToUse: s.whenToUse,
          tags: s.metadata.tags,
          capabilities: s.metadata.capabilities,
          enabled: s.metadata.enabled !== false,
        }));
      return json(200, { skills } satisfies SkillsCatalogResponse);
    } catch (err) {
      return jsonError(500, "list_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export interface ToolCatalogHealthDeps {
  readonly tools: AgentToolCatalog;
  readonly agents: AgentRegistry;
}

export type ToolCatalogHealthResponse = ToolRefReport;

export function getToolCatalogHealth(deps: ToolCatalogHealthDeps) {
  return async (): Promise<Response> => {
    try {
      const report = await reconcileToolReferences({
        catalog: deps.tools,
        registry: deps.agents,
      });
      return json(200, report);
    } catch (err) {
      return jsonError(500, "health_failed", err instanceof Error ? err.message : String(err));
    }
  };
}

export interface ToolHistoryDeps {
  readonly history: ToolHistoryStore;
}

export interface ToolHistoryResponse {
  history: ToolHistoryRecord[];
}

const SOURCE_KINDS: ReadonlyArray<ToolHistorySourceKind> = ["in-process", "file", "mcp"];

/** Parse `?n` as a finite non-negative integer, or `undefined`. */
function parseCount(raw: string | null): number | undefined {
  if (raw === null) return undefined;
  const n = Number(raw);
  return Number.isInteger(n) && n >= 0 ? n : undefined;
}

/**
 * `GET /api/agents/_catalog/tools/history` — durable audit trail of which
 * tools the host has exposed over time. Query params: `name`, `source`
 * (`in-process` | `file` | `mcp`), `since` (epoch ms), `limit`. Unknown
 * `source` values are ignored rather than rejected.
 */
export function listToolHistory(deps: ToolHistoryDeps) {
  return async (req: Request): Promise<Response> => {
    try {
      const params = new URL(req.url).searchParams;
      const name = params.get("name") ?? undefined;
      const source = params.get("source");
      const sourceKind = SOURCE_KINDS.find((k) => k === source);
      const query: ToolHistoryQuery = {
        ...(name ? { name } : {}),
        ...(sourceKind ? { sourceKind } : {}),
        ...(parseCount(params.get("since")) !== undefined
          ? { since: parseCount(params.get("since")) }
          : {}),
        ...(parseCount(params.get("limit")) !== undefined
          ? { limit: parseCount(params.get("limit")) }
          : {}),
      };
      const history = await deps.history.list(query);
      const body: ToolHistoryResponse = { history };
      return json(200, body);
    } catch (err) {
      return jsonError(500, "history_failed", err instanceof Error ? err.message : String(err));
    }
  };
}
