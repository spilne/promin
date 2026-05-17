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
  ModelCatalog,
  SerializedModelCatalogItem,
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

export interface AgentCatalogDeps {
  readonly models: ModelCatalog;
}

export interface AgentToolCatalogDeps {
  readonly tools: AgentToolCatalog;
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
