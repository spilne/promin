// ---------------------------------------------------------------------------
// Read-only metadata endpoints for the agent designer UI.
//
// `GET /api/agents/_catalog/models` returns the host's `ModelCatalog`
// stripped of the runtime `llm` field — JSON-safe wire shape that powers
// the designer's model dropdown. The `_catalog` URL prefix signals that
// these are metadata reads, not agent invocations (the `_` keeps them
// from colliding with a real agent id). Sibling catalogs list tools,
// skills, fragments and retrievers for the agent editor's pickers.
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
  ToolCatalogError,
  ToolHistoryQuery,
  ToolHistoryRecord,
  ToolHistorySourceKind,
  ToolHistoryStore,
  ToolRefReport,
} from "@promin/agent";
import { reconcileToolReferences } from "@promin/agent";
import { json } from "../router.ts";
import type { McpServersCatalogResponse, ZoryaMcpServers } from "../services/mcp/index.ts";
import { internalError } from "./agent-gateway-kit.ts";

export interface ModelsCatalogResponse {
  models: SerializedModelCatalogItem[];
}

export interface ToolsCatalogResponse {
  tools: ToolCatalogEntry[];
  /** Sources that could not be (fully) listed: an unreachable MCP server, an unusable tool. */
  errors: ToolCatalogError[];
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

export interface CatalogToolsDeps {
  /** The host's tool catalog (in-process / file tools). */
  readonly tools?: AgentToolCatalog;
  /** The operator's MCP servers, whose tools are listed alongside. */
  readonly mcp?: ZoryaMcpServers;
}

/**
 * Catalog entry for the agent editor's knowledge picker — a registered
 * retriever's id and descriptive metadata. The live `Retriever` never
 * crosses the wire; recipes store only the id plus per-binding options.
 */
export interface RetrieverCatalogEntry {
  readonly id: string;
  readonly description?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
}

export interface RetrieversCatalogResponse {
  retrievers: RetrieverCatalogEntry[];
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

/**
 * The tool picker's source: the host catalog's tools plus the allowlisted
 * MCP servers' tools. A server that can't be listed is reported in
 * `errors` instead of silently missing.
 */
export function listCatalogTools(deps: CatalogToolsDeps) {
  return async (): Promise<Response> => {
    try {
      const [host, mcp] = await Promise.all([deps.tools?.list(), deps.mcp?.catalog()]);
      const taken = new Set(host?.tools.map((t) => t.name));
      const tools = [
        ...(host?.tools ?? []),
        ...(mcp?.tools ?? []).filter((t) => !taken.has(t.name)),
      ].sort((a, b) => a.name.localeCompare(b.name));
      const body: ToolsCatalogResponse = {
        tools,
        errors: [...(host?.errors ?? []), ...(mcp?.errors ?? [])],
      };
      return json(200, body);
    } catch (err) {
      return internalError({ code: "list_failed", err });
    }
  };
}

/** The agent editor's MCP picker: allowlisted servers (never their command or env). */
export function listCatalogMcpServers(deps: { readonly mcp?: ZoryaMcpServers }) {
  return async (): Promise<Response> => {
    const body: McpServersCatalogResponse = deps.mcp?.describe() ?? {
      configured: false,
      servers: [],
      recipeHttpServers: false,
    };
    return json(200, body);
  };
}

/** List host-wired retrievers (sans the retriever object) for the knowledge picker. */
export function listCatalogRetrievers(deps: AgentRetrieverCatalogDeps) {
  return async (): Promise<Response> => {
    try {
      const retrievers: RetrieverCatalogEntry[] = deps.retrievers.list().map((r) => ({
        id: r.id,
        ...(r.description !== undefined && { description: r.description }),
        tags: r.tags,
        metadata: r.metadata,
      }));
      return json(200, { retrievers } satisfies RetrieversCatalogResponse);
    } catch (err) {
      return internalError({ code: "list_failed", err });
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
      return internalError({ code: "list_failed", err });
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
      return internalError({ code: "list_failed", err });
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
      return internalError({ code: "health_failed", err });
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
      return internalError({ code: "history_failed", err });
    }
  };
}
