// ---------------------------------------------------------------------------
// Recipe → MCP tools.
//
// `resolveMcpServerRecipes` maps a recipe's `mcpServers` onto server
// configs: `{ server }` entries must name an operator-defined server (the
// allowlist), `{ transport: "http", url }` entries are accepted only when the
// operator opted in (`recipeHttpServers`), and always run with approval,
// never as idempotent, with no credential, on public addresses only, with a
// smaller tool cap. A recipe has no way to name a command, environment,
// working directory, header or secret.
//
// `loadMcpToolsFromRecipe` then resolves each operator server's credential
// at the caller's scope, opens (or reuses) its client in the pool under the
// caller's namespace and returns the union of the servers' tools — the
// async pre-step whose result `resolveLocalAgent` takes as `deps.mcpTools`.
// ---------------------------------------------------------------------------

import type { RegisteredAgent } from "../registry/types.ts";
import type { SecretsStorage } from "../secrets/types.ts";
import type { AgentTool } from "../tool.ts";
import type { McpClientPool } from "./mcp-client-pool.ts";
import { createMcpTools } from "./mcp-tools.ts";
import {
  MCP_RECIPE_SERVER_NAME,
  RECIPE_MCP_MAX_TOOLS,
  type McpServerConfig,
  type McpServerRecipe,
} from "./types.ts";

/** A recipe's MCP entry is not allowed or not well-formed. */
export class McpRecipeError extends Error {
  readonly _tag = "McpRecipeError";
  constructor(message: string) {
    super(message);
    this.name = "McpRecipeError";
  }
}

export interface McpRecipePolicy {
  /** Operator-defined servers recipes may attach by name. */
  readonly servers: ReadonlyArray<McpServerConfig>;
  /**
   * Whether recipes may name their own http servers. They always require
   * approval, carry no credential and must pass the pool's outbound
   * policy. Default `false`: the operator opts in.
   */
  readonly recipeHttpServers?: boolean;
}

/** Fields a recipe http entry may hold. */
const RECIPE_HTTP_FIELDS = new Set(["name", "transport", "url", "tools"]);

/** Map recipe entries onto server configs. Throws `McpRecipeError`. */
export function resolveMcpServerRecipes(
  params: McpRecipePolicy & { readonly recipes: ReadonlyArray<McpServerRecipe> },
): McpServerConfig[] {
  const byName = new Map(params.servers.map((s) => [s.name, s]));
  const seen = new Set<string>();
  const out: McpServerConfig[] = [];
  for (const entry of params.recipes) {
    const config = resolveEntry({ entry, byName, policy: params });
    if (seen.has(config.name)) {
      throw new McpRecipeError(`MCP server '${config.name}' is attached twice.`);
    }
    seen.add(config.name);
    out.push(config);
  }
  return out;
}

/** Problems with a recipe's MCP entries, for write-time validation. */
export function mcpRecipeProblems(
  params: McpRecipePolicy & { readonly recipes: ReadonlyArray<McpServerRecipe> },
): string | undefined {
  try {
    resolveMcpServerRecipes(params);
    return undefined;
  } catch (err) {
    if (err instanceof McpRecipeError) return err.message;
    throw err;
  }
}

function resolveEntry(params: {
  entry: McpServerRecipe;
  byName: ReadonlyMap<string, McpServerConfig>;
  policy: McpRecipePolicy;
}): McpServerConfig {
  const { entry } = params;
  if ("server" in entry) {
    const config = params.byName.get(entry.server);
    if (!config) {
      throw new McpRecipeError(
        `MCP server '${String(entry.server).slice(0, 64)}' is not configured on this host. ` +
          "Recipes can attach only the servers the operator configured.",
      );
    }
    return entry.tools ? { ...config, tools: narrow(config.tools, entry.tools) } : config;
  }
  // Anything but a well-formed http entry is refused — notably a stdio
  // entry carrying its own command, or an http entry carrying a credential.
  const raw = entry as unknown as Record<string, unknown>;
  if (raw["transport"] !== "http") {
    throw new McpRecipeError(
      "A recipe can attach an operator-configured MCP server by name ({ server }) or an " +
        `http endpoint ({ name, transport: "http", url }); got transport ${JSON.stringify(raw["transport"]).slice(0, 32)}.`,
    );
  }
  const extra = Object.keys(raw).filter((k) => !RECIPE_HTTP_FIELDS.has(k));
  if (extra.length > 0) {
    throw new McpRecipeError(
      "A recipe http MCP endpoint takes only name, transport, url and tools; credentials, " +
        "headers and commands belong to operator-configured servers.",
    );
  }
  if (params.policy.recipeHttpServers !== true) {
    throw new McpRecipeError("This host accepts only operator-configured MCP servers.");
  }
  if (typeof entry.name !== "string" || !MCP_RECIPE_SERVER_NAME.test(entry.name)) {
    throw new McpRecipeError(`MCP server names must match ${MCP_RECIPE_SERVER_NAME}.`);
  }
  if (params.byName.has(entry.name)) {
    throw new McpRecipeError(
      `MCP server name '${entry.name}' is taken by an operator-configured server.`,
    );
  }
  if (!isHttpUrl(entry.url)) {
    throw new McpRecipeError(
      `MCP server '${entry.name}': url must be an http(s) URL without credentials.`,
    );
  }
  return {
    name: entry.name,
    transport: "http",
    url: entry.url,
    ...(entry.tools !== undefined && { tools: [...entry.tools] }),
    requireApproval: true,
    idempotent: false,
    maxTools: RECIPE_MCP_MAX_TOOLS,
  };
}

function narrow(
  allowed: ReadonlyArray<string> | undefined,
  requested: ReadonlyArray<string>,
): ReadonlyArray<string> {
  return allowed ? requested.filter((t) => allowed.includes(t)) : [...requested];
}

function isHttpUrl(raw: unknown): boolean {
  if (typeof raw !== "string") return false;
  try {
    const u = new URL(raw);
    return (
      (u.protocol === "http:" || u.protocol === "https:") &&
      u.hostname.length > 0 &&
      u.username === "" &&
      u.password === ""
    );
  } catch {
    return false;
  }
}

/** The tenant a set of MCP tools is loaded for. */
export interface McpCallerScope {
  /** Unset: the global namespace (single-tenant use). */
  readonly namespaceId?: string;
  readonly resourceId?: string;
}

export interface LoadMcpToolsFromRecipeParams extends McpRecipePolicy {
  readonly recipe: RegisteredAgent;
  readonly pool: McpClientPool;
  /** Where operator servers' `credentialRef`s resolve. Required when one declares it. */
  readonly secrets?: SecretsStorage;
  /**
   * The caller's tenant scope: isolates pooled clients and scopes
   * credentials. Required, so a caller can't share a tenant's clients by
   * forgetting it; pass `{}` only for the global namespace.
   */
  readonly scope: McpCallerScope;
  readonly signal?: AbortSignal;
}

/**
 * Open the recipe's MCP servers and return their tools keyed by
 * agent-facing name. Empty for a recipe without MCP servers. Throws when a
 * server is not allowed, a credential is missing, a server can't be reached
 * or two tools end up with the same name.
 */
export async function loadMcpToolsFromRecipe(
  params: LoadMcpToolsFromRecipeParams,
): Promise<Record<string, AgentTool<Record<string, unknown>, string>>> {
  const { recipe } = params;
  if (recipe.backend.type !== "local") return {};
  const recipes = recipe.backend.mcpServers ?? [];
  if (recipes.length === 0) return {};
  const servers = resolveMcpServerRecipes({ ...params, recipes });
  const sets = await Promise.all(
    servers.map(async (server) => {
      const credential = await resolveMcpCredential({
        server,
        ...(params.secrets !== undefined && { secrets: params.secrets }),
        scope: params.scope,
      });
      const handle = params.pool.server({
        server,
        ...(params.scope.namespaceId !== undefined && { namespaceId: params.scope.namespaceId }),
        ...(credential !== undefined && {
          credential: credential.value,
          credentialScope: credential.scope,
        }),
      });
      try {
        return await createMcpTools({
          handle,
          ...(params.signal !== undefined && { signal: params.signal }),
        });
      } catch (err) {
        throw new Error(
          `MCP server '${server.name}' is unavailable: ${err instanceof Error ? err.message : String(err)}`,
          { cause: err },
        );
      }
    }),
  );
  const merged: Record<string, AgentTool<Record<string, unknown>, string>> = {};
  for (const set of sets) {
    for (const [name, tool] of Object.entries(set.tools)) {
      if (merged[name] !== undefined) {
        throw new Error(
          `Recipe '${recipe.id}': MCP tool name '${name}' is produced by two servers ` +
            `('${merged[name]!.mcp?.server}' and '${tool.mcp?.server}'). Rename one server.`,
        );
      }
      merged[name] = tool;
    }
  }
  return merged;
}

/** A resolved MCP credential and the scope it was found at. */
export interface ResolvedMcpCredential {
  readonly value: string;
  /** `global`, `namespace:<id>` or `resource:<namespace>:<id>`. */
  readonly scope: string;
}

/**
 * The credential an operator-defined `server` needs at `scope` (resource →
 * namespace → global), or `undefined` when it declares none.
 */
export async function resolveMcpCredential(params: {
  readonly server: McpServerConfig;
  readonly secrets?: SecretsStorage;
  readonly scope?: McpCallerScope;
}): Promise<ResolvedMcpCredential | undefined> {
  const { server } = params;
  const ref = server.credentialRef;
  if (ref === undefined) return undefined;
  if (!params.secrets) {
    throw new Error(
      `MCP server '${server.name}' declares a credentialRef but no SecretsStorage is wired.`,
    );
  }
  const found = await params.secrets.resolve({
    ...(params.scope?.namespaceId !== undefined && { namespaceId: params.scope.namespaceId }),
    ...(params.scope?.resourceId !== undefined && { resourceId: params.scope.resourceId }),
    key: ref,
  });
  if (!found) throw new Error(`MCP server '${server.name}': its credential is not set.`);
  const at = found.scope;
  return {
    value: found.value,
    scope:
      at.kind === "global"
        ? "global"
        : at.kind === "namespace"
          ? `namespace:${at.namespaceId}`
          : `resource:${at.namespaceId}:${at.resourceId}`,
  };
}
