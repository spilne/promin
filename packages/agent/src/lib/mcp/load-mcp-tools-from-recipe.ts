// ---------------------------------------------------------------------------
// `loadMcpToolsFromRecipe` — pre-step that turns a recipe's
// `mcpServers` declaration into AgentTools the host can splice into
// `ResolveLocalAgentDeps.tools` before calling `resolveLocalAgent`.
//
// Same pattern as `resolveCredentialRef`: keeps the resolver sync;
// host runs this async helper to gather MCP tools, then calls the
// (sync) resolver with the merged tool record.
//
// The pool argument lets the host control client lifetime — typical
// production wiring uses `DefaultMcpClientPool` for SDK-backed
// clients; tests use `InMemoryMcpClientPool` to dodge real spawns.
// ---------------------------------------------------------------------------

import type { AgentTool } from "../tool.ts";
import type { RegisteredAgent } from "../registry/types.ts";
import type { McpClientPool } from "./mcp-client-pool.ts";
import { createMcpTools } from "./mcp-tools.ts";

export interface LoadMcpToolsFromRecipeParams {
  readonly recipe: RegisteredAgent;
  readonly pool: McpClientPool;
}

/**
 * Open clients for every `mcpServers` entry on the recipe and return
 * the union of their tools, keyed as `<serverName>:<toolName>`. When
 * the recipe has no MCP servers (or backend isn't `local`), returns
 * an empty record.
 *
 * Failure mode: if any server can't be opened (network, spawn failure,
 * auth error), the throw bubbles. Hosts that want partial-success
 * semantics can wrap individual `pool.get()` calls themselves — this
 * helper is the simple all-or-nothing path.
 */
export async function loadMcpToolsFromRecipe(
  params: LoadMcpToolsFromRecipeParams,
): Promise<Record<string, AgentTool>> {
  const { recipe, pool } = params;
  if (recipe.backend.type !== "local") return {};
  const servers = recipe.backend.mcpServers ?? [];
  if (servers.length === 0) return {};
  const merged: Record<string, AgentTool> = {};
  for (const server of servers) {
    const client = await pool.get(server);
    const tools = await createMcpTools(client);
    for (const [name, tool] of Object.entries(tools)) {
      if (merged[name] !== undefined) {
        throw new Error(
          `loadMcpToolsFromRecipe: tool name collision for '${name}' across MCP servers ` +
            "on recipe '" +
            recipe.id +
            "'. Tool names mount as <serverName>:<toolName>; either rename one server or " +
            "configure them so their server names differ.",
        );
      }
      merged[name] = tool;
    }
  }
  return merged;
}
