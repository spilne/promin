// ---------------------------------------------------------------------------
// `createMcpTools(client)` — turn one MCP server's tools into AgentTools
// the agent loop already understands. The agent doesn't know the tool
// came from MCP; it just sees `name` / `description` / `parameters` /
// `execute`.
//
// Naming: tools mount under `<serverName>:<toolName>` so multiple MCP
// servers can coexist without collisions. Agents that want a tool with
// just `<toolName>` can configure their server with `name: ""`.
//
// Parameter schemas: the MCP spec uses JSON Schema; promin's AgentTool
// expects a Zod schema. Rather than introduce a JSON-Schema → Zod
// converter (extra dep, conversion gaps), we use `z.record(z.string(), z.unknown())`
// as the AgentTool parameters and inline the JSON Schema text into the
// description so the LLM sees the field-level shape. The MCP server is
// the source of truth for validation; if the LLM passes garbage, the
// MCP server returns an error result and the loop continues.
// ---------------------------------------------------------------------------

import { z } from "zod";
import type { AgentTool, ToolExecuteContext } from "../tool.ts";
import type { McpClient, McpToolDefinition, McpToolResult } from "./types.ts";

/**
 * Build AgentTools for every tool advertised by `client`. Returns the
 * tools keyed by their MOUNTED name (`<server>:<tool>` by default), so
 * callers can spread them into an agent's tool registry.
 *
 * Calls `client.listTools()` once at construction. Hosts that need
 * hot-reloaded tool lists should re-call this when the server's
 * tool set changes.
 */
export async function createMcpTools(client: McpClient): Promise<Record<string, AgentTool>> {
  const tools = await client.listTools();
  const out: Record<string, AgentTool> = {};
  for (const def of tools) {
    const mounted = mountedName(client.name, def.name);
    out[mounted] = wrapMcpTool(client, mounted, def);
  }
  return out;
}

function mountedName(serverName: string, toolName: string): string {
  if (!serverName || serverName.length === 0) return toolName;
  return `${serverName}:${toolName}`;
}

function wrapMcpTool(client: McpClient, mountedName: string, def: McpToolDefinition): AgentTool {
  const description = composeDescription(def);
  // Parameters: accept anything (validated server-side). Using
  // `z.record(z.string(), z.unknown())` keeps the AgentTool runtime happy without
  // a JSON-Schema → Zod conversion step.
  const parameters = z.record(z.string(), z.unknown());
  return {
    name: mountedName,
    description,
    parameters,
    execute: async (input: unknown, ctx?: ToolExecuteContext) => {
      const args = (input ?? {}) as Record<string, unknown>;
      let result: McpToolResult;
      try {
        result = await client.callTool({ name: def.name, arguments: args });
      } catch (err) {
        return formatError(err);
      }
      const text = stringifyResult(result);
      // Surface errors to the agent loop without throwing — the loop
      // treats `Error: ...` content as a failed tool call but keeps
      // going (matches existing tool error semantics).
      if (result.isError) {
        ctx?.writer?.write({ phase: "mcp_error", server: client.name, tool: def.name });
        return `Error from MCP tool '${mountedName}': ${text}`;
      }
      return text;
    },
  };
}

function composeDescription(def: McpToolDefinition): string {
  const head = def.description ?? `MCP tool '${def.name}' (no description)`;
  // Inline a compact JSON Schema preview so the LLM has parameter shape
  // without us materializing a Zod schema. Limit to ~2KB to stop a
  // huge schema from drowning the system prompt.
  const schema = JSON.stringify(def.inputSchema);
  if (schema.length === 0 || schema === "{}") return head;
  const trimmed = schema.length > 2_000 ? `${schema.slice(0, 2_000)}…` : schema;
  return `${head}\n\nParameter schema (JSON Schema): ${trimmed}`;
}

function stringifyResult(result: McpToolResult): string {
  // MCP returns content blocks; the agent loop wants a single string.
  // Concatenate text blocks; for image / resource blocks include a
  // structured marker so the LLM knows the result was non-text.
  const parts: string[] = [];
  for (const block of result.content) {
    switch (block.type) {
      case "text":
        parts.push(block.text);
        break;
      case "image":
        parts.push(`[image: ${block.mimeType}, ${block.data.length} bytes base64]`);
        break;
      case "resource":
        parts.push(
          block.resource.text !== undefined
            ? `[resource ${block.resource.uri}]\n${block.resource.text}`
            : `[resource ${block.resource.uri}]`,
        );
        break;
    }
  }
  return parts.join("\n");
}

function formatError(err: unknown): string {
  return `Error: ${err instanceof Error ? err.message : String(err)}`;
}
