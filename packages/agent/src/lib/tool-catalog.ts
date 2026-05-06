// ---------------------------------------------------------------------------
// `AgentToolCatalog` — read-only metadata aggregator over every tool
// the host has wired in. Drives the Designer's tool multi-select; also
// queryable programmatically for discovery / docs.
//
// Sources of tools today:
//   - in-process tools record (host's static map)
//   - file-discovered tools (createFileToolRegistry)
//   - MCP-server tools (loaded per-recipe via McpClientPool)
//
// The catalog returns a SOURCE marker per entry so the UI can display
// where each tool comes from. Implementations stay code-side — the
// catalog never stores or returns executable functions, just metadata.
// ---------------------------------------------------------------------------

// biome-ignore lint/suspicious/noExplicitAny: tools accept arbitrary input/output shapes
import type { AgentTool } from "./tool.ts";
import { zodToJsonSchema } from "./zod-to-json-schema.ts";
import type { McpClientPool } from "./mcp/mcp-client-pool.ts";
import type { McpServerConfig } from "./mcp/types.ts";

/**
 * One tool entry in the catalog. Mirrors LLMToolDefinition (same
 * name + description + JSON Schema params shape) plus a `source`
 * discriminator the UI uses to render badges / tooltips.
 */
export interface ToolCatalogEntry {
  readonly name: string;
  readonly description: string;
  /** JSON Schema for the tool's parameters. */
  readonly parameters: Record<string, unknown>;
  /** Where the tool implementation came from. */
  readonly source: ToolCatalogSource;
  /**
   * Operator kill-switch state. `true` (default) means the tool is
   * pickable by recipes; `false` means it's wired but disabled —
   * still surfaced in the catalog so operators see the gap, but
   * resolveLocalAgent rejects recipes that reference it.
   */
  readonly enabled: boolean;
}

export type ToolCatalogSource =
  | { readonly kind: "in-process" }
  | { readonly kind: "file"; readonly path?: string }
  | { readonly kind: "mcp"; readonly server: string };

export interface AgentToolCatalog {
  /** Snapshot every available tool. */
  listAll(): Promise<ToolCatalogEntry[]>;
}

export interface DefaultAgentToolCatalogConfig {
  /**
   * Host-wired tool map (the same one passed to resolveLocalAgent
   * via deps.tools). Reported as `source: { kind: 'in-process' }`.
   */
  // biome-ignore lint/suspicious/noExplicitAny: tools accept arbitrary input/output shapes
  readonly inProcess?: Readonly<Record<string, AgentTool<any, any>>>;
  /**
   * File-discovered tools. Same shape as `inProcess` but reported
   * with `source: { kind: 'file' }`. Pass the result of
   * `createFileToolRegistry(...).getTools()`.
   */
  // biome-ignore lint/suspicious/noExplicitAny: tools accept arbitrary input/output shapes
  readonly file?: Readonly<Record<string, AgentTool<any, any>>>;
  /**
   * MCP server pool + the list of servers to enumerate. The catalog
   * walks each server's `listTools()` and reports `source: { kind: 'mcp', server: <name> }`.
   * Heavy: each enumeration is one round trip per server. Cache the
   * catalog result at the host if the dashboard calls it frequently.
   */
  readonly mcp?: {
    readonly pool: McpClientPool;
    readonly servers: ReadonlyArray<McpServerConfig>;
  };
}

/**
 * Default catalog impl. Aggregation precedence on name collision:
 *   in-process > file > mcp
 * The first source to claim a name wins; subsequent sources with the
 * same name are dropped silently (the precedence matches resolution
 * order in resolveLocalAgent).
 */
export class DefaultAgentToolCatalog implements AgentToolCatalog {
  constructor(private readonly config: DefaultAgentToolCatalogConfig) {}

  async listAll(): Promise<ToolCatalogEntry[]> {
    const out: ToolCatalogEntry[] = [];
    const seen = new Set<string>();

    if (this.config.inProcess) {
      for (const [name, tool] of Object.entries(this.config.inProcess)) {
        if (seen.has(name)) continue;
        out.push(toEntry(name, tool, { kind: "in-process" }));
        seen.add(name);
      }
    }

    if (this.config.file) {
      for (const [name, tool] of Object.entries(this.config.file)) {
        if (seen.has(name)) continue;
        out.push(toEntry(name, tool, { kind: "file" }));
        seen.add(name);
      }
    }

    if (this.config.mcp) {
      for (const server of this.config.mcp.servers) {
        try {
          const client = await this.config.mcp.pool.get(server);
          const tools = await client.listTools();
          for (const tool of tools) {
            const mounted = server.name ? `${server.name}:${tool.name}` : tool.name;
            if (seen.has(mounted)) continue;
            out.push({
              name: mounted,
              description: tool.description ?? `MCP tool '${tool.name}' (no description)`,
              parameters: { ...tool.inputSchema },
              source: { kind: "mcp", server: server.name },
              // MCP tools are always 'enabled' from the catalog's POV —
              // the MCP server itself is the kill-switch (remove server
              // from the recipe to disable). Per-tool toggling is the
              // server's responsibility.
              enabled: true,
            });
            seen.add(mounted);
          }
        } catch {
          // Server unreachable: drop its tools from the catalog
          // rather than failing the whole listAll(). The UI sees
          // 'no tools from <server>' rather than nothing.
        }
      }
    }

    out.sort((a, b) => a.name.localeCompare(b.name));
    return out;
  }
}

// biome-ignore lint/suspicious/noExplicitAny: tools accept arbitrary input/output shapes
function toEntry(
  name: string,
  tool: AgentTool<any, any>,
  source: ToolCatalogSource,
): ToolCatalogEntry {
  return {
    name,
    description: tool.description,
    parameters: zodToJsonSchema(tool.parameters),
    source,
    enabled: tool.enabled !== false,
  };
}
