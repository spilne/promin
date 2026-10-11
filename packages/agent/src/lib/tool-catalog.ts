// ---------------------------------------------------------------------------
// `AgentToolCatalog` — read-only metadata aggregator over every tool
// the host has wired in. Drives the Designer's tool multi-select; also
// queryable programmatically for discovery / docs.
//
// Sources of tools today:
//   - in-process tools record (host's static map)
//   - file-discovered tools (createFileToolRegistry)
//   - operator-configured MCP servers (listed through a McpClientPool)
//
// The catalog returns a SOURCE marker per entry so the UI can display
// where each tool comes from, and an error per source it could not list
// (an unreachable MCP server, a tool with an unusable schema). It never
// stores or returns executable functions, just metadata.
// ---------------------------------------------------------------------------

import type { AgentTool } from "./tool.ts";
import { agentToolJsonSchema } from "./tool-json-schema.ts";
import type { McpClientPool } from "./mcp/mcp-client-pool.ts";
import { createMcpTools, displayToolName, type McpSkipCode } from "./mcp/mcp-tools.ts";
import {
  resolveMcpCredential,
  type ResolvedMcpCredential,
} from "./mcp/load-mcp-tools-from-recipe.ts";
import type { McpServerConfig } from "./mcp/types.ts";
import type { SecretsStorage } from "./secrets/types.ts";

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
  /** Whether each call waits for a human approval. */
  readonly requireApproval: boolean;
  /** Whether the engine may re-run the call after a crash (`AgentTool.idempotent`). */
  readonly idempotent: boolean;
  /**
   * Secret-store keys the tool needs to run (from its scoped/elevated
   * `secrets` config; MCP servers' credential names are not exposed). Lets the
   * catalog UI badge a tool's secret dependencies and lets reconciliation
   * ask "is this resolvable at namespace X".
   */
  readonly requiredSecrets: ReadonlyArray<string>;
  /**
   * True when the tool declares scoped `memory` (receives a
   * `ctx.memory` handle). Lets the catalog UI badge memory-using tools.
   */
  readonly usesMemory: boolean;
}

/**
 * Where a tool comes from. MCP tools carry their server, the tool's MCP
 * name and the transport; their descriptions and outputs are untrusted
 * (written by the server), and they reach an agent only through the
 * recipe's `mcpServers`, never through a role's `tools`.
 */
export type ToolCatalogSource =
  | { readonly kind: "in-process" }
  | { readonly kind: "file"; readonly path?: string }
  | {
      readonly kind: "mcp";
      readonly server: string;
      readonly tool: string;
      readonly transport: McpServerConfig["transport"];
    };

/** Why a catalog source (or one of its tools) could not be listed. */
export type ToolCatalogErrorCode = "unreachable" | "credential_missing" | McpSkipCode;

/**
 * A source the catalog could not (fully) list. `message` is a fixed text
 * for `code` — never the underlying error, which may name commands, hosts
 * or server output; that goes to the source's `onError`.
 */
export interface ToolCatalogError {
  readonly kind: "mcp";
  readonly server: string;
  /** Set when one tool was left out; unset when the whole server failed. */
  readonly tool?: string;
  readonly code: ToolCatalogErrorCode;
  readonly message: string;
}

export interface ToolCatalogListing {
  readonly tools: ToolCatalogEntry[];
  readonly errors: ToolCatalogError[];
}

export interface AgentToolCatalog {
  /** Snapshot every available tool, plus what could not be listed. */
  list(): Promise<ToolCatalogListing>;
}

/** MCP servers a catalog enumerates. */
export interface McpCatalogSource {
  readonly pool: McpClientPool;
  readonly servers: ReadonlyArray<McpServerConfig>;
  /**
   * Resolves `credentialRef`s at global scope for the listing. Servers
   * whose credential only exists per tenant report an error instead.
   */
  readonly secrets?: SecretsStorage;
  /** Receives the detail behind each `errors` entry, for server-side logs. */
  readonly onError?: (params: { readonly server: string; readonly error: unknown }) => void;
}

export interface DefaultAgentToolCatalogConfig {
  /**
   * Host-wired tool map (the same one passed to resolveLocalAgent
   * via deps.tools). Reported as `source: { kind: 'in-process' }`.
   */
  readonly inProcess?: Readonly<Record<string, AgentTool<any, any>>>;
  /**
   * File-discovered tools. Same shape as `inProcess` but reported
   * with `source: { kind: 'file' }`. Pass the result of
   * `createFileToolRegistry(...).getTools()`.
   */
  readonly file?: Readonly<Record<string, AgentTool<any, any>>>;
  /**
   * Operator-configured MCP servers. Each server's tools are listed
   * (one round trip per server) and reported with
   * `source: { kind: 'mcp', … }`; a server that fails lands in `errors`.
   */
  readonly mcp?: McpCatalogSource;
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

  async list(): Promise<ToolCatalogListing> {
    const out: ToolCatalogEntry[] = [];
    const seen = new Set<string>();
    const add = (entry: ToolCatalogEntry) => {
      if (seen.has(entry.name)) return;
      out.push(entry);
      seen.add(entry.name);
    };

    for (const [name, tool] of Object.entries(this.config.inProcess ?? {})) {
      add(toEntry(name, tool, { kind: "in-process" }));
    }
    for (const [name, tool] of Object.entries(this.config.file ?? {})) {
      add(toEntry(name, tool, { kind: "file" }));
    }
    let errors: ToolCatalogError[] = [];
    if (this.config.mcp) {
      const mcp = await listMcpToolCatalog(this.config.mcp);
      for (const entry of mcp.tools) add(entry);
      errors = mcp.errors;
    }

    out.sort((a, b) => a.name.localeCompare(b.name));
    return { tools: out, errors };
  }
}

/**
 * List the tools of operator-configured MCP servers as catalog entries.
 * Servers are listed in parallel at global scope; a server that can't be
 * reached, or a tool that can't be exposed, is reported in `errors`.
 */
export async function listMcpToolCatalog(source: McpCatalogSource): Promise<ToolCatalogListing> {
  const results = await Promise.all(
    source.servers.map(async (server): Promise<ToolCatalogListing> => {
      try {
        let credential: ResolvedMcpCredential | undefined;
        try {
          credential = await resolveMcpCredential({
            server,
            ...(source.secrets !== undefined && { secrets: source.secrets }),
          });
        } catch (err) {
          source.onError?.({ server: server.name, error: err });
          return {
            tools: [],
            errors: [mcpError({ server: server.name, code: "credential_missing" })],
          };
        }
        const handle = source.pool.server({
          server,
          ...(credential !== undefined && {
            credential: credential.value,
            credentialScope: credential.scope,
          }),
        });
        const set = await createMcpTools({ handle });
        return {
          tools: Object.entries(set.tools).map(([name, tool]) =>
            toEntry(name, tool, {
              kind: "mcp",
              server: server.name,
              tool: displayToolName(tool.mcp?.tool ?? name),
              transport: server.transport,
            }),
          ),
          errors: set.skipped.map((s) =>
            mcpError({ server: server.name, tool: s.tool, code: s.code }),
          ),
        };
      } catch (err) {
        source.onError?.({ server: server.name, error: err });
        return { tools: [], errors: [mcpError({ server: server.name, code: "unreachable" })] };
      }
    }),
  );
  return {
    tools: results.flatMap((r) => r.tools),
    errors: results.flatMap((r) => r.errors),
  };
}

const MCP_ERROR_MESSAGES: Record<ToolCatalogErrorCode, string> = {
  unreachable: "The server could not be reached or listed.",
  credential_missing: "The server's credential is not set at global scope.",
  invalid_name: "The tool's name is not a valid MCP tool name.",
  schema_too_large: "The tool's input schema is too large.",
  schema_unusable: "The tool's input schema does not describe an object.",
  name_taken: "Another tool already has this tool's name.",
  too_many_tools: "The server lists more tools (or schema) than allowed.",
};

function mcpError(params: {
  server: string;
  tool?: string;
  code: ToolCatalogErrorCode;
}): ToolCatalogError {
  return {
    kind: "mcp",
    server: params.server,
    ...(params.tool !== undefined && { tool: params.tool }),
    code: params.code,
    message: MCP_ERROR_MESSAGES[params.code],
  };
}

function toEntry(
  name: string,
  tool: AgentTool<any, any>,
  source: ToolCatalogSource,
): ToolCatalogEntry {
  return {
    name,
    description: tool.description,
    parameters: agentToolJsonSchema(tool),
    source,
    enabled: tool.enabled !== false,
    requireApproval: tool.requireApproval === true,
    idempotent: tool.idempotent === true,
    requiredSecrets: tool.requiredSecrets ?? [],
    usesMemory: tool.usesMemory === true,
  };
}
