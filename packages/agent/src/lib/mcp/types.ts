// ---------------------------------------------------------------------------
// MCP integration — connect promin agents to Model Context Protocol servers.
//
// Customer-side data integration: customer runs MCP servers (Confluence,
// GitHub, Postgres, Slack, Notion, Linear, etc.) inside their network
// with their own creds; agents call those servers as tools. Data stays
// customer-side; only the per-call slice the agent needs flows out.
//
// Why an abstract `McpClient` rather than direct SDK imports everywhere:
//   1. Tests can use a fake client without spinning up a real MCP server.
//   2. Future transports / mock servers / record-replay implementations
//      slot in without touching `createMcpTools`.
//   3. The SDK API is JS-side rich; this interface narrows it to what
//      the agent layer actually needs (list + call + close).
//
// `createMcpTools(client)` adapts the four `listTools` / `callTool`
// methods into AgentTools the agent loop already understands. The agent
// loop sees them as ordinary tools — no MCP awareness required.
// ---------------------------------------------------------------------------

/**
 * Declarative MCP server connection config. The `transport` field
 * discriminates how the client talks to the server:
 *
 *   stdio — spawn a local process (typical for CLI MCP servers)
 *   http  — connect to a remote MCP server over HTTP (Streamable HTTP)
 *   sse   — connect to a remote MCP server over Server-Sent Events
 *
 * `auth` is currently bearer-only; richer auth (OAuth, mTLS) lands
 * with later transport / cloud needs.
 */
export type McpServerConfig = StdioMcpServerConfig | HttpMcpServerConfig | SseMcpServerConfig;

export interface BaseMcpServerConfig {
  /** Logical id for this server. Tools mount under `<name>:<toolName>`. */
  readonly name: string;
  /**
   * Optional credential lookup name. When set, the host resolves the
   * value from a secrets store at connect time. Lets recipes ship the
   * server config without embedding API tokens. Forward-compat with
   * promin-an9l (BYOK credentialRef).
   */
  readonly credentialRef?: string;
  /** Per-call timeout in milliseconds. Default: no timeout. */
  readonly timeoutMs?: number;
}

export interface StdioMcpServerConfig extends BaseMcpServerConfig {
  readonly transport: "stdio";
  /** Executable to spawn, e.g. `["uvx", "mcp-server-postgres"]`. */
  readonly command: ReadonlyArray<string>;
  /** Extra environment variables for the spawned process. */
  readonly env?: Readonly<Record<string, string>>;
  /** Working directory for the spawned process. */
  readonly cwd?: string;
}

export interface HttpMcpServerConfig extends BaseMcpServerConfig {
  readonly transport: "http";
  readonly url: string;
  /** Optional bearer token. Use `credentialRef` instead for BYOK. */
  readonly authToken?: string;
}

export interface SseMcpServerConfig extends BaseMcpServerConfig {
  readonly transport: "sse";
  readonly url: string;
  readonly authToken?: string;
}

/**
 * One tool advertised by an MCP server. Mirrors the MCP spec's tool
 * descriptor. `inputSchema` is JSON Schema (the spec's canonical form);
 * `createMcpTools` keeps it as JSON Schema so MCP servers stay the
 * source of truth for parameter validation.
 */
export interface McpToolDefinition {
  readonly name: string;
  readonly description?: string;
  readonly inputSchema: Readonly<Record<string, unknown>>;
}

/**
 * Result of one `callTool` invocation. MCP returns content blocks
 * (text, images, resources, etc.); for the agent loop we surface a
 * unified textual representation. Callers that need richer media
 * routing can extend the adapter to forward through the agent loop's
 * `tool.progress` channel.
 */
export interface McpToolResult {
  readonly isError?: boolean;
  readonly content: ReadonlyArray<McpContentBlock>;
}

export type McpContentBlock =
  | { readonly type: "text"; readonly text: string }
  | { readonly type: "image"; readonly data: string; readonly mimeType: string }
  | {
      readonly type: "resource";
      readonly resource: {
        readonly uri: string;
        readonly text?: string;
        readonly mimeType?: string;
      };
    };

/**
 * Abstract MCP client. The SDK-backed implementation lives in
 * `mcp-client.ts`; tests use a fake client that returns canned
 * responses without spawning a server.
 */
export interface McpClient {
  /** Logical name (matches the config) — surfaces in tool prefixes. */
  readonly name: string;
  /** Probe the server for its tool list. Cached by callers as needed. */
  listTools(): Promise<McpToolDefinition[]>;
  /** Invoke a tool by name with arbitrary JSON arguments. */
  callTool(params: {
    readonly name: string;
    readonly arguments: Readonly<Record<string, unknown>>;
  }): Promise<McpToolResult>;
  /** Tear down the connection / spawn. Idempotent. */
  close(): Promise<void>;
}
