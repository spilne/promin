// ---------------------------------------------------------------------------
// MCP integration — connect agents to Model Context Protocol servers.
//
// Two layers of config, with different trust:
//
//   McpServerConfig  — operator-defined (host code, server config). Says how
//                      to reach a server (a stdio command, an http URL), the
//                      credential it gets, and how far to trust it (approval,
//                      idempotency, timeout, concurrency, tool cap). Never
//                      comes from a recipe.
//   McpServerRecipe  — what a recipe (tenant-editable data) may say: attach
//                      an operator-defined server by name, or — when the
//                      operator opts in — name an http endpoint that the
//                      outbound policy then vets. A recipe can never supply a
//                      command, env, cwd, header or credential.
//
// `loadMcpToolsFromRecipe` turns the recipe entries into configs against
// the operator's allowlist, opens clients through a `McpClientPool` and
// adapts each server tool into an `AgentTool` (`createMcpTools`).
// ---------------------------------------------------------------------------

/** Operator-defined MCP server: how to reach it and how far to trust it. */
export type McpServerConfig = StdioMcpServerConfig | HttpMcpServerConfig;

export interface BaseMcpServerConfig {
  /**
   * Logical id. Prefixes the server's tools (`<name>__<tool>`) and names
   * the server in the catalog. Recipes attach allowlisted servers by it.
   */
  readonly name: string;
  /** What the server is for. Shown in the catalog and the agent editor. */
  readonly description?: string;
  /**
   * `SecretsStorage` key of the credential the server needs. Resolved at
   * connect time at the caller's scope (resource → namespace → global), so a
   * tenant can supply its own. An http server sends it as a bearer token to
   * its own `url` only (redirects are not followed), a stdio server receives
   * it in the environment variable `credentialEnv`. Only operator-defined
   * servers carry credentials.
   */
  readonly credentialRef?: string;
  /** Per-call time limit (ms), also the deadline for listing tools. Default `DEFAULT_MCP_TIMEOUT_MS`. */
  readonly timeoutMs?: number;
  /**
   * Calls one client runs at once; further calls wait their turn.
   * Default `DEFAULT_MCP_MAX_CONCURRENT_CALLS`.
   */
  readonly maxConcurrentCalls?: number;
  /**
   * Whether every call waits for a human approval. Default `true`: an MCP
   * server is third-party code whose tools may act on the outside world.
   * Only the operator can turn it off.
   */
  readonly requireApproval?: boolean;
  /**
   * Whether the engine may re-run a call whose outcome a crash left
   * unknown (see `AgentTool.idempotent`). Default `false`.
   */
  readonly idempotent?: boolean;
  /** Expose only these MCP tool names. Default: every tool the server lists. */
  readonly tools?: ReadonlyArray<string>;
  /**
   * Most tools exposed from this server; the rest are skipped and reported.
   * Default `DEFAULT_MCP_MAX_TOOLS`.
   */
  readonly maxTools?: number;
}

export interface StdioMcpServerConfig extends BaseMcpServerConfig {
  readonly transport: "stdio";
  /** Executable to spawn, e.g. `"uvx"`. */
  readonly command: string;
  readonly args?: ReadonlyArray<string>;
  /**
   * Extra environment variables. The process otherwise gets only the MCP
   * SDK's safe default set (`PATH`, `HOME`, `USER`, …), never the host's
   * own environment.
   */
  readonly env?: Readonly<Record<string, string>>;
  /** Working directory for the spawned process. */
  readonly cwd?: string;
  /** Environment variable that receives the resolved `credentialRef`. */
  readonly credentialEnv?: string;
}

export interface HttpMcpServerConfig extends BaseMcpServerConfig {
  /** Streamable HTTP transport. */
  readonly transport: "http";
  readonly url: string;
  /** Static request headers (never the credential: use `credentialRef`). */
  readonly headers?: Readonly<Record<string, string>>;
  /**
   * Let this server live on a private, loopback or link-local address.
   * Default `false`: like any outbound call, the URL must resolve to public
   * addresses. Set it for an in-cluster server the operator runs.
   */
  readonly allowPrivateNetwork?: boolean;
}

/**
 * A recipe's MCP entry. Either attaches an operator-defined server by name
 * (`{ server }`), or names an http endpoint (`{ name, transport: "http",
 * url }`) — accepted only when the host opts in — that the outbound policy
 * vets and that always requires approval. `tools` narrows the server's
 * tools; it can never widen them. A recipe carries no credential: recipes
 * are not owned by the tenants that invoke them, so a recipe-chosen URL
 * must never receive a tenant's secret.
 */
export type McpServerRecipe = McpServerRefRecipe | McpHttpServerRecipe;

export interface McpServerRefRecipe {
  /** Name of an operator-defined server. */
  readonly server: string;
  readonly tools?: ReadonlyArray<string>;
}

export interface McpHttpServerRecipe {
  readonly name: string;
  readonly transport: "http";
  readonly url: string;
  readonly tools?: ReadonlyArray<string>;
}

/** Names MCP servers may have (operator-defined and recipe-supplied alike). */
export const MCP_RECIPE_SERVER_NAME = /^[A-Za-z0-9_-]{1,48}$/;

/** Default per-call time limit for MCP tools (ms). */
export const DEFAULT_MCP_TIMEOUT_MS = 60_000;
/** Default bound on concurrent calls through one MCP client. */
export const DEFAULT_MCP_MAX_CONCURRENT_CALLS = 4;
/** Default bound on the tools one server exposes. */
export const DEFAULT_MCP_MAX_TOOLS = 128;
/** Bound on the tools a recipe-supplied http server exposes. */
export const RECIPE_MCP_MAX_TOOLS = 32;

/**
 * One tool advertised by an MCP server. `inputSchema` is the JSON Schema of
 * its arguments, as the server sent it.
 */
export interface McpToolDefinition {
  readonly name: string;
  readonly description?: string;
  readonly inputSchema: Readonly<Record<string, unknown>>;
}

/** Result of one `callTool`: MCP content blocks plus the server's error flag. */
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

/** Per-call options for `McpClient.callTool` / `listTools`. */
export interface McpCallOptions {
  /** Aborts the request; the server is told to cancel it. */
  readonly signal?: AbortSignal;
  /** Time limit for the request (ms); for `listTools`, for all its pages together. */
  readonly timeoutMs?: number;
}

/**
 * One connection to an MCP server. The SDK-backed implementation lives in
 * `mcp-client.ts`.
 */
export interface McpClient {
  /**
   * True once the connection is gone for good: the transport closed (a
   * stdio server exited) or a request failed below the protocol (a
   * network error). A pool drops a closed client and connects anew.
   */
  readonly closed: boolean;
  listTools(options?: McpCallOptions): Promise<McpToolDefinition[]>;
  callTool(params: {
    readonly name: string;
    readonly arguments: Readonly<Record<string, unknown>>;
    readonly options?: McpCallOptions;
  }): Promise<McpToolResult>;
  /** Tear down the connection / process. Idempotent. */
  close(): Promise<void>;
}

/**
 * Where an MCP-backed `AgentTool` came from: the server and the tool's
 * original MCP name (the agent-facing name is sanitized).
 */
export interface McpToolOrigin {
  readonly server: string;
  readonly tool: string;
}
