// ---------------------------------------------------------------------------
// ZoryaMcpServers — the operator's MCP configuration for a Zorya server.
//
// Holds the allowlist of MCP servers recipes may attach by name (the only
// way a stdio server — a process on this host — can be reached, and the only
// servers that get credentials: command, args, env, cwd and credentialRef
// are defined here, never in a recipe), whether recipes may name their own
// http endpoints (operator opt-in; vetted by the outbound policy, always
// approval-gated, never given a credential), and the client pool every agent
// shares (isolated per config, tenant namespace and credential).
//
// The host passes the same instance as `ZoryaServerConfig.mcp` (recipe
// validation, catalog, dashboard) and uses `loadTools` in its `resolve`
// callback to hand `resolveLocalAgent` the recipe's MCP tools.
// ---------------------------------------------------------------------------

import {
  listMcpToolCatalog,
  loadMcpToolsFromRecipe,
  McpClientPool,
  MCP_RECIPE_SERVER_NAME,
  mcpRecipeProblems,
  type AgentBackend,
  type AgentTool,
  type McpCallerScope,
  type McpServerConfig,
  type OutboundPolicy,
  type RegisteredAgent,
  type SecretsStorage,
  type ToolCatalogListing,
} from "@promin/agent";
import { SystemWallClock, type WallClock } from "@promin/workflow";

/** Default bound on loading one recipe's MCP tools (connect + list), in ms. */
export const DEFAULT_MCP_LOAD_TIMEOUT_MS = 30_000;

export interface ZoryaMcpServersConfig {
  /**
   * MCP servers recipes may attach by name (`{ server: "<name>" }`). Names
   * must match `MCP_RECIPE_SERVER_NAME` and be unique.
   */
  servers: ReadonlyArray<McpServerConfig>;
  /**
   * Whether recipes may name their own Streamable HTTP endpoints
   * (`{ name, transport: "http", url }`). They are checked against
   * `outboundPolicy`, always require approval and never get a credential.
   * Default `false`.
   */
  recipeHttpServers?: boolean;
  /**
   * Which hosts http MCP servers may live on. Default: public http(s)
   * hosts only; an allowlisted server's `allowPrivateNetwork` widens it
   * for that server.
   */
  outboundPolicy?: OutboundPolicy;
  /** Where `credentialRef`s resolve (usually `ZoryaServerConfig.secrets`). */
  secrets?: SecretsStorage;
  /** Open MCP clients kept at most. Default 64. */
  maxClients?: number;
  /** Bound on loading one recipe's MCP tools, and on listing the catalog. Default `DEFAULT_MCP_LOAD_TIMEOUT_MS`. */
  loadTimeoutMs?: number;
  /** Server-side log for MCP server stderr and catalog failures. Default: console. */
  log?: (line: string, detail?: unknown) => void;
  /** Time source for the load deadline. Default `SystemWallClock`. */
  clock?: WallClock;
  /** Replace the client pool (tests connect in-memory servers). */
  pool?: McpClientPool;
}

/**
 * One allowlisted server as the dashboard sees it — never its command,
 * args, env, url, headers or credential name.
 */
export interface McpServerSummary {
  readonly name: string;
  readonly description?: string;
  readonly transport: McpServerConfig["transport"];
  readonly requireApproval: boolean;
  readonly idempotent: boolean;
  readonly timeoutMs?: number;
  readonly tools?: ReadonlyArray<string>;
}

export interface McpServersCatalogResponse {
  /** Whether MCP is configured on this server at all. */
  readonly configured: boolean;
  readonly servers: ReadonlyArray<McpServerSummary>;
  /** Whether recipes may attach their own http endpoints. */
  readonly recipeHttpServers: boolean;
}

export class ZoryaMcpServers {
  readonly servers: ReadonlyArray<McpServerConfig>;
  readonly recipeHttpServers: boolean;
  readonly pool: McpClientPool;
  private readonly secrets?: SecretsStorage;
  private readonly loadTimeoutMs: number;
  private readonly log: (line: string, detail?: unknown) => void;
  private readonly clock: WallClock;

  constructor(config: ZoryaMcpServersConfig) {
    const seen = new Set<string>();
    for (const server of config.servers) {
      if (!MCP_RECIPE_SERVER_NAME.test(server.name)) {
        throw new Error(
          `ZoryaMcpServers: server name ${JSON.stringify(server.name)} must match ${MCP_RECIPE_SERVER_NAME}`,
        );
      }
      if (seen.has(server.name)) {
        throw new Error(`ZoryaMcpServers: server name '${server.name}' is configured twice`);
      }
      seen.add(server.name);
    }
    this.servers = config.servers;
    this.recipeHttpServers = config.recipeHttpServers ?? false;
    if (config.secrets) this.secrets = config.secrets;
    this.loadTimeoutMs = config.loadTimeoutMs ?? DEFAULT_MCP_LOAD_TIMEOUT_MS;
    this.log = config.log ?? ((line, detail) => console.warn(line, ...(detail ? [detail] : [])));
    this.clock = config.clock ?? SystemWallClock;
    this.pool =
      config.pool ??
      new McpClientPool({
        ...(config.outboundPolicy !== undefined && { outboundPolicy: config.outboundPolicy }),
        ...(config.maxClients !== undefined && { maxClients: config.maxClients }),
        ...(config.clock !== undefined && { clock: config.clock }),
        onServerLog: (line) => this.log(line),
      });
  }

  /** Why `backend`'s MCP entries are not allowed here, or `undefined`. */
  recipeProblems(backend: AgentBackend): string | undefined {
    if (backend.type !== "local" || !backend.mcpServers?.length) return undefined;
    return mcpRecipeProblems({
      recipes: backend.mcpServers,
      servers: this.servers,
      recipeHttpServers: this.recipeHttpServers,
    });
  }

  /**
   * The recipe's MCP tools for the caller's scope — pass them to
   * `resolveLocalAgent` as `deps.mcpTools`. Empty for a recipe without MCP
   * servers. Throws when a server is not allowed or unreachable, when
   * loading takes longer than `loadTimeoutMs`, and when the recipe attaches
   * MCP servers but no scope is given (clients are pooled per tenant, so a
   * scope-less call could otherwise share one tenant's clients with
   * another).
   */
  async loadTools(params: {
    readonly recipe: RegisteredAgent;
    readonly scope: McpCallerScope | undefined;
    readonly signal?: AbortSignal;
  }): Promise<Record<string, AgentTool<Record<string, unknown>, string>>> {
    const { recipe, scope } = params;
    if (recipe.backend.type !== "local" || !recipe.backend.mcpServers?.length) return {};
    if (scope === undefined) {
      throw new Error(
        `Recipe '${recipe.id}' attaches MCP servers; resolving it needs the caller's scope.`,
      );
    }
    return this.withDeadline({
      what: "MCP tools did not load",
      ...(params.signal !== undefined && { signal: params.signal }),
      run: (signal) =>
        loadMcpToolsFromRecipe({
          recipe,
          servers: this.servers,
          recipeHttpServers: this.recipeHttpServers,
          pool: this.pool,
          ...(this.secrets !== undefined && { secrets: this.secrets }),
          scope,
          signal,
        }),
    });
  }

  /**
   * The allowlisted servers' tools, with an error per server that could not
   * be listed. Gives up after `loadTimeoutMs` overall.
   */
  catalog(): Promise<ToolCatalogListing> {
    return this.withDeadline({
      what: "MCP catalog was not listed",
      run: () =>
        listMcpToolCatalog({
          pool: this.pool,
          servers: this.servers,
          ...(this.secrets !== undefined && { secrets: this.secrets }),
          onError: ({ server, error }) => this.log(`[mcp:${server}] catalog listing failed`, error),
        }),
    });
  }

  /** Run `run`, rejecting once `loadTimeoutMs` passes or `signal` aborts. */
  private async withDeadline<T>(params: {
    readonly what: string;
    readonly signal?: AbortSignal;
    readonly run: (signal: AbortSignal) => Promise<T>;
  }): Promise<T> {
    const controller = new AbortController();
    const onAbort = () => controller.abort(params.signal?.reason);
    params.signal?.addEventListener("abort", onAbort, { once: true });
    const timer = this.clock.setTimeout(
      () => controller.abort(new Error(`${params.what} within ${this.loadTimeoutMs}ms`)),
      this.loadTimeoutMs,
    );
    const aborted = new Promise<never>((_, reject) => {
      controller.signal.addEventListener("abort", () => reject(controller.signal.reason), {
        once: true,
      });
    });
    aborted.catch(() => {});
    try {
      return await Promise.race([params.run(controller.signal), aborted]);
    } finally {
      timer.clear();
      params.signal?.removeEventListener("abort", onAbort);
    }
  }

  /** What the agent editor's MCP picker shows. */
  describe(): McpServersCatalogResponse {
    return {
      configured: true,
      recipeHttpServers: this.recipeHttpServers,
      servers: this.servers.map((s) => ({
        name: s.name,
        ...(s.description !== undefined && { description: s.description }),
        transport: s.transport,
        requireApproval: s.requireApproval ?? true,
        idempotent: s.idempotent ?? false,
        ...(s.timeoutMs !== undefined && { timeoutMs: s.timeoutMs }),
        ...(s.tools !== undefined && { tools: s.tools }),
      })),
    };
  }

  async start(): Promise<void> {}

  /** Close every MCP client (stdio process groups are killed) and refuse new ones. */
  async stop(): Promise<void> {
    await this.pool.closeAll();
  }
}
