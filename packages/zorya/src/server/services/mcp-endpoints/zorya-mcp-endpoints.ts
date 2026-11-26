// ---------------------------------------------------------------------------
// ZoryaMcpEndpoints — the MCP endpoints this server publishes (zorya as an
// MCP *server*; `ZoryaMcpServers` is the client side). Owns the endpoint
// registry, the process-local private tools of file-defined endpoints, the
// shared tool sources endpoints may reference, and the `mcp/` folder scan.
//
// The host passes it as `ZoryaServerConfig.mcpEndpoints`; the server then
// mounts `/api/mcp-endpoints/*` (CRUD + tool preview) and serves each
// endpoint at `/ns/:namespace/mcp/:id` (tenant) and `/mcp/:id` (global).
// ---------------------------------------------------------------------------

import {
  FILE_SOURCE,
  listAllVersioned,
  McpEndpointPrivateTools,
  mergeToolSources,
  registerMcpEndpointDefinitions,
  resolveMcpEndpointTools,
  startMcpEndpointScanLoop,
  type AgentTool,
  type ApplyDiscoveredResult,
  type McpEndpointDefinition,
  type McpEndpointRegistry,
  type McpEndpointToolSource,
  type RegisteredMcpEndpoint,
  type ResolvedMcpEndpointTools,
  type ScanLoopHandle,
  type ScanLoopTick,
  type ToolAuditLogger,
} from "@promin/agent";
import type { WallClock } from "@promin/workflow";

export interface ZoryaMcpEndpointsScanConfig {
  /** The `mcp/` folder to scan (`<id>.ts`, `<id>/index.ts` + `<id>/tools/`). */
  root: string;
  /** Poll interval in ms. Default 5000. */
  intervalMs?: number;
  /** Remove scanner-written endpoints whose file is gone (never operator-authored ones). Default false. */
  sync?: boolean;
  onTick?: (tick: ScanLoopTick) => void;
  /** A failed scan tick lands here. Default: console. */
  onError?: (err: unknown) => void;
}

export interface ZoryaMcpEndpointsConfig {
  registry: McpEndpointRegistry;
  /**
   * Shared tools endpoints may reference by name — typically the same
   * in-process record and `createFileToolRegistry` the agents resolve
   * against. Earlier sources win a name clash. Read on every request, so a
   * hot-reloading registry is seen live.
   */
  tools?: ReadonlyArray<McpEndpointToolSource>;
  /**
   * Endpoints declared in host code (`McpEndpoint.builder(id)…build()`),
   * registered on `start()` with `source: "code"`. More can be added later
   * with `define()`.
   */
  endpoints?: ReadonlyArray<McpEndpointDefinition>;
  /** Folder scan of file-defined endpoints. Omit to disable. */
  scan?: ZoryaMcpEndpointsScanConfig;
  /** Per-call limit for tools that set none (ms). Default 60 000. */
  defaultTimeoutMs?: number;
  /** Audit sink handed to elevated tools an endpoint exposes. */
  toolAuditLogger?: ToolAuditLogger;
  /** Timer source for call timeouts and the scan loop. Default: system clock. */
  clock?: WallClock;
  /** Tools that threw during a call, for server-side logs. Default: console. */
  onToolError?: (params: {
    readonly endpoint: string;
    readonly tool: string;
    readonly error: unknown;
  }) => void;
}

export class ZoryaMcpEndpoints {
  readonly registry: McpEndpointRegistry;
  readonly privateTools = new McpEndpointPrivateTools();
  readonly defaultTimeoutMs: number | undefined;
  readonly toolAuditLogger: ToolAuditLogger | undefined;
  readonly clock: WallClock | undefined;
  private readonly sources: ReadonlyArray<McpEndpointToolSource>;
  private readonly scanConfig: ZoryaMcpEndpointsScanConfig | undefined;
  private readonly initial: ReadonlyArray<McpEndpointDefinition>;
  private readonly onToolErrorHook: ZoryaMcpEndpointsConfig["onToolError"];
  private scanHandle: ScanLoopHandle<ScanLoopTick> | undefined;

  constructor(config: ZoryaMcpEndpointsConfig) {
    this.registry = config.registry;
    this.sources = config.tools ?? [];
    this.scanConfig = config.scan;
    this.initial = config.endpoints ?? [];
    this.defaultTimeoutMs = config.defaultTimeoutMs;
    this.toolAuditLogger = config.toolAuditLogger;
    this.clock = config.clock;
    this.onToolErrorHook = config.onToolError;
  }

  /** The shared tools endpoints may reference, keyed by name. */
  sharedTools(): Readonly<Record<string, AgentTool<any, any>>> {
    return mergeToolSources(this.sources);
  }

  /** What `endpoint` exposes right now, and what it had to leave out. */
  resolveTools(endpoint: RegisteredMcpEndpoint): ResolvedMcpEndpointTools {
    return resolveMcpEndpointTools({
      endpoint,
      shared: this.sharedTools(),
      privateTools: this.privateTools.get(endpoint.id),
    });
  }

  onToolError(params: { endpoint: string; tool: string; error: unknown }): void {
    if (this.onToolErrorHook) {
      try {
        this.onToolErrorHook(params);
      } catch {
        // A throwing log hook must not fail the call.
      }
      return;
    }
    console.error(
      `[zorya] mcp endpoint ${params.endpoint}: tool ${params.tool} failed`,
      params.error,
    );
  }

  /**
   * Register endpoints declared in host code: installs their private tools
   * and stores a version when the definition changed (`source: "code"`).
   * Throws before writing when one is invalid or lacks an id.
   */
  async define(
    definitions: McpEndpointDefinition | ReadonlyArray<McpEndpointDefinition>,
  ): Promise<ApplyDiscoveredResult> {
    return registerMcpEndpointDefinitions({
      registry: this.registry,
      privateTools: this.privateTools,
      definitions: isDefinitionList(definitions) ? definitions : [definitions],
    });
  }

  /** Ids whose active version a file scan wrote. */
  async fileManagedIds(): Promise<string[]> {
    const managed = await listAllVersioned({ registry: this.registry, source: FILE_SOURCE });
    return managed.map((e) => e.id);
  }

  async start(): Promise<void> {
    if (this.initial.length > 0) await this.define(this.initial);
    if (this.scanHandle || !this.scanConfig) return;
    const scan = this.scanConfig;
    this.scanHandle = startMcpEndpointScanLoop({
      registry: this.registry,
      privateTools: this.privateTools,
      root: scan.root,
      ...(scan.intervalMs !== undefined && { intervalMs: scan.intervalMs }),
      ...(scan.sync !== undefined && { sync: scan.sync }),
      ...(scan.onTick !== undefined && { onTick: scan.onTick }),
      ...(scan.onError !== undefined && { onError: scan.onError }),
      ...(this.clock !== undefined && { clock: this.clock }),
    });
    // Populate now so file endpoints answer right after boot.
    await this.scanHandle.tick();
  }

  async stop(): Promise<void> {
    this.scanHandle?.stop();
    this.scanHandle = undefined;
  }
}

function isDefinitionList(
  value: McpEndpointDefinition | ReadonlyArray<McpEndpointDefinition>,
): value is ReadonlyArray<McpEndpointDefinition> {
  return Array.isArray(value);
}
