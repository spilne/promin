// ---------------------------------------------------------------------------
// handleMcpEndpointRequest — answer one Streamable HTTP request to an MCP
// endpoint.
//
// Stateless: every request gets a fresh SDK `Server` + transport (no session
// id), answered with JSON rather than an SSE stream, so any replica can serve
// any request and nothing is held between them. `GET` (the server-initiated
// stream) and `DELETE` (session end) have nothing to do and answer 405.
//
//   tools/list  the endpoint's resolved tools, JSON Schema from the tool
//   tools/call  input validated by the tool's Zod `parameters`, then
//               `execute(input, ctx)` with the caller's scope, an abort
//               signal (client cancel or timeout) and the audit sink. A
//               throw or bad input is an `isError` result, not a protocol
//               error, so the client's model sees what went wrong.
// ---------------------------------------------------------------------------

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  type CallToolResult,
  type Tool,
} from "@modelcontextprotocol/sdk/types.js";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { ToolAuditLogger } from "../tool-audit/types.ts";
import { agentToolJsonSchema } from "../tool-json-schema.ts";
import type { AgentTool, ToolScope } from "../tool.ts";
import type {
  McpEndpointResolvedTool,
  RegisteredMcpEndpoint,
  ResolvedMcpEndpointTools,
} from "./types.ts";

/** Default per-call time limit for endpoint tools (ms). */
export const DEFAULT_MCP_ENDPOINT_TOOL_TIMEOUT_MS = 60_000;

/** Server name reported in `initialize` when the endpoint has no title. */
const SERVER_NAME_PREFIX = "promin";

export interface HandleMcpEndpointRequestParams {
  readonly req: Request;
  readonly endpoint: RegisteredMcpEndpoint;
  readonly tools: ResolvedMcpEndpointTools;
  /** Scope every tool call runs in (namespace, resource). */
  readonly scope: ToolScope;
  /** Per-call limit when the tool sets none. Default `DEFAULT_MCP_ENDPOINT_TOOL_TIMEOUT_MS`. */
  readonly defaultTimeoutMs?: number;
  /** Audit sink handed to elevated tools. */
  readonly toolAuditLogger?: ToolAuditLogger;
  /** Timer source for call timeouts. Default `SystemWallClock`. */
  readonly clock?: WallClock;
  /**
   * Appended to the error a scoped tool returns when `scope` lacks a
   * namespace or resource — how this host's callers supply one.
   */
  readonly scopeHint?: string;
  /** Called with every tool that threw, for server-side logs. */
  readonly onToolError?: (params: { readonly tool: string; readonly error: unknown }) => void;
}

export async function handleMcpEndpointRequest(
  params: HandleMcpEndpointRequestParams,
): Promise<Response> {
  const { req } = params;
  if (req.method !== "POST") {
    return Response.json(
      {
        jsonrpc: "2.0",
        error: { code: -32000, message: "Method not allowed: this endpoint is stateless." },
        id: null,
      },
      { status: 405, headers: { allow: "POST" } },
    );
  }
  const server = createEndpointServer(params);
  const transport = new WebStandardStreamableHTTPServerTransport({
    sessionIdGenerator: undefined,
    enableJsonResponse: true,
  });
  await server.connect(transport);
  try {
    return await transport.handleRequest(req);
  } finally {
    await server.close();
  }
}

function createEndpointServer(params: HandleMcpEndpointRequestParams): Server {
  const { endpoint, tools } = params;
  const server = new Server(
    {
      name: `${SERVER_NAME_PREFIX}-${endpoint.id}`,
      version: endpoint.version,
      ...(endpoint.title !== null && { title: endpoint.title }),
    },
    {
      capabilities: { tools: {} },
      ...(endpoint.instructions !== null && { instructions: endpoint.instructions }),
    },
  );
  server.setRequestHandler(ListToolsRequestSchema, () => ({
    tools: [...tools.tools.values()].map(toMcpTool),
  }));
  server.setRequestHandler(CallToolRequestSchema, (request, extra) => {
    const entry = tools.tools.get(request.params.name);
    if (entry === undefined) {
      return errorResult(`Unknown tool: ${request.params.name}`);
    }
    return callTool({
      ...params,
      entry,
      args: request.params.arguments ?? {},
      signal: extra.signal,
    });
  });
  return server;
}

function toMcpTool(entry: McpEndpointResolvedTool): Tool {
  return {
    name: entry.name,
    description: entry.description,
    inputSchema: agentToolJsonSchema(entry.tool) as Tool["inputSchema"],
    annotations: {
      ...(entry.tool.idempotent === true && { idempotentHint: true }),
    },
  };
}

async function callTool(
  params: HandleMcpEndpointRequestParams & {
    readonly entry: McpEndpointResolvedTool;
    readonly args: Record<string, unknown>;
    readonly signal: AbortSignal;
  },
): Promise<CallToolResult> {
  const { entry, scope } = params;
  const tool = entry.tool;
  if ((tool.kind === "scoped" || tool.kind === "elevated") && !hasTenantScope(scope)) {
    return errorResult(
      `${entry.name} acts for a namespace and resource, and this call has none.` +
        (params.scopeHint !== undefined ? ` ${params.scopeHint}` : ""),
    );
  }
  const parsed = tool.parameters.safeParse(params.args);
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((i) => `${i.path.length > 0 ? i.path.join(".") : "(input)"}: ${i.message}`)
      .join("; ");
    return errorResult(`Invalid arguments for ${entry.name}: ${issues}`);
  }

  const clock = params.clock ?? SystemWallClock;
  const timeoutMs =
    tool.timeoutMs ?? params.defaultTimeoutMs ?? DEFAULT_MCP_ENDPOINT_TOOL_TIMEOUT_MS;
  const controller = new AbortController();
  const onClientAbort = () => controller.abort(params.signal.reason);
  if (params.signal.aborted) controller.abort(params.signal.reason);
  else params.signal.addEventListener("abort", onClientAbort, { once: true });
  const timer = clock.setTimeout(
    () => controller.abort(new Error(`${entry.name} timed out after ${timeoutMs}ms`)),
    timeoutMs,
  );
  const aborted = new Promise<never>((_, reject) => {
    const fail = () => reject(asError(controller.signal.reason));
    if (controller.signal.aborted) fail();
    else controller.signal.addEventListener("abort", fail, { once: true });
  });
  // The race below may settle first; keep the losing branch from becoming
  // an unhandled rejection.
  aborted.catch(() => {});

  try {
    const output = await Promise.race([
      tool.execute(parsed.data, {
        scope,
        signal: controller.signal,
        timeoutMs,
        toolCallId: crypto.randomUUID(),
        ...(params.toolAuditLogger !== undefined && { toolAuditLogger: params.toolAuditLogger }),
      }),
      aborted,
    ]);
    return successResult(tool, output);
  } catch (error) {
    params.onToolError?.({ tool: entry.name, error });
    return errorResult(asError(error).message);
  } finally {
    timer.clear();
    params.signal.removeEventListener("abort", onClientAbort);
  }
}

function successResult(tool: AgentTool<any, any>, output: unknown): CallToolResult {
  const text =
    tool.toModelOutput !== undefined
      ? tool.toModelOutput(output)
      : typeof output === "string"
        ? output
        : (JSON.stringify(output) ?? "");
  return {
    content: [{ type: "text", text }],
    ...(isPlainObject(output) && { structuredContent: output }),
  };
}

function errorResult(message: string): CallToolResult {
  return { isError: true, content: [{ type: "text", text: message }] };
}

function hasTenantScope(scope: ToolScope): boolean {
  return (
    typeof scope.namespaceId === "string" &&
    scope.namespaceId.length > 0 &&
    typeof scope.resourceId === "string" &&
    scope.resourceId.length > 0
  );
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value ?? "aborted"));
}
