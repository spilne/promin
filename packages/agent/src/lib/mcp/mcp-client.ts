// ---------------------------------------------------------------------------
// SDK-backed `McpClient` and the transports it runs over.
//
//   createMcpTransport  — stdio (spawn the operator's command) or Streamable
//                         HTTP (every request through the outbound fetch, so
//                         the SSRF policy and DNS pinning apply).
//   connectMcpClient    — connect an SDK `Client` over any transport (tests
//                         pass the SDK's in-memory transport) and narrow it
//                         to `McpClient`.
//
// The client marks itself `closed` when the transport closes (a stdio
// server exited) or a request fails below the protocol (a network error),
// so the pool replaces it on the next call.
//
// What a server sends is bounded: `tools/list` stops after
// `MAX_TOOL_LIST_PAGES` pages or `MAX_LISTED_TOOLS` tools and runs under one
// overall deadline; http responses are cut at `maxResponseBytes`; output
// schemas are never compiled (see mcp-schema-validator.ts); the client
// advertises no capabilities, so the server can't ask for sampling or roots.
// ---------------------------------------------------------------------------

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import {
  ErrorCode,
  McpError,
  ToolListChangedNotificationSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { createOutboundFetch } from "../outbound/outbound-fetch.ts";
import type { OutboundPolicy } from "../outbound/outbound-policy.ts";
import { ACCEPT_ANY_OUTPUT } from "./mcp-schema-validator.ts";
import { ProcessGroupStdioTransport } from "./mcp-stdio-transport.ts";
import {
  DEFAULT_MCP_TIMEOUT_MS,
  type McpCallOptions,
  type McpClient,
  type McpContentBlock,
  type McpServerConfig,
  type McpToolDefinition,
  type McpToolResult,
} from "./types.ts";

/** Most `tools/list` pages read from one server. */
export const MAX_TOOL_LIST_PAGES = 16;
/** Most tools read from one server's `tools/list`; more fails the listing. */
export const MAX_LISTED_TOOLS = 1_024;
/** Default cap on one http response body from an MCP server. */
export const DEFAULT_MCP_MAX_RESPONSE_BYTES = 8 * 1024 * 1024;

/** An http response body larger than allowed. */
export class McpResponseTooLargeError extends Error {
  constructor(limit: number) {
    super(`MCP server response exceeds ${limit} bytes`);
    this.name = "McpResponseTooLargeError";
  }
}

const CLIENT_INFO = { name: "promin-agent", version: "0.1.0" };

/**
 * Build the transport for `server`. `credential` is the resolved
 * `credentialRef`: a bearer token for http, the `credentialEnv` variable
 * for stdio. `policy` governs which hosts an http server may live on.
 */
export function createMcpTransport(params: {
  readonly server: McpServerConfig;
  readonly credential?: string;
  readonly policy?: OutboundPolicy;
  /** Cap on each http response body. Default `DEFAULT_MCP_MAX_RESPONSE_BYTES`. */
  readonly maxResponseBytes?: number;
  /** Receives a stdio server's stderr lines. */
  readonly onStderr?: (line: string) => void;
  readonly clock?: WallClock;
}): Transport {
  const { server, credential } = params;
  if (server.transport === "stdio") {
    if (credential !== undefined && server.credentialEnv === undefined) {
      throw new Error(
        `MCP server '${server.name}' has a credentialRef but no credentialEnv to pass it in.`,
      );
    }
    const env: Record<string, string> = { ...server.env };
    if (credential !== undefined && server.credentialEnv !== undefined) {
      env[server.credentialEnv] = credential;
    }
    return new ProcessGroupStdioTransport({
      command: server.command,
      args: server.args ?? [],
      env,
      ...(server.cwd !== undefined && { cwd: server.cwd }),
      ...(params.onStderr !== undefined && { onStderr: params.onStderr }),
      ...(params.clock !== undefined && { clock: params.clock }),
    });
  }
  const policy: OutboundPolicy = {
    ...params.policy,
    ...(server.allowPrivateNetwork === true && { allowPrivateNetworks: true }),
  };
  const outbound = createOutboundFetch({ policy, maxRedirects: 0 });
  const limit = params.maxResponseBytes ?? DEFAULT_MCP_MAX_RESPONSE_BYTES;
  const headers: Record<string, string> = { ...server.headers };
  if (credential !== undefined) headers["authorization"] = `Bearer ${credential}`;
  return new StreamableHTTPClientTransport(new URL(server.url), {
    fetch: async (url, init) => capResponse({ response: await outbound(String(url), init), limit }),
    requestInit: { headers },
  });
}

/** `response` with its body cut off (as an error) past `limit` bytes. */
function capResponse(params: { response: Response; limit: number }): Response {
  const { response, limit } = params;
  const declared = Number(response.headers.get("content-length") ?? "NaN");
  if (Number.isFinite(declared) && declared > limit) {
    void response.body?.cancel().catch(() => {});
    throw new McpResponseTooLargeError(limit);
  }
  if (!response.body) return response;
  let seen = 0;
  const counted = response.body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        seen += chunk.byteLength;
        if (seen > limit) controller.error(new McpResponseTooLargeError(limit));
        else controller.enqueue(chunk);
      },
    }),
  );
  return new Response(counted, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}

/** Connect an SDK client over `transport`. Throws when the handshake fails. */
export async function connectMcpClient(params: {
  readonly transport: Transport;
  readonly timeoutMs?: number;
  /** Time source for the `tools/list` deadline. Default `SystemWallClock`. */
  readonly clock?: WallClock;
  /**
   * Treat a timed-out or aborted request as fatal to the connection, so the
   * pool closes the transport. Streamable HTTP needs it: the SDK keeps a
   * request's response stream open after giving up on the request, and
   * only closing the transport cancels it.
   */
  readonly closeOnTimeout?: boolean;
}): Promise<McpClient> {
  const client = new Client(CLIENT_INFO, {
    capabilities: {},
    jsonSchemaValidator: ACCEPT_ANY_OUTPUT,
  });
  const wrapped = new SdkMcpClient({
    client,
    clock: params.clock ?? SystemWallClock,
    closeOnTimeout: params.closeOnTimeout === true,
  });
  try {
    await client.connect(params.transport, {
      ...(params.timeoutMs !== undefined && { timeout: params.timeoutMs }),
    });
  } catch (err) {
    await client.close().catch(() => {});
    throw err;
  }
  return wrapped;
}

class SdkMcpClient implements McpClient {
  private dead = false;
  private toolList: Promise<McpToolDefinition[]> | undefined;
  private readonly client: Client;
  private readonly clock: WallClock;
  private readonly closeOnTimeout: boolean;

  constructor(params: { client: Client; clock: WallClock; closeOnTimeout: boolean }) {
    this.client = params.client;
    this.clock = params.clock;
    this.closeOnTimeout = params.closeOnTimeout;
    const client = params.client;
    client.onclose = () => {
      this.dead = true;
    };
    client.setNotificationHandler(ToolListChangedNotificationSchema, () => {
      this.toolList = undefined;
    });
  }

  get closed(): boolean {
    return this.dead;
  }

  listTools(options?: McpCallOptions): Promise<McpToolDefinition[]> {
    // Cached for the connection's life; a `tools/list_changed`
    // notification drops the cache.
    this.toolList ??= this.fetchTools(options);
    const pending = this.toolList;
    pending.catch(() => {
      if (this.toolList === pending) this.toolList = undefined;
    });
    return pending;
  }

  /** Every page of `tools/list`, bounded in pages, tools and total time. */
  private async fetchTools(options?: McpCallOptions): Promise<McpToolDefinition[]> {
    const deadline = this.clock.currentTimeMs() + (options?.timeoutMs ?? DEFAULT_MCP_TIMEOUT_MS);
    const tools: McpToolDefinition[] = [];
    const cursors = new Set<string>();
    let cursor: string | undefined;
    for (let page = 0; ; page += 1) {
      if (page >= MAX_TOOL_LIST_PAGES) {
        throw new Error(`tools/list returned more than ${MAX_TOOL_LIST_PAGES} pages`);
      }
      const remaining = deadline - this.clock.currentTimeMs();
      if (remaining <= 0) throw new Error("tools/list did not finish in time");
      const pageCursor = cursor;
      const result = await this.guard({
        signal: options?.signal,
        run: () =>
          this.client.listTools(pageCursor !== undefined ? { cursor: pageCursor } : undefined, {
            ...(options?.signal !== undefined && { signal: options.signal }),
            timeout: remaining,
          }),
      });
      for (const t of result.tools) {
        tools.push({
          name: t.name,
          ...(t.description !== undefined && { description: t.description }),
          inputSchema: t.inputSchema as Readonly<Record<string, unknown>>,
        });
      }
      if (tools.length > MAX_LISTED_TOOLS) {
        throw new Error(`tools/list returned more than ${MAX_LISTED_TOOLS} tools`);
      }
      cursor = result.nextCursor;
      if (cursor === undefined || cursors.has(cursor)) return tools;
      cursors.add(cursor);
    }
  }

  async callTool(params: {
    readonly name: string;
    readonly arguments: Readonly<Record<string, unknown>>;
    readonly options?: McpCallOptions;
  }): Promise<McpToolResult> {
    const response = await this.guard({
      signal: params.options?.signal,
      run: () =>
        this.client.callTool(
          { name: params.name, arguments: { ...params.arguments } },
          undefined,
          requestOptions(params.options),
        ),
    });
    const content = Array.isArray(response.content) ? response.content : [];
    return {
      ...(response.isError === true && { isError: true }),
      content: content.map((block) => normalizeContentBlock(block as Record<string, unknown>)),
    };
  }

  async close(): Promise<void> {
    this.dead = true;
    await this.client.close().catch(() => {});
  }

  /**
   * Run a request; a failure that is not an MCP protocol error (the
   * server answered with an error, the request timed out) and not the
   * caller's own abort means the connection itself broke, so the client is
   * marked dead. With `closeOnTimeout`, a timeout or abort does too.
   */
  private async guard<T>(params: {
    readonly signal: AbortSignal | undefined;
    readonly run: () => Promise<T>;
  }): Promise<T> {
    try {
      return await params.run();
    } catch (err) {
      const protocolError = err instanceof McpError && err.code !== ErrorCode.ConnectionClosed;
      const aborted = params.signal?.aborted === true;
      const timedOut = err instanceof McpError && err.code === ErrorCode.RequestTimeout;
      if (!protocolError && !aborted) this.dead = true;
      if (this.closeOnTimeout && (aborted || timedOut)) this.dead = true;
      throw err;
    }
  }
}

function requestOptions(options: McpCallOptions | undefined) {
  return {
    ...(options?.signal !== undefined && { signal: options.signal }),
    ...(options?.timeoutMs !== undefined && { timeout: options.timeoutMs }),
  };
}

function normalizeContentBlock(block: Record<string, unknown>): McpContentBlock {
  const type = typeof block["type"] === "string" ? block["type"] : "text";
  if (type === "image") {
    return {
      type: "image",
      data: typeof block["data"] === "string" ? block["data"] : "",
      mimeType:
        typeof block["mimeType"] === "string" ? block["mimeType"] : "application/octet-stream",
    };
  }
  if (type === "resource") {
    const resource = (block["resource"] as Record<string, unknown> | undefined) ?? {};
    return {
      type: "resource",
      resource: {
        uri: typeof resource["uri"] === "string" ? resource["uri"] : "",
        ...(typeof resource["text"] === "string" && { text: resource["text"] }),
        ...(typeof resource["mimeType"] === "string" && { mimeType: resource["mimeType"] }),
      },
    };
  }
  if (type === "text") {
    return { type: "text", text: typeof block["text"] === "string" ? block["text"] : "" };
  }
  // Unknown content type (audio, resource_link, …): keep it visible as text.
  return { type: "text", text: JSON.stringify(block) };
}
