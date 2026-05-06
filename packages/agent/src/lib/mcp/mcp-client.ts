// ---------------------------------------------------------------------------
// SdkMcpClient — production `McpClient` implementation backed by
// `@modelcontextprotocol/sdk`. Tests prefer a fake client (see
// `mcp-tools.test.ts`) so this file isolates the SDK surface area.
//
// Transport selection is config-driven:
//   stdio → spawn a local process (CLI MCP servers)
//   http  → Streamable HTTP transport
//   sse   → Server-Sent Events transport
//
// Auth: bearer tokens go through the transport's `headers` for HTTP /
// SSE. stdio inherits env (auth token can be passed via `env`).
// ---------------------------------------------------------------------------

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type {
  McpClient,
  McpContentBlock,
  McpServerConfig,
  McpToolDefinition,
  McpToolResult,
} from "./types.ts";

/** Internal sentinel — clients pass this via `env` if they want to inherit os env. */
const PROMIN_VERSION = "0.1.0";

export interface SdkMcpClientConfig {
  readonly server: McpServerConfig;
  /**
   * Resolver that turns a `credentialRef` into a concrete bearer token
   * at connect time. Optional — when omitted, only configs with an
   * inline `authToken` work. Forward-compat with promin-an9l.
   */
  readonly resolveCredential?: (ref: string) => Promise<string>;
}

/**
 * Construct a connected `McpClient`. Throws on connect failure so the
 * caller can decide whether to retry or fall back. Returned client
 * MUST be `close()`-d when done; callers are responsible for lifetime.
 */
export async function createSdkMcpClient(config: SdkMcpClientConfig): Promise<McpClient> {
  const client = new Client({ name: "promin-agent", version: PROMIN_VERSION });
  const transport = await buildTransport(config);
  await client.connect(transport);
  return new SdkMcpClient(config.server.name, client);
}

class SdkMcpClient implements McpClient {
  constructor(
    public readonly name: string,
    private readonly client: Client,
  ) {}

  async listTools(): Promise<McpToolDefinition[]> {
    const response = await this.client.listTools();
    return response.tools.map((t) => ({
      name: t.name,
      ...(t.description !== undefined && { description: t.description }),
      inputSchema: (t.inputSchema ?? {}) as Readonly<Record<string, unknown>>,
    }));
  }

  async callTool(params: {
    name: string;
    arguments: Readonly<Record<string, unknown>>;
  }): Promise<McpToolResult> {
    const response = await this.client.callTool({
      name: params.name,
      arguments: params.arguments as Record<string, unknown>,
    });
    return {
      ...(response.isError !== undefined && { isError: response.isError as boolean }),
      content: ((response.content ?? []) as Array<Record<string, unknown>>).map((block) =>
        normalizeContentBlock(block),
      ),
    };
  }

  async close(): Promise<void> {
    await this.client.close().catch(() => {});
  }
}

async function buildTransport(config: SdkMcpClientConfig) {
  const { server } = config;
  const headers = await resolveHeaders(server, config.resolveCredential);
  switch (server.transport) {
    case "stdio": {
      const [cmd, ...args] = server.command;
      if (!cmd) {
        throw new Error(
          `MCP server '${server.name}' has empty command — stdio transport needs at least one arg.`,
        );
      }
      return new StdioClientTransport({
        command: cmd,
        args,
        ...(server.env !== undefined && { env: { ...server.env } }),
        ...(server.cwd !== undefined && { cwd: server.cwd }),
      });
    }
    case "http":
      return new StreamableHTTPClientTransport(new URL(server.url), {
        ...(headers && {
          requestInit: { headers },
        }),
      });
    case "sse":
      return new SSEClientTransport(new URL(server.url), {
        ...(headers && {
          requestInit: { headers },
        }),
      });
  }
}

async function resolveHeaders(
  server: McpServerConfig,
  resolveCredential?: (ref: string) => Promise<string>,
): Promise<Record<string, string> | null> {
  if (server.transport === "stdio") return null;
  let token: string | undefined = server.authToken;
  if (!token && server.credentialRef) {
    if (!resolveCredential) {
      throw new Error(
        `MCP server '${server.name}' has credentialRef '${server.credentialRef}' ` +
          "but no `resolveCredential` was supplied to createSdkMcpClient.",
      );
    }
    token = await resolveCredential(server.credentialRef);
  }
  if (!token) return null;
  return { Authorization: `Bearer ${token}` };
}

function normalizeContentBlock(block: Record<string, unknown>): McpContentBlock {
  const type = typeof block.type === "string" ? block.type : "text";
  if (type === "image") {
    return {
      type: "image",
      data: typeof block.data === "string" ? block.data : "",
      mimeType: typeof block.mimeType === "string" ? block.mimeType : "application/octet-stream",
    };
  }
  if (type === "resource") {
    const resource = (block.resource as Record<string, unknown> | undefined) ?? {};
    return {
      type: "resource",
      resource: {
        uri: typeof resource.uri === "string" ? resource.uri : "",
        ...(typeof resource.text === "string" && { text: resource.text }),
        ...(typeof resource.mimeType === "string" && { mimeType: resource.mimeType }),
      },
    };
  }
  if (type === "text") {
    return { type: "text", text: typeof block.text === "string" ? block.text : "" };
  }
  // Unknown content type — flatten to text so the agent still sees something.
  return { type: "text", text: JSON.stringify(block) };
}
