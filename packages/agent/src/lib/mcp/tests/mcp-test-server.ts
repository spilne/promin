// ---------------------------------------------------------------------------
// In-process MCP servers for tests: a real SDK `Server` answering
// `tools/list` / `tools/call`, connected to the client over the SDK's
// in-memory transport pair. `inMemoryConnect` plugs it into a
// `McpClientPool` and records every connection the pool opens.
// ---------------------------------------------------------------------------

import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  type CallToolResult,
} from "@modelcontextprotocol/sdk/types.js";
import { connectMcpClient } from "../mcp-client.ts";
import type { McpConnect } from "../mcp-client-pool.ts";
import type { McpServerConfig, McpToolResult } from "../types.ts";

export interface TestMcpTool {
  readonly name: string;
  readonly description?: string;
  readonly inputSchema: { readonly type: "object"; readonly [key: string]: unknown };
  readonly handler: (params: {
    readonly arguments: Record<string, unknown>;
    readonly signal: AbortSignal;
  }) => CallToolResult | Promise<CallToolResult>;
}

/** A low-level SDK server exposing `tools`. */
export function createTestMcpServer(params: {
  readonly tools: ReadonlyArray<TestMcpTool>;
}): Server {
  const server = new Server(
    { name: "test-server", version: "1.0.0" },
    { capabilities: { tools: {} } },
  );
  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: params.tools.map((t) => ({
      name: t.name,
      ...(t.description !== undefined && { description: t.description }),
      inputSchema: { ...t.inputSchema },
    })),
  }));
  server.setRequestHandler(CallToolRequestSchema, async (request, extra) => {
    const t = params.tools.find((x) => x.name === request.params.name);
    if (!t) return { isError: true, content: [{ type: "text", text: "unknown tool" }] };
    return t.handler({ arguments: request.params.arguments ?? {}, signal: extra.signal });
  });
  return server;
}

export interface InMemoryConnection {
  readonly server: McpServerConfig;
  readonly credential: string | undefined;
  readonly mcpServer: Server;
}

/** A pool `connect` that serves each connection from a fresh in-process server. */
export function inMemoryConnect(
  build: (params: { readonly server: McpServerConfig; readonly credential?: string }) => Server,
): { readonly connect: McpConnect; readonly connections: InMemoryConnection[] } {
  const connections: InMemoryConnection[] = [];
  const connect: McpConnect = async (params) => {
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    const mcpServer = build(params);
    await mcpServer.connect(serverTransport);
    connections.push({ server: params.server, credential: params.credential, mcpServer });
    return connectMcpClient({ transport: clientTransport });
  };
  return { connect, connections };
}

/** A text tool result. */
export function text(value: string): CallToolResult {
  return { content: [{ type: "text", text: value }] };
}

/** The client-side `McpToolResult` of a `text(value)` server result. */
export function textResult(value: string): McpToolResult {
  return { content: [{ type: "text", text: value }] };
}

/** Yield to the event loop until `cond()` holds, bounded by iterations. */
export async function until(cond: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000 && !cond(); i++) await new Promise((r) => setTimeout(r, 0));
  if (!cond()) throw new Error("until: condition never held");
}
