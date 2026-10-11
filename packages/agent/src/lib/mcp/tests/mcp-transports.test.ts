// ---------------------------------------------------------------------------
// The pool's real transports.
//
// stdio: credential via the configured env var, no host env inherited,
// stderr to the log callback, a dead process replaced, and the server's
// whole process group gone on close.
//
// Streamable HTTP: every request — the POSTs and the SSE GET stream — goes
// through the outbound policy (private addresses refused unless the
// operator allows them, allowed-host list applied), the bearer credential
// is sent, and an oversize response is cut off.
// ---------------------------------------------------------------------------

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js";
import { McpClientPool } from "../mcp-client-pool.ts";
import type { McpServerConfig } from "../types.ts";
import { createTestMcpServer, text, textResult } from "./mcp-test-server.ts";

const FIXTURE = join(import.meta.dir, "fixtures", "stdio-mcp-server.ts");

function resultText(result: { content: ReadonlyArray<{ type: string }> }): string {
  const block = result.content[0];
  return block && "text" in block && typeof block.text === "string" ? block.text : "";
}

function processAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Poll a real process condition (bounded by iterations, 10 ms apart). */
async function eventually(cond: () => boolean): Promise<boolean> {
  for (let i = 0; i < 300 && !cond(); i++) await new Promise((r) => setTimeout(r, 10));
  return cond();
}

describe("stdio transport", () => {
  const stdio: McpServerConfig = {
    name: "fixture",
    transport: "stdio",
    command: process.execPath,
    args: [FIXTURE],
    credentialRef: "FIXTURE_TOKEN",
    credentialEnv: "MCP_TOKEN",
  };

  it("passes the credential in the configured env var and replaces a dead process", async () => {
    const pool = new McpClientPool({ onServerLog: () => {} });
    const handle = pool.server({ server: stdio, namespaceId: "acme", credential: "tok-1" });
    const [pid, token] = resultText(await handle.callTool({ name: "whoami", arguments: {} })).split(
      ":",
    );
    expect(token).toBe("tok-1");

    await expect(handle.callTool({ name: "crash", arguments: {} })).rejects.toThrow();
    const [nextPid] = resultText(await handle.callTool({ name: "whoami", arguments: {} })).split(
      ":",
    );
    expect(nextPid).not.toBe(pid);
    await pool.closeAll();
  }, 20_000);

  it("does not hand the host's environment to the server", async () => {
    process.env["PROMIN_MCP_HOST_SECRET"] = "host-only";
    try {
      const pool = new McpClientPool({ onServerLog: () => {} });
      const handle = pool.server({ server: { ...stdio, env: { PASSED: "yes" } } });
      const read = async (name: string) =>
        resultText(await handle.callTool({ name: "env", arguments: { name } }));
      expect(await read("PROMIN_MCP_HOST_SECRET")).toBe("<unset>");
      expect(await read("PASSED")).toBe("yes");
      await pool.closeAll();
    } finally {
      delete process.env["PROMIN_MCP_HOST_SECRET"];
    }
  }, 20_000);

  it("sends the server's stderr to the log callback with its name", async () => {
    const lines: string[] = [];
    const pool = new McpClientPool({ onServerLog: (line) => lines.push(line) });
    await pool.server({ server: stdio }).listTools();
    expect(await eventually(() => lines.length > 0)).toBe(true);
    expect(lines[0]).toBe("[mcp:fixture] fixture server ready");
    await pool.closeAll();
  }, 20_000);

  it("kills the server's whole process group on close", async () => {
    const pool = new McpClientPool({ onServerLog: () => {} });
    const handle = pool.server({ server: stdio });
    const helper = Number(resultText(await handle.callTool({ name: "spawn", arguments: {} })));
    expect(processAlive(helper)).toBe(true);
    await pool.closeAll();
    expect(await eventually(() => !processAlive(helper))).toBe(true);
  }, 20_000);
});

describe("http transport", () => {
  let server: ReturnType<typeof Bun.serve>;
  const requests: Array<{ method: string; authorization: string | null }> = [];
  let port = 0;
  let url = "";

  beforeAll(async () => {
    // One MCP server + transport per session, so every pool gets a fresh one.
    const sessions = new Map<string, WebStandardStreamableHTTPServerTransport>();
    server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: async (req) => {
        requests.push({ method: req.method, authorization: req.headers.get("authorization") });
        const id = req.headers.get("mcp-session-id");
        let transport = id ? sessions.get(id) : undefined;
        if (!transport) {
          const created = new WebStandardStreamableHTTPServerTransport({
            sessionIdGenerator: () => randomUUID(),
            enableJsonResponse: true,
            onsessioninitialized: (sid) => {
              sessions.set(sid, created);
            },
          });
          await createTestMcpServer({
            tools: [
              {
                name: "ping",
                description: "x".repeat(4_000),
                inputSchema: { type: "object" },
                handler: () => text("pong"),
              },
            ],
          }).connect(created);
          transport = created;
        }
        return transport.handleRequest(req);
      },
    });
    port = server.port ?? 0;
    url = `http://127.0.0.1:${port}/mcp`;
  });

  afterAll(async () => {
    await server.stop(true);
  });

  it("refuses a server on a private address by default", async () => {
    const pool = new McpClientPool();
    const before = requests.length;
    const handle = pool.server({ server: { name: "local", transport: "http", url } });
    await expect(handle.listTools()).rejects.toThrow(/non-public address/);
    expect(requests.length).toBe(before);
    await pool.closeAll();
  });

  it("reaches an operator server that allows the private network, with the bearer credential", async () => {
    const pool = new McpClientPool();
    const handle = pool.server({
      server: { name: "local", transport: "http", url, allowPrivateNetwork: true },
      credential: "secret-token",
    });
    expect(await handle.callTool({ name: "ping", arguments: {} })).toEqual(textResult("pong"));
    expect(requests.at(-1)?.authorization).toBe("Bearer secret-token");
    await pool.closeAll();
  });

  it("checks every request, including the SSE GET stream, against the policy", async () => {
    const checked: string[] = [];
    const pool = new McpClientPool({
      outboundPolicy: {
        resolveHost: async (host) => {
          checked.push(host);
          return ["127.0.0.1"];
        },
      },
    });
    const before = requests.length;
    const handle = pool.server({
      server: {
        name: "named",
        transport: "http",
        url: `http://mcp.test:${port}/mcp`,
        allowPrivateNetwork: true,
      },
    });
    await handle.callTool({ name: "ping", arguments: {} });
    for (let i = 0; i < 100 && !requests.slice(before).some((r) => r.method === "GET"); i++) {
      await new Promise((r) => setTimeout(r, 0));
    }
    const seen = requests.slice(before);
    expect(seen.some((r) => r.method === "GET")).toBe(true);
    // One policy check (DNS resolution through the policy) per request that arrived.
    expect(checked.length).toBeGreaterThanOrEqual(seen.length);
    expect(new Set(checked)).toEqual(new Set(["mcp.test"]));
    await pool.closeAll();
  });

  it("applies the pool's allowed-host list on top", async () => {
    const pool = new McpClientPool({ outboundPolicy: { allowedHosts: ["mcp.example.com"] } });
    const handle = pool.server({
      server: { name: "local", transport: "http", url, allowPrivateNetwork: true },
    });
    await expect(handle.listTools()).rejects.toThrow(/not in the allowed host list/);
    await pool.closeAll();
  });

  it("cuts off a response larger than maxResponseBytes", async () => {
    const pool = new McpClientPool({ maxResponseBytes: 2_048 });
    const handle = pool.server({
      server: { name: "local", transport: "http", url, allowPrivateNetwork: true },
    });
    await expect(handle.listTools()).rejects.toThrow(/exceeds 2048 bytes/);
    await pool.closeAll();
  });
});

describe("http requests that time out", () => {
  let server: ReturnType<typeof Bun.serve>;
  let open = 0;
  let url = "";

  beforeAll(() => {
    // Answers the handshake and tools/list, then keeps every tools/call
    // response open as an SSE stream that trickles comments and never
    // delivers a result.
    server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      fetch: async (req) => {
        if (req.method === "GET") return new Response(null, { status: 405 });
        if (req.method === "DELETE") return new Response(null, { status: 200 });
        const msg = (await req.json()) as {
          id?: number;
          method: string;
          params?: { protocolVersion?: string };
        };
        if (msg.id === undefined) return new Response(null, { status: 202 });
        if (msg.method === "initialize") {
          return Response.json({
            jsonrpc: "2.0",
            id: msg.id,
            result: {
              protocolVersion: msg.params?.protocolVersion,
              capabilities: { tools: {} },
              serverInfo: { name: "slow", version: "1" },
            },
          });
        }
        if (msg.method === "tools/list") {
          return Response.json({
            jsonrpc: "2.0",
            id: msg.id,
            result: { tools: [{ name: "slow", inputSchema: { type: "object" } }] },
          });
        }
        open += 1;
        let timer: ReturnType<typeof setInterval> | undefined;
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            timer = setInterval(() => {
              try {
                controller.enqueue(new TextEncoder().encode(": tick\n\n"));
              } catch {
                clearInterval(timer);
              }
            }, 20);
          },
          cancel() {
            clearInterval(timer);
            open -= 1;
          },
        });
        return new Response(body, { headers: { "content-type": "text/event-stream" } });
      },
    });
    url = `http://127.0.0.1:${server.port}/mcp`;
  });

  afterAll(async () => {
    await server.stop(true);
  });

  it("closes the response stream of every timed-out call", async () => {
    const pool = new McpClientPool();
    const handle = pool.server({
      server: { name: "slow", transport: "http", url, allowPrivateNetwork: true },
    });
    for (let i = 0; i < 3; i++) {
      await expect(
        handle.callTool({ name: "slow", arguments: {}, options: { timeoutMs: 100 } }),
      ).rejects.toThrow(/timed out/i);
    }
    expect(await eventually(() => open === 0)).toBe(true);
    await pool.closeAll();
  }, 20_000);
});
