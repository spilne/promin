// ---------------------------------------------------------------------------
// McpClientPool against real in-process MCP servers: client reuse and
// isolation by (config, namespace, credential), dead-client eviction,
// bounded concurrency, abort and timeout.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { McpClientPool } from "../mcp-client-pool.ts";
import type { McpServerConfig } from "../types.ts";
import {
  createTestMcpServer,
  inMemoryConnect,
  text,
  textResult,
  until,
} from "./mcp-test-server.ts";

const ECHO_SCHEMA = { type: "object", properties: { v: { type: "string" } } } as const;

function echoServer() {
  return createTestMcpServer({
    tools: [
      {
        name: "echo",
        inputSchema: ECHO_SCHEMA,
        handler: ({ arguments: args }) => text(String(args["v"])),
      },
    ],
  });
}

const github: McpServerConfig = { name: "github", transport: "stdio", command: "gh-mcp" };

describe("McpClientPool — keys", () => {
  it("reuses one client for the same config, namespace and credential", async () => {
    const { connect, connections } = inMemoryConnect(() => echoServer());
    const pool = new McpClientPool({ connect });
    const a = pool.server({ server: github, namespaceId: "acme" });
    const b = pool.server({ server: { ...github }, namespaceId: "acme" });
    expect(a.key).toBe(b.key);
    await a.callTool({ name: "echo", arguments: { v: "1" } });
    await b.callTool({ name: "echo", arguments: { v: "2" } });
    expect(connections).toHaveLength(1);
    await pool.closeAll();
  });

  it("isolates tenants: the same server under two namespaces gets two clients", async () => {
    const { connect, connections } = inMemoryConnect(() => echoServer());
    const pool = new McpClientPool({ connect });
    const acme = pool.server({ server: github, namespaceId: "acme" });
    const globex = pool.server({ server: github, namespaceId: "globex" });
    expect(acme.key).not.toBe(globex.key);
    await acme.callTool({ name: "echo", arguments: { v: "x" } });
    await globex.callTool({ name: "echo", arguments: { v: "y" } });
    expect(connections).toHaveLength(2);
    expect(pool.size).toBe(2);
    await pool.closeAll();
  });

  it("never shares a client between two configs that share a name", async () => {
    const { connect, connections } = inMemoryConnect(() => echoServer());
    const pool = new McpClientPool({ connect });
    const one = pool.server({ server: { ...github, args: ["--org", "a"] }, namespaceId: "acme" });
    const two = pool.server({ server: { ...github, args: ["--org", "b"] }, namespaceId: "acme" });
    await one.callTool({ name: "echo", arguments: { v: "x" } });
    await two.callTool({ name: "echo", arguments: { v: "y" } });
    expect(connections.map((c) => c.server)).toEqual([
      { ...github, args: ["--org", "a"] },
      { ...github, args: ["--org", "b"] },
    ]);
    await pool.closeAll();
  });

  it("keys by credential: each credential reaches only its own connection", async () => {
    const { connect, connections } = inMemoryConnect(() => echoServer());
    const pool = new McpClientPool({ connect });
    await pool
      .server({ server: github, namespaceId: "acme", credential: "tok-a" })
      .callTool({ name: "echo", arguments: { v: "x" } });
    await pool
      .server({ server: github, namespaceId: "acme", credential: "tok-b" })
      .callTool({ name: "echo", arguments: { v: "y" } });
    expect(connections.map((c) => c.credential)).toEqual(["tok-a", "tok-b"]);
    await pool.closeAll();
  });

  it("closes the least recently used idle clients beyond maxClients", async () => {
    const { connect, connections } = inMemoryConnect(() => echoServer());
    const pool = new McpClientPool({ connect, maxClients: 2 });
    for (const ns of ["a", "b", "c"]) {
      await pool.server({ server: github, namespaceId: ns }).listTools();
    }
    expect(pool.size).toBe(2);
    // The first client was closed and dropped; using it again reconnects.
    await pool.server({ server: github, namespaceId: "a" }).listTools();
    expect(connections).toHaveLength(4);
    await pool.closeAll();
  });
});

describe("McpClientPool — dead clients", () => {
  it("drops a client whose transport closed and reconnects on the next call", async () => {
    const { connect, connections } = inMemoryConnect(() => echoServer());
    const pool = new McpClientPool({ connect });
    const handle = pool.server({ server: github, namespaceId: "acme" });
    expect(await handle.callTool({ name: "echo", arguments: { v: "1" } })).toEqual(textResult("1"));

    await connections[0]!.mcpServer.close(); // the server went away
    expect(await handle.callTool({ name: "echo", arguments: { v: "2" } })).toEqual(textResult("2"));
    expect(connections).toHaveLength(2);
    await pool.closeAll();
  });

  it("retries the connect after a failed one", async () => {
    let attempts = 0;
    const inner = inMemoryConnect(() => echoServer());
    const pool = new McpClientPool({
      connect: async (params) => {
        attempts += 1;
        if (attempts === 1) throw new Error("spawn failed");
        return inner.connect(params);
      },
    });
    const handle = pool.server({ server: github });
    await expect(handle.listTools()).rejects.toThrow("spawn failed");
    expect((await handle.listTools()).map((t) => t.name)).toEqual(["echo"]);
    await pool.closeAll();
  });
});

describe("McpClientPool — calls", () => {
  function gatedServer(state: { inFlight: number; peak: number; gates: Array<() => void> }) {
    return createTestMcpServer({
      tools: [
        {
          name: "slow",
          inputSchema: { type: "object" },
          handler: async () => {
            state.inFlight += 1;
            state.peak = Math.max(state.peak, state.inFlight);
            await new Promise<void>((resolve) => state.gates.push(resolve));
            state.inFlight -= 1;
            return text("done");
          },
        },
      ],
    });
  }

  it("bounds concurrent calls per client by maxConcurrentCalls", async () => {
    const state = { inFlight: 0, peak: 0, gates: [] as Array<() => void> };
    const { connect } = inMemoryConnect(() => gatedServer(state));
    const pool = new McpClientPool({ connect });
    const handle = pool.server({ server: { ...github, maxConcurrentCalls: 2 } });
    const calls = Array.from({ length: 5 }, () => handle.callTool({ name: "slow", arguments: {} }));
    await until(() => state.gates.length === 2);
    expect(state.inFlight).toBe(2);
    while (state.gates.length > 0 || state.inFlight > 0) {
      state.gates.shift()?.();
      await new Promise((r) => setTimeout(r, 0));
    }
    expect(await Promise.all(calls)).toHaveLength(5);
    expect(state.peak).toBe(2);
    await pool.closeAll();
  });

  it("a call aborted while queued leaves the queue without taking a slot", async () => {
    const state = { inFlight: 0, peak: 0, gates: [] as Array<() => void> };
    const { connect } = inMemoryConnect(() => gatedServer(state));
    const pool = new McpClientPool({ connect });
    const handle = pool.server({ server: { ...github, maxConcurrentCalls: 1 } });
    const first = handle.callTool({ name: "slow", arguments: {} });
    await until(() => state.gates.length === 1);
    const controller = new AbortController();
    const queued = handle.callTool({
      name: "slow",
      arguments: {},
      options: { signal: controller.signal },
    });
    controller.abort(new Error("cancelled by caller"));
    await expect(queued).rejects.toThrow("cancelled by caller");
    state.gates.shift()!();
    await first;
    // The slot is free again: the next call runs.
    const next = handle.callTool({ name: "slow", arguments: {} });
    await until(() => state.gates.length === 1);
    state.gates.shift()!();
    expect(await next).toEqual(textResult("done"));
    await pool.closeAll();
  });

  it("aborting an in-flight call cancels it on the server and keeps the client", async () => {
    let serverSignal: AbortSignal | undefined;
    const { connect, connections } = inMemoryConnect(() =>
      createTestMcpServer({
        tools: [
          {
            name: "hang",
            inputSchema: { type: "object" },
            handler: ({ signal }) => {
              serverSignal = signal;
              return new Promise(() => {});
            },
          },
          { name: "echo", inputSchema: ECHO_SCHEMA, handler: () => text("still here") },
        ],
      }),
    );
    const pool = new McpClientPool({ connect });
    const handle = pool.server({ server: github });
    const controller = new AbortController();
    const call = handle.callTool({
      name: "hang",
      arguments: {},
      options: { signal: controller.signal },
    });
    await until(() => serverSignal !== undefined);
    controller.abort(new Error("turn cancelled"));
    await expect(call).rejects.toThrow("turn cancelled");
    await until(() => serverSignal?.aborted === true);
    expect(await handle.callTool({ name: "echo", arguments: {} })).toEqual(
      textResult("still here"),
    );
    expect(connections).toHaveLength(1);
    await pool.closeAll();
  });

  it("enforces timeoutMs on the request and keeps the client", async () => {
    const { connect, connections } = inMemoryConnect(() =>
      createTestMcpServer({
        tools: [
          { name: "hang", inputSchema: { type: "object" }, handler: () => new Promise(() => {}) },
          { name: "echo", inputSchema: ECHO_SCHEMA, handler: () => text("ok") },
        ],
      }),
    );
    const pool = new McpClientPool({ connect });
    const handle = pool.server({ server: github });
    await expect(
      handle.callTool({ name: "hang", arguments: {}, options: { timeoutMs: 20 } }),
    ).rejects.toThrow(/timed out/i);
    expect(await handle.callTool({ name: "echo", arguments: {} })).toEqual(textResult("ok"));
    expect(connections).toHaveLength(1);
    await pool.closeAll();
  });
});
