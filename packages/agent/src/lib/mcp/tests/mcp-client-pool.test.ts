// ---------------------------------------------------------------------------
// McpClientPool tests — InMemoryMcpClientPool covers the lifecycle
// semantics; DefaultMcpClientPool is exercised indirectly via the
// caching contract (without spinning up real MCP servers, which would
// turn this into an integration test).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryMcpClientPool } from "../mcp-client-pool.ts";
import { loadMcpToolsFromRecipe } from "../load-mcp-tools-from-recipe.ts";
import type { McpClient, McpServerConfig, McpToolDefinition, McpToolResult } from "../types.ts";
import type { RegisteredAgent } from "../../registry/types.ts";

class FakeMcpClient implements McpClient {
  public closed = false;
  public callLog: Array<{ name: string }> = [];
  constructor(
    public readonly name: string,
    private readonly tools: McpToolDefinition[],
  ) {}
  async listTools() {
    return this.tools;
  }
  async callTool(params: {
    name: string;
    arguments: Readonly<Record<string, unknown>>;
  }): Promise<McpToolResult> {
    this.callLog.push({ name: params.name });
    return { content: [{ type: "text", text: `${this.name}:${params.name}` }] };
  }
  async close() {
    this.closed = true;
  }
}

function recipe(servers: McpServerConfig[]): RegisteredAgent {
  return {
    id: "test-agent",
    version: "v1",
    backend: {
      type: "local",
      model: { provider: "anthropic", id: "claude-sonnet-4-6" },
      role: { inline: { systemPrompt: null, tools: [] } },
      mcpServers: servers,
    },
    metadata: { description: null, capabilities: [], tags: [] },
    createdAt: 0,
    updatedAt: 0,
  };
}

describe("InMemoryMcpClientPool", () => {
  it("returns the registered factory's client on first get", async () => {
    const pool = new InMemoryMcpClientPool();
    const client = new FakeMcpClient("github", [{ name: "list_repos", inputSchema: {} }]);
    pool.register("github", () => client);
    const got = await pool.get({ name: "github", transport: "stdio", command: ["x"] });
    expect(got).toBe(client);
  });

  it("caches clients across get() calls — second get reuses first", async () => {
    const pool = new InMemoryMcpClientPool();
    let factoryCalls = 0;
    pool.register("fs", () => {
      factoryCalls += 1;
      return new FakeMcpClient("fs", [{ name: "read", inputSchema: {} }]);
    });
    const a = await pool.get({ name: "fs", transport: "stdio", command: ["x"] });
    const b = await pool.get({ name: "fs", transport: "stdio", command: ["y"] }); // different args, same name
    expect(a).toBe(b);
    expect(factoryCalls).toBe(1);
  });

  it("throws clearly when no factory is registered for a server name", async () => {
    const pool = new InMemoryMcpClientPool();
    expect(pool.get({ name: "missing", transport: "stdio", command: ["x"] })).rejects.toThrow(
      /no factory registered/,
    );
  });

  it("closeAll closes every cached client", async () => {
    const pool = new InMemoryMcpClientPool();
    const a = new FakeMcpClient("a", []);
    const b = new FakeMcpClient("b", []);
    pool.register("a", () => a);
    pool.register("b", () => b);
    await pool.get({ name: "a", transport: "stdio", command: ["x"] });
    await pool.get({ name: "b", transport: "stdio", command: ["x"] });
    await pool.closeAll();
    expect(a.closed).toBe(true);
    expect(b.closed).toBe(true);
  });

  it("closeAll resets the cache — next get() opens fresh", async () => {
    const pool = new InMemoryMcpClientPool();
    let factoryCalls = 0;
    pool.register("fs", () => {
      factoryCalls += 1;
      return new FakeMcpClient("fs", []);
    });
    await pool.get({ name: "fs", transport: "stdio", command: ["x"] });
    await pool.closeAll();
    await pool.get({ name: "fs", transport: "stdio", command: ["x"] });
    expect(factoryCalls).toBe(2);
  });
});

describe("loadMcpToolsFromRecipe", () => {
  it("returns empty record when recipe has no mcpServers", async () => {
    const pool = new InMemoryMcpClientPool();
    const r: RegisteredAgent = {
      id: "test",
      version: "v1",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: { inline: { systemPrompt: null, tools: [] } },
      },
      metadata: { description: null, capabilities: [], tags: [] },
      createdAt: 0,
      updatedAt: 0,
    };
    const tools = await loadMcpToolsFromRecipe({ recipe: r, pool });
    expect(tools).toEqual({});
  });

  it("returns empty record for non-local backends", async () => {
    const pool = new InMemoryMcpClientPool();
    const r: RegisteredAgent = {
      id: "remote",
      version: "v1",
      backend: { type: "remote", endpoint: "https://x", remoteAgentId: "x" },
      metadata: { description: null, capabilities: [], tags: [] },
      createdAt: 0,
      updatedAt: 0,
    };
    const tools = await loadMcpToolsFromRecipe({ recipe: r, pool });
    expect(tools).toEqual({});
  });

  it("merges tools from multiple servers, mounted as <server>:<tool>", async () => {
    const pool = new InMemoryMcpClientPool();
    pool.register(
      "github",
      () =>
        new FakeMcpClient("github", [
          { name: "list_repos", description: "list repos", inputSchema: {} },
          { name: "get_issue", description: "fetch issue", inputSchema: {} },
        ]),
    );
    pool.register(
      "wiki",
      () =>
        new FakeMcpClient("wiki", [{ name: "fetch", description: "fetch doc", inputSchema: {} }]),
    );
    const r = recipe([
      { name: "github", transport: "stdio", command: ["mcp-github"] },
      { name: "wiki", transport: "http", url: "https://wiki.example.com" },
    ]);
    const tools = await loadMcpToolsFromRecipe({ recipe: r, pool });
    expect(Object.keys(tools).sort()).toEqual([
      "github:get_issue",
      "github:list_repos",
      "wiki:fetch",
    ]);
  });

  it("throws on tool-name collision across servers (same server name twice in recipe)", async () => {
    const pool = new InMemoryMcpClientPool();
    pool.register("dup", () => new FakeMcpClient("dup", [{ name: "tool1", inputSchema: {} }]));
    // Recipe lists the same server twice (same name) — pool returns the
    // cached client both times, but loadMcpToolsFromRecipe walks both
    // entries and tries to insert dup:tool1 twice → collision.
    const r = recipe([
      { name: "dup", transport: "stdio", command: ["x"] },
      { name: "dup", transport: "stdio", command: ["x"] },
    ]);
    expect(loadMcpToolsFromRecipe({ recipe: r, pool })).rejects.toThrow(/tool name collision/);
  });
});
