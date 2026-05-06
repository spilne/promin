// ---------------------------------------------------------------------------
// AgentToolCatalog — covers per-source aggregation, collision precedence,
// MCP server enumeration, and graceful degradation when a server can't
// be reached.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { DefaultAgentToolCatalog } from "../tool-catalog.ts";
import { tool } from "../tool.ts";
import { InMemoryMcpClientPool } from "../mcp/mcp-client-pool.ts";
import type { McpClient, McpToolDefinition, McpToolResult } from "../mcp/types.ts";

const search = tool({
  name: "search",
  description: "Web search",
  parameters: z.object({ query: z.string() }),
  execute: async () => "(results)",
});

const memory = tool({
  name: "memory",
  description: "Read user memory",
  parameters: z.object({ key: z.string() }),
  execute: async () => "(value)",
});

class FakeMcpClient implements McpClient {
  constructor(
    public readonly name: string,
    private readonly tools: McpToolDefinition[],
  ) {}
  async listTools() {
    return this.tools;
  }
  async callTool(): Promise<McpToolResult> {
    return { content: [{ type: "text", text: "ok" }] };
  }
  async close() {}
}

describe("DefaultAgentToolCatalog", () => {
  it("returns in-process tools sorted by name with kind='in-process'", async () => {
    const cat = new DefaultAgentToolCatalog({ inProcess: { search, memory } });
    const all = await cat.listAll();
    expect(all.map((e) => e.name)).toEqual(["memory", "search"]);
    for (const e of all) expect(e.source.kind).toBe("in-process");
  });

  it("converts Zod parameters to JSON Schema", async () => {
    const cat = new DefaultAgentToolCatalog({ inProcess: { search } });
    const [entry] = await cat.listAll();
    expect(entry?.parameters).toBeDefined();
    expect(typeof entry?.parameters).toBe("object");
    // Light shape pin — zodToJsonSchema returns objects with a `properties` field for z.object
    expect(JSON.stringify(entry?.parameters)).toContain("query");
  });

  it("merges file-discovered tools with kind='file'", async () => {
    const cat = new DefaultAgentToolCatalog({ file: { search } });
    const all = await cat.listAll();
    expect(all[0]?.source.kind).toBe("file");
  });

  it("in-process wins over file on name collision", async () => {
    const cat = new DefaultAgentToolCatalog({
      inProcess: { search },
      file: { search }, // same name, different "registry"
    });
    const all = await cat.listAll();
    expect(all).toHaveLength(1);
    expect(all[0]?.source.kind).toBe("in-process");
  });

  it("enumerates MCP servers with kind='mcp' and source.server set", async () => {
    const pool = new InMemoryMcpClientPool();
    pool.register(
      "github",
      () =>
        new FakeMcpClient("github", [
          { name: "list_repos", description: "list user repos", inputSchema: { type: "object" } },
          { name: "get_issue", description: "fetch issue", inputSchema: { type: "object" } },
        ]),
    );
    const cat = new DefaultAgentToolCatalog({
      mcp: {
        pool,
        servers: [{ name: "github", transport: "stdio", command: ["x"] }],
      },
    });
    const all = await cat.listAll();
    const names = all.map((e) => e.name);
    expect(names).toContain("github:get_issue");
    expect(names).toContain("github:list_repos");
    for (const e of all) {
      if (e.source.kind === "mcp") expect(e.source.server).toBe("github");
    }
  });

  it("aggregates across all three sources in one call", async () => {
    const pool = new InMemoryMcpClientPool();
    pool.register("wiki", () => new FakeMcpClient("wiki", [{ name: "fetch", inputSchema: {} }]));
    const cat = new DefaultAgentToolCatalog({
      inProcess: { search },
      file: { memory },
      mcp: {
        pool,
        servers: [{ name: "wiki", transport: "http", url: "https://x" }],
      },
    });
    const all = await cat.listAll();
    const byName = new Map(all.map((e) => [e.name, e]));
    expect(byName.get("search")?.source.kind).toBe("in-process");
    expect(byName.get("memory")?.source.kind).toBe("file");
    expect(byName.get("wiki:fetch")?.source.kind).toBe("mcp");
  });

  it("gracefully degrades when an MCP server is unreachable (drops its tools, keeps others)", async () => {
    const pool = new InMemoryMcpClientPool();
    // 'github' factory throws — simulates connect failure.
    pool.register("github", () => {
      throw new Error("connect failed");
    });
    pool.register("wiki", () => new FakeMcpClient("wiki", [{ name: "fetch", inputSchema: {} }]));
    const cat = new DefaultAgentToolCatalog({
      mcp: {
        pool,
        servers: [
          { name: "github", transport: "stdio", command: ["x"] },
          { name: "wiki", transport: "http", url: "https://wiki" },
        ],
      },
    });
    const all = await cat.listAll();
    const names = all.map((e) => e.name);
    expect(names).toContain("wiki:fetch");
    expect(names.find((n) => n.startsWith("github:"))).toBeUndefined();
  });

  it("returns empty array when nothing configured", async () => {
    const cat = new DefaultAgentToolCatalog({});
    expect(await cat.listAll()).toEqual([]);
  });

  it("uses bare tool name when MCP server name is empty (single-server bare-mount)", async () => {
    const pool = new InMemoryMcpClientPool();
    pool.register("", () => new FakeMcpClient("", [{ name: "ping", inputSchema: {} }]));
    const cat = new DefaultAgentToolCatalog({
      mcp: {
        pool,
        servers: [{ name: "", transport: "stdio", command: ["x"] }],
      },
    });
    const all = await cat.listAll();
    expect(all.map((e) => e.name)).toEqual(["ping"]);
  });
});
