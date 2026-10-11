// ---------------------------------------------------------------------------
// The operator's MCP allowlist: the config-file schema and ZoryaMcpServers'
// own checks (unique, provider-safe server names; stop closes the clients;
// nothing sensitive described; loading needs a scope and has a deadline).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { McpClientPool, type McpClient, type RegisteredAgent } from "@promin/agent";
import { FakeWallClock } from "@promin/workflow";
import { parseMcpServersConfig } from "../mcp-servers-config.ts";
import { ZoryaMcpServers } from "../zorya-mcp-servers.ts";

describe("parseMcpServersConfig", () => {
  it("accepts stdio and http servers", () => {
    const servers = parseMcpServersConfig([
      {
        name: "github",
        transport: "stdio",
        command: "github-mcp-server",
        args: ["stdio"],
        credentialRef: "GITHUB_TOKEN",
        credentialEnv: "GITHUB_PERSONAL_ACCESS_TOKEN",
        tools: ["search_issues"],
      },
      {
        name: "docs",
        transport: "http",
        url: "http://docs-mcp.internal:8080/mcp",
        allowPrivateNetwork: true,
        requireApproval: false,
        idempotent: true,
      },
    ]);
    expect(servers.map((s) => s.name)).toEqual(["github", "docs"]);
  });

  it("rejects unknown fields, bad names and non-http urls", () => {
    expect(() =>
      parseMcpServersConfig([
        { name: "x", transport: "stdio", command: "a", requireAproval: false },
      ]),
    ).toThrow(/Invalid MCP server config/);
    expect(() =>
      parseMcpServersConfig([{ name: "has space", transport: "stdio", command: "a" }]),
    ).toThrow(/Invalid MCP server config/);
    expect(() =>
      parseMcpServersConfig([{ name: "x", transport: "http", url: "file:///etc/passwd" }]),
    ).toThrow(/Invalid MCP server config/);
    expect(() =>
      parseMcpServersConfig([{ name: "x", transport: "sse", url: "https://a" }]),
    ).toThrow(/Invalid MCP server config/);
  });
});

describe("ZoryaMcpServers", () => {
  it("refuses duplicate or unsafe server names", () => {
    expect(
      () =>
        new ZoryaMcpServers({
          servers: [
            { name: "a", transport: "stdio", command: "x" },
            { name: "a", transport: "stdio", command: "y" },
          ],
        }),
    ).toThrow(/configured twice/);
    expect(
      () => new ZoryaMcpServers({ servers: [{ name: "a.b", transport: "stdio", command: "x" }] }),
    ).toThrow(/must match/);
  });

  it("closes every MCP client on stop", async () => {
    let closed = 0;
    const client: McpClient = {
      closed: false,
      listTools: async () => [],
      callTool: async () => ({ content: [] }),
      close: async () => {
        closed += 1;
      },
    };
    const pool = new McpClientPool({ connect: async () => client });
    const mcp = new ZoryaMcpServers({
      servers: [{ name: "a", transport: "stdio", command: "x" }],
      pool,
    });
    await mcp.catalog();
    expect(pool.size).toBe(1);
    await mcp.stop();
    expect(closed).toBe(1);
    expect(pool.size).toBe(0);
  });

  it("never describes a server's url, command or credential name", () => {
    const mcp = new ZoryaMcpServers({
      servers: [
        {
          name: "docs",
          transport: "http",
          url: "http://docs-mcp.platform.svc:8080/mcp",
          credentialRef: "DOCS_TOKEN",
        },
        { name: "gh", transport: "stdio", command: "/opt/gh-mcp", args: ["--secret-flag"] },
      ],
    });
    const described = JSON.stringify(mcp.describe());
    for (const hidden of ["docs-mcp", "DOCS_TOKEN", "/opt/gh-mcp", "--secret-flag"]) {
      expect(described).not.toContain(hidden);
    }
    expect(mcp.describe().recipeHttpServers).toBe(false);
  });

  const recipe: RegisteredAgent = {
    id: "triage",
    version: "v1",
    backend: {
      type: "local",
      model: { provider: "test", id: "m" },
      role: { inline: { systemPrompt: null, tools: [] } },
      mcpServers: [{ server: "a" }],
    },
    metadata: { description: null, capabilities: [], tags: [] },
    createdAt: 0,
    updatedAt: 0,
  };

  it("refuses to load a recipe's MCP tools without the caller's scope", async () => {
    let connects = 0;
    const pool = new McpClientPool({
      connect: async () => {
        connects += 1;
        return {
          closed: false,
          listTools: async () => [],
          callTool: async () => ({ content: [] }),
          close: async () => {},
        };
      },
    });
    const mcp = new ZoryaMcpServers({
      servers: [{ name: "a", transport: "stdio", command: "x" }],
      pool,
    });
    await expect(mcp.loadTools({ recipe, scope: undefined })).rejects.toThrow(
      /needs the caller's scope/,
    );
    expect(connects).toBe(0);
    expect(await mcp.loadTools({ recipe, scope: { namespaceId: "acme" } })).toEqual({});
    expect(connects).toBe(1);
  });

  it("gives up loading a recipe's MCP tools after loadTimeoutMs", async () => {
    const clock = FakeWallClock.create(0);
    const pool = new McpClientPool({ connect: () => new Promise<McpClient>(() => {}) });
    const mcp = new ZoryaMcpServers({
      servers: [{ name: "a", transport: "stdio", command: "x" }],
      pool,
      clock,
      loadTimeoutMs: 1_000,
    });
    const loading = mcp.loadTools({ recipe, scope: { namespaceId: "acme" } });
    for (let i = 0; i < 100 && clock.pendingCount() === 0; i++) await Promise.resolve();
    clock.advance(1_000);
    await expect(loading).rejects.toThrow(/did not load within 1000ms/);
  });
});
