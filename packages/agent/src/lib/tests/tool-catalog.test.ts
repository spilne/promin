// ---------------------------------------------------------------------------
// AgentToolCatalog — covers per-source aggregation, collision precedence,
// MCP server enumeration (real in-process servers), and error reporting
// when a server can't be listed.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { DefaultAgentToolCatalog } from "../tool-catalog.ts";
import { createScopedTool, tool } from "../tool.ts";
import { InMemorySecretsStorage } from "../secrets/in-memory-secrets-storage.ts";
import { SecretScope } from "../secrets/types.ts";
import { InMemoryMemoryStore } from "../memory/in-memory-memory-store.ts";
import { McpClientPool } from "../mcp/mcp-client-pool.ts";
import type { McpServerConfig } from "../mcp/types.ts";
import { createTestMcpServer, inMemoryConnect, text } from "../mcp/tests/mcp-test-server.ts";

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

/** A pool whose servers each list the named tools; `down` servers fail to connect. */
function mcpPool(params: {
  readonly tools: Readonly<Record<string, ReadonlyArray<string>>>;
  readonly down?: ReadonlyArray<string>;
}) {
  const { connect, connections } = inMemoryConnect(({ server }) =>
    createTestMcpServer({
      tools: (params.tools[server.name] ?? []).map((name) => ({
        name,
        description: `${name} tool`,
        inputSchema: { type: "object" as const },
        handler: () => text("ok"),
      })),
    }),
  );
  const pool = new McpClientPool({
    connect: async (p) => {
      if (params.down?.includes(p.server.name)) throw new Error("connect failed");
      return connect(p);
    },
  });
  return { pool, connections };
}

const GITHUB: McpServerConfig = { name: "github", transport: "stdio", command: "gh-mcp" };
const WIKI: McpServerConfig = { name: "wiki", transport: "http", url: "https://wiki.example.com" };

describe("DefaultAgentToolCatalog", () => {
  it("returns in-process tools sorted by name with kind='in-process'", async () => {
    const cat = new DefaultAgentToolCatalog({ inProcess: { search, memory } });
    const { tools: all } = await cat.list();
    expect(all.map((e) => e.name)).toEqual(["memory", "search"]);
    for (const e of all) expect(e.source.kind).toBe("in-process");
  });

  it("surfaces requiredSecrets + usesMemory from scoped tool factories", async () => {
    const slackPost = createScopedTool({
      name: "slack_post",
      description: "Post to Slack",
      parameters: z.object({ text: z.string() }),
      secrets: {
        storage: new InMemorySecretsStorage(),
        refs: {
          token: { ref: "SLACK_BOT_TOKEN" },
          // Optional ref — must NOT appear in requiredSecrets.
          debug: { ref: "DEBUG_FLAG", required: false },
        },
      },
      memory: { store: new InMemoryMemoryStore() },
      execute: async () => "ok",
    });
    const cat = new DefaultAgentToolCatalog({ inProcess: { search, slack_post: slackPost } });
    const { tools: all } = await cat.list();

    const slack = all.find((e) => e.name === "slack_post")!;
    expect(slack.requiredSecrets).toEqual(["SLACK_BOT_TOKEN"]);
    expect(slack.usesMemory).toBe(true);

    // A bare tool() reports neither.
    const plain = all.find((e) => e.name === "search")!;
    expect(plain.requiredSecrets).toEqual([]);
    expect(plain.usesMemory).toBe(false);
  });

  it("converts Zod parameters to JSON Schema", async () => {
    const cat = new DefaultAgentToolCatalog({ inProcess: { search } });
    const [entry] = (await cat.list()).tools;
    expect(entry?.parameters).toBeDefined();
    expect(typeof entry?.parameters).toBe("object");
    // Light shape pin — agentToolJsonSchema returns objects with a `properties` field for z.object
    expect(JSON.stringify(entry?.parameters)).toContain("query");
  });

  it("merges file-discovered tools with kind='file'", async () => {
    const cat = new DefaultAgentToolCatalog({ file: { search } });
    const { tools: all } = await cat.list();
    expect(all[0]?.source.kind).toBe("file");
  });

  it("in-process wins over file on name collision", async () => {
    const cat = new DefaultAgentToolCatalog({
      inProcess: { search },
      file: { search }, // same name, different "registry"
    });
    const { tools: all } = await cat.list();
    expect(all).toHaveLength(1);
    expect(all[0]?.source.kind).toBe("in-process");
  });

  it("enumerates MCP servers with their server, MCP tool name, transport and trust", async () => {
    const { pool } = mcpPool({ tools: { github: ["list_repos", "get.issue"] } });
    const cat = new DefaultAgentToolCatalog({ mcp: { pool, servers: [GITHUB] } });
    const { tools, errors } = await cat.list();
    expect(errors).toEqual([]);
    const listRepos = tools.find((e) => e.name === "github__list_repos")!;
    expect(listRepos.source).toEqual({
      kind: "mcp",
      server: "github",
      tool: "list_repos",
      transport: "stdio",
    });
    expect(listRepos.requireApproval).toBe(true);
    expect(listRepos.idempotent).toBe(false);
    const getIssue = tools.find((e) => e.source.kind === "mcp" && e.source.tool === "get.issue");
    expect(getIssue?.name).toMatch(/^github__get_issue_[0-9a-f]{8}$/);
    await pool.closeAll();
  });

  it("aggregates across all three sources in one call", async () => {
    const { pool } = mcpPool({ tools: { wiki: ["fetch"] } });
    const cat = new DefaultAgentToolCatalog({
      inProcess: { search },
      file: { memory },
      mcp: { pool, servers: [WIKI] },
    });
    const { tools } = await cat.list();
    const byName = new Map(tools.map((e) => [e.name, e]));
    expect(byName.get("search")?.source.kind).toBe("in-process");
    expect(byName.get("memory")?.source.kind).toBe("file");
    expect(byName.get("wiki__fetch")?.source.kind).toBe("mcp");
    await pool.closeAll();
  });

  it("reports an unreachable MCP server as an error and keeps the other servers' tools", async () => {
    const { pool } = mcpPool({ tools: { wiki: ["fetch"] }, down: ["github"] });
    const logged: unknown[] = [];
    const cat = new DefaultAgentToolCatalog({
      mcp: { pool, servers: [GITHUB, WIKI], onError: ({ error }) => logged.push(error) },
    });
    const { tools, errors } = await cat.list();
    expect(tools.map((e) => e.name)).toEqual(["wiki__fetch"]);
    expect(errors).toEqual([
      {
        kind: "mcp",
        server: "github",
        code: "unreachable",
        message: "The server could not be reached or listed.",
      },
    ]);
    // The detail (which may name commands or hosts) goes to the log only.
    expect(JSON.stringify(errors)).not.toContain("connect failed");
    expect(String(logged[0])).toContain("connect failed");
    await pool.closeAll();
  });

  it("reports a missing global credential instead of listing the server", async () => {
    const { pool, connections } = mcpPool({ tools: { github: ["list_repos"] } });
    const withCredential: McpServerConfig = {
      ...GITHUB,
      credentialRef: "GITHUB_TOKEN",
      credentialEnv: "GITHUB_TOKEN",
    };
    const secrets = new InMemorySecretsStorage();
    const cat = new DefaultAgentToolCatalog({
      mcp: { pool, servers: [withCredential], secrets },
    });
    const missing = await cat.list();
    expect(missing.errors[0]?.code).toBe("credential_missing");
    expect(JSON.stringify(missing)).not.toContain("GITHUB_TOKEN");
    expect(connections).toHaveLength(0);

    await secrets.set({ scope: SecretScope.global(), key: "GITHUB_TOKEN", value: "t" });
    const listed = await cat.list();
    // Credential names stay server-side.
    expect(listed.tools[0]?.requiredSecrets).toEqual([]);
    expect(connections.map((c) => c.credential)).toEqual(["t"]);
    await pool.closeAll();
  });

  it("returns an empty listing when nothing is configured", async () => {
    const cat = new DefaultAgentToolCatalog({});
    expect(await cat.list()).toEqual({ tools: [], errors: [] });
  });
});
