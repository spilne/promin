// ---------------------------------------------------------------------------
// MCP through a real ZoryaServer: recipe writes are checked against the
// operator allowlist (a recipe-supplied stdio command never gets in), the
// tool catalog lists allowlisted servers' tools with trust info and
// reports the ones it can't reach, the MCP picker never shows a server's
// command or env, and an invoked agent's MCP tools are approval-gated and
// pooled per tenant.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentRegistry,
  InMemoryMemoryStore,
  McpClientPool,
  resolveLocalAgent,
  type LLMProvider,
  type McpClient,
  type McpServerConfig,
  type RegisteredAgent,
  type ToolCatalogEntry,
} from "@promin/agent";
import { FakeWallClock, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents, ZoryaMcpServers } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";
import type { McpServersCatalogResponse } from "../../services/mcp/index.ts";
import type { ToolsCatalogResponse } from "../agent-catalog.ts";
import { jsonOf, localBackend, req } from "./agent-gateway-harness.ts";

const GITHUB: McpServerConfig = {
  name: "github",
  description: "GitHub issues",
  transport: "stdio",
  command: "github-mcp",
  args: ["--token-file", "/etc/secret"],
  env: { GITHUB_HOST: "ghe.internal" },
};
const BROKEN: McpServerConfig = { name: "broken", transport: "stdio", command: "missing" };

/** A fake MCP server listing `search`, counting calls and connections per namespace. */
function fakeMcp(opts: { readonly recipeHttpServers?: boolean } = {}) {
  const state = { calls: 0, connections: 0 };
  const pool = new McpClientPool({
    connect: async ({ server }) => {
      if (server.name === "broken") throw new Error("spawn missing ENOENT");
      state.connections += 1;
      const client: McpClient = {
        closed: false,
        listTools: async () => [
          {
            name: "search",
            description: "Search issues",
            inputSchema: { type: "object", properties: { q: { type: "string" } } },
          },
        ],
        callTool: async () => {
          state.calls += 1;
          return { content: [{ type: "text", text: "issue #1" }] };
        },
        close: async () => {},
      };
      return client;
    },
  });
  const logged: unknown[] = [];
  const mcp = new ZoryaMcpServers({
    servers: [GITHUB, BROKEN],
    pool,
    log: (line, detail) => logged.push(detail ?? line),
    ...(opts.recipeHttpServers !== undefined && { recipeHttpServers: opts.recipeHttpServers }),
  });
  return { state, mcp, logged };
}

async function boot(params: { readonly mcp?: ZoryaMcpServers; readonly llm?: LLMProvider } = {}) {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const memory = new InMemoryMemoryStore();
  const registry = new InMemoryAgentRegistry({ clock });
  const llm: LLMProvider = params.llm ?? {
    chat: async () => ({ content: "ok", finishReason: "stop" }),
  };
  const mcp = params.mcp;
  const agents = new ZoryaAgents({
    registry,
    resolve: async (recipe: RegisteredAgent, scope) =>
      resolveLocalAgent(recipe, {
        runner,
        memory,
        llm: () => llm,
        tools: {},
        ...(mcp && { mcpTools: await mcp.loadTools({ recipe, scope }) }),
      }),
    memory,
  });
  const server = new ZoryaServer({
    insecure: true,
    workflows: new LocalWorkflows({ storage, runner, definitions: {}, sleepScanIntervalMs: 0 }),
    agents,
    clock,
    ...(mcp && { mcp }),
  });
  return { server, registry };
}

function createBody(mcpServers: unknown) {
  return { id: "triage", backend: { ...localBackend, mcpServers } };
}

describe("recipe MCP entries", () => {
  it("rejects a recipe that supplies its own stdio command", async () => {
    const { server } = await boot({ mcp: fakeMcp().mcp });
    const res = await server.handle(
      req(
        "POST",
        "/api/agents",
        createBody([{ name: "pwn", transport: "stdio", command: "sh", args: ["-c", "id"] }]),
      ),
    );
    expect(res.status).toBe(400);
    expect((await jsonOf<{ error: string }>(res)).error).toBe("invalid_backend");
  });

  it("rejects extra fields on an allowlist reference (no env or command smuggling)", async () => {
    const { server } = await boot({ mcp: fakeMcp().mcp });
    const res = await server.handle(
      req("POST", "/api/agents", createBody([{ server: "github", command: "sh" }])),
    );
    expect(res.status).toBe(400);
  });

  it("rejects a server name the operator did not allowlist", async () => {
    const { server } = await boot({ mcp: fakeMcp().mcp });
    const res = await server.handle(req("POST", "/api/agents", createBody([{ server: "shell" }])));
    expect(res.status).toBe(400);
    const body = await jsonOf<{ error: string; message: string }>(res);
    expect(body.error).toBe("invalid_mcp_servers");
    expect(body.message).toContain("'shell' is not configured");
  });

  it("refuses MCP servers when the server has no MCP configuration", async () => {
    const { server } = await boot();
    const res = await server.handle(req("POST", "/api/agents", createBody([{ server: "github" }])));
    expect(res.status).toBe(400);
    expect((await jsonOf<{ error: string }>(res)).error).toBe("invalid_mcp_servers");
  });

  it("refuses recipe http endpoints unless the operator opted in", async () => {
    const { server } = await boot({ mcp: fakeMcp().mcp });
    const res = await server.handle(
      req(
        "POST",
        "/api/agents",
        createBody([{ name: "wiki", transport: "http", url: "https://wiki.example.com/mcp" }]),
      ),
    );
    expect(res.status).toBe(400);
    expect((await jsonOf<{ error: string }>(res)).error).toBe("invalid_mcp_servers");
  });

  it("refuses a recipe http endpoint that names a credential", async () => {
    const { server } = await boot({ mcp: fakeMcp({ recipeHttpServers: true }).mcp });
    const res = await server.handle(
      req(
        "POST",
        "/api/agents",
        createBody([
          {
            name: "x",
            transport: "http",
            url: "https://attacker.example/mcp",
            credentialRef: "ANTHROPIC_API_KEY",
          },
        ]),
      ),
    );
    expect(res.status).toBe(400);
    expect((await jsonOf<{ error: string }>(res)).error).toBe("invalid_backend");
  });

  it("accepts an allowlisted server and, when allowed, a public http endpoint", async () => {
    const { server } = await boot({ mcp: fakeMcp({ recipeHttpServers: true }).mcp });
    const res = await server.handle(
      req(
        "POST",
        "/api/agents",
        createBody([
          { server: "github", tools: ["search"] },
          { name: "wiki", transport: "http", url: "https://wiki.example.com/mcp" },
        ]),
      ),
    );
    expect(res.status).toBe(201);
  });
});

describe("MCP catalog", () => {
  it("lists allowlisted servers' tools with trust info and reports unreachable servers", async () => {
    const { mcp, logged } = fakeMcp();
    const { server } = await boot({ mcp });
    const res = await server.handle(req("GET", "/api/agents/_catalog/tools"));
    const body = await jsonOf<ToolsCatalogResponse>(res);
    const search = body.tools.find((t: ToolCatalogEntry) => t.name === "github__search");
    expect(search).toMatchObject({
      source: { kind: "mcp", server: "github", tool: "search", transport: "stdio" },
      requireApproval: true,
      idempotent: false,
    });
    expect(body.errors).toEqual([
      {
        kind: "mcp",
        server: "broken",
        code: "unreachable",
        message: "The server could not be reached or listed.",
      },
    ]);
    // The detail names a command; it is logged, never returned.
    expect(JSON.stringify(body)).not.toContain("spawn missing");
    expect(String(logged[0])).toContain("spawn missing ENOENT");
  });

  it("describes the allowlist without commands, args or env", async () => {
    const { server } = await boot({ mcp: fakeMcp().mcp });
    const res = await server.handle(req("GET", "/api/agents/_catalog/mcp-servers"));
    const body = await jsonOf<McpServersCatalogResponse>(res);
    expect(body.configured).toBe(true);
    expect(body.recipeHttpServers).toBe(false);
    expect(body.servers[0]).toEqual({
      name: "github",
      description: "GitHub issues",
      transport: "stdio",
      requireApproval: true,
      idempotent: false,
    });
    expect(JSON.stringify(body)).not.toContain("/etc/secret");
    expect(JSON.stringify(body)).not.toContain("ghe.internal");
  });

  it("answers configured: false without an MCP configuration", async () => {
    const { server } = await boot();
    const res = await server.handle(req("GET", "/api/agents/_catalog/mcp-servers"));
    expect((await jsonOf<McpServersCatalogResponse>(res)).configured).toBe(false);
  });
});

describe("invoking an agent with MCP servers", () => {
  it("gates the MCP call on approval and pools clients per tenant", async () => {
    const { mcp, state } = fakeMcp();
    const llm: LLMProvider = {
      chat: async ({ messages }) =>
        messages.at(-1)?.role === "tool"
          ? { content: "done", finishReason: "stop" }
          : {
              content: null,
              finishReason: "tool_calls",
              toolCalls: [{ id: "c-1", name: "github__search", input: { q: "bug" } }],
            },
    };
    const { server, registry } = await boot({ mcp, llm });
    await registry.register({
      id: "triage",
      backend: { ...localBackend, mcpServers: [{ server: "github" }] },
    });

    for (const namespaceId of ["acme", "globex"]) {
      const res = await server.handle(
        req("POST", `/api/agents/triage/threads/t-${namespaceId}/stream`, {
          task: "find bugs",
          namespaceId,
          resourceId: "u-1",
        }),
      );
      expect(res.status).toBe(200);
      const events = await res.text();
      expect(events).toContain("event: approval-requested");
      expect(events).toContain('"toolName":"github__search"');
    }
    expect(state.calls).toBe(0);
    expect(state.connections).toBe(2);
  });
});
