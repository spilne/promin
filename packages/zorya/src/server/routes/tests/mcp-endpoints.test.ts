// ---------------------------------------------------------------------------
// MCP endpoints end to end: a real ZoryaServer, an MCP SDK client talking to
// it through `server.handle`, the operator key plane, the scope authorizer
// (a caller may act only in the namespace its `x-tenant` header names;
// global endpoints need `x-operator`), the endpoint's namespace allowlist,
// the namespace registry, the kill switch and the registry CRUD.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  createScopedTool,
  InMemoryAgentRegistry,
  InMemoryMcpEndpointRegistry,
  McpEndpoint,
  tool,
} from "@promin/agent";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import {
  createWorkflowRunner,
  InMemoryNamespaceRegistry,
  InMemoryWorkflowStorage,
} from "@promin/workflow";
import { z } from "zod";
import { ZoryaServer } from "../../server.ts";
import { ZoryaAgents } from "../../services/agents/index.ts";
import { ZoryaMcpEndpoints } from "../../services/mcp-endpoints/index.ts";
import { LocalWorkflows } from "../../services/workflows/index.ts";
import type { ScopeAuthorizer } from "../agent-gateway-kit.ts";

const API_KEY = "operator-key";

const add = tool({
  name: "add",
  description: "Add two numbers.",
  parameters: z.object({ a: z.number(), b: z.number() }),
  execute: async ({ a, b }) => a + b,
});

const whoami = createScopedTool({
  name: "whoami",
  description: "The caller scope.",
  parameters: z.object({}),
  execute: async (_input, ctx) => `${ctx.namespaceId}/${ctx.resourceId}`,
});

const authorizeScope: ScopeAuthorizer = ({ req, scope }) =>
  scope.kind === "operator"
    ? req.headers.get("x-operator") === "yes"
    : req.headers.get("x-tenant") === scope.namespaceId;

async function boot() {
  const storage = new InMemoryWorkflowStorage();
  const workflows = new LocalWorkflows({
    storage,
    runner: createWorkflowRunner({ storage }),
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  const agents = new ZoryaAgents({
    registry: new InMemoryAgentRegistry(),
    resolve: () => {
      throw new Error("no agents in this test");
    },
    authorizeScope,
  });
  const namespaces = new InMemoryNamespaceRegistry();
  await namespaces.create({ id: "acme" });
  await namespaces.create({ id: "globex" });
  const registry = new InMemoryMcpEndpointRegistry();
  const endpoints = new ZoryaMcpEndpoints({
    registry,
    tools: [{ add, whoami }],
    onToolError: () => {},
  });
  await registry.register({
    id: "math",
    title: "Math",
    tools: [{ tool: "add", name: "sum" }, { tool: "whoami" }, { tool: "missing" }],
  });
  await registry.register({ id: "acme-only", tools: [{ tool: "add" }], namespaces: ["acme"] });
  const server = new ZoryaServer({
    apiKeys: [API_KEY],
    workflows,
    agents,
    namespaces,
    mcpEndpoints: endpoints,
  });
  return { server, registry, namespaces };
}

async function connect(params: {
  server: ZoryaServer;
  path: string;
  headers?: Record<string, string>;
}): Promise<Client> {
  const transport = new StreamableHTTPClientTransport(new URL(`http://zorya.test${params.path}`), {
    requestInit: {
      headers: { authorization: `Bearer ${API_KEY}`, ...params.headers },
    },
    fetch: (url, init) => params.server.handle(new Request(String(url), init)),
  });
  const client = new Client({ name: "test", version: "1" });
  await client.connect(transport);
  return client;
}

/** A raw MCP `initialize`, for asserting HTTP status codes. */
function initialize(path: string, headers: Record<string, string> = {}): Request {
  return new Request(`http://zorya.test${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      authorization: `Bearer ${API_KEY}`,
      ...headers,
    },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {
        protocolVersion: "2025-06-18",
        capabilities: {},
        clientInfo: { name: "t", version: "1" },
      },
    }),
  });
}

function api(method: string, path: string, body?: unknown, headers: Record<string, string> = {}) {
  return new Request(`http://zorya.test${path}`, {
    method,
    headers: { "content-type": "application/json", authorization: `Bearer ${API_KEY}`, ...headers },
    ...(body !== undefined && { body: JSON.stringify(body) }),
  });
}

describe("serving MCP endpoints", () => {
  it("serves a tenant endpoint with the tenant scope", async () => {
    const { server } = await boot();
    const client = await connect({
      server,
      path: "/ns/acme/mcp/math",
      headers: { "x-tenant": "acme", "x-zorya-resource": "alice" },
    });
    expect(client.getServerVersion()?.title).toBe("Math");
    const { tools } = await client.listTools();
    expect(tools.map((t) => t.name)).toEqual(["sum", "whoami"]);
    const sum = await client.callTool({ name: "sum", arguments: { a: 1, b: 2 } });
    expect(sum.content).toEqual([{ type: "text", text: "3" }]);
    const me = await client.callTool({ name: "whoami", arguments: {} });
    expect(me.content).toEqual([{ type: "text", text: "acme/alice" }]);
    await client.close();
  });

  it("fails scoped tools without a resource header, as a tool error", async () => {
    const { server } = await boot();
    const client = await connect({
      server,
      path: "/ns/acme/mcp/math",
      headers: { "x-tenant": "acme" },
    });
    const me = await client.callTool({ name: "whoami", arguments: {} });
    expect(me.isError).toBe(true);
    expect(JSON.stringify(me.content)).toContain("X-Zorya-Resource");
    await client.close();
  });

  it("needs the operator key", async () => {
    const { server } = await boot();
    const res = await server.handle(
      initialize("/ns/acme/mcp/math", { authorization: "Bearer wrong", "x-tenant": "acme" }),
    );
    expect(res.status).toBe(401);
  });

  it("keeps tenants to their own namespace and allowlist", async () => {
    const { server } = await boot();
    const otherTenant = await server.handle(
      initialize("/ns/globex/mcp/math", { "x-tenant": "acme" }),
    );
    expect(otherTenant.status).toBe(403);
    const notAllowed = await server.handle(
      initialize("/ns/globex/mcp/acme-only", { "x-tenant": "globex" }),
    );
    expect(notAllowed.status).toBe(404);
    const allowed = await server.handle(
      initialize("/ns/acme/mcp/acme-only", { "x-tenant": "acme" }),
    );
    expect(allowed.status).toBe(200);
  });

  it("refuses unregistered and archived namespaces", async () => {
    const { server, namespaces } = await boot();
    const unknown = await server.handle(
      initialize("/ns/initech/mcp/math", { "x-tenant": "initech" }),
    );
    expect(unknown.status).toBe(404);
    await namespaces.update({ id: "acme", patch: { status: "archived" } });
    const archived = await server.handle(initialize("/ns/acme/mcp/math", { "x-tenant": "acme" }));
    expect(archived.status).toBe(409);
  });

  it("serves the global path only to operators", async () => {
    const { server } = await boot();
    expect((await server.handle(initialize("/mcp/math", { "x-tenant": "acme" }))).status).toBe(403);
    const client = await connect({ server, path: "/mcp/math", headers: { "x-operator": "yes" } });
    const sum = await client.callTool({ name: "sum", arguments: { a: 2, b: 2 } });
    expect(sum.content).toEqual([{ type: "text", text: "4" }]);
    await client.close();
  });

  it("answers 404 for unknown and 410 for disabled endpoints", async () => {
    const { server, registry } = await boot();
    const unknown = await server.handle(initialize("/ns/acme/mcp/nope", { "x-tenant": "acme" }));
    expect(unknown.status).toBe(404);
    await registry.setEnabled({ id: "math", enabled: false });
    const disabled = await server.handle(initialize("/ns/acme/mcp/math", { "x-tenant": "acme" }));
    expect(disabled.status).toBe(410);
  });
});

describe("endpoints defined in host code", () => {
  it("registers builder endpoints on start and serves their inline tools", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = new InMemoryMcpEndpointRegistry();
    const echo = tool({
      name: "echo",
      description: "Echo the text back.",
      parameters: z.object({ text: z.string() }),
      execute: async ({ text }) => text,
    });
    const endpoints = new ZoryaMcpEndpoints({
      registry,
      tools: [{ add }],
      endpoints: [
        McpEndpoint.builder("toolkit")
          .title("Toolkit")
          .tool({ tool: "add", name: "plus" })
          .tool(echo)
          .build(),
      ],
    });
    const server = new ZoryaServer({
      apiKeys: [API_KEY],
      workflows: new LocalWorkflows({
        storage,
        runner: createWorkflowRunner({ storage }),
        definitions: {},
        sleepScanIntervalMs: 0,
      }),
      mcpEndpoints: endpoints,
    });
    await endpoints.start();
    expect((await registry.get({ id: "toolkit" }))?.source).toBe("code");

    const client = await connect({ server, path: "/mcp/toolkit" });
    expect((await client.listTools()).tools.map((t) => t.name)).toEqual(["plus", "echo"]);
    const echoed = await client.callTool({ name: "echo", arguments: { text: "hi" } });
    expect(echoed.content).toEqual([{ type: "text", text: "hi" }]);
    await client.close();
    await endpoints.stop();
  });
});

describe("/api/mcp-endpoints", () => {
  it("creates, previews, edits and kill-switches an endpoint", async () => {
    const { server } = await boot();
    const created = await server.handle(
      api("POST", "/api/mcp-endpoints", {
        id: "calc",
        title: "Calc",
        tools: [{ tool: "add" }, { tool: "nope" }],
      }),
    );
    expect(created.status).toBe(201);
    const etag = created.headers.get("etag") ?? "";

    const preview = await server.handle(api("GET", "/api/mcp-endpoints/calc/tools"));
    const body = (await preview.json()) as {
      tools: Array<{ name: string; parameters: { type: string } }>;
      skipped: Array<{ tool: string; private: boolean; code: string }>;
    };
    expect(body.tools.map((t) => t.name)).toEqual(["add"]);
    expect(body.tools[0]?.parameters.type).toBe("object");
    expect(body.skipped).toEqual([{ tool: "nope", private: false, code: "not_found" }]);

    const edited = await server.handle(
      api("PATCH", "/api/mcp-endpoints/calc", { instructions: "Sums only." }, { "if-match": etag }),
    );
    expect(edited.status).toBe(200);
    const after = (await edited.json()) as { title: string; instructions: string; version: string };
    expect(after).toMatchObject({ title: "Calc", instructions: "Sums only.", version: "v2" });

    const off = await server.handle(
      api("PATCH", "/api/mcp-endpoints/calc", { enabled: false }, { "if-match": `W/"v2"` }),
    );
    expect(((await off.json()) as { enabled: boolean }).enabled).toBe(false);
  });

  it("lists the tools endpoints may expose", async () => {
    const { server } = await boot();
    const res = await server.handle(api("GET", "/api/mcp-endpoints/_catalog"));
    const body = (await res.json()) as {
      shared: Array<{ name: string; kind: string }>;
      private: Record<string, unknown>;
    };
    expect(body.shared.map((t) => `${t.name}:${t.kind}`)).toEqual(["add:plain", "whoami:scoped"]);
    expect(body.private).toEqual({});
  });

  it("rejects bad bodies and duplicate ids", async () => {
    const { server } = await boot();
    const bad = await server.handle(
      api("POST", "/api/mcp-endpoints", { id: "x", tools: [{ tool: "add", name: "has space" }] }),
    );
    expect(bad.status).toBe(400);
    const dup = await server.handle(api("POST", "/api/mcp-endpoints", { id: "math", tools: [] }));
    expect(dup.status).toBe(409);
    const reserved = await server.handle(
      api("POST", "/api/mcp-endpoints", { id: "_x", tools: [] }),
    );
    expect(reserved.status).toBe(400);
  });
});
