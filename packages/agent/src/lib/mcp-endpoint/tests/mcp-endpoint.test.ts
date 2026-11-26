import { describe, expect, it } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { FakeWallClock } from "@promin/workflow";
import { z } from "zod";
import { createElevatedTool, createScopedTool, tool, type AgentTool } from "../../tool.ts";
import { InMemoryMcpEndpointRegistry } from "../../versioned-registry/in-memory-registries.ts";
import { RegistryInputError } from "../../versioned-registry/types.ts";
import { mcpEndpoint, splitMcpEndpointTools } from "../define-mcp-endpoint.ts";
import { handleMcpEndpointRequest } from "../mcp-endpoint-handler.ts";
import { McpEndpointPrivateTools } from "../mcp-endpoint-private-tools.ts";
import { mergeToolSources, resolveMcpEndpointTools } from "../resolve-mcp-endpoint-tools.ts";
import type { McpEndpointRecipe, RegisteredMcpEndpoint } from "../types.ts";

const add = tool({
  name: "add",
  description: "Add two numbers.",
  parameters: z.object({ a: z.number(), b: z.number() }),
  idempotent: true,
  execute: async ({ a, b }) => ({ sum: a + b }),
});

const whoami = createScopedTool({
  name: "whoami",
  description: "Returns the caller scope.",
  parameters: z.object({}),
  execute: async (_input, ctx) => `${ctx.namespaceId}/${ctx.resourceId}`,
});

const boom = tool({
  name: "boom",
  description: "Always fails.",
  parameters: z.object({}),
  execute: async (): Promise<string> => {
    throw new Error("kaboom");
  },
});

const hang = tool({
  name: "hang",
  description: "Never finishes on its own.",
  parameters: z.object({}),
  timeoutMs: 1_000,
  execute: (_input, ctx) =>
    new Promise<string>((_, reject) => {
      ctx?.signal?.addEventListener("abort", () => reject(new Error("aborted")));
    }),
});

function recipe(overrides: Partial<McpEndpointRecipe> = {}): McpEndpointRecipe {
  return {
    title: null,
    instructions: null,
    tools: [],
    capabilities: [],
    namespaces: null,
    trustClientApproval: false,
    metadata: { description: null, tags: [] },
    ...overrides,
  };
}

function registered(overrides: Partial<McpEndpointRecipe> = {}): RegisteredMcpEndpoint {
  return {
    id: "demo",
    version: "v1",
    enabled: true,
    createdAt: 0,
    ...recipe(overrides),
  } as unknown as RegisteredMcpEndpoint;
}

async function connect(params: {
  endpoint: RegisteredMcpEndpoint;
  shared: Record<string, AgentTool<any, any>>;
  scope?: { namespaceId?: string; resourceId?: string };
  clock?: FakeWallClock;
}): Promise<Client> {
  const tools = resolveMcpEndpointTools({ endpoint: params.endpoint, shared: params.shared });
  const transport = new StreamableHTTPClientTransport(new URL("http://zorya.test/mcp/demo"), {
    fetch: (url, init) =>
      handleMcpEndpointRequest({
        req: new Request(String(url), init),
        endpoint: params.endpoint,
        tools,
        scope: params.scope ?? {},
        ...(params.clock !== undefined && { clock: params.clock }),
      }),
  });
  const client = new Client({ name: "test", version: "1" });
  await client.connect(transport);
  return client;
}

describe("MCP_ENDPOINT_KIND", () => {
  it("fills defaults and keeps only known binding fields", async () => {
    const registry = new InMemoryMcpEndpointRegistry();
    const entry = await registry.register({
      id: "billing",
      tools: [{ tool: "add", name: "sum", extra: 1 } as never],
    });
    expect(entry.title).toBeNull();
    expect(entry.namespaces).toBeNull();
    expect(entry.trustClientApproval).toBe(false);
    expect(entry.tools).toEqual([{ tool: "add", name: "sum" }]);
  });

  it("rejects bad ids, names and namespaces", async () => {
    const registry = new InMemoryMcpEndpointRegistry();
    await expect(registry.register({ id: "_x", tools: [] })).rejects.toBeInstanceOf(
      RegistryInputError,
    );
    await expect(
      registry.register({ id: "ok", tools: [{ tool: "add", name: "has space" }] }),
    ).rejects.toBeInstanceOf(RegistryInputError);
    await expect(
      registry.register({ id: "ok", tools: [], namespaces: [""] }),
    ).rejects.toBeInstanceOf(RegistryInputError);
  });
});

describe("resolveMcpEndpointTools", () => {
  it("drops tools it cannot serve, with a reason", () => {
    const approval = tool({ ...add, name: "pay", requireApproval: true });
    const off = tool({ ...add, name: "off", enabled: false });
    const admin = createElevatedTool({
      name: "admin",
      description: "admin",
      parameters: z.object({}),
      execute: async (_i, ctx) => {
        ctx.audit({ action: "x" });
        return "ok";
      },
    });
    const resolved = resolveMcpEndpointTools({
      endpoint: recipe({
        tools: [
          { tool: "add" },
          { tool: "missing" },
          { tool: "pay" },
          { tool: "off" },
          { tool: "admin" },
          { tool: "add", name: "bad name" },
          { tool: "whoami", name: "add" },
        ],
      }),
      shared: { add, pay: approval, off, admin, whoami },
    });
    expect([...resolved.tools.keys()]).toEqual(["add"]);
    expect(resolved.skipped.map((s) => `${s.tool}:${s.code}`)).toEqual([
      "missing:not_found",
      "pay:requires_approval",
      "off:disabled",
      "admin:capability_missing",
      "add:invalid_name",
      "whoami:name_taken",
    ]);
  });

  it("serves approval and elevated tools only when the endpoint opts in", () => {
    const pay = tool({ ...add, name: "pay", requireApproval: true });
    const admin = createElevatedTool({
      name: "admin",
      description: "admin",
      requires: "billing-admin",
      parameters: z.object({}),
      execute: async () => "ok",
    });
    const resolved = resolveMcpEndpointTools({
      endpoint: recipe({
        tools: [{ tool: "pay" }, { tool: "admin" }],
        trustClientApproval: true,
        capabilities: ["billing-admin"],
      }),
      shared: { pay, admin },
    });
    expect([...resolved.tools.keys()]).toEqual(["pay", "admin"]);
  });

  it("looks private bindings up in the endpoint's own tools only", () => {
    const resolved = resolveMcpEndpointTools({
      endpoint: recipe({
        tools: [
          { tool: "add", private: true },
          { tool: "boom", private: true },
        ],
      }),
      shared: { boom },
      privateTools: { add },
    });
    expect([...resolved.tools.keys()]).toEqual(["add"]);
    expect(resolved.skipped).toEqual([{ tool: "boom", private: true, code: "not_found" }]);
  });

  it("merges sources first-wins and reads registries live", () => {
    let live: Record<string, AgentTool<any, any>> = {};
    const registry = { getTools: () => live };
    const other = tool({ ...add, description: "other" });
    expect(mergeToolSources([{ add }, registry]).add).toBe(add);
    live = { add: other, boom };
    const merged = mergeToolSources([{}, registry]);
    expect(merged.add).toBe(other);
    expect(merged.boom).toBe(boom);
  });
});

describe("mcpEndpoint definitions", () => {
  it("splits shared names, inline tools and overrides", () => {
    const def = mcpEndpoint({
      tools: ["add", boom, { tool: whoami, name: "me" }, { tool: "add", name: "plus" }],
    });
    const split = splitMcpEndpointTools(def.tools);
    expect(split.bindings).toEqual([
      { tool: "add" },
      { tool: "boom", private: true },
      { tool: "whoami", private: true, name: "me" },
      { tool: "add", name: "plus" },
    ]);
    expect(Object.keys(split.privateTools).sort()).toEqual(["boom", "whoami"]);
  });

  it("rejects two different inline tools with one name", () => {
    const twin = tool({ ...boom });
    expect(() => splitMcpEndpointTools([boom, twin])).toThrow(/two inline tools/);
  });
});

describe("McpEndpointPrivateTools", () => {
  it("keeps unseen endpoints' tools when the scan was incomplete", () => {
    const set = new McpEndpointPrivateTools();
    const toolMap = (entries: Array<[string, Record<string, AgentTool<any, any>>]>) =>
      new Map(entries);
    set.replace({
      scanned: toolMap([
        ["a", { add }],
        ["b", { boom }],
      ]),
      complete: true,
    });
    set.replace({ scanned: toolMap([["a", { boom }]]), complete: false });
    expect(Object.keys(set.get("a"))).toEqual(["boom"]);
    expect(Object.keys(set.get("b"))).toEqual(["boom"]);
    set.replace({ scanned: toolMap([["a", { boom }]]), complete: true });
    expect(set.ids()).toEqual(["a"]);
  });
});

describe("handleMcpEndpointRequest", () => {
  it("lists tools with schema, title and instructions", async () => {
    const endpoint = registered({
      title: "Demo",
      instructions: "Use add for sums.",
      tools: [{ tool: "add", description: "Sum two numbers." }],
    });
    const client = await connect({ endpoint, shared: { add } });
    expect(client.getServerVersion()).toMatchObject({ name: "promin-demo", title: "Demo" });
    expect(client.getInstructions()).toBe("Use add for sums.");
    const { tools } = await client.listTools();
    expect(tools).toHaveLength(1);
    expect(tools[0]).toMatchObject({
      name: "add",
      description: "Sum two numbers.",
      annotations: { idempotentHint: true },
      inputSchema: { type: "object", required: ["a", "b"] },
    });
    await client.close();
  });

  it("calls a tool and returns text plus structured content", async () => {
    const client = await connect({
      endpoint: registered({ tools: [{ tool: "add" }] }),
      shared: { add },
    });
    const result = await client.callTool({ name: "add", arguments: { a: 2, b: 3 } });
    expect(result.isError).toBeFalsy();
    expect(result.content).toEqual([{ type: "text", text: '{"sum":5}' }]);
    expect(result.structuredContent).toEqual({ sum: 5 });
    await client.close();
  });

  it("runs scoped tools in the caller's scope", async () => {
    const client = await connect({
      endpoint: registered({ tools: [{ tool: "whoami" }] }),
      shared: { whoami },
      scope: { namespaceId: "acme", resourceId: "alice" },
    });
    const result = await client.callTool({ name: "whoami", arguments: {} });
    expect(result.content).toEqual([{ type: "text", text: "acme/alice" }]);
    await client.close();
  });

  it("answers bad input, throws and unknown tools with isError results", async () => {
    const client = await connect({
      endpoint: registered({ tools: [{ tool: "add" }, { tool: "boom" }] }),
      shared: { add, boom },
    });
    const bad = await client.callTool({ name: "add", arguments: { a: "x" } });
    expect(bad.isError).toBe(true);
    expect(JSON.stringify(bad.content)).toContain("Invalid arguments for add");
    const thrown = await client.callTool({ name: "boom", arguments: {} });
    expect(thrown).toMatchObject({ isError: true, content: [{ type: "text", text: "kaboom" }] });
    const unknown = await client.callTool({ name: "nope", arguments: {} });
    expect(unknown.isError).toBe(true);
    await client.close();
  });

  it("aborts a call at its timeout", async () => {
    const clock = FakeWallClock.create(0);
    const client = await connect({
      endpoint: registered({ tools: [{ tool: "hang" }] }),
      shared: { hang },
      clock,
    });
    const pending = client.callTool({ name: "hang", arguments: {} });
    for (let i = 0; i < 1_000 && clock.pendingCount() === 0; i++) {
      await new Promise((r) => setTimeout(r, 0));
    }
    clock.advance(1_000);
    const result = await pending;
    expect(result.isError).toBe(true);
    expect(JSON.stringify(result.content)).toContain("timed out after 1000ms");
    await client.close();
  });

  it("answers GET and DELETE with 405", async () => {
    for (const method of ["GET", "DELETE"]) {
      const res = await handleMcpEndpointRequest({
        req: new Request("http://zorya.test/mcp/demo", { method }),
        endpoint: registered(),
        tools: { tools: new Map(), skipped: [] },
        scope: {},
      });
      expect(res.status).toBe(405);
    }
  });
});
