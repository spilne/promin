import { describe, expect, it } from "bun:test";
import { z } from "zod";
import {
  CODE_SOURCE,
  applyDiscoveredMcpEndpoints,
  registerMcpEndpointDefinitions,
} from "../../discovery/mcp-endpoint-scanner.ts";
import { tool } from "../../tool.ts";
import { InMemoryMcpEndpointRegistry } from "../../versioned-registry/in-memory-registries.ts";
import { isMcpEndpointDefinition, mcpEndpoint } from "../define-mcp-endpoint.ts";
import { McpEndpoint } from "../mcp-endpoint-builder.ts";
import { McpEndpointPrivateTools } from "../mcp-endpoint-private-tools.ts";

const lookupInvoice = tool({
  name: "lookupInvoice",
  description: "Find an invoice.",
  parameters: z.object({ id: z.string() }),
  execute: async ({ id }) => ({ id, total: 10 }),
});

function billing(instructions = "Look an invoice up before refunding it.") {
  return McpEndpoint.builder("billing")
    .title("Billing")
    .instructions(instructions)
    .description("Billing tools")
    .tags(["finance"])
    .tool("calculate")
    .tool({ tool: "currentTime", name: "now" })
    .tool(lookupInvoice)
    .namespaces(["acme"])
    .capabilities(["billing-admin"])
    .trustClientApproval()
    .build();
}

describe("McpEndpoint.builder", () => {
  it("builds the same definition mcpEndpoint({...}) would", () => {
    const built = billing();
    expect(isMcpEndpointDefinition(built)).toBe(true);
    const literal = mcpEndpoint({
      id: "billing",
      title: "Billing",
      instructions: "Look an invoice up before refunding it.",
      tools: ["calculate", { tool: "currentTime", name: "now" }, lookupInvoice],
      capabilities: ["billing-admin"],
      namespaces: ["acme"],
      trustClientApproval: true,
      metadata: { description: "Billing tools", tags: ["finance"] },
    });
    expect({ ...built }).toEqual({ ...literal });
  });

  it("fails at build() on a bad id, exposed name or inline clash", () => {
    expect(() => McpEndpoint.builder("has space").build()).toThrow(/valid endpoint id/);
    expect(() =>
      McpEndpoint.builder("x").tool({ tool: "calculate", name: "bad name" }).build(),
    ).toThrow(/valid MCP tool name/);
    expect(() =>
      McpEndpoint.builder("x")
        .tool(lookupInvoice)
        .tool(tool({ ...lookupInvoice }))
        .build(),
    ).toThrow(/two inline tools/);
  });

  it("allows no id for file exports", () => {
    expect(McpEndpoint.builder().tool("calculate").build().id).toBeUndefined();
  });
});

describe("registerMcpEndpointDefinitions", () => {
  it("installs private tools and stores a code version once", async () => {
    const registry = new InMemoryMcpEndpointRegistry();
    const privateTools = new McpEndpointPrivateTools();
    const first = await registerMcpEndpointDefinitions({
      registry,
      privateTools,
      definitions: [billing()],
    });
    expect(first.added).toEqual(["billing"]);
    const stored = await registry.get({ id: "billing" });
    expect(stored?.source).toBe(CODE_SOURCE);
    expect(stored?.tools).toEqual([
      { tool: "calculate" },
      { tool: "currentTime", name: "now" },
      { tool: "lookupInvoice", private: true },
    ]);
    expect(privateTools.get("billing").lookupInvoice).toBe(lookupInvoice);

    // A restart with the same definition writes nothing.
    const again = await registerMcpEndpointDefinitions({
      registry,
      privateTools,
      definitions: [billing()],
    });
    expect(again.changed).toEqual([]);
    expect(await registry.versions({ id: "billing" })).toHaveLength(1);

    // A changed definition becomes the next active version.
    await registerMcpEndpointDefinitions({
      registry,
      privateTools,
      definitions: [billing("New instructions.")],
    });
    expect((await registry.get({ id: "billing" }))?.version).toBe("v2");
  });

  it("leaves an operator's active version alone", async () => {
    const registry = new InMemoryMcpEndpointRegistry();
    const privateTools = new McpEndpointPrivateTools();
    await registerMcpEndpointDefinitions({ registry, privateTools, definitions: [billing()] });
    await registry.register({ id: "billing", tools: [{ tool: "calculate" }], promote: true });
    const result = await registerMcpEndpointDefinitions({
      registry,
      privateTools,
      definitions: [billing("Changed in code.")],
    });
    expect(result.warnings).toHaveLength(1);
    expect((await registry.get({ id: "billing" }))?.source).toBeUndefined();
  });

  it("is untouched by folder scans and their sync", async () => {
    const registry = new InMemoryMcpEndpointRegistry();
    const privateTools = new McpEndpointPrivateTools();
    await registerMcpEndpointDefinitions({ registry, privateTools, definitions: [billing()] });
    privateTools.replace({ scanned: new Map(), complete: true });
    await applyDiscoveredMcpEndpoints({ registry, endpoints: [], sync: true });
    expect(await registry.get({ id: "billing" })).not.toBeNull();
    expect(Object.keys(privateTools.get("billing"))).toEqual(["lookupInvoice"]);
  });

  it("rejects missing and duplicate ids before writing", async () => {
    const registry = new InMemoryMcpEndpointRegistry();
    const privateTools = new McpEndpointPrivateTools();
    await expect(
      registerMcpEndpointDefinitions({
        registry,
        privateTools,
        definitions: [McpEndpoint.builder().build()],
      }),
    ).rejects.toThrow(/needs an id/);
    await expect(
      registerMcpEndpointDefinitions({
        registry,
        privateTools,
        definitions: [billing(), billing()],
      }),
    ).rejects.toThrow(/two MCP endpoints/);
    expect((await registry.list()).items).toEqual([]);
  });
});
