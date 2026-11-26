import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FakeWallClock } from "@promin/workflow";
import { McpEndpointPrivateTools } from "../../mcp-endpoint/mcp-endpoint-private-tools.ts";
import { InMemoryMcpEndpointRegistry } from "../../versioned-registry/in-memory-registries.ts";
import { McpEndpointScanner, startMcpEndpointScanLoop } from "../mcp-endpoint-scanner.ts";

const TOOL_MODULE = join(import.meta.dir, "..", "..", "tool.ts");
const DEFINE_MODULE = join(import.meta.dir, "..", "..", "mcp-endpoint", "define-mcp-endpoint.ts");
// Fixture files live in a temp dir with no node_modules: import by resolved path.
const ZOD_MODULE = import.meta.resolve("zod");
const HEADER = `
import { z } from ${JSON.stringify(ZOD_MODULE)};
import { tool } from ${JSON.stringify(TOOL_MODULE)};
import { mcpEndpoint } from ${JSON.stringify(DEFINE_MODULE)};
`;

function inlineTool(name: string): string {
  return `tool({ name: ${JSON.stringify(name)}, description: "d", parameters: z.object({}), execute: async () => "ok" })`;
}

let root: string;

beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "mcp-endpoints-"));
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

async function write(rel: string, body: string): Promise<void> {
  const full = join(root, rel);
  await mkdir(join(full, ".."), { recursive: true });
  await writeFile(full, HEADER + body);
}

describe("McpEndpointScanner", () => {
  it("reads file endpoints, defaulting the id to the basename", async () => {
    await write(
      "billing.ts",
      `export default mcpEndpoint({ title: "Billing", tools: ["calculate", ${inlineTool("lookupInvoice")}] });`,
    );
    const scan = await McpEndpointScanner.scanFolder(root);
    expect(scan.errors).toEqual([]);
    expect(scan.endpoints).toHaveLength(1);
    const [billing] = scan.endpoints;
    expect(billing?.id).toBe("billing");
    expect(billing?.input).toMatchObject({
      title: "Billing",
      tools: [{ tool: "calculate" }, { tool: "lookupInvoice", private: true }],
    });
    expect(Object.keys(billing?.privateTools ?? {})).toEqual(["lookupInvoice"]);
  });

  it("reads folder endpoints and includes every tools/ file", async () => {
    await write(
      "support/index.ts",
      `export default mcpEndpoint({ tools: ["calculate", { tool: "search", name: "find" }] });`,
    );
    await write("support/tools/search.ts", `export default ${inlineTool("search")};`);
    await write("support/tools/nested/escalate.ts", `export default ${inlineTool("escalate")};`);
    const scan = await McpEndpointScanner.scanFolder(root);
    expect(scan.errors).toEqual([]);
    const [support] = scan.endpoints;
    expect(support?.id).toBe("support");
    expect(support?.input.tools).toEqual([
      { tool: "calculate" },
      { tool: "search", name: "find", private: true },
      { tool: "escalate", private: true },
    ]);
    expect(Object.keys(support?.privateTools ?? {}).sort()).toEqual(["escalate", "search"]);
  });

  it("reports definitions it cannot use as errors", async () => {
    await write(
      "two.ts",
      `export const a = mcpEndpoint({ tools: [] }); export const b = mcpEndpoint({ tools: [] });`,
    );
    await write("badid.ts", `export default mcpEndpoint({ id: "has space", tools: [] });`);
    await write(
      "multi/index.ts",
      `export const a = mcpEndpoint({ id: "a", tools: [] }); export const b = mcpEndpoint({ id: "b", tools: [] });`,
    );
    await write("withbadtool/index.ts", `export default mcpEndpoint({ tools: [] });`);
    await write("withbadtool/tools/x.ts", `export default 42;`);
    await write("notes/readme.ts", `export const x = 1;`);
    const scan = await McpEndpointScanner.scanFolder(root);
    expect(scan.endpoints).toEqual([]);
    expect(scan.errors).toHaveLength(4);
  });

  it("scan loop stores endpoints and installs their private tools", async () => {
    await write(
      "billing.ts",
      `export default mcpEndpoint({ tools: [${inlineTool("lookupInvoice")}] });`,
    );
    const registry = new InMemoryMcpEndpointRegistry();
    const privateTools = new McpEndpointPrivateTools();
    const loop = startMcpEndpointScanLoop({
      registry,
      privateTools,
      root,
      clock: FakeWallClock.create(0),
    });
    const tick = await loop.tick();
    loop.stop();
    expect(tick.added).toEqual(["billing"]);
    const stored = await registry.get({ id: "billing" });
    expect(stored?.source).toBe("file");
    expect(stored?.tools).toEqual([{ tool: "lookupInvoice", private: true }]);
    expect(Object.keys(privateTools.get("billing"))).toEqual(["lookupInvoice"]);
  });
});
