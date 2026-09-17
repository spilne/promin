// ---------------------------------------------------------------------------
// `createMcpTools` — adapter from McpClient to AgentTool. Covers tool
// discovery, name mounting, success path, error path, and content-block
// stringification. Uses a fake McpClient — no SDK dependency in tests.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { createMcpTools } from "../mcp-tools.ts";
import type { McpClient, McpToolDefinition, McpToolResult } from "../types.ts";

class FakeMcpClient implements McpClient {
  public readonly name: string;
  public callLog: Array<{ name: string; arguments: Record<string, unknown> }> = [];
  constructor(
    name: string,
    private readonly tools: McpToolDefinition[],
    private readonly handler: (params: {
      name: string;
      arguments: Record<string, unknown>;
    }) => Promise<McpToolResult> | McpToolResult,
  ) {
    this.name = name;
  }
  async listTools(): Promise<McpToolDefinition[]> {
    return this.tools;
  }
  async callTool(params: {
    name: string;
    arguments: Readonly<Record<string, unknown>>;
  }): Promise<McpToolResult> {
    this.callLog.push({ name: params.name, arguments: { ...params.arguments } });
    return this.handler({ name: params.name, arguments: { ...params.arguments } });
  }
  async close(): Promise<void> {}
}

describe("createMcpTools — adapter", () => {
  it("mounts each MCP tool as an AgentTool keyed by `<server>:<tool>`", async () => {
    const client = new FakeMcpClient(
      "github",
      [
        { name: "list_repos", description: "list user repos", inputSchema: {} },
        {
          name: "get_issue",
          description: "fetch one issue",
          inputSchema: { type: "object", properties: { id: { type: "number" } } },
        },
      ],
      async () => ({ content: [{ type: "text", text: "ok" }] }),
    );
    const tools = await createMcpTools(client);
    expect(Object.keys(tools).sort()).toEqual(["github:get_issue", "github:list_repos"]);
    expect(tools["github:list_repos"]?.description).toContain("list user repos");
  });

  it("inlines JSON Schema into the description for non-empty schemas", async () => {
    const schema = { type: "object", properties: { repo: { type: "string" } }, required: ["repo"] };
    const client = new FakeMcpClient(
      "gh",
      [{ name: "star", description: "star a repo", inputSchema: schema }],
      async () => ({ content: [{ type: "text", text: "" }] }),
    );
    const tools = await createMcpTools(client);
    const desc = tools["gh:star"]!.description;
    expect(desc).toContain("Parameter schema (JSON Schema)");
    expect(desc).toContain("repo");
  });

  it("does not append schema when inputSchema is empty", async () => {
    const client = new FakeMcpClient(
      "gh",
      [{ name: "ping", description: "no args", inputSchema: {} }],
      async () => ({ content: [{ type: "text", text: "pong" }] }),
    );
    const tools = await createMcpTools(client);
    expect(tools["gh:ping"]!.description).not.toContain("Parameter schema");
  });

  it("execute() forwards args to client.callTool and returns concatenated text", async () => {
    const client = new FakeMcpClient(
      "fs",
      [{ name: "read_file", inputSchema: { type: "object" } }],
      async ({ arguments: args }) => ({
        content: [
          { type: "text", text: `path=${args.path}` },
          { type: "text", text: "extra" },
        ],
      }),
    );
    const tools = await createMcpTools(client);
    const out = await tools["fs:read_file"]!.execute({ path: "/etc/hosts" });
    expect(out).toBe("path=/etc/hosts\nextra");
    expect(client.callLog).toHaveLength(1);
    expect(client.callLog[0]?.arguments.path).toBe("/etc/hosts");
  });

  it("execute() surfaces isError results as 'Error from MCP tool ...'", async () => {
    const client = new FakeMcpClient("fs", [{ name: "read_file", inputSchema: {} }], async () => ({
      isError: true,
      content: [{ type: "text", text: "ENOENT: no such file" }],
    }));
    const tools = await createMcpTools(client);
    const out = await tools["fs:read_file"]!.execute({});
    expect(typeof out).toBe("string");
    expect(out as string).toContain("Error from MCP tool 'fs:read_file'");
    expect(out as string).toContain("ENOENT");
  });

  it("execute() catches client.callTool throws and returns 'Error: ...' string", async () => {
    const client = new FakeMcpClient("fs", [{ name: "read_file", inputSchema: {} }], async () => {
      throw new Error("connection lost");
    });
    const tools = await createMcpTools(client);
    const out = await tools["fs:read_file"]!.execute({});
    expect(out).toBe("Error: connection lost");
  });

  it("renders image content blocks as size markers", async () => {
    const client = new FakeMcpClient(
      "vision",
      [{ name: "screenshot", inputSchema: {} }],
      async () => ({
        content: [
          { type: "text", text: "Captured:" },
          { type: "image", data: "AAAA", mimeType: "image/png" },
        ],
      }),
    );
    const tools = await createMcpTools(client);
    const out = (await tools["vision:screenshot"]!.execute({})) as string;
    expect(out).toContain("Captured:");
    expect(out).toContain("[image: image/png");
  });

  it("renders resource content blocks with uri + inline text when present", async () => {
    const client = new FakeMcpClient(
      "wiki",
      [{ name: "fetch_doc", inputSchema: {} }],
      async () => ({
        content: [
          {
            type: "resource",
            resource: {
              uri: "wiki://onboarding",
              text: "Welcome to the company.",
              mimeType: "text/markdown",
            },
          },
        ],
      }),
    );
    const tools = await createMcpTools(client);
    const out = (await tools["wiki:fetch_doc"]!.execute({})) as string;
    expect(out).toContain("[resource wiki://onboarding]");
    expect(out).toContain("Welcome to the company.");
  });

  it("uses bare tool name when serverName is empty", async () => {
    const client = new FakeMcpClient("", [{ name: "ping", inputSchema: {} }], async () => ({
      content: [{ type: "text", text: "pong" }],
    }));
    const tools = await createMcpTools(client);
    expect(Object.keys(tools)).toEqual(["ping"]);
  });

  it("AgentTool parameters accept arbitrary objects (server-side validation)", async () => {
    const client = new FakeMcpClient("x", [{ name: "anything", inputSchema: {} }], async () => ({
      content: [{ type: "text", text: "ok" }],
    }));
    const tools = await createMcpTools(client);
    const def = tools["x:anything"]!;
    // The Zod parameters should accept arbitrary objects without throwing.
    expect(() => def.parameters.parse({ foo: 1, bar: ["nested"] })).not.toThrow();
  });
});
