// ---------------------------------------------------------------------------
// End to end: a real in-process MCP server, attached by a recipe, resolved
// through loadMcpToolsFromRecipe + resolveLocalAgent, sent to the Anthropic
// and OpenAI adapters (request shape checked on the wire), and run by the
// engine (approval gate by default, fenced output).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { anthropic } from "../../adapters/anthropic.ts";
import { openai } from "../../adapters/openai.ts";
import { fakeFetch, jsonResponse } from "../../adapters/tests/fake-fetch.ts";
import { agentAction } from "../../agent-action.ts";
import type { LLMProvider } from "../../llm-provider.ts";
import { resolveLocalAgent } from "../../registry/resolve-local-agent.ts";
import type { LocalAgentBackend, RegisteredAgent } from "../../registry/types.ts";
import type { ToolCall } from "../../message.ts";
import { freshRunner, lastMessage, reactiveLLM } from "../../tests/engine-harness.ts";
import { loadMcpToolsFromRecipe } from "../load-mcp-tools-from-recipe.ts";
import { McpClientPool } from "../mcp-client-pool.ts";
import { PROVIDER_TOOL_NAME } from "../mcp-tool-name.ts";
import type { McpServerConfig, McpServerRecipe } from "../types.ts";
import { createTestMcpServer, inMemoryConnect, text, type TestMcpTool } from "./mcp-test-server.ts";

const SERVER_TOOLS: TestMcpTool[] = [
  {
    name: "search.issues",
    description: "Search issues",
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string" },
        state: { type: "string", enum: ["open", "closed"] },
        labels: { type: "array", items: { type: "string" } },
      },
      required: ["query"],
    },
    handler: ({ arguments: args }) => text(`found issues for ${String(args["query"])}`),
  },
  {
    name: `create_issue_with_a_very_long_name_${"x".repeat(60)}`,
    inputSchema: { type: "object", properties: { title: { type: "string" } } },
    handler: () => text("created"),
  },
  { name: "ping", inputSchema: { type: "object" }, handler: () => text("pong") },
];

const GITHUB: McpServerConfig = {
  name: "github",
  transport: "stdio",
  command: "github-mcp",
  args: ["--read-only"],
};

function recipe(mcpServers: ReadonlyArray<McpServerRecipe>): RegisteredAgent & {
  readonly backend: LocalAgentBackend;
} {
  return {
    id: "triage",
    version: "v1",
    backend: {
      type: "local",
      model: { provider: "test", id: "m" },
      role: { inline: { systemPrompt: "Triage issues.", tools: [] } },
      mcpServers,
    },
    metadata: { description: null, capabilities: [], tags: [] },
    createdAt: 0,
    active: true,
    enabled: true,
    trust: "trusted",
  };
}

function setup(params: { readonly servers?: ReadonlyArray<McpServerConfig> } = {}) {
  let serverCalls = 0;
  const { connect } = inMemoryConnect(() =>
    createTestMcpServer({
      tools: SERVER_TOOLS.map((t) => ({
        ...t,
        handler: (p) => {
          serverCalls += 1;
          return t.handler(p);
        },
      })),
    }),
  );
  const pool = new McpClientPool({ connect });
  const servers = params.servers ?? [GITHUB];
  return {
    pool,
    serverCalls: () => serverCalls,
    load: () =>
      loadMcpToolsFromRecipe({
        recipe: recipe([{ server: "github" }]),
        servers,
        pool,
        scope: { namespaceId: "acme", resourceId: "u-1" },
      }),
  };
}

const ANTHROPIC_OK = {
  content: [{ type: "text", text: "ok" }],
  stop_reason: "end_turn",
  usage: { input_tokens: 1, output_tokens: 1 },
};
const OPENAI_OK = {
  choices: [{ message: { role: "assistant", content: "ok" }, finish_reason: "stop" }],
  usage: { prompt_tokens: 1, completion_tokens: 1 },
};

async function invokeWith(params: {
  llm: LLMProvider;
  mcpTools: Awaited<ReturnType<typeof loadMcpToolsFromRecipe>>;
}) {
  // Non-streaming: the fake answers with plain JSON, not an SSE stream.
  const chatOnly: LLMProvider = { chat: (p) => params.llm.chat(p) };
  const agent = await resolveLocalAgent(recipe([{ server: "github" }]), {
    runner: freshRunner().runner,
    llm: () => chatOnly,
    tools: {},
    mcpTools: params.mcpTools,
    namespaceId: "acme",
    resourceId: "u-1",
  });
  const out = await agent.invoke({ task: "triage" });
  await out.text;
}

describe("MCP end to end", () => {
  it("sends provider-valid tool names and object schemas to Anthropic", async () => {
    const { load, pool } = setup();
    const mcpTools = await load();
    const { fetch, requests } = fakeFetch(() => jsonResponse(ANTHROPIC_OK));
    await invokeWith({ llm: anthropic("m", { apiKey: "k", fetch }), mcpTools });

    const tools = requests[0]!.body["tools"] as Array<{
      name: string;
      description: string;
      input_schema: Record<string, unknown>;
    }>;
    expect(tools).toHaveLength(3);
    for (const t of tools) {
      expect(t.name).toMatch(PROVIDER_TOOL_NAME);
      expect(t.input_schema["type"]).toBe("object");
    }
    const search = tools.find((t) => t.description.includes("'search.issues'"))!;
    expect(search.input_schema).toEqual({ ...SERVER_TOOLS[0]!.inputSchema });
    expect(search.description).not.toContain("JSON Schema");
    await pool.closeAll();
  });

  it("sends provider-valid tool names and object schemas to OpenAI", async () => {
    const { load, pool } = setup();
    const mcpTools = await load();
    const { fetch, requests } = fakeFetch(() => jsonResponse(OPENAI_OK));
    await invokeWith({ llm: openai("m", { apiKey: "k", fetch }), mcpTools });

    const tools = requests[0]!.body["tools"] as Array<{
      type: string;
      function: { name: string; parameters: Record<string, unknown> };
    }>;
    expect(tools).toHaveLength(3);
    for (const t of tools) {
      expect(t.function.name).toMatch(PROVIDER_TOOL_NAME);
      expect(t.function.parameters["type"]).toBe("object");
    }
    await pool.closeAll();
  });

  it("resolveLocalAgent refuses a recipe with MCP servers when they were not loaded", async () => {
    await expect(
      resolveLocalAgent(recipe([{ server: "github" }]), {
        runner: freshRunner().runner,
        llm: () => anthropic("m", { apiKey: "k" }),
        tools: {},
      }),
    ).rejects.toThrow(/attaches MCP servers but deps.mcpTools is unset/);
  });

  function callPingThenAnswer() {
    return reactiveLLM((messages) => {
      const last = lastMessage(messages);
      return last?.role === "tool"
        ? { content: `answer: ${last.content}`, finishReason: "stop" }
        : {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "c-1", name: "github__ping", input: {} }],
          };
    });
  }

  it("gates MCP calls behind approval by default: a denied call never reaches the server", async () => {
    const { load, pool, serverCalls } = setup();
    const tools = await load();
    const asked: ToolCall[] = [];
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({
        name: "mcp-approval",
        llm: callPingThenAnswer(),
        tools,
        onApprovalRequired: async (call) => {
          asked.push(call);
          return { approved: false, reason: "not now" };
        },
      }),
      workflowId: "mcp-approval-1",
      input: { task: "ping" },
    });
    expect(asked.map((c) => c.name)).toEqual(["github__ping"]);
    expect(serverCalls()).toBe(0);
    expect(result.answer).toContain("answer:");
    await pool.closeAll();
  });

  it("runs the call when the operator turned approval off, and fences the output", async () => {
    const { load, pool, serverCalls } = setup({
      servers: [{ ...GITHUB, requireApproval: false }],
    });
    const tools = await load();
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({ name: "mcp-run", llm: callPingThenAnswer(), tools }),
      workflowId: "mcp-run-1",
      input: { task: "ping" },
    });
    expect(serverCalls()).toBe(1);
    const toolMessage = result.messages.find((m) => m.role === "tool");
    expect(toolMessage?.content).toContain('<reference-data source="mcp:github">');
    expect(toolMessage?.content).toContain("pong");
    await pool.closeAll();
  });
});
