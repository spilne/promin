// ---------------------------------------------------------------------------
// callAgent wired the way a host (the zorya demo) wires it: one async
// resolver serves direct invocations and network delegation, loading each
// recipe's role from the role registry for the scope it runs in. A callee
// bound to a role by `ref` resolves through it and answers the caller.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import type { Agent } from "../../agent/types.ts";
import { InMemoryAgentInstanceRegistry } from "../../instance/in-memory-agent-instance-registry.ts";
import type { LLMProvider } from "../../llm-provider.ts";
import { InMemoryMemoryStore } from "../../memory/in-memory-memory-store.ts";
import { InMemoryAgentRegistry } from "../../registry/in-memory-agent-registry.ts";
import { resolveLocalAgent } from "../../registry/resolve-local-agent.ts";
import type { RegisteredAgent } from "../../registry/types.ts";
import { InMemoryRoleRegistry } from "../../role/in-memory-role-registry.ts";
import { resolveRoleBinding } from "../../role/resolve-role.ts";
import { lastMessage, reactiveLLM, scriptedLLM } from "../../tests/engine-harness.ts";
import type { AgentScope } from "../../agent/types.ts";
import { loadMcpToolsFromRecipe } from "../../mcp/load-mcp-tools-from-recipe.ts";
import { McpClientPool } from "../../mcp/mcp-client-pool.ts";
import type { McpServerConfig } from "../../mcp/types.ts";
import { createTestMcpServer, inMemoryConnect, text } from "../../mcp/tests/mcp-test-server.ts";
import { InMemorySecretsStorage } from "../../secrets/in-memory-secrets-storage.ts";
import { SecretScope } from "../../secrets/types.ts";

describe("callAgent — host resolver wiring", () => {
  it("delegates to a role-ref callee through the host's async resolver", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const memory = new InMemoryMemoryStore();
    const registry = new InMemoryAgentRegistry();
    const roles = new InMemoryRoleRegistry();
    const instanceRegistry = new InMemoryAgentInstanceRegistry();

    await roles.register({
      id: "haiku-writer",
      definition: { systemPrompt: "You write haikus.", tools: [] },
    });
    await registry.register({
      id: "writer",
      backend: {
        type: "local",
        model: { provider: "test", id: "m" },
        role: { ref: { id: "haiku-writer" } },
      },
      metadata: { capabilities: ["writing"], tags: [] },
    });
    const coordinator = await registry.register({
      id: "coordinator",
      backend: {
        type: "local",
        model: { provider: "test", id: "m" },
        role: { inline: { systemPrompt: "You coordinate.", tools: [] } },
        network: { networks: ["default"], canDiscover: true, canCall: true },
      },
      metadata: { capabilities: [], tags: [] },
    });

    const writerLlm = scriptedLLM([{ content: "an old silent pond", finishReason: "stop" }]);
    const coordinatorLlm = reactiveLLM((messages) => {
      const last = lastMessage(messages);
      return last?.role === "tool"
        ? { content: `writer says: ${last.content}`, finishReason: "stop" }
        : {
            content: null,
            finishReason: "tool_calls",
            toolCalls: [{ id: "c1", name: "callAgent", input: { id: "writer", prompt: "haiku" } }],
          };
    });
    const llms: Record<string, LLMProvider> = { writer: writerLlm, coordinator: coordinatorLlm };
    const resolvedScopes: Array<Record<string, string | undefined>> = [];

    // The demo's resolver shape: async, scope-aware, loads the role.
    async function resolveAgent(
      recipe: RegisteredAgent,
      scope?: { readonly namespaceId?: string; readonly resourceId?: string },
    ): Promise<Agent> {
      resolvedScopes.push({ id: recipe.id, ...scope });
      if (recipe.backend.type !== "local") throw new Error("local recipes only");
      const role = await resolveRoleBinding(recipe.backend.role, { roles });
      return resolveLocalAgent(recipe, {
        runner,
        memory,
        role,
        llm: () => llms[recipe.id]!,
        tools: {},
        network: { registry, resolve: resolveAgent, instanceRegistry },
      });
    }

    const agent = (await resolveAgent(coordinator)).withScope({
      namespaceId: "acme",
      resourceId: "acme::coordinator::alice",
      ownerId: "alice",
    });
    const out = await agent.invoke({ task: "get me a haiku" }, { runId: "coord-run" });

    expect(await out.text).toBe("writer says: an old silent pond");
    // The callee ran with its role's prompt, under alice's writer instance.
    expect(writerLlm.calls[0]?.messages[0]).toEqual({
      role: "system",
      content: "You write haikus.",
    });
    expect(resolvedScopes).toContainEqual({
      id: "writer",
      namespaceId: "acme",
      resourceId: "acme::writer::alice",
      ownerId: "alice",
    });
    const child = await storage.loadWorkflow("coord-run.0.c1");
    expect(child?.parentWorkflowId).toBe("coord-run");
    expect(child?.status).toBe("completed");
  });

  it("delegates to a callee that attaches MCP tools, loaded for the caller's scope", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const registry = new InMemoryAgentRegistry();
    const secrets = new InMemorySecretsStorage();
    await secrets.set({
      scope: SecretScope.namespace("acme"),
      key: "KB_TOKEN",
      value: "acme-token",
    });
    const KB: McpServerConfig = {
      name: "kb",
      transport: "stdio",
      command: "kb-mcp",
      credentialRef: "KB_TOKEN",
      credentialEnv: "KB_TOKEN",
      tools: ["search"],
      // A read-only lookup the operator trusts.
      requireApproval: false,
    };
    const { connect, connections } = inMemoryConnect(() =>
      createTestMcpServer({
        tools: [
          {
            name: "search",
            inputSchema: { type: "object" as const },
            handler: () => text("found: the runbook"),
          },
        ],
      }),
    );
    const pool = new McpClientPool({ connect });
    await registry.register({
      id: "searcher",
      backend: {
        type: "local",
        model: { provider: "test", id: "m" },
        role: { inline: { systemPrompt: "You search.", tools: [] } },
        mcpServers: [{ server: "kb" }],
      },
      metadata: { capabilities: [], tags: [] },
    });
    const coordinator = await registry.register({
      id: "coordinator",
      backend: {
        type: "local",
        model: { provider: "test", id: "m" },
        role: { inline: { systemPrompt: "You coordinate.", tools: [] } },
        network: { networks: ["default"], canDiscover: true, canCall: true },
      },
      metadata: { capabilities: [], tags: [] },
    });
    const llms: Record<string, LLMProvider> = {
      searcher: reactiveLLM((messages) =>
        lastMessage(messages)?.role === "tool"
          ? { content: `searcher: ${String(lastMessage(messages)?.content)}`, finishReason: "stop" }
          : {
              content: null,
              finishReason: "tool_calls",
              toolCalls: [{ id: "s1", name: "kb__search", input: {} }],
            },
      ),
      coordinator: reactiveLLM((messages) =>
        lastMessage(messages)?.role === "tool"
          ? { content: `got ${String(lastMessage(messages)?.content)}`, finishReason: "stop" }
          : {
              content: null,
              finishReason: "tool_calls",
              toolCalls: [
                { id: "c1", name: "callAgent", input: { id: "searcher", prompt: "find it" } },
              ],
            },
      ),
    };

    // The host resolver: MCP tools need the scope the agent runs in.
    async function resolveAgent(recipe: RegisteredAgent, scope: AgentScope): Promise<Agent> {
      const mcpTools = await loadMcpToolsFromRecipe({
        recipe,
        servers: [KB],
        pool,
        secrets,
        scope: {
          ...(scope.namespaceId !== undefined && { namespaceId: scope.namespaceId }),
          ...(scope.resourceId !== undefined && { resourceId: scope.resourceId }),
        },
      });
      return resolveLocalAgent(recipe, {
        runner,
        llm: () => llms[recipe.id]!,
        tools: {},
        mcpTools,
        network: { registry, resolve: resolveAgent },
      });
    }

    const agent = (
      await resolveAgent(coordinator, { namespaceId: "acme", resourceId: "alice" })
    ).withScope({ namespaceId: "acme", resourceId: "alice" });
    const out = await agent.invoke({ task: "look it up" }, { runId: "mcp-run" });

    // The MCP result reached the callee (fenced as reference data) and its
    // answer the caller.
    const answer = await out.text;
    expect(answer.startsWith("got searcher:")).toBe(true);
    expect(answer).toContain("found: the runbook");
    expect(connections.map((c) => c.credential)).toEqual(["acme-token"]);
    expect((await storage.loadWorkflow("mcp-run.0.c1"))?.status).toBe("completed");
    await pool.closeAll();
  });
});
