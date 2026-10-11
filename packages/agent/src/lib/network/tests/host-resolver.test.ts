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
});
