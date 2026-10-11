// ---------------------------------------------------------------------------
// findAgent / callAgent guards, exercised on the tools directly: disabled
// peers, cycles on the call chain, the host's authorizeCall hook, and the
// scope (namespace, resource, owner) a callee runs in.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { Agent, AgentRunOutput, AgentScope } from "../../agent/types.ts";
import { InMemoryAgentInstanceRegistry } from "../../instance/in-memory-agent-instance-registry.ts";
import { composeAgentInstanceId } from "../../instance/types.ts";
import { InMemoryAgentRegistry } from "../../registry/in-memory-agent-registry.ts";
import type { RegisteredAgent } from "../../registry/types.ts";
import { runInCallContext } from "../depth.ts";
import {
  createCallAgentTool,
  createFindAgentTool,
  type NetworkCallAuthorizer,
  type NetworkCallerScope,
} from "../runtime.ts";

/** A callee that records the scope it was bound to and answers "ok". */
function recordingAgent(scopes: AgentScope[]): Agent {
  const output = {
    text: Promise.resolve("ok"),
    finishReason: Promise.resolve("stop"),
    usage: Promise.resolve({ inputTokens: 0, outputTokens: 0 }),
    messages: Promise.resolve([]),
  } as unknown as AgentRunOutput;
  const agent = {
    invoke: async () => output,
    withScope: (scope: AgentScope) => {
      scopes.push(scope);
      return agent;
    },
  } as unknown as Agent;
  return agent;
}

async function setup(opts?: {
  disabled?: ReadonlyArray<string>;
  instances?: boolean;
  authorizeCall?: NetworkCallAuthorizer;
}) {
  const registry = new InMemoryAgentRegistry();
  const register = (id: string) =>
    registry.register({
      id,
      backend: {
        type: "local",
        model: { provider: "test", id: "m" },
        role: { inline: { systemPrompt: null, tools: [] } },
        network: { networks: ["default"], canDiscover: true, canCall: true, maxDepth: 5 },
      },
      metadata: {
        capabilities: [],
        tags: [],
        ...(opts?.disabled?.includes(id) && { enabled: false }),
      },
    });
  const coordinator = await register("coordinator");
  await register("writer");
  await register("reviewer");
  const scopes: AgentScope[] = [];
  const resolved: string[] = [];
  const instanceRegistry = opts?.instances ? new InMemoryAgentInstanceRegistry() : undefined;
  const deps = {
    registry,
    resolve: (recipe: RegisteredAgent) => {
      resolved.push(recipe.id);
      return recordingAgent(scopes);
    },
    ...(instanceRegistry !== undefined && { instanceRegistry }),
    ...(opts?.authorizeCall !== undefined && { authorizeCall: opts.authorizeCall }),
  };
  const network =
    coordinator.backend.type === "local" ? coordinator.backend.network! : (undefined as never);
  const tools = (scope: NetworkCallerScope) => ({
    find: createFindAgentTool({ deps, scope, callerRecipe: coordinator, network }),
    call: createCallAgentTool({ deps, scope, callerRecipe: coordinator, network }),
  });
  return { tools, scopes, resolved, instanceRegistry };
}

describe("findAgent", () => {
  it("hides disabled agents", async () => {
    const { tools } = await setup({ disabled: ["writer"] });
    const out = await tools({ namespaceId: "acme" }).find.execute({});
    expect(out.hits.map((h) => h.id)).toEqual(["reviewer"]);
  });
});

describe("callAgent", () => {
  it("refuses a disabled callee without running it", async () => {
    const { tools, resolved } = await setup({ disabled: ["writer"] });
    const out = await tools({ namespaceId: "acme" }).call.execute({ id: "writer", prompt: "hi" });
    expect(out.ok).toBe(false);
    expect(out.error).toContain("agent_disabled");
    expect(resolved).toEqual([]);
  });

  it("refuses to call itself", async () => {
    const { tools, resolved } = await setup();
    const out = await tools({ namespaceId: "acme" }).call.execute({
      id: "coordinator",
      prompt: "hi",
    });
    expect(out.ok).toBe(false);
    expect(out.error).toContain("cycle");
    expect(resolved).toEqual([]);
  });

  it("refuses a callee already waiting on the call chain", async () => {
    const { tools, resolved } = await setup();
    // writer → coordinator is in flight; coordinator calling writer loops.
    const out = await runInCallContext({ depth: 1, chain: ["writer"] }, () =>
      tools({ namespaceId: "acme" }).call.execute({ id: "writer", prompt: "hi" }),
    );
    expect(out.ok).toBe(false);
    expect(out.error).toContain("cycle");
    expect(resolved).toEqual([]);
  });

  it("still calls a peer that is not on the chain", async () => {
    const { tools, resolved } = await setup();
    const out = await runInCallContext({ depth: 1, chain: ["writer"] }, () =>
      tools({ namespaceId: "acme" }).call.execute({ id: "reviewer", prompt: "hi" }),
    );
    expect(out.ok).toBe(true);
    expect(resolved).toEqual(["reviewer"]);
  });

  it("asks the host's authorizeCall and refuses when it says no", async () => {
    const seen: Parameters<NetworkCallAuthorizer>[0][] = [];
    const { tools, resolved } = await setup({
      authorizeCall: (params) => {
        seen.push(params);
        return params.calleeAgentId !== "reviewer";
      },
    });
    const scope = { namespaceId: "acme", resourceId: "r-1", ownerId: "alice" };
    const denied = await tools(scope).call.execute({ id: "reviewer", prompt: "hi" });
    expect(denied.ok).toBe(false);
    expect(denied.error).toContain("refused by the host");
    const allowed = await tools(scope).call.execute({ id: "writer", prompt: "hi" });
    expect(allowed.ok).toBe(true);
    expect(resolved).toEqual(["writer"]);
    expect(seen[0]).toEqual({
      callerAgentId: "coordinator",
      calleeAgentId: "reviewer",
      scope,
      chain: ["coordinator"],
    });
  });

  it("runs the callee in the caller's namespace under the caller's owner", async () => {
    const { tools, scopes, instanceRegistry } = await setup({ instances: true });
    const callerResource = composeAgentInstanceId({
      namespaceId: "acme",
      registeredAgentId: "coordinator",
      ownerId: "alice",
    });
    const out = await tools({
      namespaceId: "acme",
      resourceId: callerResource,
      ownerId: "alice",
    }).call.execute({ id: "writer", prompt: "hi" });
    const writerInstance = composeAgentInstanceId({
      namespaceId: "acme",
      registeredAgentId: "writer",
      ownerId: "alice",
    });
    expect(out.resolvedResourceId).toBe(writerInstance);
    expect(scopes).toEqual([{ namespaceId: "acme", resourceId: writerInstance, ownerId: "alice" }]);
    expect((await instanceRegistry!.get(writerInstance))?.ownerId).toBe("alice");
  });

  it("never derives an owner from a resource id that merely looks like an instance id", async () => {
    const { tools, scopes, instanceRegistry } = await setup({ instances: true });
    // A raw resource a client chose to shape like bob's instance id.
    const out = await tools({
      namespaceId: "acme",
      resourceId: "acme::coordinator::bob",
    }).call.execute({ id: "writer", prompt: "hi" });
    expect(out.ok).toBe(true);
    expect(scopes).toEqual([{ namespaceId: "acme", resourceId: "acme::coordinator::bob" }]);
    expect(await instanceRegistry!.list()).toEqual([]);
  });
});
