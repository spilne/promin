// ---------------------------------------------------------------------------
// findAgent / callAgent guards, exercised on the tools directly: disabled
// peers, cycles on the call chain, the host's authorizeCall hook, and the
// scope (namespace, resource, owner) a callee runs in. callAgent's guards
// live in its route step (`delegation.route`), which the engine runs once
// per call with the frame the callee would run in.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { Agent, AgentRunOutput, AgentScope } from "../../agent/types.ts";
import { InMemoryAgentInstanceRegistry } from "../../instance/in-memory-agent-instance-registry.ts";
import { composeAgentInstanceId } from "../../instance/types.ts";
import { InMemoryAgentRegistry } from "../../registry/in-memory-agent-registry.ts";
import type { RegisteredAgent } from "../../registry/types.ts";
import type { DelegationFrame, SubAgentRouteResult } from "../../delegation/types.ts";
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
    resolve: async (recipe: RegisteredAgent) => {
      resolved.push(recipe.id);
      return recordingAgent(scopes);
    },
    ...(instanceRegistry !== undefined && { instanceRegistry }),
    ...(opts?.authorizeCall !== undefined && { authorizeCall: opts.authorizeCall }),
  };
  const network =
    coordinator.backend.type === "local" ? coordinator.backend.network! : (undefined as never);
  const tools = (scope: NetworkCallerScope) => {
    const call = createCallAgentTool({ deps, scope, callerRecipe: coordinator, network });
    return {
      find: createFindAgentTool({ deps, scope, callerRecipe: coordinator, network }),
      /** Route one call as the engine would, from a turn in `frame` (default: top level). */
      call: (
        input: { id: string; prompt: string },
        frame: DelegationFrame = { depth: 1, chain: ["coordinator"] },
      ): Promise<SubAgentRouteResult> => call.delegation!.route({ input, frame }),
    };
  };
  return { tools, scopes, resolved, instanceRegistry };
}

function errorOf(result: SubAgentRouteResult): string | undefined {
  return result.ok ? undefined : result.error;
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
    const out = await tools({ namespaceId: "acme" }).call({ id: "writer", prompt: "hi" });
    expect(errorOf(out)).toContain("agent_disabled");
    expect(resolved).toEqual([]);
  });

  it("refuses to call itself", async () => {
    const { tools, resolved } = await setup();
    const out = await tools({ namespaceId: "acme" }).call({
      id: "coordinator",
      prompt: "hi",
    });
    expect(errorOf(out)).toContain("cycle");
    expect(resolved).toEqual([]);
  });

  it("refuses a callee already waiting on the call chain", async () => {
    const { tools, resolved } = await setup();
    // writer → coordinator is in flight; coordinator calling writer loops.
    const out = await tools({ namespaceId: "acme" }).call(
      { id: "writer", prompt: "hi" },
      { depth: 2, chain: ["writer", "coordinator"] },
    );
    expect(errorOf(out)).toContain("cycle");
    expect(resolved).toEqual([]);
  });

  it("still calls a peer that is not on the chain", async () => {
    const { tools, resolved } = await setup();
    const out = await tools({ namespaceId: "acme" }).call(
      { id: "reviewer", prompt: "hi" },
      { depth: 2, chain: ["writer", "coordinator"] },
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
    const denied = await tools(scope).call({ id: "reviewer", prompt: "hi" });
    expect(errorOf(denied)).toContain("refused by the host");
    const allowed = await tools(scope).call({ id: "writer", prompt: "hi" });
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
    }).call({ id: "writer", prompt: "hi" });
    const writerInstance = composeAgentInstanceId({
      namespaceId: "acme",
      registeredAgentId: "writer",
      ownerId: "alice",
    });
    expect(out.ok && out.route.data).toMatchObject({
      scope: { namespaceId: "acme", resourceId: writerInstance, ownerId: "alice" },
    });
    expect(scopes).toEqual([{ namespaceId: "acme", resourceId: writerInstance, ownerId: "alice" }]);
    expect((await instanceRegistry!.get(writerInstance))?.ownerId).toBe("alice");
  });

  it("never derives an owner from a resource id that merely looks like an instance id", async () => {
    const { tools, scopes, instanceRegistry } = await setup({ instances: true });
    // A raw resource a client chose to shape like bob's instance id.
    const out = await tools({
      namespaceId: "acme",
      resourceId: "acme::coordinator::bob",
    }).call({ id: "writer", prompt: "hi" });
    expect(out.ok).toBe(true);
    expect(scopes).toEqual([{ namespaceId: "acme", resourceId: "acme::coordinator::bob" }]);
    expect(await instanceRegistry!.list()).toEqual([]);
  });
});
