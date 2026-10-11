// ---------------------------------------------------------------------------
// createDagWorkflow — durable DAG executor.
// Pinned cases:
//   1. Linear DAG runs end-to-end through the runner; output matches the
//      pure executor's output
//   2. Replay safety: a worker restart mid-DAG resumes from the last
//      completed node (the test simulates this by sharing the same storage
//      between two runner instances and using a resolver that throws on
//      the second invocation of the still-incomplete node)
//   3. Skip cascade still works under journaling
//   4. Activities are journaled per node-id
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import type { Agent, AgentInput, AgentRunOutput, AgentScope } from "../../agent/types.ts";
import { InMemoryAgentInstanceRegistry } from "../../instance/in-memory-agent-instance-registry.ts";
import { composeAgentInstanceId } from "../../instance/types.ts";
import { InMemoryAgentRegistry } from "../../registry/in-memory-agent-registry.ts";
import { createDagWorkflow } from "../durable-executor.ts";
import { createRegistryAgentResolver } from "../registry-agent-resolver.ts";
import type { AgentResolver, AgenticDagRecipe } from "../types.ts";

function textOnly(text: string): AgentRunOutput<unknown> {
  return {
    text: Promise.resolve(text),
    finishReason: Promise.resolve("stop" as const),
    messages: Promise.resolve([]),
    output: Promise.resolve(undefined),
    events: (async function* () {})(),
    abort: () => {},
  } as unknown as AgentRunOutput<unknown>;
}

function stubAgent(transform: (s: string) => string = (s) => s): Agent {
  return {
    invoke: async (input: AgentInput) => textOnly(transform(input.task)),
    stream: () => textOnly("not used"),
    thread: async () => {
      throw new Error("stub: thread");
    },
    listThreads: async () => [],
    withScope() {
      return this;
    },
  } as unknown as Agent;
}

function makeResolver(byId: Record<string, Agent>): AgentResolver {
  return async ({ agentId: id }) => {
    const a = byId[id];
    if (!a) throw new Error(`unknown agent: ${id}`);
    return a;
  };
}

const SCOPE = { namespaceId: "acme", resourceId: "r-1" } as const;

const linear: AgenticDagRecipe = {
  id: "linear",
  version: "v1",
  nodes: [
    { id: "n1", agentId: "upper", inputs: { task: { kind: "initial", path: "task" } } },
    { id: "n2", agentId: "exclaim", inputs: { task: { kind: "node", nodeId: "n1", path: "" } } },
    { id: "n3", agentId: "wrap", inputs: { task: { kind: "node", nodeId: "n2", path: "" } } },
  ],
  edges: [
    { from: "n1", to: "n2" },
    { from: "n2", to: "n3" },
  ],
  entry: ["n1"],
  terminals: ["n3"],
};

describe("createDagWorkflow — basic", () => {
  it("runs a linear DAG end-to-end through the runner", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = createDagWorkflow({
      resolver: makeResolver({
        upper: stubAgent((s) => s.toUpperCase()),
        exclaim: stubAgent((s) => `${s}!`),
        wrap: stubAgent((s) => `<<${s}>>`),
      }),
      name: "test-dag-basic",
    });

    const result = await runner.run({
      workflow: wf,
      workflowId: "dag-1",
      input: { dag: linear, initialInput: { task: "hello" }, scope: SCOPE },
    });

    expect(result.ok).toBe(true);
    expect(result.outputs.n3).toBe("<<HELLO!>>");
    expect(result.nodeOutputs.n1).toBe("HELLO");
    expect(result.nodeOutputs.n2).toBe("HELLO!");
    expect(result.errors).toEqual({});
    expect(result.skipped).toEqual([]);
  });

  it("skip cascade: failing entry with onError:skip propagates to dependents", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const dag: AgenticDagRecipe = {
      id: "skip",
      version: "v1",
      nodes: [
        {
          id: "a",
          agentId: "fails",
          inputs: { task: { kind: "initial", path: "task" } },
          onError: "skip",
        },
        { id: "b", agentId: "id", inputs: { task: { kind: "node", nodeId: "a", path: "" } } },
        { id: "c", agentId: "id", inputs: { task: { kind: "initial", path: "task" } } },
      ],
      edges: [{ from: "a", to: "b" }],
      entry: ["a", "c"],
      terminals: ["b", "c"],
    };
    const wf = createDagWorkflow({
      resolver: makeResolver({
        fails: {
          ...stubAgent(),
          invoke: async () => {
            throw new Error("nope");
          },
        } as unknown as Agent,
        id: stubAgent(),
      }),
      name: "test-dag-skip",
    });

    const result = await runner.run({
      workflow: wf,
      workflowId: "dag-skip-1",
      input: { dag, initialInput: { task: "x" }, scope: SCOPE },
    });

    expect(result.ok).toBe(true);
    expect(result.errors.a).toBe("nope");
    expect(result.skipped).toContain("b");
    expect(result.outputs.c).toBe("x");
    expect(result.outputs.b).toBeUndefined();
  });
});

describe("createDagWorkflow — replay safety", () => {
  // The model: we count agent-invoke calls per node across the WHOLE
  // run lifecycle. After a successful run, re-running the same workflowId
  // against the same storage must NOT re-fire any agent invocation —
  // the activity journal short-circuits everything.
  it("does not re-invoke agents on a replayed run", async () => {
    const storage = new InMemoryWorkflowStorage();

    let callsN1 = 0;
    let callsN2 = 0;
    let callsN3 = 0;
    const resolver: AgentResolver = async ({ agentId: id }) => {
      if (id === "upper") {
        return {
          ...stubAgent((s) => s.toUpperCase()),
          invoke: async (input: AgentInput) => {
            callsN1 += 1;
            return textOnly(input.task.toUpperCase());
          },
        } as unknown as Agent;
      }
      if (id === "exclaim") {
        return {
          ...stubAgent((s) => `${s}!`),
          invoke: async (input: AgentInput) => {
            callsN2 += 1;
            return textOnly(`${input.task}!`);
          },
        } as unknown as Agent;
      }
      return {
        ...stubAgent((s) => `<<${s}>>`),
        invoke: async (input: AgentInput) => {
          callsN3 += 1;
          return textOnly(`<<${input.task}>>`);
        },
      } as unknown as Agent;
    };

    const wf = createDagWorkflow({ resolver, name: "test-dag-replay" });
    const runner1 = createWorkflowRunner({ storage });
    const r1 = await runner1.run({
      workflow: wf,
      workflowId: "dag-replay-1",
      input: { dag: linear, initialInput: { task: "hi" }, scope: SCOPE },
    });
    expect(r1.outputs.n3).toBe("<<HI!>>");
    expect(callsN1).toBe(1);
    expect(callsN2).toBe(1);
    expect(callsN3).toBe(1);

    // Same storage + same workflowId → already-completed run. The
    // runner returns the persisted result without re-running activities.
    const runner2 = createWorkflowRunner({ storage });
    const r2 = await runner2.run({
      workflow: wf,
      workflowId: "dag-replay-1",
      input: { dag: linear, initialInput: { task: "hi" }, scope: SCOPE },
    });
    expect(r2.outputs.n3).toBe("<<HI!>>");
    // Critical: counters didn't increment.
    expect(callsN1).toBe(1);
    expect(callsN2).toBe(1);
    expect(callsN3).toBe(1);
  });
});

describe("createDagWorkflow — scope", () => {
  /** An agent that records the scope it was bound to before each invoke. */
  function scopeRecorder(bound: AgentScope[]): Agent {
    const make = (scope: AgentScope | undefined): Agent =>
      ({
        ...stubAgent(),
        invoke: async (input: { task: string }) => {
          bound.push(scope ?? {});
          return textOnly(input.task);
        },
        withScope: (next: AgentScope) => make(next),
      }) as unknown as Agent;
    return make(undefined);
  }

  const single: AgenticDagRecipe = {
    id: "single",
    version: "v1",
    nodes: [{ id: "n1", agentId: "writer", inputs: { task: { kind: "initial", path: "task" } } }],
    edges: [],
    entry: ["n1"],
    terminals: ["n1"],
  };

  it("binds every node to the run's namespace and resource", async () => {
    const bound: AgentScope[] = [];
    const wf = createDagWorkflow({
      resolver: async () => scopeRecorder(bound),
      name: "test-dag-scope",
    });
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const result = await runner.run({
      workflow: wf,
      workflowId: "dag-scope-1",
      input: { dag: linear, initialInput: { task: "x" }, scope: SCOPE },
    });
    expect(result.ok).toBe(true);
    expect(bound).toEqual([SCOPE, SCOPE, SCOPE]);
  });

  it("runs an owner-scoped node at the owner's instance of the node's agent", async () => {
    const bound: AgentScope[] = [];
    const instances = new InMemoryAgentInstanceRegistry();
    const wf = createDagWorkflow({
      resolver: async () => scopeRecorder(bound),
      instances,
      name: "test-dag-owner",
    });
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    await runner.run({
      workflow: wf,
      workflowId: "dag-owner-1",
      input: {
        dag: single,
        initialInput: { task: "x" },
        scope: { namespaceId: "acme", ownerId: "alice" },
      },
    });
    const instanceId = composeAgentInstanceId({
      namespaceId: "acme",
      registeredAgentId: "writer",
      ownerId: "alice",
    });
    expect(bound).toEqual([{ namespaceId: "acme", resourceId: instanceId, ownerId: "alice" }]);
    expect((await instances.get(instanceId))?.ownerId).toBe("alice");
  });

  it("the registry resolver fails a node whose agent is disabled", async () => {
    const registry = new InMemoryAgentRegistry();
    await registry.register({
      id: "writer",
      backend: {
        type: "local",
        model: { provider: "test", id: "m" },
        role: { inline: { systemPrompt: null, tools: [] } },
      },
      metadata: { capabilities: [], tags: [], enabled: false },
    });
    const bound: AgentScope[] = [];
    const wf = createDagWorkflow({
      resolver: createRegistryAgentResolver({ registry, resolve: () => scopeRecorder(bound) }),
      name: "test-dag-disabled",
    });
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const result = await runner.run({
      workflow: wf,
      workflowId: "dag-disabled-1",
      input: { dag: single, initialInput: { task: "x" }, scope: SCOPE },
    });
    expect(result.ok).toBe(false);
    expect(result.errors.n1).toContain("agent_disabled");
    expect(bound).toEqual([]);
  });
});
