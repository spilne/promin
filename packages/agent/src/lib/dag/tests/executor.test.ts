// ---------------------------------------------------------------------------
// DagExecutor — pinned cases for the v0 walker.
//   1. Linear A → B → C: outputs thread through; final = terminal output
//   2. Diamond A → {B,C} → D: parallel-eligible nodes both run; D sees both
//   3. Conditional edge: edge.condition gates whether downstream fires
//   4. onError: "skip" lets a successor with no other dependency continue
//   5. onError: "abort" halts the run
//   6. Validation: cycle / unknown ref / unreachable surfaces DagValidationError
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { Agent, AgentInvokeOpts, AgentRunOutput } from "../../agent/types.ts";
import { executeDag, pickPath, type AgentResolver } from "../executor.ts";
import { type AgenticDagRecipe, DagValidationError } from "../types.ts";
import { validateDag } from "../validate.ts";

// ---------------------------------------------------------------------------
// stub Agent that echoes its task back, optionally with a transform
// ---------------------------------------------------------------------------

function stubAgent(transform: (input: string) => string = (s) => s): Agent {
  return {
    invoke: async (input) => textOnly(transform((input as { task: string }).task)),
    stream: () => textOnly("not used"),
    thread: async () => {
      throw new Error("stub: thread not implemented");
    },
    listThreads: async () => [],
    compactThread: async () => {
      throw new Error("stub: compactThread not implemented");
    },
    distillThread: async () => {
      throw new Error("stub: distillThread not implemented");
    },
    distillResource: async () => {
      throw new Error("stub: distillResource not implemented");
    },
  } as unknown as Agent;
}

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

function failingAgent(message: string): Agent {
  return {
    ...stubAgent(),
    invoke: async () => {
      throw new Error(message);
    },
  } as unknown as Agent;
}

function makeResolver(byId: Record<string, Agent>): AgentResolver {
  return async (agentId) => {
    const a = byId[agentId];
    if (!a) throw new Error(`unknown agent: ${agentId}`);
    return a;
  };
}

describe("executeDag — linear A → B → C", () => {
  it("threads outputs along edges; final = terminal output", async () => {
    const dag: AgenticDagRecipe = {
      id: "linear",
      version: "v1",
      nodes: [
        {
          id: "n1",
          agentId: "uppercase",
          inputs: { task: { kind: "initial", path: "task" } },
        },
        {
          id: "n2",
          agentId: "exclaim",
          inputs: { task: { kind: "node", nodeId: "n1", path: "" } },
        },
        {
          id: "n3",
          agentId: "wrap",
          inputs: { task: { kind: "node", nodeId: "n2", path: "" } },
        },
      ],
      edges: [
        { from: "n1", to: "n2" },
        { from: "n2", to: "n3" },
      ],
      entry: ["n1"],
      terminals: ["n3"],
    };
    const resolver = makeResolver({
      uppercase: stubAgent((s) => s.toUpperCase()),
      exclaim: stubAgent((s) => `${s}!`),
      wrap: stubAgent((s) => `<<${s}>>`),
    });
    const result = await executeDag({
      dag,
      initialInput: { task: "hello" },
      resolver,
    });
    expect(result.ok).toBe(true);
    expect(result.outputs.n3).toBe("<<HELLO!>>");
    expect(result.state.outputs.n1).toBe("HELLO");
    expect(result.state.outputs.n2).toBe("HELLO!");
  });
});

describe("executeDag — diamond A → {B,C} → D", () => {
  it("D sees both B and C outputs", async () => {
    const dag: AgenticDagRecipe = {
      id: "diamond",
      version: "v1",
      nodes: [
        { id: "a", agentId: "id", inputs: { task: { kind: "initial", path: "task" } } },
        { id: "b", agentId: "upper", inputs: { task: { kind: "node", nodeId: "a", path: "" } } },
        { id: "c", agentId: "exclaim", inputs: { task: { kind: "node", nodeId: "a", path: "" } } },
        {
          id: "d",
          agentId: "join",
          inputs: {
            left: { kind: "node", nodeId: "b", path: "" },
            right: { kind: "node", nodeId: "c", path: "" },
          },
        },
      ],
      edges: [
        { from: "a", to: "b" },
        { from: "a", to: "c" },
        { from: "b", to: "d" },
        { from: "c", to: "d" },
      ],
      entry: ["a"],
      terminals: ["d"],
    };
    const resolver = makeResolver({
      id: stubAgent(),
      upper: stubAgent((s) => s.toUpperCase()),
      exclaim: stubAgent((s) => `${s}!`),
      join: stubAgent((s) => {
        const parsed = JSON.parse(s) as { left: string; right: string };
        return `${parsed.left} | ${parsed.right}`;
      }),
    });
    const result = await executeDag({ dag, initialInput: { task: "go" }, resolver });
    expect(result.ok).toBe(true);
    expect(result.outputs.d).toBe("GO | go!");
  });
});

describe("executeDag — conditional edges", () => {
  it("downstream node skipped when edge condition is not met", async () => {
    const dag: AgenticDagRecipe = {
      id: "cond",
      version: "v1",
      nodes: [
        { id: "router", agentId: "router", inputs: { task: { kind: "initial", path: "task" } } },
        { id: "yes", agentId: "yes", inputs: { task: { kind: "literal", value: "yes-branch" } } },
        { id: "no", agentId: "no", inputs: { task: { kind: "literal", value: "no-branch" } } },
      ],
      edges: [
        { from: "router", to: "yes", condition: { kind: "equals", path: "", value: "yes" } },
        { from: "router", to: "no", condition: { kind: "equals", path: "", value: "no" } },
      ],
      entry: ["router"],
      terminals: ["yes", "no"],
    };
    const events: string[] = [];
    const result = await executeDag({
      dag,
      initialInput: { task: "yes" },
      resolver: makeResolver({
        router: stubAgent((s) => s),
        yes: stubAgent(),
        no: stubAgent(),
      }),
      onEvent: (e) => events.push(`${e.kind}:${e.nodeId}`),
    });
    expect(result.ok).toBe(true);
    expect(result.outputs.yes).toBeDefined();
    expect(result.outputs.no).toBeUndefined();
    expect(events).toContain("node-skipped:no");
  });
});

describe("executeDag — onError handling", () => {
  it("onError: skip lets a successor with no other dep continue", async () => {
    // a (fails) ─→ b (skipped because a failed)
    // c ─→ d (should still run since not depending on a/b)
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
        { id: "d", agentId: "id", inputs: { task: { kind: "node", nodeId: "c", path: "" } } },
      ],
      edges: [
        { from: "a", to: "b" },
        { from: "c", to: "d" },
      ],
      entry: ["a", "c"],
      terminals: ["b", "d"],
    };
    const result = await executeDag({
      dag,
      initialInput: { task: "x" },
      resolver: makeResolver({
        fails: failingAgent("nope"),
        id: stubAgent(),
      }),
    });
    expect(result.ok).toBe(true);
    expect(result.state.errors.a).toBe("nope");
    expect(result.outputs.b).toBeUndefined();
    expect(result.outputs.d).toBe("x");
  });

  it("onError: abort halts the run; no further nodes execute", async () => {
    const dag: AgenticDagRecipe = {
      id: "abort",
      version: "v1",
      nodes: [
        { id: "a", agentId: "fails", inputs: { task: { kind: "initial", path: "task" } } },
        { id: "b", agentId: "id", inputs: { task: { kind: "node", nodeId: "a", path: "" } } },
        { id: "c", agentId: "id", inputs: { task: { kind: "initial", path: "task" } } },
      ],
      edges: [{ from: "a", to: "b" }],
      entry: ["a", "c"],
      terminals: ["b", "c"],
    };
    const result = await executeDag({
      dag,
      initialInput: { task: "x" },
      resolver: makeResolver({ fails: failingAgent("boom"), id: stubAgent() }),
    });
    expect(result.ok).toBe(false);
    expect(result.state.errors.a).toBe("boom");
    // Topological order may have surfaced c before the abort, but the
    // important thing is that b never ran.
    expect(result.outputs.b).toBeUndefined();
  });
});

describe("validateDag", () => {
  it("rejects cycles", () => {
    const dag: AgenticDagRecipe = {
      id: "cyc",
      version: "v1",
      nodes: [
        { id: "a", agentId: "x", inputs: {} },
        { id: "b", agentId: "x", inputs: {} },
      ],
      edges: [
        { from: "a", to: "b" },
        { from: "b", to: "a" },
      ],
      entry: ["a"],
      terminals: ["b"],
    };
    expect(() => validateDag(dag)).toThrow(DagValidationError);
  });

  it("rejects edges to unknown nodes", () => {
    const dag: AgenticDagRecipe = {
      id: "bad",
      version: "v1",
      nodes: [{ id: "a", agentId: "x", inputs: {} }],
      edges: [{ from: "a", to: "ghost" }],
      entry: ["a"],
      terminals: ["a"],
    };
    expect(() => validateDag(dag)).toThrow(/edge.to not in nodes/);
  });

  it("rejects entry node pulling from another node's output", () => {
    const dag: AgenticDagRecipe = {
      id: "bad-entry",
      version: "v1",
      nodes: [
        {
          id: "a",
          agentId: "x",
          inputs: { task: { kind: "node", nodeId: "b", path: "" } },
        },
        { id: "b", agentId: "x", inputs: {} },
      ],
      edges: [{ from: "b", to: "a" }],
      entry: ["a"],
      terminals: ["a"],
    };
    expect(() => validateDag(dag)).toThrow(/entry node a input task pulls from another node/);
  });

  it("rejects unreachable nodes", () => {
    const dag: AgenticDagRecipe = {
      id: "unreach",
      version: "v1",
      nodes: [
        { id: "a", agentId: "x", inputs: {} },
        { id: "b", agentId: "x", inputs: {} },
      ],
      edges: [],
      entry: ["a"],
      terminals: ["a"],
    };
    expect(() => validateDag(dag)).toThrow(/not reachable from entry: b/);
  });
});

describe("pickPath", () => {
  it("empty path returns the value", () => {
    expect(pickPath("hi", "")).toBe("hi");
    expect(pickPath({ a: 1 }, "")).toEqual({ a: 1 });
  });
  it("walks dotted paths through objects + arrays", () => {
    expect(pickPath({ a: { b: [1, 2, 3] } }, "a.b.1")).toBe(2);
    expect(pickPath({ a: { b: 5 } }, "a.b")).toBe(5);
    expect(pickPath({ a: 1 }, "a.b.c")).toBeUndefined();
  });
});
