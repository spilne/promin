import { describe, it, expect } from "bun:test";
import { type AgenticDagRecipe, DagValidationError } from "../types.ts";
import { validateDag } from "../validate.ts";

describe("validateDag", () => {
  it("rejects cycles", () => {
    const dag: AgenticDagRecipe = {
      id: "cyc",
      version: "v1",
      nodes: [
        { id: "a", kind: "agent", agentId: "x", inputs: {} },
        { id: "b", kind: "agent", agentId: "x", inputs: {} },
      ],
      edges: [
        { from: "a", to: "b" },
        { from: "b", to: "a" },
      ],
      entry: ["a"],
      terminals: ["b"],
    };
    expect(() => validateDag({ dag })).toThrow(DagValidationError);
  });

  it("rejects edges to unknown nodes", () => {
    const dag: AgenticDagRecipe = {
      id: "bad",
      version: "v1",
      nodes: [{ id: "a", kind: "agent", agentId: "x", inputs: {} }],
      edges: [{ from: "a", to: "ghost" }],
      entry: ["a"],
      terminals: ["a"],
    };
    expect(() => validateDag({ dag })).toThrow(/edge.to not in nodes/);
  });

  it("rejects entry node pulling from another node's output", () => {
    const dag: AgenticDagRecipe = {
      id: "bad-entry",
      version: "v1",
      nodes: [
        {
          id: "a",
          kind: "agent",
          agentId: "x",
          inputs: { task: { kind: "node", nodeId: "b", path: "" } },
        },
        { id: "b", kind: "agent", agentId: "x", inputs: {} },
      ],
      edges: [{ from: "b", to: "a" }],
      entry: ["a"],
      terminals: ["a"],
    };
    expect(() => validateDag({ dag })).toThrow(/entry node a input task pulls from another node/);
  });

  it("rejects unreachable nodes", () => {
    const dag: AgenticDagRecipe = {
      id: "unreach",
      version: "v1",
      nodes: [
        { id: "a", kind: "agent", agentId: "x", inputs: {} },
        { id: "b", kind: "agent", agentId: "x", inputs: {} },
      ],
      edges: [],
      entry: ["a"],
      terminals: ["a"],
    };
    expect(() => validateDag({ dag })).toThrow(/not reachable from entry: b/);
  });

  it("rejects a transform from that is not an input", () => {
    const dag: AgenticDagRecipe = {
      id: "bad-transform-from",
      version: "v1",
      nodes: [
        {
          id: "t",
          kind: "transform",
          inputs: { x: { kind: "literal", value: [] } },
          from: "missing",
          ops: [{ op: "count" }],
        },
      ],
      edges: [],
      entry: ["t"],
      terminals: ["t"],
    };
    expect(() => validateDag({ dag })).toThrow(DagValidationError);
  });

  it("rejects collect after the first operation", () => {
    const dag: AgenticDagRecipe = {
      id: "bad-transform-collect-order",
      version: "v1",
      nodes: [
        {
          id: "t",
          kind: "transform",
          inputs: { x: { kind: "literal", value: [] } },
          from: "x",
          ops: [{ op: "compact" }, { op: "collect", fields: ["x"] }],
        },
      ],
      edges: [],
      entry: ["t"],
      terminals: ["t"],
    };
    expect(() => validateDag({ dag })).toThrow(DagValidationError);
  });

  it("rejects collect when from is set", () => {
    const dag: AgenticDagRecipe = {
      id: "bad-transform-collect-from",
      version: "v1",
      nodes: [
        {
          id: "t",
          kind: "transform",
          inputs: { x: { kind: "literal", value: [] } },
          from: "x",
          ops: [{ op: "collect", fields: ["x"] }],
        },
      ],
      edges: [],
      entry: ["t"],
      terminals: ["t"],
    };
    expect(() => validateDag({ dag })).toThrow(DagValidationError);
  });

  it("checks forEach upstream references and reserved input fields", () => {
    const bad: AgenticDagRecipe = {
      id: "bad-foreach",
      version: "v1",
      nodes: [
        { id: "a", kind: "agent", agentId: "x", inputs: {} },
        {
          id: "b",
          kind: "agent",
          agentId: "x",
          inputs: {
            item: { kind: "literal", value: "collision" },
            index: { kind: "literal", value: 0 },
          },
          forEach: {
            items: { kind: "node", nodeId: "c", path: "items" },
            as: "item",
            maxItems: 4,
          },
        },
        { id: "c", kind: "agent", agentId: "x", inputs: {} },
      ],
      edges: [{ from: "a", to: "b" }],
      entry: ["a", "c"],
      terminals: ["b", "c"],
    };

    expect(() => validateDag({ dag: bad })).toThrow(DagValidationError);
    try {
      validateDag({ dag: bad });
    } catch (error) {
      expect(error).toBeInstanceOf(DagValidationError);
      const issues = (error as DagValidationError).issues;
      expect(issues.some((issue) => issue.includes("forEach.items"))).toBe(true);
      expect(issues.some((issue) => issue.includes("input item"))).toBe(true);
      expect(issues.some((issue) => issue.includes("input index"))).toBe(true);
    }
  });
});
