import { describe, it, expect } from "bun:test";
import { type AgenticDagRecipe, DagValidationError } from "../types.ts";
import { validateDag } from "../validate.ts";

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
