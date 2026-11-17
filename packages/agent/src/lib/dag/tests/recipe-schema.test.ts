import { describe, expect, it } from "bun:test";

import { recipeIssues } from "../recipe-schema.ts";

const ok = {
  nodes: [
    {
      id: "n1",
      kind: "agent",
      agentId: "a",
      inputs: { task: { kind: "initial", path: "task" } },
    },
  ],
  edges: [],
  entry: ["n1"],
  terminals: ["n1"],
};

describe("DAG recipe schema", () => {
  it("accepts a minimal agent recipe", () => {
    expect(recipeIssues(ok)).toEqual([]);
  });

  it("names the path of each mistake in a near-miss recipe", () => {
    const issues = recipeIssues({
      ...ok,
      nodes: [
        { id: "n1", kind: "Agent", agentId: "a", inputs: {} },
        { id: "n.2", kind: "agent", agentId: "a", inputs: {}, foreach: {} },
      ],
    });
    expect(issues.some((issue) => issue.startsWith("nodes[0].kind"))).toBe(true);
    expect(issues.some((issue) => issue.startsWith("nodes[1].id"))).toBe(true);
    expect(issues.some((issue) => issue.startsWith("nodes[1]") && issue.includes("foreach"))).toBe(
      true,
    );
  });

  it("rejects a node without kind", () => {
    expect(recipeIssues({ ...ok, nodes: [{ id: "n1", agentId: "a", inputs: {} }] })[0]).toMatch(
      /^nodes\[0\]/,
    );
  });

  it("rejects the removed outputPath", () => {
    expect(recipeIssues({ ...ok, nodes: [{ ...ok.nodes[0], outputPath: "text" }] }).length).toBe(1);
  });

  it("rejects a condition nested deeper than 8", () => {
    let condition: unknown = { kind: "exists", path: "x" };
    for (let i = 0; i < 8; i++) condition = { kind: "not", of: condition };
    const issues = recipeIssues({
      ...ok,
      nodes: [ok.nodes[0], { ...ok.nodes[0], id: "n2" }],
      edges: [{ from: "n1", to: "n2", condition }],
    });
    expect(issues.some((issue) => issue.startsWith("edges[0].condition"))).toBe(true);
  });

  it("rejects invalid transform sizes and join modes at the node", () => {
    const transform = {
      id: "t",
      kind: "transform",
      inputs: { x: { kind: "literal", value: [] } },
      from: "x",
    };
    for (const node of [
      { ...transform, ops: Array.from({ length: 17 }, () => ({ op: "count" })) },
      { ...transform, ops: [] },
      { ...ok.nodes[0], join: "some" },
    ]) {
      expect(
        recipeIssues({ ...ok, nodes: [node] }).some((issue) => issue.startsWith("nodes[0]")),
      ).toBe(true);
    }
  });

  it("refuses a recipe whose output schema is unusable", () => {
    for (const schema of [
      { type: "objct" },
      { type: "array" },
      "x",
      { type: "object", description: "x".repeat(17_000) },
    ]) {
      const issues = recipeIssues({
        ...ok,
        nodes: [{ ...ok.nodes[0], output: { schema } }],
      });
      expect(issues.some((issue) => issue.startsWith("nodes[0].output.schema"))).toBe(true);
    }
  });
});
