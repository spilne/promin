// ---------------------------------------------------------------------------
// graph-layout — Sugiyama-lite DAG layout shared by the step DAG and the
// agent run-trace graph.
// Pinned cases:
//   1. Single node sits at the padding origin
//   2. Linear chain ranks 0,1,2 — horizontal places ranks along x
//   3. Diamond — converging node lands one rank past its deepest parent
//   4. Orientation swap mirrors width/height
//   5. Dangling `dependsOn` ids are ignored (no phantom edge)
//   6. Cycles are tolerated — layout terminates
//   7. Within a rank, nodes are ordered by a stable id sort
//   8. Edge endpoints resolve to the placed nodes
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { layoutGraph, graphEdgePath, type GraphNode } from "../graph-layout.ts";

const OPTS = { nodeW: 100, nodeH: 40, colGap: 90, rowGap: 20, padding: 24 } as const;

function n(id: string, ...dependsOn: string[]): GraphNode {
  return { id, dependsOn };
}

describe("layoutGraph", () => {
  it("places a single node at the padding origin", () => {
    const r = layoutGraph([n("a")], { orientation: "horizontal", ...OPTS });
    expect(r.nodes).toHaveLength(1);
    expect(r.nodes[0]!.rank).toBe(0);
    expect(r.nodes[0]!.x).toBe(24);
    expect(r.nodes[0]!.y).toBe(24);
    expect(r.width).toBe(24 * 2 + 100);
    expect(r.height).toBe(24 * 2 + 40);
  });

  it("ranks a linear chain and steps it along x when horizontal", () => {
    const r = layoutGraph([n("a"), n("b", "a"), n("c", "b")], {
      orientation: "horizontal",
      ...OPTS,
    });
    const byId = new Map(r.nodes.map((p) => [p.node.id, p]));
    expect(byId.get("a")!.rank).toBe(0);
    expect(byId.get("b")!.rank).toBe(1);
    expect(byId.get("c")!.rank).toBe(2);
    // rankStep = nodeW + colGap = 190
    expect(byId.get("b")!.x - byId.get("a")!.x).toBe(190);
    expect(byId.get("c")!.x - byId.get("b")!.x).toBe(190);
    // single row → all share y
    expect(byId.get("a")!.y).toBe(byId.get("c")!.y);
  });

  it("places a converging node one rank past its deepest parent", () => {
    const r = layoutGraph([n("a"), n("b", "a"), n("c", "a"), n("d", "b", "c")], {
      orientation: "horizontal",
      ...OPTS,
    });
    const byId = new Map(r.nodes.map((p) => [p.node.id, p]));
    expect(byId.get("d")!.rank).toBe(2);
    expect(r.edges).toHaveLength(4);
  });

  it("flows ranks along x when horizontal and along y when vertical", () => {
    const nodes = [n("a"), n("b", "a"), n("c", "a")];
    const h = layoutGraph(nodes, { orientation: "horizontal", ...OPTS });
    const v = layoutGraph(nodes, { orientation: "vertical", ...OPTS });
    const hA = h.nodes.find((p) => p.node.id === "a")!;
    const hB = h.nodes.find((p) => p.node.id === "b")!;
    const vA = v.nodes.find((p) => p.node.id === "a")!;
    const vB = v.nodes.find((p) => p.node.id === "b")!;
    // Horizontal: deeper rank → larger x, same y band.
    expect(hB.x).toBeGreaterThan(hA.x);
    // Vertical: deeper rank → larger y.
    expect(vB.y).toBeGreaterThan(vA.y);
  });

  it("ignores dangling dependsOn ids", () => {
    const r = layoutGraph([n("a", "ghost"), n("b", "a")], {
      orientation: "horizontal",
      ...OPTS,
    });
    // "ghost" resolves to nothing — no edge into a, a stays rank 0.
    expect(r.edges).toHaveLength(1);
    expect(r.nodes.find((p) => p.node.id === "a")!.rank).toBe(0);
  });

  it("tolerates cycles without looping forever", () => {
    const r = layoutGraph([n("a", "b"), n("b", "a")], { orientation: "horizontal", ...OPTS });
    expect(r.nodes).toHaveLength(2);
    expect(r.edges).toHaveLength(2);
  });

  it("orders nodes within a rank by a stable id sort", () => {
    // Three rank-1 siblings inserted out of order.
    const r = layoutGraph([n("root"), n("c", "root"), n("a", "root"), n("b", "root")], {
      orientation: "horizontal",
      ...OPTS,
    });
    const rank1 = r.nodes.filter((p) => p.rank === 1).sort((x, y) => x.row - y.row);
    expect(rank1.map((p) => p.node.id)).toEqual(["a", "b", "c"]);
  });

  it("emits edges whose endpoints are the placed nodes", () => {
    const r = layoutGraph([n("a"), n("b", "a")], { orientation: "horizontal", ...OPTS });
    expect(r.edges).toHaveLength(1);
    expect(r.edges[0]!.from.node.id).toBe("a");
    expect(r.edges[0]!.to.node.id).toBe("b");
  });
});

describe("graphEdgePath", () => {
  it("draws a horizontal edge from the trailing edge of `from`", () => {
    const r = layoutGraph([n("a"), n("b", "a")], { orientation: "horizontal", ...OPTS });
    const path = graphEdgePath(r.edges[0]!, { orientation: "horizontal", nodeW: 100, nodeH: 40 });
    // Starts at from.x + nodeW, from.y + nodeH/2 = (124, 44).
    expect(path.startsWith("M 124 44 C")).toBe(true);
  });

  it("draws a vertical edge from the bottom of `from`", () => {
    const r = layoutGraph([n("a"), n("b", "a")], { orientation: "vertical", ...OPTS });
    const path = graphEdgePath(r.edges[0]!, { orientation: "vertical", nodeW: 100, nodeH: 40 });
    expect(path.startsWith("M ")).toBe(true);
    expect(path).toContain(" C ");
  });
});
