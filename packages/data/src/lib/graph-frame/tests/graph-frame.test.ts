import { describe, it, expect } from "bun:test";
import { DataFrame } from "../../dataframe/dataframe.ts";
import { col } from "../../dataframe/expr.ts";
import { GraphFrame } from "../graph-frame.ts";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/** Small directed graph:  a → b → c , a → c */
function tinyTriangle() {
  return GraphFrame.from({
    vertices: DataFrame.fromArray([
      { id: "a", name: "Alice" },
      { id: "b", name: "Bob" },
      { id: "c", name: "Carol" },
    ]),
    edges: DataFrame.fromArray([
      { src: "a", dst: "b", weight: 1.0, type: "friend" },
      { src: "b", dst: "c", weight: 0.5, type: "colleague" },
      { src: "a", dst: "c", weight: 0.8, type: "friend" },
    ]),
  });
}

// ---------------------------------------------------------------------------
// Construction and structural queries
// ---------------------------------------------------------------------------

describe("GraphFrame — construction and filtering", () => {
  it("constructs from vertices + edges DataFrames", async () => {
    const g = tinyTriangle();
    expect(await g.vertexCount()).toBe(3);
    expect(await g.edgeCount()).toBe(3);
  });

  it("filterEdges reduces the edge set but keeps all vertices", async () => {
    const g = tinyTriangle();
    const friends = g.filterEdges(col("type").eq("friend"));
    expect(await friends.edgeCount()).toBe(2);
    expect(await friends.vertexCount()).toBe(3);
  });

  it("filterVertices reduces the vertex set without mutating edges", async () => {
    const g = tinyTriangle();
    const active = g.filterVertices(col("name").neq("Bob"));
    expect(await active.vertexCount()).toBe(2);
    // Edges reference Bob but remain in the subgraph — callers chain
    // filterEdges if they want orphan-pruning semantics.
    expect(await active.edgeCount()).toBe(3);
  });
});

describe("GraphFrame.neighbors", () => {
  it("returns direct neighbors at depth 1 (both directions)", async () => {
    const g = tinyTriangle();
    const nbrs = await g.neighbors("b");
    const rows = await nbrs.collect();
    expect(rows.map((r: any) => r.id).sort()).toEqual(["a", "c"]);
  });

  it("respects direction=out", async () => {
    const g = tinyTriangle();
    const nbrs = await g.neighbors("b", { direction: "out" });
    const rows = await nbrs.collect();
    expect(rows.map((r: any) => r.id)).toEqual(["c"]);
  });

  it("respects direction=in", async () => {
    const g = tinyTriangle();
    const nbrs = await g.neighbors("c", { direction: "in" });
    const rows = await nbrs.collect();
    expect(rows.map((r: any) => r.id).sort()).toEqual(["a", "b"]);
  });

  it("depth > 1 expands the frontier", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]),
      edges: DataFrame.fromArray([
        { src: 1, dst: 2 },
        { src: 2, dst: 3 },
        { src: 3, dst: 4 },
      ]),
    });
    const twoHop = await g.neighbors(1, { depth: 2, direction: "out" });
    const rows = await twoHop.collect();
    expect(rows.map((r: any) => r.id).sort()).toEqual([2, 3]);
  });

  it("excludes the starting vertex itself", async () => {
    const g = tinyTriangle();
    const nbrs = await g.neighbors("a");
    const rows = await nbrs.collect();
    expect(rows.find((r: any) => r.id === "a")).toBeUndefined();
  });
});

describe("GraphFrame.degrees", () => {
  it("computes in/out/total degrees", async () => {
    const g = tinyTriangle();
    const rows = await (await g.degrees()).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r]));
    expect(byId.get("a")).toEqual({ id: "a", inDegree: 0, outDegree: 2, degree: 2 });
    expect(byId.get("b")).toEqual({ id: "b", inDegree: 1, outDegree: 1, degree: 2 });
    expect(byId.get("c")).toEqual({ id: "c", inDegree: 2, outDegree: 0, degree: 2 });
  });

  it("reports zero for isolated vertices", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "loner" }, { id: "x" }, { id: "y" }]),
      edges: DataFrame.fromArray([{ src: "x", dst: "y" }]),
    });
    const rows = await (await g.degrees()).collect();
    const loner = rows.find((r: any) => r.id === "loner")!;
    expect(loner).toEqual({ id: "loner", inDegree: 0, outDegree: 0, degree: 0 });
  });
});

// ---------------------------------------------------------------------------
// PageRank
// ---------------------------------------------------------------------------

describe("pageRank", () => {
  it("ranks sum to 1 on a non-trivial graph", async () => {
    const g = tinyTriangle();
    const rows = await (await g.pageRank()).collect();
    const total = rows.reduce((s: number, r: any) => s + r.pagerank, 0);
    expect(total).toBeCloseTo(1, 10);
  });

  it("a vertex with more incoming edges ranks higher", async () => {
    const g = tinyTriangle();
    const rows = await (await g.pageRank()).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r.pagerank]));
    // 'c' has in-degree 2, 'a' has in-degree 0
    expect(byId.get("c")!).toBeGreaterThan(byId.get("a")!);
  });

  it("is stable (uniform) on a graph with no edges", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }]),
      edges: DataFrame.fromArray<{ src: number; dst: number }>([]),
    });
    const rows = await (await g.pageRank()).collect();
    for (const r of rows as any[]) expect(r.pagerank).toBeCloseTo(1 / 3, 10);
  });

  it("handles dangling nodes without losing mass", async () => {
    // a → b (b has no outgoing edge)
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "a" }, { id: "b" }]),
      edges: DataFrame.fromArray([{ src: "a", dst: "b" }]),
    });
    const rows = await (await g.pageRank()).collect();
    const total = rows.reduce((s: number, r: any) => s + r.pagerank, 0);
    expect(total).toBeCloseTo(1, 10);
  });

  it("converges on a symmetric 2-cycle (both vertices equal)", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "x" }, { id: "y" }]),
      edges: DataFrame.fromArray([
        { src: "x", dst: "y" },
        { src: "y", dst: "x" },
      ]),
    });
    const rows = await (await g.pageRank({ maxIter: 100 })).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r.pagerank]));
    expect(byId.get("x")!).toBeCloseTo(byId.get("y")!, 10);
    expect(byId.get("x")! + byId.get("y")!).toBeCloseTo(1, 10);
  });

  it("damping factor affects ranks in the expected direction", async () => {
    const g = tinyTriangle();
    const low = await (await g.pageRank({ dampingFactor: 0.1 })).collect();
    const high = await (await g.pageRank({ dampingFactor: 0.99 })).collect();
    const lowC = (low.find((r: any) => r.id === "c") as any).pagerank;
    const highC = (high.find((r: any) => r.id === "c") as any).pagerank;
    // With higher damping, flow concentrates more at the sink (c)
    expect(highC).toBeGreaterThan(lowC);
  });

  it("empty graph yields empty result", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray<{ id: number }>([]),
      edges: DataFrame.fromArray<{ src: number; dst: number }>([]),
    });
    const rows = await (await g.pageRank()).collect();
    expect(rows).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Connected components
// ---------------------------------------------------------------------------

describe("connectedComponents", () => {
  it("assigns the same component to connected vertices", async () => {
    // Two triangles disconnected
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([
        { id: "a" },
        { id: "b" },
        { id: "c" },
        { id: "x" },
        { id: "y" },
        { id: "z" },
      ]),
      edges: DataFrame.fromArray([
        { src: "a", dst: "b" },
        { src: "b", dst: "c" },
        { src: "a", dst: "c" },
        { src: "x", dst: "y" },
        { src: "y", dst: "z" },
      ]),
    });
    const rows = await (await g.connectedComponents()).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r.component]));
    // Same component within each cluster
    expect(byId.get("a")).toBe(byId.get("b"));
    expect(byId.get("b")).toBe(byId.get("c"));
    expect(byId.get("x")).toBe(byId.get("y"));
    expect(byId.get("y")).toBe(byId.get("z"));
    // Different components across clusters
    expect(byId.get("a")).not.toBe(byId.get("x"));
  });

  it("isolated vertices each get their own component", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }]),
      edges: DataFrame.fromArray<{ src: number; dst: number }>([]),
    });
    const rows = await (await g.connectedComponents()).collect();
    const ids = new Set(rows.map((r: any) => r.component));
    expect(ids.size).toBe(3);
  });

  it("treats edges as undirected for component membership", async () => {
    // a → b → c forms one undirected component
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "a" }, { id: "b" }, { id: "c" }]),
      edges: DataFrame.fromArray([
        { src: "a", dst: "b" },
        { src: "b", dst: "c" },
      ]),
    });
    const rows = await (await g.connectedComponents()).collect();
    const components = new Set(rows.map((r: any) => r.component));
    expect(components.size).toBe(1);
  });

  it("component IDs are stable and start at 0", async () => {
    const g = tinyTriangle();
    const rows = await (await g.connectedComponents()).collect();
    const comps = rows.map((r: any) => r.component);
    expect(Math.min(...comps)).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Shortest paths
// ---------------------------------------------------------------------------

describe("shortestPaths", () => {
  it("unweighted: distance equals hop count", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]),
      edges: DataFrame.fromArray([
        { src: 1, dst: 2 },
        { src: 2, dst: 3 },
        { src: 3, dst: 4 },
      ]),
    });
    const rows = await (await g.shortestPaths({ from: 1 })).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r.distance]));
    expect(byId.get(1)).toBe(0);
    expect(byId.get(2)).toBe(1);
    expect(byId.get(3)).toBe(2);
    expect(byId.get(4)).toBe(3);
  });

  it("weighted: picks the lower-cost path even if it has more hops", async () => {
    //   a →(10)→ d
    //   a →(1)→ b →(1)→ c →(1)→ d
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "a" }, { id: "b" }, { id: "c" }, { id: "d" }]),
      edges: DataFrame.fromArray([
        { src: "a", dst: "d", weight: 10 },
        { src: "a", dst: "b", weight: 1 },
        { src: "b", dst: "c", weight: 1 },
        { src: "c", dst: "d", weight: 1 },
      ]),
    });
    const rows = await (await g.shortestPaths({ from: "a", weightColumn: "weight" })).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r.distance]));
    expect(byId.get("d")).toBe(3);
  });

  it("records predecessors for path reconstruction", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }]),
      edges: DataFrame.fromArray([
        { src: 1, dst: 2 },
        { src: 2, dst: 3 },
      ]),
    });
    const rows = await (await g.shortestPaths({ from: 1 })).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r]));
    expect((byId.get(2) as any).predecessor).toBe(1);
    expect((byId.get(3) as any).predecessor).toBe(2);
  });

  it("reports null distance for unreachable vertices", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }]),
      edges: DataFrame.fromArray([{ src: 1, dst: 2 }]),
    });
    const rows = await (await g.shortestPaths({ from: 1 })).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r]));
    expect((byId.get(3) as any).distance).toBeNull();
    expect((byId.get(3) as any).predecessor).toBeNull();
  });

  it("source has distance 0 and null predecessor", async () => {
    const g = tinyTriangle();
    const rows = await (await g.shortestPaths({ from: "a", weightColumn: "weight" })).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r]));
    expect((byId.get("a") as any).distance).toBe(0);
    expect((byId.get("a") as any).predecessor).toBeNull();
  });

  it("rejects negative edge weights", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }]),
      edges: DataFrame.fromArray([{ src: 1, dst: 2, weight: -1 }]),
    });
    await expect(g.shortestPaths({ from: 1, weightColumn: "weight" })).rejects.toThrow();
  });
});

// ---------------------------------------------------------------------------
// Triangle count
// ---------------------------------------------------------------------------

describe("triangleCount", () => {
  it("single triangle contributes 1 to each participating vertex", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "a" }, { id: "b" }, { id: "c" }]),
      edges: DataFrame.fromArray([
        { src: "a", dst: "b" },
        { src: "b", dst: "c" },
        { src: "a", dst: "c" },
      ]),
    });
    const rows = await (await g.triangleCount()).collect();
    for (const r of rows as any[]) expect(r.triangles).toBe(1);
  });

  it("two triangles sharing an edge counts correctly", async () => {
    // a-b, b-c, a-c forms one triangle (abc)
    // b-c, c-d, b-d forms another (bcd) — share edge bc
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "a" }, { id: "b" }, { id: "c" }, { id: "d" }]),
      edges: DataFrame.fromArray([
        { src: "a", dst: "b" },
        { src: "b", dst: "c" },
        { src: "a", dst: "c" },
        { src: "c", dst: "d" },
        { src: "b", dst: "d" },
      ]),
    });
    const rows = await (await g.triangleCount()).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r.triangles]));
    expect(byId.get("a")).toBe(1); // abc
    expect(byId.get("b")).toBe(2); // abc + bcd
    expect(byId.get("c")).toBe(2); // abc + bcd
    expect(byId.get("d")).toBe(1); // bcd
  });

  it("ignores self-loops", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "a" }, { id: "b" }, { id: "c" }]),
      edges: DataFrame.fromArray([
        { src: "a", dst: "a" },
        { src: "a", dst: "b" },
        { src: "b", dst: "c" },
        { src: "a", dst: "c" },
      ]),
    });
    const rows = await (await g.triangleCount()).collect();
    for (const r of rows as any[]) expect(r.triangles).toBe(1);
  });

  it("parallel edges don't double count", async () => {
    // Even if edge a-b appears twice, triangle count shouldn't change.
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "a" }, { id: "b" }, { id: "c" }]),
      edges: DataFrame.fromArray([
        { src: "a", dst: "b" },
        { src: "b", dst: "a" }, // reverse of the same undirected edge
        { src: "b", dst: "c" },
        { src: "a", dst: "c" },
      ]),
    });
    const rows = await (await g.triangleCount()).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r.triangles]));
    expect(byId.get("a")).toBe(1);
    expect(byId.get("b")).toBe(1);
    expect(byId.get("c")).toBe(1);
  });

  it("graph with no triangles returns all zeros", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]),
      edges: DataFrame.fromArray([
        { src: 1, dst: 2 },
        { src: 2, dst: 3 },
        { src: 3, dst: 4 },
      ]),
    });
    const rows = await (await g.triangleCount()).collect();
    for (const r of rows as any[]) expect(r.triangles).toBe(0);
  });

  it("complete graph K4 — every vertex in 3 triangles", async () => {
    // K4 has C(4,3) = 4 triangles, each vertex participates in 3 of them.
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "a" }, { id: "b" }, { id: "c" }, { id: "d" }]),
      edges: DataFrame.fromArray([
        { src: "a", dst: "b" },
        { src: "a", dst: "c" },
        { src: "a", dst: "d" },
        { src: "b", dst: "c" },
        { src: "b", dst: "d" },
        { src: "c", dst: "d" },
      ]),
    });
    const rows = await (await g.triangleCount()).collect();
    for (const r of rows as any[]) expect(r.triangles).toBe(3);
  });
});
