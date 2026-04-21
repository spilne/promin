import { describe, it, expect } from "bun:test";
import { DataFrame } from "../../dataframe/dataframe.ts";
import { GraphFrame } from "../graph-frame.ts";

// ---------------------------------------------------------------------------
// Small graphs with obvious structure
// ---------------------------------------------------------------------------

describe("communityDetection — small graphs", () => {
  it("empty graph returns no rows", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray<{ id: number }>([]),
      edges: DataFrame.fromArray<{ src: number; dst: number }>([]),
    });
    const rows = await (await g.communityDetection()).collect();
    expect(rows).toEqual([]);
  });

  it("isolated vertices each get their own community", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }]),
      edges: DataFrame.fromArray<{ src: number; dst: number }>([]),
    });
    const rows = await (await g.communityDetection()).collect();
    const comms = new Set(rows.map((r: any) => r.community));
    expect(comms.size).toBe(3);
  });

  it("fully-connected triangle ⇒ single community", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "a" }, { id: "b" }, { id: "c" }]),
      edges: DataFrame.fromArray([
        { src: "a", dst: "b" },
        { src: "b", dst: "c" },
        { src: "a", dst: "c" },
      ]),
    });
    const rows = await (await g.communityDetection()).collect();
    const comms = new Set(rows.map((r: any) => r.community));
    expect(comms.size).toBe(1);
  });

  it("two disconnected triangles ⇒ two communities", async () => {
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
        { src: "x", dst: "z" },
      ]),
    });
    const rows = await (await g.communityDetection()).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r.community]));
    // Same community within each triangle
    expect(byId.get("a")).toBe(byId.get("b"));
    expect(byId.get("b")).toBe(byId.get("c"));
    expect(byId.get("x")).toBe(byId.get("y"));
    expect(byId.get("y")).toBe(byId.get("z"));
    // Different community across
    expect(byId.get("a")).not.toBe(byId.get("x"));
    const uniq = new Set(rows.map((r: any) => r.community));
    expect(uniq.size).toBe(2);
  });

  it("two triangles joined by a single bridge edge ⇒ two communities", async () => {
    // a-b-c triangle, x-y-z triangle, single bridge c-x
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
        { src: "x", dst: "z" },
        { src: "c", dst: "x" }, // bridge
      ]),
    });
    const rows = await (await g.communityDetection()).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r.community]));
    // The triangles are strong communities; the bridge is too weak to merge.
    expect(byId.get("a")).toBe(byId.get("b"));
    expect(byId.get("a")).toBe(byId.get("c"));
    expect(byId.get("x")).toBe(byId.get("y"));
    expect(byId.get("x")).toBe(byId.get("z"));
    expect(byId.get("a")).not.toBe(byId.get("x"));
  });

  it("respects weighted edges — heavy intra, light inter", async () => {
    // Two pairs connected weakly by a light edge between them.
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]),
      edges: DataFrame.fromArray([
        { src: 1, dst: 2, w: 10 },
        { src: 3, dst: 4, w: 10 },
        { src: 2, dst: 3, w: 1 }, // light bridge
      ]),
    });
    const rows = await (await g.communityDetection({ weightColumn: "w" })).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r.community]));
    expect(byId.get(1)).toBe(byId.get(2));
    expect(byId.get(3)).toBe(byId.get(4));
    expect(byId.get(1)).not.toBe(byId.get(3));
  });

  it("higher resolution splits communities more aggressively", async () => {
    // A cycle of 8 nodes; at γ=1 likely 2 communities, at γ=3 likely more.
    const vertices = Array.from({ length: 8 }, (_, i) => ({ id: i }));
    const edges = Array.from({ length: 8 }, (_, i) => ({ src: i, dst: (i + 1) % 8 }));
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray(vertices),
      edges: DataFrame.fromArray(edges),
    });
    const lowRes = await (await g.communityDetection({ resolution: 1 })).collect();
    const highRes = await (await g.communityDetection({ resolution: 3 })).collect();
    const lowCount = new Set(lowRes.map((r: any) => r.community)).size;
    const highCount = new Set(highRes.map((r: any) => r.community)).size;
    expect(highCount).toBeGreaterThanOrEqual(lowCount);
  });

  it("parallel/reverse edges are coalesced (same answer as a single edge)", async () => {
    const base = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }]),
      edges: DataFrame.fromArray([
        { src: 1, dst: 2 },
        { src: 2, dst: 3 },
        { src: 1, dst: 3 },
      ]),
    });
    const duped = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }]),
      edges: DataFrame.fromArray([
        { src: 1, dst: 2 },
        { src: 2, dst: 1 }, // reverse
        { src: 2, dst: 3 },
        { src: 1, dst: 3 },
      ]),
    });
    const baseRows = await (await base.communityDetection()).collect();
    const dupeRows = await (await duped.communityDetection()).collect();
    // Both should settle on a single community (triangle).
    expect(new Set(baseRows.map((r: any) => r.community)).size).toBe(1);
    expect(new Set(dupeRows.map((r: any) => r.community)).size).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Zachary's karate club — the canonical reference test for Louvain.
// Expected: ≥ 2 communities, modularity ≥ 0.35 (full Louvain typically ~0.44).
// ---------------------------------------------------------------------------

// Edge list for Zachary's karate club (1-indexed as in the original paper).
const KARATE_EDGES: [number, number][] = [
  [1, 2],
  [1, 3],
  [1, 4],
  [1, 5],
  [1, 6],
  [1, 7],
  [1, 8],
  [1, 9],
  [1, 11],
  [1, 12],
  [1, 13],
  [1, 14],
  [1, 18],
  [1, 20],
  [1, 22],
  [1, 32],
  [2, 3],
  [2, 4],
  [2, 8],
  [2, 14],
  [2, 18],
  [2, 20],
  [2, 22],
  [2, 31],
  [3, 4],
  [3, 8],
  [3, 9],
  [3, 10],
  [3, 14],
  [3, 28],
  [3, 29],
  [3, 33],
  [4, 8],
  [4, 13],
  [4, 14],
  [5, 7],
  [5, 11],
  [6, 7],
  [6, 11],
  [6, 17],
  [7, 17],
  [9, 31],
  [9, 33],
  [9, 34],
  [10, 34],
  [14, 34],
  [15, 33],
  [15, 34],
  [16, 33],
  [16, 34],
  [19, 33],
  [19, 34],
  [20, 34],
  [21, 33],
  [21, 34],
  [23, 33],
  [23, 34],
  [24, 26],
  [24, 28],
  [24, 30],
  [24, 33],
  [24, 34],
  [25, 26],
  [25, 28],
  [25, 32],
  [26, 32],
  [27, 30],
  [27, 34],
  [28, 34],
  [29, 32],
  [29, 34],
  [30, 33],
  [30, 34],
  [31, 33],
  [31, 34],
  [32, 33],
  [32, 34],
  [33, 34],
];

function karateGraph() {
  const vertices = Array.from({ length: 34 }, (_, i) => ({ id: i + 1 }));
  const edges = KARATE_EDGES.map(([src, dst]) => ({ src, dst }));
  return GraphFrame.from({
    vertices: DataFrame.fromArray(vertices),
    edges: DataFrame.fromArray(edges),
  });
}

/** Compute modularity Q of a partition on an undirected unweighted graph. */
function modularity(edges: [number, number][], community: Map<number, number>): number {
  const degree = new Map<number, number>();
  for (const [u, v] of edges) {
    degree.set(u, (degree.get(u) ?? 0) + 1);
    degree.set(v, (degree.get(v) ?? 0) + 1);
  }
  const m = edges.length;
  const twoM = 2 * m;

  // Σ_ij [A_ij - (k_i k_j)/(2m)] δ(c_i, c_j) — iterate only over actual edges
  // plus the expectation term computed per community pair via degree sums.
  let q = 0;
  // intraEdges[c] = 2 × (edges within community c) — count each undirected edge
  // twice to match Σ_ij.
  const intra = new Map<number, number>();
  const degByComm = new Map<number, number>();
  for (const [u, v] of edges) {
    const cu = community.get(u)!;
    const cv = community.get(v)!;
    if (cu === cv) intra.set(cu, (intra.get(cu) ?? 0) + 2);
  }
  for (const [v, k] of degree) {
    const c = community.get(v)!;
    degByComm.set(c, (degByComm.get(c) ?? 0) + k);
  }
  for (const c of new Set(community.values())) {
    const inC = intra.get(c) ?? 0;
    const totC = degByComm.get(c) ?? 0;
    q += inC / twoM - (totC / twoM) ** 2;
  }
  return q;
}

describe("communityDetection — Zachary's karate club", () => {
  it("finds multiple communities with reasonable modularity", async () => {
    const g = karateGraph();
    const rows = await (await g.communityDetection()).collect();
    const community = new Map<number, number>(
      rows.map((r: any) => [r.id as number, r.community as number]),
    );
    const uniq = new Set(community.values());
    expect(uniq.size).toBeGreaterThanOrEqual(2);
    expect(uniq.size).toBeLessThanOrEqual(6);

    const q = modularity(KARATE_EDGES, community);
    // Reference Louvain implementations reach Q ≈ 0.44 on karate. Assert a
    // conservative floor so implementation variance doesn't break the test.
    expect(q).toBeGreaterThanOrEqual(0.35);
  }, 10_000);

  it("assignments are deterministic for fixed input order", async () => {
    const g1 = karateGraph();
    const g2 = karateGraph();
    const rows1 = await (await g1.communityDetection()).collect();
    const rows2 = await (await g2.communityDetection()).collect();
    const byId1 = new Map(rows1.map((r: any) => [r.id, r.community]));
    const byId2 = new Map(rows2.map((r: any) => [r.id, r.community]));
    for (const [id, c] of byId1) expect(byId2.get(id)).toBe(c);
  });
});
