import { describe, it, expect } from "bun:test";
import { DataFrame } from "../dataframe/dataframe.ts";
import { GraphFrame } from "./graph-frame.ts";

// ---------------------------------------------------------------------------
// Shared fixture — a simplified "channels + shared-audience" graph.
// Five channels; weights represent shared subscriber count.
// ---------------------------------------------------------------------------

function channelGraph() {
  // Two tight clusters joined by a light bridge:
  //   {music-a, music-b, music-c}   {games-a, games-b}
  return GraphFrame.from({
    vertices: DataFrame.fromArray([
      { id: "music-a", topic: "music" },
      { id: "music-b", topic: "music" },
      { id: "music-c", topic: "music" },
      { id: "games-a", topic: "games" },
      { id: "games-b", topic: "games" },
    ]),
    edges: DataFrame.fromArray([
      { src: "music-a", dst: "music-b", shared: 5000 },
      { src: "music-a", dst: "music-c", shared: 4000 },
      { src: "music-b", dst: "music-c", shared: 4500 },
      { src: "games-a", dst: "games-b", shared: 3000 },
      { src: "music-c", dst: "games-a", shared: 50 }, // light cross-topic bridge
    ]),
  });
}

// ---------------------------------------------------------------------------
// Personalized PageRank
// ---------------------------------------------------------------------------

describe("pageRank — personalizedFrom", () => {
  it("seed vertex receives the highest rank", async () => {
    const g = channelGraph();
    const rows = await (await g.pageRank({ personalizedFrom: "music-a", maxIter: 100 })).collect();
    const byId = new Map(rows.map((r: any) => [r.id, r.pagerank]));
    const seedRank = byId.get("music-a")!;
    for (const [id, rank] of byId) {
      if (id === "music-a") continue;
      expect(seedRank).toBeGreaterThan(rank);
    }
  });

  it("seeding concentrates probability mass near the seed vs uniform PR", async () => {
    const g = channelGraph();
    const classic = await (await g.pageRank()).collect();
    const personalized = await (
      await g.pageRank({ personalizedFrom: "music-a", maxIter: 100 })
    ).collect();
    const classicMap = new Map(classic.map((r: any) => [r.id, r.pagerank]));
    const pprMap = new Map(personalized.map((r: any) => [r.id, r.pagerank]));
    // music-a's rank should grow under personalization; it's the seed.
    expect(pprMap.get("music-a")!).toBeGreaterThan(classicMap.get("music-a")!);
  });

  it("ranks sum to 1 (personalized)", async () => {
    const g = channelGraph();
    const rows = await (await g.pageRank({ personalizedFrom: "music-a" })).collect();
    const total = rows.reduce((s: number, r: any) => s + r.pagerank, 0);
    expect(total).toBeCloseTo(1, 6);
  });

  it("multiple seeds bias toward both neighbourhoods", async () => {
    const g = channelGraph();
    const singleSeed = await (await g.pageRank({ personalizedFrom: "music-a" })).collect();
    const multiSeed = await (
      await g.pageRank({ personalizedFrom: ["music-a", "games-a"] })
    ).collect();
    const single = new Map(singleSeed.map((r: any) => [r.id, r.pagerank]));
    const multi = new Map(multiSeed.map((r: any) => [r.id, r.pagerank]));
    // games-b should gain rank when games-a joins the seed set.
    expect(multi.get("games-b")!).toBeGreaterThan(single.get("games-b")!);
  });

  it("unknown seed falls back to uniform teleport", async () => {
    const g = channelGraph();
    const unknown = await (
      await g.pageRank({ personalizedFrom: "does-not-exist", maxIter: 100 })
    ).collect();
    const classic = await (await g.pageRank({ maxIter: 100 })).collect();
    const us = new Map(unknown.map((r: any) => [r.id, r.pagerank]));
    const cs = new Map(classic.map((r: any) => [r.id, r.pagerank]));
    for (const [id, rank] of us) expect(rank).toBeCloseTo(cs.get(id)!, 10);
  });

  it("classic pageRank (no seed) still produces normalized ranks", async () => {
    const g = channelGraph();
    const rows = await (await g.pageRank()).collect();
    const total = rows.reduce((s: number, r: any) => s + r.pagerank, 0);
    expect(total).toBeCloseTo(1, 6);
  });
});

// ---------------------------------------------------------------------------
// Jaccard similarity
// ---------------------------------------------------------------------------

describe("jaccardSimilarity", () => {
  const square = GraphFrame.from({
    // A square: 1-2, 2-3, 3-4, 4-1. Two non-adjacent corners share 2 neighbours;
    // adjacent corners share 0 (no triangles).
    vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]),
    edges: DataFrame.fromArray([
      { src: 1, dst: 2 },
      { src: 2, dst: 3 },
      { src: 3, dst: 4 },
      { src: 4, dst: 1 },
    ]),
  });

  it("non-adjacent corners of a square have the highest similarity", async () => {
    const rows = await (await square.jaccardSimilarity()).collect();
    // Diagonal pairs (1,3) and (2,4) share {2,4} and {1,3} respectively,
    // union of size 2 → similarity 1. Adjacent pairs share 0 neighbours.
    const byPair = new Map(
      rows.map((r: any) => [[r.a, r.b].sort().join("-"), r.similarity as number]),
    );
    expect(byPair.get("1-3")).toBeCloseTo(1, 10);
    expect(byPair.get("2-4")).toBeCloseTo(1, 10);
    expect(byPair.get("1-2") ?? 0).toBeCloseTo(0, 10);
  });

  it("threshold filters out low-similarity pairs", async () => {
    const rows = await (await square.jaccardSimilarity({ threshold: 0.5 })).collect();
    for (const r of rows as any[]) expect(r.similarity).toBeGreaterThanOrEqual(0.5);
  });

  it("top returns the highest-scoring pairs only", async () => {
    const rows = await (await square.jaccardSimilarity({ top: 1 })).collect();
    expect(rows).toHaveLength(1);
  });

  it("vertex option returns only pairs involving that vertex", async () => {
    const rows = await (await square.jaccardSimilarity({ vertex: 1 })).collect();
    for (const r of rows as any[]) expect(r.a === 1 || r.b === 1).toBe(true);
  });

  it("isolated vertex returns empty", async () => {
    const g = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "x" }, { id: "y" }]),
      edges: DataFrame.fromArray<{ src: string; dst: string }>([]),
    });
    const rows = await (await g.jaccardSimilarity()).collect();
    expect(rows).toEqual([]);
  });

  it("unknown vertex argument returns empty", async () => {
    const rows = await (await square.jaccardSimilarity({ vertex: 999 })).collect();
    expect(rows).toEqual([]);
  });

  it("ignores self-loops when computing neighbour sets", async () => {
    const withLoop = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "a" }, { id: "b" }, { id: "c" }]),
      edges: DataFrame.fromArray([
        { src: "a", dst: "a" }, // self-loop
        { src: "a", dst: "b" },
        { src: "a", dst: "c" },
        { src: "b", dst: "c" },
      ]),
    });
    const withoutLoop = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: "a" }, { id: "b" }, { id: "c" }]),
      edges: DataFrame.fromArray([
        { src: "a", dst: "b" },
        { src: "a", dst: "c" },
        { src: "b", dst: "c" },
      ]),
    });
    const withRows = await (await withLoop.jaccardSimilarity()).collect();
    const withoutRows = await (await withoutLoop.jaccardSimilarity()).collect();
    // The self-loop should not change any pairwise similarity.
    const keyOf = (r: any) => [String(r.a), String(r.b)].sort().join("|");
    const withMap = new Map(withRows.map((r: any) => [keyOf(r), r.similarity as number]));
    const withoutMap = new Map(withoutRows.map((r: any) => [keyOf(r), r.similarity as number]));
    for (const [k, sim] of withMap) expect(withoutMap.get(k)).toBeCloseTo(sim, 10);
  });

  it("output is deterministic (same order across runs)", async () => {
    const one = (await (await square.jaccardSimilarity()).collect()).map((r: any) =>
      [r.a, r.b].join("-"),
    );
    const two = (await (await square.jaccardSimilarity()).collect()).map((r: any) =>
      [r.a, r.b].join("-"),
    );
    expect(one).toEqual(two);
  });
});

// ---------------------------------------------------------------------------
// Multi-graph composition
// ---------------------------------------------------------------------------

describe("GraphFrame.union", () => {
  it("merges vertices (dedup by id) and concats edges", async () => {
    const a = GraphFrame.from({
      vertices: DataFrame.fromArray([
        { id: "x", src_graph: "a" },
        { id: "y", src_graph: "a" },
      ]),
      edges: DataFrame.fromArray([{ src: "x", dst: "y" }]),
    });
    const b = GraphFrame.from({
      vertices: DataFrame.fromArray([
        { id: "y", src_graph: "b" }, // overlaps with a
        { id: "z", src_graph: "b" },
      ]),
      edges: DataFrame.fromArray([{ src: "y", dst: "z" }]),
    });
    const u = await a.union(b);
    expect(await u.vertexCount()).toBe(3);
    expect(await u.edgeCount()).toBe(2);

    // a wins on property conflict for shared id "y"
    const vertices = await u.vertices.collect();
    const yRow = vertices.find((v: any) => v.id === "y") as any;
    expect(yRow.src_graph).toBe("a");
  });

  it("is associative (A ∪ B ∪ C same regardless of order)", async () => {
    const a = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }]),
      edges: DataFrame.fromArray([{ src: 1, dst: 2 }]),
    });
    const b = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 3 }]),
      edges: DataFrame.fromArray<{ src: number; dst: number }>([]),
    });
    const c = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 4 }]),
      edges: DataFrame.fromArray([{ src: 3, dst: 4 }]),
    });
    const left = await (await a.union(b)).union(c);
    const right = await a.union(await b.union(c));
    expect(await left.vertexCount()).toBe(await right.vertexCount());
    expect(await left.edgeCount()).toBe(await right.edgeCount());
  });
});

describe("GraphFrame.intersection", () => {
  it("keeps only vertices present in both, and edges with both endpoints kept", async () => {
    const a = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }]),
      edges: DataFrame.fromArray([
        { src: 1, dst: 2 },
        { src: 2, dst: 3 },
      ]),
    });
    const b = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 2 }, { id: 3 }, { id: 4 }]),
      edges: DataFrame.fromArray<{ src: number; dst: number }>([]),
    });
    const inter = await a.intersection(b);
    const vs = await inter.vertices.collect();
    expect(vs.map((v: any) => v.id).sort()).toEqual([2, 3]);
    // Edge (1,2) is dropped (vertex 1 gone); (2,3) survives.
    const es = await inter.edges.collect();
    expect(es).toHaveLength(1);
    expect(es[0]).toEqual({ src: 2, dst: 3 });
  });
});

describe("GraphFrame.difference", () => {
  it("keeps vertices in this but not other", async () => {
    const a = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }]),
      edges: DataFrame.fromArray([
        { src: 1, dst: 2 },
        { src: 2, dst: 3 },
      ]),
    });
    const b = GraphFrame.from({
      vertices: DataFrame.fromArray([{ id: 2 }]),
      edges: DataFrame.fromArray<{ src: number; dst: number }>([]),
    });
    const diff = await a.difference(b);
    const vs = await diff.vertices.collect();
    expect(vs.map((v: any) => v.id).sort()).toEqual([1, 3]);
    // Both edges touch vertex 2 which is gone — no edges survive.
    expect(await diff.edgeCount()).toBe(0);
  });
});

describe("GraphFrame.bridge", () => {
  it("unions two graphs and appends cross-graph linking edges", async () => {
    const yt = GraphFrame.from({
      vertices: DataFrame.fromArray([
        { id: "yt:acme", platform: "youtube" },
        { id: "yt:beta", platform: "youtube" },
      ]),
      edges: DataFrame.fromArray([{ src: "yt:acme", dst: "yt:beta" }]),
    });
    const ig = GraphFrame.from({
      vertices: DataFrame.fromArray([
        { id: "ig:acme", platform: "instagram" },
        { id: "ig:gamma", platform: "instagram" },
      ]),
      edges: DataFrame.fromArray([{ src: "ig:acme", dst: "ig:gamma" }]),
    });
    const bridges = DataFrame.fromArray([{ src: "yt:acme", dst: "ig:acme", kind: "same-creator" }]);
    const combined = await yt.bridge(ig, bridges);
    expect(await combined.vertexCount()).toBe(4);
    expect(await combined.edgeCount()).toBe(3); // 1 + 1 + 1 bridge
    // Cross-platform connected component via the bridge edge
    const comps = await (await combined.connectedComponents()).collect();
    const uniq = new Set(comps.map((r: any) => r.component));
    expect(uniq.size).toBe(1); // all four vertices now connected
  });
});
