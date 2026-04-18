// ---------------------------------------------------------------------------
// Louvain community detection — modularity maximization.
//
// Two-phase algorithm iterated to fixed point:
//   Phase 1 (local moves): each vertex considers moving to a neighbor's
//   community; accepts the move with the largest positive modularity gain.
//   Phase 2 (aggregation): collapse each community into a super-vertex;
//   inter-community edge weights are summed, intra-community weights become
//   self-loops on the super-vertex.
// Repeat phases until a full pass produces no improvement.
//
// Edges are treated as undirected. Weighted graphs are supported via a
// `weightColumn`; default weight per edge is 1. Parallel and reverse edges
// are coalesced on ingest.
// ---------------------------------------------------------------------------

import { DataFrame } from "../../dataframe/dataframe.ts";
import type { GraphFrame, Vertex, Edge, VertexId } from "../graph-frame.ts";

export interface CommunityDetectionOptions<E extends Edge> {
  /** Column that holds the numeric edge weight. Default: every edge weighs 1. */
  weightColumn?: keyof E & string;
  /** Resolution γ. Higher values yield more, smaller communities. Default: 1. */
  resolution?: number;
  /** Minimum modularity improvement per full pass before stopping. Default: 1e-7. */
  tolerance?: number;
  /** Cap on the number of phase-1 passes per level. Default: 50. */
  maxLocalPasses?: number;
  /** Cap on the number of level iterations. Default: 20. */
  maxLevels?: number;
}

export async function communityDetection<V extends Vertex, E extends Edge>(
  graph: GraphFrame<V, E>,
  options: CommunityDetectionOptions<E> = {},
): Promise<DataFrame<V & { community: number }>> {
  const vertices = (await graph.vertices.collect()) as V[];
  const edges = (await graph.edges.collect()) as E[];
  const weightCol = options.weightColumn;
  const resolution = options.resolution ?? 1;
  const tolerance = options.tolerance ?? 1e-7;
  const maxLocalPasses = options.maxLocalPasses ?? 50;
  const maxLevels = options.maxLevels ?? 20;

  if (vertices.length === 0) {
    return DataFrame.fromArray([]) as DataFrame<V & { community: number }>;
  }

  // Build the initial weighted graph keyed by vertex id.
  const weighted = buildWeighted(
    vertices.map((v) => v.id),
    edges,
    weightCol,
  );

  // Track the cumulative community assignment for each original vertex.
  // Initially each vertex is in its own community named by its id.
  let currentCommunity = new Map<VertexId, VertexId>();
  for (const v of vertices) currentCommunity.set(v.id, v.id);

  let level = weighted;
  for (let levelIdx = 0; levelIdx < maxLevels; levelIdx++) {
    // Phase 1 — local moves
    const { moved, communityOf } = localMoves(level, resolution, tolerance, maxLocalPasses);
    if (!moved && levelIdx > 0) break; // no improvement this level

    // Update the mapping from original vertex → super-node's community.
    const next = new Map<VertexId, VertexId>();
    for (const [orig, super1] of currentCommunity) {
      const c = communityOf.get(super1);
      next.set(orig, c !== undefined ? c : super1);
    }
    currentCommunity = next;

    if (!moved) break; // level 0 with no moves → final answer

    // Phase 2 — aggregate communities into super-vertices
    level = aggregate(level, communityOf);
  }

  // Normalise community labels to contiguous 0..K-1 integers in the order
  // communities are first seen while walking vertices.
  const commId = new Map<VertexId, number>();
  let nextId = 0;
  const rows = vertices.map((v) => {
    const raw = currentCommunity.get(v.id)!;
    let id = commId.get(raw);
    if (id === undefined) {
      id = nextId++;
      commId.set(raw, id);
    }
    return { ...v, community: id };
  });

  return DataFrame.fromArray(rows) as DataFrame<V & { community: number }>;
}

// ---------------------------------------------------------------------------
// Weighted undirected graph representation
// ---------------------------------------------------------------------------

interface WeightedGraph {
  /** Unique vertex ids in insertion order. */
  nodes: VertexId[];
  /** adj[v] = Map<neighbour, summed weight>. Self-loops live under adj[v][v]. */
  adj: Map<VertexId, Map<VertexId, number>>;
  /** degree[v] = sum of weights of edges incident to v, self-loops counted twice. */
  degree: Map<VertexId, number>;
  /** Total weight (single-count for self-loops). Used as m. */
  m: number;
}

function buildWeighted<E extends Edge>(
  nodes: VertexId[],
  edges: E[],
  weightColumn: (keyof E & string) | undefined,
): WeightedGraph {
  const adj = new Map<VertexId, Map<VertexId, number>>();
  const degree = new Map<VertexId, number>();
  const nodeSet = new Set(nodes);

  for (const n of nodes) {
    adj.set(n, new Map());
    degree.set(n, 0);
  }

  let m = 0;
  for (const e of edges) {
    if (!nodeSet.has(e.src) || !nodeSet.has(e.dst)) continue;
    const w = weightColumn === undefined ? 1 : Number(e[weightColumn] ?? 1);
    if (!Number.isFinite(w) || w <= 0) continue;

    const a = adj.get(e.src)!;
    const b = adj.get(e.dst)!;

    if (e.src === e.dst) {
      a.set(e.src, (a.get(e.src) ?? 0) + w);
      degree.set(e.src, degree.get(e.src)! + 2 * w);
      m += w;
    } else {
      a.set(e.dst, (a.get(e.dst) ?? 0) + w);
      b.set(e.src, (b.get(e.src) ?? 0) + w);
      degree.set(e.src, degree.get(e.src)! + w);
      degree.set(e.dst, degree.get(e.dst)! + w);
      m += w;
    }
  }

  return { nodes, adj, degree, m };
}

// ---------------------------------------------------------------------------
// Phase 1 — local moves
// ---------------------------------------------------------------------------

interface Phase1Result {
  /** True if at least one vertex changed community during the run. */
  moved: boolean;
  /** Community assignment after phase 1 (keyed by the graph's vertex ids). */
  communityOf: Map<VertexId, VertexId>;
}

function localMoves(
  g: WeightedGraph,
  resolution: number,
  tolerance: number,
  maxPasses: number,
): Phase1Result {
  const community = new Map<VertexId, VertexId>();
  const totalDegree = new Map<VertexId, number>(); // Σtot per community
  for (const v of g.nodes) {
    community.set(v, v);
    totalDegree.set(v, g.degree.get(v)!);
  }

  const twoM = 2 * g.m;
  if (twoM === 0) return { moved: false, communityOf: community };

  let movedAnyPass = false;
  for (let pass = 0; pass < maxPasses; pass++) {
    let movedThisPass = false;
    for (const v of g.nodes) {
      const ki = g.degree.get(v)!;
      const currentComm = community.get(v)!;

      // Sum of edge weights from v to each neighbouring community (excluding
      // self-loop — it shifts with v and cancels out of ΔQ).
      const toComm = new Map<VertexId, number>();
      for (const [nbr, w] of g.adj.get(v)!) {
        if (nbr === v) continue;
        const c = community.get(nbr)!;
        toComm.set(c, (toComm.get(c) ?? 0) + w);
      }

      // Virtually remove v from its community so Σtot reflects the post-remove
      // state when evaluating any candidate community (including the original).
      totalDegree.set(currentComm, totalDegree.get(currentComm)! - ki);

      let bestComm = currentComm;
      let bestGain = 0;
      const scale = (resolution * ki) / twoM;
      for (const [c, kiToC] of toComm) {
        const gain = kiToC - scale * totalDegree.get(c)!;
        if (gain > bestGain + tolerance) {
          bestGain = gain;
          bestComm = c;
        }
      }
      // Also consider the original community explicitly — gain of staying is 0,
      // but the guard above already covers it since moving to a new comm must
      // strictly improve by more than `tolerance`.

      // Re-add v's degree into the chosen community's Σtot.
      totalDegree.set(bestComm, totalDegree.get(bestComm)! + ki);
      community.set(v, bestComm);

      if (bestComm !== currentComm) {
        movedThisPass = true;
        movedAnyPass = true;
      }
    }
    if (!movedThisPass) break;
  }

  return { moved: movedAnyPass, communityOf: community };
}

// ---------------------------------------------------------------------------
// Phase 2 — aggregation
// ---------------------------------------------------------------------------

function aggregate(g: WeightedGraph, communityOf: Map<VertexId, VertexId>): WeightedGraph {
  // Unique community ids become the nodes of the new graph.
  const newNodes: VertexId[] = [];
  const seen = new Set<VertexId>();
  for (const v of g.nodes) {
    const c = communityOf.get(v)!;
    if (!seen.has(c)) {
      seen.add(c);
      newNodes.push(c);
    }
  }

  // Sum weights between every ordered pair of (community, community) using the
  // adjacency. Each undirected edge u→v contributes to adj[u][v] and adj[v][u],
  // so we divide by two at the end except for self-loops (which the convention
  // already single-counts inside adj[u][u]).
  const pairWeight = new Map<VertexId, Map<VertexId, number>>();
  const add = (a: VertexId, b: VertexId, w: number) => {
    let row = pairWeight.get(a);
    if (!row) {
      row = new Map();
      pairWeight.set(a, row);
    }
    row.set(b, (row.get(b) ?? 0) + w);
  };

  for (const v of g.nodes) {
    const cv = communityOf.get(v)!;
    for (const [nbr, w] of g.adj.get(v)!) {
      const cn = communityOf.get(nbr)!;
      add(cv, cn, w);
    }
  }

  // pairWeight[cv][cn] now double-counts every non-self-loop edge; self-loops
  // (including the original self-loops stored under adj[v][v]) appear once
  // per incident vertex. Collapse to the canonical single-weight form.
  const adj = new Map<VertexId, Map<VertexId, number>>();
  const degree = new Map<VertexId, number>();
  for (const c of newNodes) {
    adj.set(c, new Map());
    degree.set(c, 0);
  }
  let m = 0;

  // Iterate each unordered pair only once.
  for (const [a, row] of pairWeight) {
    for (const [b, wSum] of row) {
      if (a === b) {
        // wSum counts each intra-community undirected edge twice (once per
        // endpoint) — divide to get the canonical self-loop weight.
        const w = wSum / 2;
        if (w <= 0) continue;
        adj.get(a)!.set(a, (adj.get(a)!.get(a) ?? 0) + w);
        degree.set(a, degree.get(a)! + 2 * w);
        m += w;
      } else {
        // Skip the mirror pair so we count each undirected edge once.
        if (cmpId(a, b) > 0) continue;
        const w = wSum; // equals the other direction; unordered edge weight is wSum/1? No:
        // pairWeight[a][b] = wSum = total adj weight from a→b = actual edge weight (since only adj[u→v] adds it).
        // pairWeight[b][a] = the same value (from adj[nbr]'s side). We pick one direction via the cmpId guard.
        if (w <= 0) continue;
        adj.get(a)!.set(b, (adj.get(a)!.get(b) ?? 0) + w);
        adj.get(b)!.set(a, (adj.get(b)!.get(a) ?? 0) + w);
        degree.set(a, degree.get(a)! + w);
        degree.set(b, degree.get(b)! + w);
        m += w;
      }
    }
  }

  return { nodes: newNodes, adj, degree, m };
}

function cmpId(a: VertexId, b: VertexId): number {
  if (typeof a === "number" && typeof b === "number") return a - b;
  return String(a).localeCompare(String(b));
}
