// ---------------------------------------------------------------------------
// PageRank — iterative power method on a graph.
//
// Each vertex starts with rank 1/N. At each step, a vertex's rank is
// (1 - d) / N + d * Σ rank[u] / outDegree[u] over all edges u → v.
// ---------------------------------------------------------------------------

import { DataFrame } from "../../dataframe/dataframe.ts";
import type { GraphFrame, Vertex, Edge, VertexId } from "../graph-frame.ts";

export interface PageRankOptions {
  /** Maximum number of iterations. Default: 20. */
  maxIter?: number;
  /** Damping factor; 1 - d is the teleport probability. Default: 0.85. */
  dampingFactor?: number;
  /** Early-exit when the L1 delta drops below this. Default: 1e-6. */
  tolerance?: number;
  /**
   * Personalized PageRank seed — a single vertex id or a list of vertex ids
   * the random walker teleports to instead of the uniform distribution. Ranks
   * concentrate near the seed set, giving "importance relative to these nodes"
   * semantics. Use this for "similar to X" queries on co-subscription graphs.
   */
  personalizedFrom?: import("../graph-frame.ts").VertexId | import("../graph-frame.ts").VertexId[];
}

export async function pageRank<V extends Vertex, E extends Edge>(
  graph: GraphFrame<V, E>,
  options: PageRankOptions = {},
): Promise<DataFrame<V & { pagerank: number }>> {
  const maxIter = options.maxIter ?? 20;
  const damping = options.dampingFactor ?? 0.85;
  const tolerance = options.tolerance ?? 1e-6;

  const vertices = (await graph.vertices.collect()) as V[];
  const edges = (await graph.edges.collect()) as E[];
  const n = vertices.length;
  if (n === 0) return DataFrame.fromArray([]) as DataFrame<V & { pagerank: number }>;

  // Compute outgoing edges per source vertex
  const outEdges = new Map<VertexId, VertexId[]>();
  const outDegree = new Map<VertexId, number>();
  for (const e of edges) {
    const list = outEdges.get(e.src);
    if (list) list.push(e.dst);
    else outEdges.set(e.src, [e.dst]);
    outDegree.set(e.src, (outDegree.get(e.src) ?? 0) + 1);
  }

  // Teleport distribution — uniform (classic PageRank) or concentrated on the
  // seed set (personalized PageRank).
  const teleportDist = buildTeleportDistribution(vertices, options.personalizedFrom);

  // Initial distribution — start at the teleport distribution so personalized
  // runs converge faster; equivalent to uniform for classic PR.
  let rank = new Map<VertexId, number>();
  for (const v of vertices) rank.set(v.id, teleportDist.get(v.id) ?? 0);

  for (let iter = 0; iter < maxIter; iter++) {
    const next = new Map<VertexId, number>();
    // Start each vertex at its share of the teleport term
    for (const v of vertices) next.set(v.id, (1 - damping) * teleportDist.get(v.id)!);

    // Dangling mass — vertices with no outgoing edges redistribute via the
    // teleport distribution (preserves personalization for PPR).
    let danglingSum = 0;
    for (const v of vertices) {
      if ((outDegree.get(v.id) ?? 0) === 0) danglingSum += rank.get(v.id)!;
    }
    if (danglingSum > 0) {
      for (const v of vertices) {
        next.set(v.id, next.get(v.id)! + damping * danglingSum * teleportDist.get(v.id)!);
      }
    }

    // Distribute each non-dangling vertex's rank across its out-neighbors
    for (const [src, neighbors] of outEdges) {
      const share = (damping * rank.get(src)!) / neighbors.length;
      for (const dst of neighbors) {
        next.set(dst, (next.get(dst) ?? 0) + share);
      }
    }

    // Check convergence
    let delta = 0;
    for (const v of vertices) delta += Math.abs(next.get(v.id)! - rank.get(v.id)!);
    rank = next;
    if (delta < tolerance) break;
  }

  const rows = vertices.map((v) => ({ ...v, pagerank: rank.get(v.id)! }));
  return DataFrame.fromArray(rows) as DataFrame<V & { pagerank: number }>;
}

/**
 * Build a (vertex → probability) teleport distribution summing to 1.
 * Uniform when no seed is given; concentrated uniformly over the seed set
 * otherwise. Unknown seed ids are silently dropped; if all seeds are unknown
 * we fall back to uniform so callers don't hit a cryptic divide-by-zero.
 */
function buildTeleportDistribution<V extends Vertex>(
  vertices: V[],
  seed: VertexId | VertexId[] | undefined,
): Map<VertexId, number> {
  const n = vertices.length;
  const dist = new Map<VertexId, number>();
  if (seed === undefined) {
    const uniform = 1 / n;
    for (const v of vertices) dist.set(v.id, uniform);
    return dist;
  }
  const vertexIds = new Set(vertices.map((v) => v.id));
  const seedList = Array.isArray(seed) ? seed : [seed];
  const valid = seedList.filter((id) => vertexIds.has(id));
  if (valid.length === 0) {
    const uniform = 1 / n;
    for (const v of vertices) dist.set(v.id, uniform);
    return dist;
  }
  const share = 1 / valid.length;
  for (const v of vertices) dist.set(v.id, 0);
  for (const id of valid) dist.set(id, (dist.get(id) ?? 0) + share);
  return dist;
}
