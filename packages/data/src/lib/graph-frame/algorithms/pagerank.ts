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

  // Initial uniform distribution
  const initial = 1 / n;
  let rank = new Map<VertexId, number>();
  for (const v of vertices) rank.set(v.id, initial);

  const teleport = (1 - damping) / n;

  for (let iter = 0; iter < maxIter; iter++) {
    const next = new Map<VertexId, number>();
    // Start each vertex at the teleport term
    for (const v of vertices) next.set(v.id, teleport);

    // Dangling mass — vertices with no outgoing edges distribute uniformly
    let danglingSum = 0;
    for (const v of vertices) {
      if ((outDegree.get(v.id) ?? 0) === 0) danglingSum += rank.get(v.id)!;
    }
    const danglingContribution = (damping * danglingSum) / n;
    if (danglingContribution > 0) {
      for (const v of vertices) next.set(v.id, next.get(v.id)! + danglingContribution);
    }

    // Distribute each non-dangling vertex's rank across its out-neighbors
    for (const [src, neighbors] of outEdges) {
      const share = (damping * rank.get(src)!) / neighbors.length;
      for (const dst of neighbors) {
        next.set(dst, (next.get(dst) ?? teleport) + share);
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
