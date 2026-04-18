// ---------------------------------------------------------------------------
// Jaccard neighbour similarity.
//
// Treats edges as undirected. For two vertices a and b, the Jaccard index is
//   |adj[a] ∩ adj[b]| / |adj[a] ∪ adj[b]|
// where adj[v] is v's set of undirected neighbours (self-excluded). Vertices
// with no neighbours are reported as similarity 0 against everything else.
// ---------------------------------------------------------------------------

import { DataFrame } from "../../dataframe/dataframe.ts";
import type { GraphFrame, Vertex, Edge, VertexId } from "../graph-frame.ts";

export interface JaccardOptions {
  /** Restrict output to pairs involving this vertex. Default: all pairs. */
  vertex?: VertexId;
  /** Return only pairs with similarity ≥ this value. Default: 0. */
  threshold?: number;
  /** Return at most this many rows, highest similarity first. */
  top?: number;
}

export interface JaccardRow {
  a: VertexId;
  b: VertexId;
  similarity: number;
  intersection: number;
  union: number;
}

export async function jaccardSimilarity<V extends Vertex, E extends Edge>(
  graph: GraphFrame<V, E>,
  options: JaccardOptions = {},
): Promise<DataFrame<JaccardRow>> {
  const vertices = (await graph.vertices.collect()) as V[];
  const edges = (await graph.edges.collect()) as E[];

  // Undirected adjacency as Sets, self-excluded.
  const adj = new Map<VertexId, Set<VertexId>>();
  for (const v of vertices) adj.set(v.id, new Set());
  for (const e of edges) {
    if (e.src === e.dst) continue;
    adj.get(e.src)?.add(e.dst);
    adj.get(e.dst)?.add(e.src);
  }

  const results: JaccardRow[] = [];

  const emitPair = (a: VertexId, b: VertexId) => {
    const A = adj.get(a)!;
    const B = adj.get(b)!;
    if (A.size === 0 && B.size === 0) return;
    let intersection = 0;
    // Iterate the smaller set for intersection counting.
    const [small, large] = A.size <= B.size ? [A, B] : [B, A];
    for (const x of small) if (large.has(x)) intersection++;
    const union = A.size + B.size - intersection;
    if (union === 0) return;
    const similarity = intersection / union;
    if (similarity < (options.threshold ?? 0)) return;
    results.push({ a, b, similarity, intersection, union });
  };

  if (options.vertex !== undefined) {
    if (!adj.has(options.vertex)) {
      return DataFrame.fromArray([]);
    }
    for (const v of vertices) {
      if (v.id === options.vertex) continue;
      emitPair(options.vertex, v.id);
    }
  } else {
    // All pairs: enumerate each unordered pair exactly once.
    for (let i = 0; i < vertices.length; i++) {
      for (let j = i + 1; j < vertices.length; j++) {
        emitPair(vertices[i]!.id, vertices[j]!.id);
      }
    }
  }

  // Sort by descending similarity, break ties deterministically by ids so
  // the output is reproducible across runs.
  results.sort((x, y) => {
    if (y.similarity !== x.similarity) return y.similarity - x.similarity;
    const ax = String(x.a);
    const ay = String(y.a);
    if (ax !== ay) return ax < ay ? -1 : 1;
    const bx = String(x.b);
    const by = String(y.b);
    return bx < by ? -1 : bx > by ? 1 : 0;
  });

  const limited = options.top !== undefined ? results.slice(0, options.top) : results;
  return DataFrame.fromArray(limited);
}
