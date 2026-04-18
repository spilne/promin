// ---------------------------------------------------------------------------
// Triangle count — per-vertex count of triangles the vertex participates in.
//
// Treats edges as undirected and deduplicates parallel edges. A triangle is
// an unordered set of three mutually adjacent vertices {u, v, w}. Each
// triangle contributes +1 to each of its three vertices.
// ---------------------------------------------------------------------------

import { DataFrame } from "../../dataframe/dataframe.ts";
import type { GraphFrame, Vertex, Edge, VertexId } from "../graph-frame.ts";

export async function triangleCount<V extends Vertex, E extends Edge>(
  graph: GraphFrame<V, E>,
): Promise<DataFrame<V & { triangles: number }>> {
  const vertices = (await graph.vertices.collect()) as V[];
  const edges = (await graph.edges.collect()) as E[];

  // Build undirected adjacency as sorted unique neighbor lists.
  // Using Set eliminates duplicate/parallel edges; Array ordering is not
  // semantically important but keeps intersection cheap.
  const adj = new Map<VertexId, Set<VertexId>>();
  for (const v of vertices) adj.set(v.id, new Set());
  for (const e of edges) {
    if (e.src === e.dst) continue; // self-loop cannot form a triangle
    const a = adj.get(e.src);
    const b = adj.get(e.dst);
    if (a) a.add(e.dst);
    if (b) b.add(e.src);
  }

  const triangles = new Map<VertexId, number>();
  for (const v of vertices) triangles.set(v.id, 0);

  // For each edge {u, v} with u < v, count common neighbors w where v < w.
  // This enforces u < v < w so every triangle is found exactly once.
  // We iterate over each vertex's neighbors and, for each neighbor pair,
  // check membership.
  for (const v of vertices) {
    const nbrs = adj.get(v.id)!;
    // Only consider neighbors with id > v.id (canonical ordering)
    const greater: VertexId[] = [];
    for (const n of nbrs) if (cmp(n, v.id) > 0) greater.push(n);
    for (let i = 0; i < greater.length; i++) {
      const u = greater[i]!;
      const uAdj = adj.get(u)!;
      for (let j = i + 1; j < greater.length; j++) {
        const w = greater[j]!;
        if (uAdj.has(w)) {
          triangles.set(v.id, (triangles.get(v.id) ?? 0) + 1);
          triangles.set(u, (triangles.get(u) ?? 0) + 1);
          triangles.set(w, (triangles.get(w) ?? 0) + 1);
        }
      }
    }
  }

  const rows = vertices.map((v) => ({ ...v, triangles: triangles.get(v.id) ?? 0 }));
  return DataFrame.fromArray(rows) as DataFrame<V & { triangles: number }>;
}

function cmp(a: VertexId, b: VertexId): number {
  if (typeof a === "number" && typeof b === "number") return a - b;
  return String(a).localeCompare(String(b));
}
