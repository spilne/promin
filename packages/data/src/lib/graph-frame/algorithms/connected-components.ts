// ---------------------------------------------------------------------------
// Connected components — union-find (disjoint-set) over the edge list.
// Treats edges as undirected for the purpose of component membership.
// ---------------------------------------------------------------------------

import { DataFrame } from "../../dataframe/dataframe.ts";
import type { GraphFrame, Vertex, Edge, VertexId } from "../graph-frame.ts";

export async function connectedComponents<V extends Vertex, E extends Edge>(
  graph: GraphFrame<V, E>,
): Promise<DataFrame<V & { component: number }>> {
  const vertices = (await graph.vertices.collect()) as V[];
  const edges = (await graph.edges.collect()) as E[];

  const parent = new Map<VertexId, VertexId>();
  const rank = new Map<VertexId, number>();

  for (const v of vertices) {
    parent.set(v.id, v.id);
    rank.set(v.id, 0);
  }

  const find = (x: VertexId): VertexId => {
    // Iterative path compression
    let root = x;
    while (parent.get(root) !== root) root = parent.get(root)!;
    let cur = x;
    while (parent.get(cur) !== root) {
      const next = parent.get(cur)!;
      parent.set(cur, root);
      cur = next;
    }
    return root;
  };

  const union = (a: VertexId, b: VertexId) => {
    const ra = find(a);
    const rb = find(b);
    if (ra === rb) return;
    const rankA = rank.get(ra) ?? 0;
    const rankB = rank.get(rb) ?? 0;
    if (rankA < rankB) parent.set(ra, rb);
    else if (rankA > rankB) parent.set(rb, ra);
    else {
      parent.set(rb, ra);
      rank.set(ra, rankA + 1);
    }
  };

  for (const e of edges) {
    if (parent.has(e.src) && parent.has(e.dst)) union(e.src, e.dst);
  }

  // Assign stable integer component ids in order of first appearance
  const componentId = new Map<VertexId, number>();
  let nextId = 0;
  const rows = vertices.map((v) => {
    const root = find(v.id);
    let id = componentId.get(root);
    if (id === undefined) {
      id = nextId++;
      componentId.set(root, id);
    }
    return { ...v, component: id };
  });

  return DataFrame.fromArray(rows) as DataFrame<V & { component: number }>;
}
