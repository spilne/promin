// ---------------------------------------------------------------------------
// Shortest paths — Dijkstra from a single source.
//
// Uses a small array-based binary min-heap (no external dep). Edge weights
// default to 1 per edge; pass `weightColumn` to use a numeric column.
//
// Output: one row per vertex with distance (null if unreachable) and
// predecessor (null for the source or unreachable nodes).
// ---------------------------------------------------------------------------

import { DataFrame } from "../../dataframe/dataframe.ts";
import type { GraphFrame, Vertex, Edge, VertexId } from "../graph-frame.ts";

export interface ShortestPathsParams<E extends Edge> {
  from: VertexId;
  weightColumn?: keyof E & string;
}

export async function shortestPaths<V extends Vertex, E extends Edge>(
  graph: GraphFrame<V, E>,
  params: ShortestPathsParams<E>,
): Promise<DataFrame<{ id: VertexId; distance: number | null; predecessor: VertexId | null }>> {
  const { from, weightColumn } = params;
  const vertices = (await graph.vertices.collect()) as V[];
  const edges = (await graph.edges.collect()) as E[];

  const adj = new Map<VertexId, { dst: VertexId; w: number }[]>();
  for (const e of edges) {
    const w = weightColumn === undefined ? 1 : Number(e[weightColumn] ?? 1);
    if (!Number.isFinite(w) || w < 0) {
      throw new Error(`shortestPaths: negative or non-finite edge weight (${w}) not supported`);
    }
    const list = adj.get(e.src);
    if (list) list.push({ dst: e.dst, w });
    else adj.set(e.src, [{ dst: e.dst, w }]);
  }

  const distance = new Map<VertexId, number>();
  const predecessor = new Map<VertexId, VertexId>();
  distance.set(from, 0);

  const heap = new MinHeap<VertexId>();
  heap.push(from, 0);

  while (heap.size > 0) {
    const top = heap.pop()!;
    const u = top.value;
    const d = top.priority;
    if (d > (distance.get(u) ?? Infinity)) continue; // stale
    const neighbors = adj.get(u);
    if (!neighbors) continue;
    for (const { dst, w } of neighbors) {
      const candidate = d + w;
      if (candidate < (distance.get(dst) ?? Infinity)) {
        distance.set(dst, candidate);
        predecessor.set(dst, u);
        heap.push(dst, candidate);
      }
    }
  }

  const rows = vertices.map((v) => ({
    id: v.id,
    distance: distance.has(v.id) ? distance.get(v.id)! : null,
    predecessor: predecessor.has(v.id) ? predecessor.get(v.id)! : null,
  }));
  return DataFrame.fromArray(rows);
}

// ---------------------------------------------------------------------------
// Tiny binary min-heap — generic over value type, keyed on numeric priority.
// ---------------------------------------------------------------------------

class MinHeap<T> {
  private readonly values: T[] = [];
  private readonly prios: number[] = [];

  get size(): number {
    return this.values.length;
  }

  push(value: T, priority: number): void {
    this.values.push(value);
    this.prios.push(priority);
    this.siftUp(this.values.length - 1);
  }

  pop(): { value: T; priority: number } | undefined {
    const n = this.values.length;
    if (n === 0) return undefined;
    const value = this.values[0]!;
    const priority = this.prios[0]!;
    const lastValue = this.values.pop()!;
    const lastPrio = this.prios.pop()!;
    if (n > 1) {
      this.values[0] = lastValue;
      this.prios[0] = lastPrio;
      this.siftDown(0);
    }
    return { value, priority };
  }

  private siftUp(i: number): void {
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this.prios[parent]! <= this.prios[i]!) break;
      this.swap(i, parent);
      i = parent;
    }
  }

  private siftDown(i: number): void {
    const n = this.values.length;
    while (true) {
      const left = 2 * i + 1;
      const right = 2 * i + 2;
      let smallest = i;
      if (left < n && this.prios[left]! < this.prios[smallest]!) smallest = left;
      if (right < n && this.prios[right]! < this.prios[smallest]!) smallest = right;
      if (smallest === i) break;
      this.swap(i, smallest);
      i = smallest;
    }
  }

  private swap(i: number, j: number): void {
    [this.values[i], this.values[j]] = [this.values[j]!, this.values[i]!];
    [this.prios[i], this.prios[j]] = [this.prios[j]!, this.prios[i]!];
  }
}
