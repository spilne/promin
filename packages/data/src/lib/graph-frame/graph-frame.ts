// ---------------------------------------------------------------------------
// GraphFrame — graph abstraction built on top of DataFrame.
//
// Vertices and edges are stored as DataFrames, so every DataFrame operation
// (select, filter, join, groupBy) applies directly. Graph algorithms are
// implemented as iterative DataFrame ops plus lightweight JS traversal.
// ---------------------------------------------------------------------------

import { DataFrame } from "../dataframe/dataframe.ts";
import type { Expr } from "../dataframe/expr.ts";

export type VertexId = string | number;

export interface Vertex {
  id: VertexId;
}

export interface Edge {
  src: VertexId;
  dst: VertexId;
}

export interface GraphFrameInit<V extends Vertex, E extends Edge> {
  vertices: DataFrame<V>;
  edges: DataFrame<E>;
}

/**
 * A graph with vertices and edges exposed as DataFrames.
 *
 * The `vertices` DataFrame must include an `id` column. The `edges` DataFrame
 * must include `src` and `dst` columns referring to vertex ids. Any number of
 * additional columns are allowed on both — they flow through DataFrame ops
 * unchanged.
 */
export class GraphFrame<V extends Vertex = Vertex, E extends Edge = Edge> {
  readonly vertices: DataFrame<V>;
  readonly edges: DataFrame<E>;

  constructor(init: GraphFrameInit<V, E>) {
    this.vertices = init.vertices;
    this.edges = init.edges;
  }

  static from<V extends Vertex, E extends Edge>(init: GraphFrameInit<V, E>): GraphFrame<V, E> {
    return new GraphFrame(init);
  }

  // ---------------------------------------------------------------------------
  // Structural queries
  // ---------------------------------------------------------------------------

  /** Return a new GraphFrame whose edges satisfy the predicate. */
  filterEdges(predicate: Expr | ((row: E) => boolean)): GraphFrame<V, E> {
    return new GraphFrame({ vertices: this.vertices, edges: this.edges.filter(predicate as any) });
  }

  /** Return a new GraphFrame whose vertices satisfy the predicate. */
  filterVertices(predicate: Expr | ((row: V) => boolean)): GraphFrame<V, E> {
    return new GraphFrame({ vertices: this.vertices.filter(predicate as any), edges: this.edges });
  }

  /**
   * Vertices adjacent to `id`, up to `depth` hops (default 1). Direction is
   * `both` by default — follows outgoing and incoming edges.
   */
  async neighbors(
    id: VertexId,
    options: { depth?: number; direction?: "out" | "in" | "both" } = {},
  ): Promise<DataFrame<V>> {
    const depth = options.depth ?? 1;
    const direction = options.direction ?? "both";
    const edges = (await this.edges.collect()) as E[];

    const reached = new Set<VertexId>([id]);
    const frontier = new Set<VertexId>([id]);
    for (let hop = 0; hop < depth; hop++) {
      const next = new Set<VertexId>();
      for (const e of edges) {
        if (
          (direction === "out" || direction === "both") &&
          frontier.has(e.src) &&
          !reached.has(e.dst)
        ) {
          next.add(e.dst);
          reached.add(e.dst);
        }
        if (
          (direction === "in" || direction === "both") &&
          frontier.has(e.dst) &&
          !reached.has(e.src)
        ) {
          next.add(e.src);
          reached.add(e.src);
        }
      }
      if (next.size === 0) break;
      frontier.clear();
      for (const v of next) frontier.add(v);
    }
    reached.delete(id);

    const vertices = await this.vertices.collect();
    const matched = (vertices as V[]).filter((v) => reached.has(v.id));
    return DataFrame.fromArray(matched);
  }

  /** Per-vertex in-degree, out-degree, and total degree as a DataFrame. */
  async degrees(): Promise<
    DataFrame<{ id: VertexId; inDegree: number; outDegree: number; degree: number }>
  > {
    const edges = (await this.edges.collect()) as E[];
    const vertices = (await this.vertices.collect()) as V[];

    const outMap = new Map<VertexId, number>();
    const inMap = new Map<VertexId, number>();
    for (const e of edges) {
      outMap.set(e.src, (outMap.get(e.src) ?? 0) + 1);
      inMap.set(e.dst, (inMap.get(e.dst) ?? 0) + 1);
    }

    const rows = vertices.map((v) => {
      const outDegree = outMap.get(v.id) ?? 0;
      const inDegree = inMap.get(v.id) ?? 0;
      return { id: v.id, inDegree, outDegree, degree: inDegree + outDegree };
    });
    return DataFrame.fromArray(rows);
  }

  /** Number of vertices. */
  async vertexCount(): Promise<number> {
    return (await this.vertices.collect()).length;
  }

  /** Number of edges. */
  async edgeCount(): Promise<number> {
    return (await this.edges.collect()).length;
  }

  // ---------------------------------------------------------------------------
  // Algorithms — delegate to algorithm modules
  // ---------------------------------------------------------------------------

  async pageRank(
    options: { maxIter?: number; dampingFactor?: number; tolerance?: number } = {},
  ): Promise<DataFrame<V & { pagerank: number }>> {
    const { pageRank } = await import("./algorithms/pagerank.ts");
    return pageRank(this, options);
  }

  async connectedComponents(): Promise<DataFrame<V & { component: number }>> {
    const { connectedComponents } = await import("./algorithms/connected-components.ts");
    return connectedComponents(this);
  }

  async shortestPaths(params: {
    from: VertexId;
    weightColumn?: keyof E & string;
  }): Promise<DataFrame<{ id: VertexId; distance: number | null; predecessor: VertexId | null }>> {
    const { shortestPaths } = await import("./algorithms/shortest-paths.ts");
    return shortestPaths(this, params);
  }

  async triangleCount(): Promise<DataFrame<V & { triangles: number }>> {
    const { triangleCount } = await import("./algorithms/triangle-count.ts");
    return triangleCount(this);
  }
}
