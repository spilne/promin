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
    options: {
      maxIter?: number;
      dampingFactor?: number;
      tolerance?: number;
      personalizedFrom?: VertexId | VertexId[];
    } = {},
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

  async communityDetection(
    options: {
      weightColumn?: keyof E & string;
      resolution?: number;
      tolerance?: number;
      maxLocalPasses?: number;
      maxLevels?: number;
    } = {},
  ): Promise<DataFrame<V & { community: number }>> {
    const { communityDetection } = await import("./algorithms/community-detection.ts");
    return communityDetection(this, options);
  }

  /**
   * Jaccard neighbour similarity. Returns pairs of vertices with their
   * similarity scores. Use `vertex` to restrict to pairs involving a single
   * vertex (the common "top-K similar to X" query); use `top` to limit the
   * result size.
   */
  async jaccardSimilarity(
    options: { vertex?: VertexId; threshold?: number; top?: number } = {},
  ): Promise<
    DataFrame<{
      a: VertexId;
      b: VertexId;
      similarity: number;
      intersection: number;
      union: number;
    }>
  > {
    const { jaccardSimilarity } = await import("./algorithms/jaccard-similarity.ts");
    return jaccardSimilarity(this, options);
  }

  // ---------------------------------------------------------------------------
  // Multi-graph composition
  // ---------------------------------------------------------------------------

  /**
   * Combine this graph with another. Vertices are deduplicated by id — this
   * graph's attributes win on conflict. Edges from both sides are concatenated
   * (duplicates are NOT removed; chain `.filterEdges` if you need that).
   */
  async union<V2 extends Vertex, E2 extends Edge>(
    other: GraphFrame<V2, E2>,
  ): Promise<GraphFrame<V | V2, E | E2>> {
    const thisVertices = (await this.vertices.collect()) as V[];
    const otherVertices = (await other.vertices.collect()) as V2[];
    const seen = new Set<VertexId>();
    const mergedVertices: (V | V2)[] = [];
    for (const v of thisVertices) {
      if (!seen.has(v.id)) {
        seen.add(v.id);
        mergedVertices.push(v);
      }
    }
    for (const v of otherVertices) {
      if (!seen.has(v.id)) {
        seen.add(v.id);
        mergedVertices.push(v);
      }
    }
    const thisEdges = (await this.edges.collect()) as E[];
    const otherEdges = (await other.edges.collect()) as E2[];
    return new GraphFrame({
      vertices: DataFrame.fromArray(mergedVertices) as DataFrame<V | V2>,
      edges: DataFrame.fromArray([...thisEdges, ...otherEdges]) as DataFrame<E | E2>,
    });
  }

  /**
   * Vertices present in both graphs (matched by id); edges kept only when
   * both endpoints survive the intersection. Edge attributes come from this
   * graph.
   */
  async intersection<V2 extends Vertex, E2 extends Edge>(
    other: GraphFrame<V2, E2>,
  ): Promise<GraphFrame<V, E>> {
    const otherIds = new Set((await other.vertices.collect()).map((v: V2) => v.id));
    const thisVertices = (await this.vertices.collect()) as V[];
    const kept = thisVertices.filter((v) => otherIds.has(v.id));
    const keptIds = new Set(kept.map((v) => v.id));
    const thisEdges = (await this.edges.collect()) as E[];
    const edges = thisEdges.filter((e) => keptIds.has(e.src) && keptIds.has(e.dst));
    return new GraphFrame({
      vertices: DataFrame.fromArray(kept),
      edges: DataFrame.fromArray(edges),
    });
  }

  /**
   * Vertices in this graph but not in `other`, and edges whose both endpoints
   * survive. Useful for "accounts on YouTube not on Instagram" queries.
   */
  async difference<V2 extends Vertex, E2 extends Edge>(
    other: GraphFrame<V2, E2>,
  ): Promise<GraphFrame<V, E>> {
    const otherIds = new Set((await other.vertices.collect()).map((v: V2) => v.id));
    const thisVertices = (await this.vertices.collect()) as V[];
    const kept = thisVertices.filter((v) => !otherIds.has(v.id));
    const keptIds = new Set(kept.map((v) => v.id));
    const thisEdges = (await this.edges.collect()) as E[];
    const edges = thisEdges.filter((e) => keptIds.has(e.src) && keptIds.has(e.dst));
    return new GraphFrame({
      vertices: DataFrame.fromArray(kept),
      edges: DataFrame.fromArray(edges),
    });
  }

  /**
   * Union two graphs and append a set of cross-graph linking edges. Use this
   * when the same real-world entity appears as a vertex in both graphs and
   * you want the traversal algorithms to treat them as connected — e.g.
   * linking a creator's YouTube and Instagram accounts.
   */
  async bridge<V2 extends Vertex, E2 extends Edge, B extends Edge>(
    other: GraphFrame<V2, E2>,
    bridgeEdges: DataFrame<B>,
  ): Promise<GraphFrame<V | V2, E | E2 | B>> {
    const unioned = (await this.union(other)) as unknown as GraphFrame<V | V2, E | E2 | B>;
    const unionedEdges = (await unioned.edges.collect()) as (E | E2 | B)[];
    const bridges = (await bridgeEdges.collect()) as B[];
    return new GraphFrame({
      vertices: unioned.vertices,
      edges: DataFrame.fromArray([...unionedEdges, ...bridges]) as DataFrame<E | E2 | B>,
    });
  }
}
