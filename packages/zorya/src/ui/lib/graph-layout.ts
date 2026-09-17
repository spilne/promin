// ---------------------------------------------------------------------------
// graph-layout — Sugiyama-lite DAG layout, shared by every SVG graph view
// in the dashboard (workflow step DAG, agent run-trace graph).
//
// Layout: longest-path topological rank on one axis, simple row packing on
// the other. Edges are emitted as cubic Béziers so multi-hop arcs read
// cleanly. The caller supplies fixed node dimensions and an orientation;
// this module is pure geometry and pulls in no DOM or framework.
// ---------------------------------------------------------------------------

export type GraphOrientation = "horizontal" | "vertical";

/** Minimal node shape the layout needs: an id and its upstream ids. */
export interface GraphNode {
  readonly id: string;
  readonly dependsOn: readonly string[];
}

/** A node with its computed rank/row and top-left pixel position. */
export interface PlacedNode<N extends GraphNode> {
  readonly node: N;
  readonly rank: number;
  readonly row: number;
  readonly x: number;
  readonly y: number;
}

export interface GraphEdge<N extends GraphNode> {
  readonly from: PlacedNode<N>;
  readonly to: PlacedNode<N>;
}

export interface GraphLayoutOptions {
  readonly orientation: GraphOrientation;
  /** Fixed node width in px. */
  readonly nodeW: number;
  /** Fixed node height in px. */
  readonly nodeH: number;
  /** Gap between ranks (the "flow" axis). Default 90. */
  readonly colGap?: number;
  /** Gap between rows within a rank. Default 20. */
  readonly rowGap?: number;
  /** Outer padding around the whole diagram. Default 24. */
  readonly padding?: number;
}

export interface GraphLayoutResult<N extends GraphNode> {
  readonly nodes: ReadonlyArray<PlacedNode<N>>;
  readonly edges: ReadonlyArray<GraphEdge<N>>;
  readonly width: number;
  readonly height: number;
}

const DEFAULT_COL_GAP = 90;
const DEFAULT_ROW_GAP = 20;
const DEFAULT_PADDING = 24;

/**
 * Lay out a DAG of `nodes` for rendering as an SVG diagram.
 *
 * Ranks are assigned by longest-path from the roots; within a rank,
 * nodes are ordered by a stable sort on `id` (callers that want a
 * specific within-rank order should encode it into the id, e.g. a
 * zero-padded sequence prefix). `dependsOn` ids that don't resolve to a
 * node are ignored. Back-edges (cycles) are tolerated — the offending
 * edge is treated as rank 0 so layout still terminates.
 */
export function layoutGraph<N extends GraphNode>(
  inputNodes: readonly N[],
  options: GraphLayoutOptions,
): GraphLayoutResult<N> {
  const { orientation, nodeW, nodeH } = options;
  const colGap = options.colGap ?? DEFAULT_COL_GAP;
  const rowGap = options.rowGap ?? DEFAULT_ROW_GAP;
  const padding = options.padding ?? DEFAULT_PADDING;

  const byId = new Map<string, N>();
  for (const n of inputNodes) byId.set(n.id, n);

  // Longest-path rank. `inProgress` guards against cycles: a back-edge
  // into a node still on the recursion stack contributes rank 0 rather
  // than recursing forever.
  const rank = new Map<string, number>();
  const inProgress = new Set<string>();
  const visit = (id: string): number => {
    const cached = rank.get(id);
    if (cached !== undefined) return cached;
    const n = byId.get(id);
    if (!n) {
      rank.set(id, 0);
      return 0;
    }
    if (inProgress.has(id)) return 0;
    inProgress.add(id);
    const parents = n.dependsOn.filter((d) => byId.has(d));
    const r = parents.length === 0 ? 0 : Math.max(...parents.map(visit)) + 1;
    inProgress.delete(id);
    rank.set(id, r);
    return r;
  };
  for (const n of inputNodes) visit(n.id);

  // Bucket nodes by rank; order within a rank by stable id sort.
  const byRank = new Map<number, N[]>();
  for (const n of inputNodes) {
    const r = rank.get(n.id)!;
    const list = byRank.get(r) ?? [];
    list.push(n);
    byRank.set(r, list);
  }
  for (const list of byRank.values()) {
    list.sort((a, b) => a.id.localeCompare(b.id));
  }

  const maxRank = Math.max(0, ...rank.values());
  const maxRows = Math.max(0, ...Array.from(byRank.values(), (l) => l.length));

  // Horizontal: rank → x, row-within-rank → y. Vertical swaps the axes.
  const horizontal = orientation === "horizontal";
  const rankStep = horizontal ? nodeW + colGap : nodeH + colGap;
  const rowStep = horizontal ? nodeH + rowGap : nodeW + rowGap;
  const rankSize = horizontal ? nodeW : nodeH;
  const rowSize = horizontal ? nodeH : nodeW;

  const placed: PlacedNode<N>[] = [];
  const placedById = new Map<string, PlacedNode<N>>();
  for (let r = 0; r <= maxRank; r++) {
    const list = byRank.get(r) ?? [];
    // Centre each rank's band so the diagram stays balanced.
    const bandLen = list.length * rowStep - rowGap;
    const totalBand = maxRows * rowStep - rowGap;
    const offset = (totalBand - bandLen) / 2;
    for (let i = 0; i < list.length; i++) {
      const n = list[i]!;
      const rankCoord = padding + r * rankStep;
      const rowCoord = padding + offset + i * rowStep;
      const p: PlacedNode<N> = {
        node: n,
        rank: r,
        row: i,
        x: horizontal ? rankCoord : rowCoord,
        y: horizontal ? rowCoord : rankCoord,
      };
      placed.push(p);
      placedById.set(n.id, p);
    }
  }

  const edges: GraphEdge<N>[] = [];
  for (const n of inputNodes) {
    const to = placedById.get(n.id);
    if (!to) continue;
    for (const parentId of n.dependsOn) {
      const from = placedById.get(parentId);
      if (from) edges.push({ from, to });
    }
  }

  const width =
    padding * 2 +
    (horizontal
      ? (maxRank + 1) * rankSize + maxRank * colGap
      : maxRows * rowSize + (maxRows - 1) * rowGap);
  const height =
    padding * 2 +
    (horizontal
      ? maxRows * rowSize + (maxRows - 1) * rowGap
      : (maxRank + 1) * rankSize + maxRank * colGap);

  return { nodes: placed, edges, width, height };
}

/**
 * Cubic-Bézier path string for an edge between two placed nodes. The
 * curve leaves the trailing edge of `from` and lands just shy of the
 * leading edge of `to` so an arrowhead marker sits flush.
 */
export function graphEdgePath<N extends GraphNode>(
  edge: GraphEdge<N>,
  options: { orientation: GraphOrientation; nodeW: number; nodeH: number },
): string {
  const { from, to } = edge;
  const { orientation, nodeW, nodeH } = options;
  if (orientation === "horizontal") {
    const x1 = from.x + nodeW;
    const y1 = from.y + nodeH / 2;
    const x2 = to.x;
    const y2 = to.y + nodeH / 2;
    const midX = (x1 + x2) / 2;
    return `M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2 - 4} ${y2}`;
  }
  const x1 = from.x + nodeW / 2;
  const y1 = from.y + nodeH;
  const x2 = to.x + nodeW / 2;
  const y2 = to.y;
  const midY = (y1 + y2) / 2;
  return `M ${x1} ${y1} C ${x1} ${midY}, ${x2} ${midY}, ${x2} ${y2 - 4}`;
}
