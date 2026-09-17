// ---------------------------------------------------------------------------
// TraceGraph — SVG call-graph view of an agent run trace.
//
// Renders the flat DAG from `buildTraceGraph` through the shared
// `graph-layout` engine: user / assistant / tool-call nodes, threaded
// in conversation order. Clicking a node selects it (the modal shows the
// detail panel). A tool-name query rings matching tool nodes.
// ---------------------------------------------------------------------------

import { useMemo } from "preact/hooks";
import { graphEdgePath, layoutGraph, type PlacedNode } from "../../lib/graph-layout.ts";
import { useGraphOrientation } from "../../hooks/use-graph-orientation.ts";
import type { TraceGraphModel, TraceGraphNode } from "../../lib/trace-graph.ts";

const NODE_W = 210;
const NODE_H = 56;
const STRIPE_W = 4;

interface TraceGraphProps {
  model: TraceGraphModel;
  selectedId?: string;
  onSelect: (id: string | undefined) => void;
  /** Lower-cased tool-name query; non-empty rings matching tool nodes. */
  toolQuery?: string;
}

export function TraceGraph({ model, selectedId, onSelect, toolQuery }: TraceGraphProps) {
  const horizontal = useMemo(
    () => layoutGraph(model.nodes, { orientation: "horizontal", nodeW: NODE_W, nodeH: NODE_H }),
    [model],
  );
  const vertical = useMemo(
    () => layoutGraph(model.nodes, { orientation: "vertical", nodeW: NODE_W, nodeH: NODE_H }),
    [model],
  );
  const { containerRef, orientation } = useGraphOrientation({
    horizontalWidth: horizontal.width,
    verticalWidth: vertical.width,
  });

  const { nodes, edges, width, height } = orientation === "vertical" ? vertical : horizontal;
  const query = (toolQuery ?? "").trim().toLowerCase();

  if (model.nodes.length === 0) {
    return (
      <div class="flex-1 p-8 text-center text-base-content/50 text-sm">
        Empty thread — nothing to graph yet.
      </div>
    );
  }

  return (
    <div ref={containerRef} class="flex-1 overflow-auto p-4">
      <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} class="block mx-auto">
        <defs>
          {/* Hard-coded fill — a Tailwind utility here expands to a CSS
              variable that isn't always resolved on first paint, leaving
              invisible arrowheads. See step-dag.tsx for the full story. */}
          <marker
            id="trace-arrow"
            viewBox="0 0 10 10"
            refX={9}
            refY={5}
            markerWidth={7}
            markerHeight={7}
            orient="auto"
          >
            <path d="M0,0 L10,5 L0,10 Z" fill="currentColor" />
          </marker>
        </defs>

        <g class="text-base-content/50" stroke="currentColor">
          {edges.map((e, i) => (
            <path
              key={`e-${i}`}
              d={graphEdgePath(e, { orientation, nodeW: NODE_W, nodeH: NODE_H })}
              fill="none"
              stroke-width={2}
              marker-end="url(#trace-arrow)"
            />
          ))}
        </g>

        {nodes.map((n) => (
          <TraceNodeRect
            key={n.node.id}
            placed={n}
            isSelected={selectedId === n.node.id}
            isHit={
              query.length > 0 &&
              n.node.kind === "tool-call" &&
              n.node.label.toLowerCase().includes(query)
            }
            dimmed={
              query.length > 0 &&
              !(n.node.kind === "tool-call" && n.node.label.toLowerCase().includes(query))
            }
            onSelect={() => onSelect(selectedId === n.node.id ? undefined : n.node.id)}
          />
        ))}
      </svg>
    </div>
  );
}

function TraceNodeRect({
  placed,
  isSelected,
  isHit,
  dimmed,
  onSelect,
}: {
  placed: PlacedNode<TraceGraphNode>;
  isSelected: boolean;
  isHit: boolean;
  dimmed: boolean;
  onSelect: () => void;
}) {
  const n = placed.node;
  const v = visualForNode(n);
  const borderClass = isSelected
    ? "stroke-primary"
    : isHit
      ? "stroke-warning"
      : n.failed
        ? "stroke-error"
        : "stroke-base-content/40";

  return (
    <g
      transform={`translate(${placed.x} ${placed.y})`}
      onClick={onSelect}
      class="cursor-pointer"
      opacity={dimmed ? 0.35 : 1}
    >
      <rect
        width={NODE_W}
        height={NODE_H}
        rx={8}
        class={`fill-base-300 ${borderClass} transition-all`}
        stroke-width={isSelected || isHit ? 2 : 1.5}
      />
      {/* Kind stripe on the left. */}
      <rect x={0} y={0} width={STRIPE_W + 4} height={NODE_H} rx={8} class={v.stripClass} />
      {/* Kind label. */}
      <text
        x={STRIPE_W + 16}
        y={NODE_H / 2 - 6}
        class={`${v.textClass}`}
        font-family="ui-monospace, monospace"
        font-size={10}
      >
        {v.kindLabel}
        {n.depth > 0 ? `  ↳ sub-agent L${n.depth}` : ""}
        {n.failed ? "  ✕ failed" : n.orphan ? "  ⚠ orphan" : ""}
      </text>
      {/* Main label. */}
      <text
        x={STRIPE_W + 16}
        y={NODE_H / 2 + 11}
        class="fill-base-content font-semibold"
        font-family="ui-sans-serif, system-ui"
        font-size={12}
      >
        {n.label}
      </text>
    </g>
  );
}

interface NodeVisual {
  kindLabel: string;
  stripClass: string;
  textClass: string;
}

function visualForNode(n: TraceGraphNode): NodeVisual {
  switch (n.kind) {
    case "user":
      return { kindLabel: "USER", stripClass: "fill-info", textClass: "fill-info/80" };
    case "assistant":
      return { kindLabel: "ASSISTANT", stripClass: "fill-success", textClass: "fill-success/80" };
    case "tool-call":
      return {
        kindLabel: "TOOL",
        stripClass: n.failed ? "fill-error" : "fill-base-content/40",
        textClass: n.failed ? "fill-error/90" : "fill-base-content/50",
      };
    case "system":
    default:
      return {
        kindLabel: "SYSTEM",
        stripClass: "fill-base-content/30",
        textClass: "fill-base-content/50",
      };
  }
}
