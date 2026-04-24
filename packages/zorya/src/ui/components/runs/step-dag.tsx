// ---------------------------------------------------------------------------
// StepDag — SVG renderer for a workflow's step DAG.
//
// Layout: Sugiyama-lite. Topological-rank on x, simple row packing on y.
// Edges are cubic Beziers so they look clean even with multiple hops.
// Nodes are colored by their executed status (planned = dashed).
// ---------------------------------------------------------------------------

import { useMemo } from "preact/hooks";
import type { RunDto, StepDto } from "../../../server/api-types.ts";
import { STEP_STATUS_VISUAL, STEP_TYPE_ICON } from "../../lib/format.ts";

interface StepDagProps {
  run: RunDto;
  selectedStep?: string;
  onSelectStep?: (stepName: string | undefined) => void;
}

const NODE_W = 180;
const NODE_H = 48;
const COL_GAP = 80;
const ROW_GAP = 20;
const PADDING = 24;

interface LaidOutNode {
  step: StepDto;
  rank: number;
  row: number;
  x: number;
  y: number;
}

export function StepDag({ run, selectedStep, onSelectStep }: StepDagProps) {
  const { nodes, edges, width, height } = useMemo(() => layout(run.steps), [run.steps]);

  if (run.steps.length === 0) {
    return (
      <div class="card bg-base-100 shadow">
        <div class="card-body">
          <h3 class="card-title text-base">Graph</h3>
          <div class="text-base-content/50 py-8 text-center">No steps</div>
        </div>
      </div>
    );
  }

  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4">
        <div class="flex items-center gap-4 mb-3">
          <h3 class="card-title text-base">Graph</h3>
          <span class="text-sm text-base-content/60">
            {run.steps.length} {run.steps.length === 1 ? "step" : "steps"}
          </span>
        </div>
        <div class="overflow-auto">
          <svg
            width={width}
            height={height}
            viewBox={`0 0 ${width} ${height}`}
            class="block mx-auto"
          >
            <defs>
              <marker
                id="dag-arrow"
                viewBox="0 0 10 10"
                refX={9}
                refY={5}
                markerWidth={6}
                markerHeight={6}
                orient="auto"
              >
                <path d="M0,0 L10,5 L0,10 Z" class="fill-base-content/40" />
              </marker>
            </defs>

            {/* Edges */}
            {edges.map((e, i) => (
              <path
                key={`e-${i}`}
                d={edgePath(e.from, e.to)}
                fill="none"
                stroke-width={1.5}
                class="stroke-base-content/25"
                marker-end="url(#dag-arrow)"
              />
            ))}

            {/* Nodes */}
            {nodes.map((n) => (
              <NodeRect
                key={n.step.stepName}
                node={n}
                isSelected={selectedStep === n.step.stepName}
                onSelect={() =>
                  onSelectStep?.(selectedStep === n.step.stepName ? undefined : n.step.stepName)
                }
              />
            ))}
          </svg>
        </div>
      </div>
    </div>
  );
}

function NodeRect({
  node,
  isSelected,
  onSelect,
}: {
  node: LaidOutNode;
  isSelected: boolean;
  onSelect: () => void;
}) {
  const step = node.step;
  const v = STEP_STATUS_VISUAL[step.status];
  const isPlanned = step.isPlanned === true;

  // Map DaisyUI bar class names to SVG fill colors. Using the CSS variable
  // would be cleaner, but a small static table is easier to reason about.
  const fill = isPlanned ? "transparent" : barToSvgFill(v.barClass);
  const strokeClass = isSelected
    ? "stroke-primary"
    : isPlanned
      ? "stroke-base-content/30"
      : "stroke-base-content/40";

  return (
    <g transform={`translate(${node.x} ${node.y})`} onClick={onSelect} class="cursor-pointer">
      <rect
        width={NODE_W}
        height={NODE_H}
        rx={6}
        class={`${strokeClass} transition-all`}
        fill={fill}
        fill-opacity={isPlanned ? 0 : 0.9}
        stroke-width={isSelected ? 2.5 : 1.5}
        stroke-dasharray={isPlanned ? "4 3" : undefined}
      />
      {/* Type icon badge */}
      <text
        x={12}
        y={NODE_H / 2 + 4}
        class="fill-base-content/60"
        font-family="ui-monospace, monospace"
        font-size={12}
      >
        {STEP_TYPE_ICON[step.stepType]}
      </text>
      {/* Step name */}
      <text
        x={32}
        y={NODE_H / 2 - 3}
        class={`${isPlanned ? "fill-base-content/60" : "fill-base-100"} font-medium`}
        font-family="ui-sans-serif, system-ui"
        font-size={13}
      >
        {truncate(step.stepName, 20)}
      </text>
      {/* Status line */}
      <text
        x={32}
        y={NODE_H / 2 + 12}
        class={`${v.textClass} opacity-90`}
        font-family="ui-sans-serif, system-ui"
        font-size={10}
      >
        {v.icon} {v.label}
        {step.attempt > 1 ? ` · ×${step.attempt}` : ""}
      </text>
    </g>
  );
}

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

function layout(steps: StepDto[]): {
  nodes: LaidOutNode[];
  edges: Array<{ from: LaidOutNode; to: LaidOutNode }>;
  width: number;
  height: number;
} {
  const byName = new Map<string, StepDto>();
  for (const s of steps) byName.set(s.stepName, s);

  // Assign ranks via longest-path topological sort.
  const rank = new Map<string, number>();
  const visit = (name: string): number => {
    if (rank.has(name)) return rank.get(name)!;
    const s = byName.get(name);
    if (!s) {
      rank.set(name, 0);
      return 0;
    }
    const parents = s.dependsOn.filter((d) => byName.has(d));
    const r = parents.length === 0 ? 0 : Math.max(...parents.map(visit)) + 1;
    rank.set(name, r);
    return r;
  };
  for (const s of steps) visit(s.stepName);

  // Group by rank, then within each rank pick row by stable sort on name
  // (fallback — prefer keeping same-parent children adjacent).
  const byRank = new Map<number, StepDto[]>();
  for (const s of steps) {
    const r = rank.get(s.stepName)!;
    const list = byRank.get(r) ?? [];
    list.push(s);
    byRank.set(r, list);
  }
  for (const list of byRank.values()) {
    list.sort((a, b) => a.stepName.localeCompare(b.stepName));
  }

  const maxRank = Math.max(0, ...rank.values());
  const maxRows = Math.max(0, ...Array.from(byRank.values(), (l) => l.length));

  const nodes: LaidOutNode[] = [];
  const nodeByName = new Map<string, LaidOutNode>();
  for (let r = 0; r <= maxRank; r++) {
    const list = byRank.get(r) ?? [];
    // Vertically centre each column so the diagram is balanced.
    const colHeight = list.length * (NODE_H + ROW_GAP) - ROW_GAP;
    const totalHeight = maxRows * (NODE_H + ROW_GAP) - ROW_GAP;
    const yOffset = (totalHeight - colHeight) / 2;
    for (let i = 0; i < list.length; i++) {
      const s = list[i]!;
      const n: LaidOutNode = {
        step: s,
        rank: r,
        row: i,
        x: PADDING + r * (NODE_W + COL_GAP),
        y: PADDING + yOffset + i * (NODE_H + ROW_GAP),
      };
      nodes.push(n);
      nodeByName.set(s.stepName, n);
    }
  }

  const edges: Array<{ from: LaidOutNode; to: LaidOutNode }> = [];
  for (const s of steps) {
    const to = nodeByName.get(s.stepName);
    if (!to) continue;
    for (const parent of s.dependsOn) {
      const from = nodeByName.get(parent);
      if (from) edges.push({ from, to });
    }
  }

  const width = PADDING * 2 + (maxRank + 1) * NODE_W + maxRank * COL_GAP;
  const height = PADDING * 2 + maxRows * (NODE_H + ROW_GAP) - ROW_GAP;
  return { nodes, edges, width, height };
}

function edgePath(from: LaidOutNode, to: LaidOutNode): string {
  const x1 = from.x + NODE_W;
  const y1 = from.y + NODE_H / 2;
  const x2 = to.x;
  const y2 = to.y + NODE_H / 2;
  const midX = (x1 + x2) / 2;
  return `M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2 - 4} ${y2}`;
}

function truncate(s: string, n: number): string {
  if (s.length <= n) return s;
  return s.slice(0, n - 1) + "…";
}

function barToSvgFill(barClass: string): string {
  switch (barClass) {
    case "bg-success":
      return "hsl(var(--su))";
    case "bg-info":
      return "hsl(var(--in))";
    case "bg-warning":
      return "hsl(var(--wa))";
    case "bg-error":
      return "hsl(var(--er))";
    default:
      return "hsl(var(--b3))";
  }
}
