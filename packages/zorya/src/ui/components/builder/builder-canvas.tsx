// ---------------------------------------------------------------------------
// BuilderCanvas — the SVG editing surface of the workflow builder.
//
// Renders `graphFromSchema(schema)`: step nodes, the typed Input node, and
// dependency edges. Interactions (all reported up; the page owns the
// schema):
//   - drag a node to move it;
//   - drag from a node's right handle (or the Input node's) onto another
//     node to connect them — the target then waits for the source;
//     clicking a handle and then a node does the same;
//   - click an edge's × to remove that dependency;
//   - drop a palette item to add a node there;
//   - Delete / Backspace removes the selected node, Escape cancels a
//     pending connection.
// The canvas is drawn at 1:1 scale inside a scrolling frame, so nodes keep
// their size however large the workflow grows.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import type { ActivityDescriptor, StepSchema, WorkflowSchema } from "@promin/workflow";
import {
  INPUT_H,
  INPUT_NODE_ID,
  INPUT_W,
  NODE_H,
  NODE_W,
  graphFromSchema,
  stepActivityRefs,
  wouldCreateCycle,
  type GraphNode,
} from "../../lib/workflow-builder-graph.ts";
import type { DraftIssue } from "../../lib/workflow-builder-validation.ts";

/** MIME type of palette drags: JSON `{ kind, activityId? }`. */
export const PALETTE_DRAG_MIME = "application/x-zorya-builder-node";

export interface PaletteDrop {
  readonly kind: StepSchema["type"];
  readonly activityId?: string;
}

interface Point {
  x: number;
  y: number;
}

const PAD = 80;
const MIN_W = 960;
const MIN_H = 520;

const KIND_ICON: Record<StepSchema["type"], string> = {
  step: "▣",
  map: "⋮⋮",
  sleep: "⏱",
  signal: "⚑",
  approval: "✓",
  branch: "⑂",
  parallel: "⫴",
};

export function BuilderCanvas(props: {
  schema: WorkflowSchema;
  activities: ReadonlyMap<string, ActivityDescriptor>;
  inputLabel: string;
  issues: ReadonlyArray<DraftIssue>;
  selected: string | null;
  onSelect: (id: string | null) => void;
  onMove: (params: { name: string; x: number; y: number }) => void;
  onConnect: (params: { from: string; to: string }) => void;
  onDisconnect: (params: { from: string; to: string }) => void;
  onDrop: (params: { item: PaletteDrop; at: Point }) => void;
  onDelete: (name: string) => void;
}) {
  const { schema, selected } = props;
  const svgRef = useRef<SVGSVGElement | null>(null);
  const graph = useMemo(() => graphFromSchema(schema), [schema]);
  const [drag, setDrag] = useState<{ name: string; dx: number; dy: number } | null>(null);
  // A connection in progress: from a handle, following the pointer (`at`)
  // while dragging, or parked (`at` null) after a click on the handle.
  const [link, setLink] = useState<{ from: string; at: Point | null } | null>(null);

  const issuesByNode = useMemo(() => {
    const map = new Map<string, DraftIssue[]>();
    for (const issue of props.issues) {
      const key = issue.input ? INPUT_NODE_ID : issue.step;
      if (key === undefined) continue;
      map.set(key, [...(map.get(key) ?? []), issue]);
    }
    return map;
  }, [props.issues]);

  const bounds = useMemo(() => {
    const xs = [graph.input.x, ...graph.nodes.map((n) => n.x)];
    const ys = [graph.input.y, ...graph.nodes.map((n) => n.y)];
    const minX = Math.min(0, ...xs) - PAD;
    const minY = Math.min(0, ...ys) - PAD / 2;
    const maxX = Math.max(...graph.nodes.map((n) => n.x + NODE_W), graph.input.x + INPUT_W) + PAD;
    const maxY = Math.max(...graph.nodes.map((n) => n.y + NODE_H), graph.input.y + INPUT_H) + PAD;
    return {
      x: minX,
      y: minY,
      w: Math.max(MIN_W, maxX - minX),
      h: Math.max(MIN_H, maxY - minY),
    };
  }, [graph]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement | null;
      if (target && /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName)) return;
      if (e.key === "Escape") setLink(null);
      if ((e.key === "Delete" || e.key === "Backspace") && selected && selected !== INPUT_NODE_ID) {
        e.preventDefault();
        props.onDelete(selected);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [selected, props.onDelete]);

  function toCanvas(e: { clientX: number; clientY: number }): Point | null {
    const svg = svgRef.current;
    const matrix = svg?.getScreenCTM();
    if (!svg || !matrix) return null;
    const point = svg.createSVGPoint();
    point.x = e.clientX;
    point.y = e.clientY;
    const p = point.matrixTransform(matrix.inverse());
    return { x: p.x, y: p.y };
  }

  function nodeAt(p: Point): GraphNode | undefined {
    return graph.nodes.find(
      (n) => p.x >= n.x - 10 && p.x <= n.x + NODE_W && p.y >= n.y && p.y <= n.y + NODE_H,
    );
  }

  function canReceive(node: GraphNode, from: string): boolean {
    if (node.id === from) return false;
    if (from === INPUT_NODE_ID) return node.step.type !== "map";
    return !wouldCreateCycle({ schema, from, to: node.id });
  }

  function finishLink(to: string | undefined): void {
    if (link && to && to !== link.from) props.onConnect({ from: link.from, to });
    setLink(null);
  }

  function onPointerMove(e: PointerEvent): void {
    const p = toCanvas(e);
    if (!p) return;
    if (drag) {
      props.onMove({
        name: drag.name,
        x: Math.max(0, p.x - drag.dx),
        y: Math.max(0, p.y - drag.dy),
      });
    } else if (link?.at) {
      setLink({ ...link, at: p });
    }
  }

  function onPointerUp(e: PointerEvent): void {
    if (drag) setDrag(null);
    if (link?.at) {
      const p = toCanvas(e);
      const target = p ? nodeAt(p) : undefined;
      // Released over empty canvas after a real drag: cancel. A release on
      // the handle itself (a click) parks the link for click-to-connect.
      if (target) finishLink(target.id);
      else if (p && Math.hypot(p.x - linkStart(link.from).x, p.y - linkStart(link.from).y) > 6) {
        setLink(null);
      } else setLink({ from: link.from, at: null });
    }
  }

  function linkStart(from: string): Point {
    if (from === INPUT_NODE_ID)
      return { x: graph.input.x + INPUT_W, y: graph.input.y + INPUT_H / 2 };
    const node = graph.nodes.find((n) => n.id === from);
    return node ? { x: node.x + NODE_W, y: node.y + NODE_H / 2 } : { x: 0, y: 0 };
  }

  function startLink(e: PointerEvent, from: string): void {
    if (e.button !== 0) return;
    e.stopPropagation();
    const p = toCanvas(e);
    if (!p) return;
    svgRef.current?.setPointerCapture?.(e.pointerId);
    setLink({ from, at: p });
    props.onSelect(from);
  }

  function startDrag(e: PointerEvent, node: GraphNode): void {
    if (e.button !== 0) return;
    const p = toCanvas(e);
    if (!p) return;
    if (link && !link.at) {
      // Click-to-connect: a parked link completes on the clicked node.
      e.stopPropagation();
      if (canReceive(node, link.from)) finishLink(node.id);
      else setLink(null);
      return;
    }
    svgRef.current?.setPointerCapture?.(e.pointerId);
    setDrag({ name: node.id, dx: p.x - node.x, dy: p.y - node.y });
    props.onSelect(node.id);
  }

  function onDrop(e: DragEvent): void {
    const raw = e.dataTransfer?.getData(PALETTE_DRAG_MIME);
    if (!raw) return;
    e.preventDefault();
    const p = toCanvas(e);
    if (!p) return;
    try {
      const item = JSON.parse(raw) as PaletteDrop;
      props.onDrop({
        item,
        at: { x: Math.max(0, p.x - NODE_W / 2), y: Math.max(0, p.y - NODE_H / 2) },
      });
    } catch {
      // Not ours.
    }
  }

  const byId = new Map(graph.nodes.map((n) => [n.id, n]));
  const inputIssues = issuesByNode.get(INPUT_NODE_ID) ?? [];
  const pendingFrom = link?.from;

  return (
    <div class="space-y-2">
      <div class="flex flex-wrap items-center justify-between gap-2 text-xs text-base-content/55">
        {pendingFrom ? (
          <span>
            Connecting from{" "}
            <span class="font-mono text-base-content">
              {pendingFrom === INPUT_NODE_ID ? "Input" : pendingFrom}
            </span>{" "}
            — click or release on the step that should wait for it.{" "}
            <button type="button" class="btn btn-xs btn-ghost" onClick={() => setLink(null)}>
              Cancel
            </button>
          </span>
        ) : (
          <span>
            Drag nodes to move them; drag from a node's right handle onto another node to connect
            them. Drop items from the palette to add steps.
          </span>
        )}
      </div>
      <div class="overflow-auto rounded border border-base-content/10 bg-base-200/40 max-h-[70vh]">
        <svg
          ref={svgRef}
          width={bounds.w}
          height={bounds.h}
          viewBox={`${bounds.x} ${bounds.y} ${bounds.w} ${bounds.h}`}
          class="block select-none"
          role="application"
          aria-label="Workflow canvas"
          onPointerMove={onPointerMove}
          onPointerUp={onPointerUp}
          onPointerDown={() => {
            if (link && !link.at) setLink(null);
            props.onSelect(null);
          }}
          onDragOver={(e) => {
            if (!e.dataTransfer?.types.includes(PALETTE_DRAG_MIME)) return;
            e.preventDefault();
            e.dataTransfer.dropEffect = "copy";
          }}
          onDrop={onDrop}
        >
          <defs>
            <pattern id="wb-grid" width="24" height="24" patternUnits="userSpaceOnUse">
              <circle cx="1" cy="1" r="1" class="fill-base-content/10" />
            </pattern>
            <marker id="wb-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
              <path d="M0,0 L8,4 L0,8 Z" class="fill-base-content/40" />
            </marker>
          </defs>
          <rect x={bounds.x} y={bounds.y} width={bounds.w} height={bounds.h} fill="url(#wb-grid)" />

          {graph.edges.map((edge) => {
            const to = byId.get(edge.to);
            if (!to) return null;
            const from = edge.from === INPUT_NODE_ID ? undefined : byId.get(edge.from);
            const sx = from ? from.x + NODE_W : graph.input.x + INPUT_W;
            const sy = from ? from.y + NODE_H / 2 : graph.input.y + INPUT_H / 2;
            const tx = to.x;
            const ty = to.y + NODE_H / 2;
            const mid = { x: (sx + tx) / 2, y: (sy + ty) / 2 };
            return (
              <g key={`${edge.from}->${edge.to}`}>
                <path
                  d={edgePath({ sx, sy, tx, ty })}
                  fill="none"
                  stroke="currentColor"
                  stroke-width="2"
                  stroke-dasharray={edge.kind === "array" ? "6 4" : undefined}
                  class={edge.kind === "input" ? "text-success/50" : "text-base-content/35"}
                  marker-end="url(#wb-arrow)"
                />
                {edge.kind === "dependency" && (
                  <g
                    class="cursor-pointer text-base-content/50 hover:text-error"
                    transform={`translate(${mid.x}, ${mid.y})`}
                    onPointerDown={(e) => e.stopPropagation()}
                    onClick={() => props.onDisconnect({ from: edge.from, to: edge.to })}
                  >
                    <title>{`Remove ${edge.from} → ${edge.to}`}</title>
                    <circle r="8" class="fill-base-100 stroke-current" />
                    <path d="M-3,-3 L3,3 M3,-3 L-3,3" stroke="currentColor" stroke-width="1.5" />
                  </g>
                )}
              </g>
            );
          })}

          {link?.at && (
            <path
              d={edgePath({
                sx: linkStart(link.from).x,
                sy: linkStart(link.from).y,
                tx: link.at.x,
                ty: link.at.y,
              })}
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-dasharray="6 4"
              class="pointer-events-none text-primary"
            />
          )}

          {/* Input node */}
          <g
            transform={`translate(${graph.input.x}, ${graph.input.y})`}
            class="cursor-pointer"
            onPointerDown={(e) => {
              e.stopPropagation();
              if (link && !link.at) setLink(null);
              props.onSelect(INPUT_NODE_ID);
            }}
          >
            <title>
              {inputIssues.length > 0
                ? inputIssues.map((i) => i.message).join("\n")
                : "Workflow input — select to declare its schema"}
            </title>
            <rect
              width={INPUT_W}
              height={INPUT_H}
              rx="10"
              stroke-width={selected === INPUT_NODE_ID ? 2 : 1.5}
              class={
                inputIssues.length > 0
                  ? "fill-warning/15 stroke-warning"
                  : selected === INPUT_NODE_ID
                    ? "fill-success/20 stroke-success"
                    : "fill-success/10 stroke-success/60"
              }
            />
            <text x="14" y="26" class="fill-current text-[13px] font-semibold">
              ⇢ Input
            </text>
            <text x="14" y="46" class="fill-current text-[10px] opacity-60">
              {truncate(props.inputLabel, 26)}
            </text>
            <Handle
              x={INPUT_W}
              y={INPUT_H / 2}
              active={pendingFrom === INPUT_NODE_ID}
              title="Drag onto a step to make it read the workflow input"
              onPointerDown={(e) => startLink(e, INPUT_NODE_ID)}
            />
          </g>

          {graph.nodes.map((node) => {
            const nodeIssues = issuesByNode.get(node.id) ?? [];
            const errors = nodeIssues.some((i) => i.severity === "error");
            const isSelected = selected === node.id;
            const receivable = pendingFrom !== undefined && canReceive(node, pendingFrom);
            const label = schema.ui?.[node.id]?.label;
            const subtitle = nodeSubtitle({ step: node.step, activities: props.activities });
            return (
              <g key={node.id} transform={`translate(${node.x}, ${node.y})`}>
                <g
                  class={drag?.name === node.id ? "cursor-grabbing" : "cursor-grab"}
                  onPointerDown={(e) => {
                    e.stopPropagation();
                    startDrag(e, node);
                  }}
                >
                  <title>
                    {nodeIssues.length > 0
                      ? nodeIssues.map((i) => i.message).join("\n")
                      : `${node.step.type} · ${node.id}`}
                  </title>
                  <rect
                    width={NODE_W}
                    height={NODE_H}
                    rx="8"
                    stroke-width={isSelected || receivable ? 2 : 1}
                    class={
                      errors
                        ? "fill-error/10 stroke-error"
                        : nodeIssues.length > 0
                          ? "fill-warning/10 stroke-warning"
                          : isSelected
                            ? "fill-primary/15 stroke-primary"
                            : receivable
                              ? "fill-base-100 stroke-primary/60"
                              : "fill-base-100 stroke-base-content/25"
                    }
                  />
                  <text x="12" y="24" class="fill-current text-[13px] font-semibold">
                    {KIND_ICON[node.step.type]} {truncate(label ?? node.id, 20)}
                  </text>
                  <text x="12" y="44" class="fill-current text-[11px] opacity-60">
                    {truncate(subtitle, 28)}
                  </text>
                  <text x="12" y="62" class="fill-current text-[10px] opacity-45">
                    {label ? `${node.step.type} · ${truncate(node.id, 18)}` : node.step.type}
                  </text>
                  {nodeIssues.length > 0 && (
                    <g transform={`translate(${NODE_W - 16}, 16)`}>
                      <circle r="7" class={errors ? "fill-error" : "fill-warning"} />
                      <text
                        x="0"
                        y="4"
                        text-anchor="middle"
                        class="fill-base-100 text-[10px] font-bold"
                      >
                        !
                      </text>
                    </g>
                  )}
                </g>
                {/* Target handle (left) — visual cue; any part of the node receives. */}
                <circle
                  cx="0"
                  cy={NODE_H / 2}
                  r="6"
                  stroke-width="2"
                  class={`fill-base-100 ${receivable ? "stroke-primary" : "stroke-base-content/30"}`}
                />
                <Handle
                  x={NODE_W}
                  y={NODE_H / 2}
                  active={pendingFrom === node.id}
                  title={`Drag onto a step that should wait for ${node.id}`}
                  onPointerDown={(e) => startLink(e, node.id)}
                />
              </g>
            );
          })}
        </svg>
      </div>
    </div>
  );
}

function Handle(props: {
  x: number;
  y: number;
  active: boolean;
  title: string;
  onPointerDown: (e: PointerEvent) => void;
}) {
  return (
    <g
      transform={`translate(${props.x}, ${props.y})`}
      class={`cursor-crosshair ${props.active ? "text-primary" : "text-base-content/45 hover:text-primary"}`}
      onPointerDown={props.onPointerDown}
    >
      <title>{props.title}</title>
      <circle r="9" class="fill-base-100 stroke-current" stroke-width="2" />
      <circle r="3.5" class="fill-current" />
    </g>
  );
}

function edgePath(p: { sx: number; sy: number; tx: number; ty: number }): string {
  const dx = Math.max(60, Math.abs(p.tx - p.sx) * 0.5);
  return `M ${p.sx} ${p.sy} C ${p.sx + dx} ${p.sy}, ${p.tx - dx} ${p.ty}, ${p.tx} ${p.ty}`;
}

/** What a node does, in a few words: its activity title, wait, or signal. */
function nodeSubtitle(params: {
  step: StepSchema;
  activities: ReadonlyMap<string, ActivityDescriptor>;
}): string {
  const { step, activities } = params;
  const title = (ref: string) => activities.get(ref)?.title ?? (ref || "—");
  switch (step.type) {
    case "step":
      return title(step.activityRef);
    case "map":
      return `each of ${step.arrayFrom || "?"} → ${title(step.activityRef)}`;
    case "sleep":
      return `wait ${formatMs(step.ms)}`;
    case "signal":
      return `signal ${step.signalName}`;
    case "approval":
      return `signal ${step.signalName ?? `${step.name}-approved`}`;
    case "branch":
      return `if ${title(step.conditionRef)}`;
    case "parallel":
      return stepActivityRefs(step)
        .map((r) => r.role.replace("branch ", ""))
        .join(" ‖ ");
  }
}

function formatMs(ms: number): string {
  if (ms < 1_000) return `${ms} ms`;
  if (ms < 60_000) return `${ms / 1_000} s`;
  if (ms < 3_600_000) return `${Math.round(ms / 60_000)} min`;
  return `${Math.round(ms / 3_600_000)} h`;
}

function truncate(text: string, max: number): string {
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}
