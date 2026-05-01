// ---------------------------------------------------------------------------
// StepDag — SVG renderer for a workflow's step DAG.
//
// Layout: Sugiyama-lite. Topological-rank on x, simple row packing on y.
// Edges are cubic Beziers so they look clean even with multiple hops.
// Nodes are colored by their executed status (planned = dashed).
// ---------------------------------------------------------------------------

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "preact/hooks";
import type { RunDto, StepDto } from "../../../server/api-types.ts";
import type { JournalEntryDto } from "../../../server/routes/run-extras.ts";
import { api } from "../../api/client.ts";
import { STEP_STATUS_VISUAL, STEP_TYPE_ICON, effectiveStepStatus } from "../../lib/format.ts";
import type { ExtendedStepStatus } from "../../../server/api-types.ts";

interface StepDagProps {
  run: RunDto;
  selectedStep?: string;
  onSelectStep?: (stepName: string | undefined) => void;
}

const NODE_W = 200;
const NODE_H = 56;
const COL_GAP = 90;
const ROW_GAP = 20;
const PADDING = 24;
const STRIPE_W = 4;
// Container can be a few pixels narrower than the horizontal layout (e.g.
// scrollbar, padding rounding) without it being worth flipping to a
// taller vertical layout. Empirically 24px swallows the common cases
// without letting a real overflow slip through.
const ORIENTATION_HYSTERESIS_PX = 24;

type Orientation = "horizontal" | "vertical";

interface LaidOutNode {
  step: StepDto;
  rank: number;
  row: number;
  x: number;
  y: number;
}

export function StepDag({ run, selectedStep, onSelectStep }: StepDagProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  // `null` = not yet measured (don't pick an orientation). Once a real
  // width arrives the value is a positive number. Mid-flap rendering (the
  // first paint with containerW=0 then the post-effect with the real
  // width) was visible to users as the diagram briefly flipping
  // horizontal → vertical → horizontal.
  const [containerW, setContainerW] = useState<number | null>(null);

  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    // Synchronously measure before paint, so the first painted frame is
    // already in the correct orientation.
    setContainerW(el.clientWidth || null);
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        if (entry.contentRect.width > 0) setContainerW(entry.contentRect.width);
      }
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  // Latest workerId per step — bulk fetch attempts for the run, pick
  // the most recent attempt's workerId. Surfaces "where did this step
  // run?" right on the graph node without forcing the user to drill in.
  // Single request per run (not per step), sub-1ms on the SQLite path.
  const [workerByStep, setWorkerByStep] = useState<Record<string, string>>({});
  useEffect(() => {
    let cancelled = false;
    api
      .getRunAttempts(run.workflowId)
      .then((r) => {
        if (cancelled) return;
        const map: Record<string, string> = {};
        // Last write wins — attempts arrive in some order; the API
        // sorts by attempt asc, so iterating overwrites with the latest.
        for (const a of r.attempts) {
          if (a.workerId) map[a.stepName] = a.workerId;
        }
        setWorkerByStep(map);
      })
      .catch(() => setWorkerByStep({}));
    return () => {
      cancelled = true;
    };
  }, [run.workflowId]);

  // Fetch activity-journal entries for every non-planned step so we can
  // expand `.journaled()` steps into per-activity sub-nodes. Same shape as
  // the timeline's bulk fetch — keeps request count proportional to the
  // workflow's step count, not the user's interaction.
  const [journalsByStep, setJournalsByStep] = useState<Record<string, JournalEntryDto[]>>({});
  useEffect(() => {
    let cancelled = false;
    const stepNames = run.steps.filter((s) => !s.isPlanned && s.startedAt).map((s) => s.stepName);
    if (stepNames.length === 0) return;
    Promise.all(
      stepNames.map((stepName) =>
        api
          .getRunStepJournal(run.workflowId, stepName)
          .then((r) => ({ stepName, entries: r.supported ? r.entries : [] }))
          .catch(() => ({ stepName, entries: [] as JournalEntryDto[] })),
      ),
    ).then((rows) => {
      if (cancelled) return;
      const map: Record<string, JournalEntryDto[]> = {};
      for (const r of rows) if (r.entries.length > 0) map[r.stepName] = r.entries;
      setJournalsByStep(map);
    });
    return () => {
      cancelled = true;
    };
  }, [run.workflowId, run.steps.map((s) => s.stepName).join("|")]);

  // Expand journaled steps into a chain of activity nodes — the parent
  // keeps its place in the original DAG; each activity becomes a synthetic
  // step whose `dependsOn` chains it sequentially after the parent. The
  // post-parent steps still depend on the original parent, so the chain
  // hangs off as a sub-graph rather than rerouting the main flow.
  const expandedSteps = useMemo(
    () => expandJournaledSteps(run.steps, journalsByStep),
    [run.steps, journalsByStep],
  );
  const horizontal = useMemo(() => layout(expandedSteps, "horizontal"), [expandedSteps]);
  const vertical = useMemo(() => layout(expandedSteps, "vertical"), [expandedSteps]);

  // Default to horizontal. Switch to vertical only after we've actually
  // measured the container AND the horizontal layout overflows by more
  // than ORIENTATION_HYSTERESIS_PX so a 1-pixel overshoot doesn't flap
  // the layout when the container is right at the boundary. Holding the
  // initial render in horizontal also matches the most common case
  // (most users have wide enough panels for a 3-step DAG).
  const orientation: Orientation =
    containerW != null &&
    horizontal.width > containerW + ORIENTATION_HYSTERESIS_PX &&
    vertical.width <= horizontal.width
      ? "vertical"
      : "horizontal";

  // Diagnostic log — opt in via `window.__DAG_DEBUG = true` in the
  // console. Was load-bearing while tracking down the SSE-replaces-
  // dependsOn regression; left in place so the next time the DAG view
  // misbehaves we don't have to redo the instrumentation.
  if (
    typeof console !== "undefined" &&
    (window as { __DAG_DEBUG?: boolean }).__DAG_DEBUG === true
  ) {
    console.log("[dag] render", {
      steps: run.steps.length,
      containerW,
      horizontalWidth: horizontal.width,
      verticalWidth: vertical.width,
      orientation,
      edges: orientation === "vertical" ? vertical.edges.length : horizontal.edges.length,
    });
  }

  const { nodes, edges, width, height } = orientation === "vertical" ? vertical : horizontal;

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
            {orientation === "vertical" && (
              <span class="ml-2 badge badge-xs badge-ghost">vertical layout</span>
            )}
          </span>
        </div>
        <div ref={containerRef} class="overflow-auto">
          <svg
            width={width}
            height={height}
            viewBox={`0 0 ${width} ${height}`}
            class="block mx-auto"
          >
            <defs>
              {/*
                The marker's arrow head uses a hard-coded fill rather than
                a Tailwind utility — `class="fill-base-content/80"` here
                expands to a CSS variable (`hsl(var(--bc) / 0.8)`) that
                isn't always resolved on the very first paint. The result
                was edge paths drawn but tiny invisible arrowheads, which
                read as "no arrows" until a refresh primed the CSS cache.
              */}
              <marker
                id="dag-arrow"
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

            {/* Edges. The wrapping <g> sets `currentColor` for the
                marker's arrowhead; same color flows into the path's
                stroke too via inheritance — keeps the heads + lines
                visually unified. Edges that land on a synthetic
                activity node render dashed so the journal-expansion
                sub-chain reads visually distinct from the main DAG. */}
            <g class="text-base-content/60" stroke="currentColor">
              {edges.map((e, i) => {
                const intoActivity = e.to.step.metadata?.["journalActivityName"] !== undefined;
                return (
                  <path
                    key={`e-${i}`}
                    d={edgePath(e.from, e.to, orientation)}
                    fill="none"
                    stroke-width={2.5}
                    marker-end="url(#dag-arrow)"
                    stroke-dasharray={intoActivity ? "4 3" : undefined}
                    opacity={intoActivity ? 0.65 : 1}
                  />
                );
              })}
            </g>

            {/* Nodes */}
            {nodes.map((n) => (
              <NodeRect
                key={n.step.stepName}
                node={n}
                isSelected={selectedStep === n.step.stepName}
                workerId={workerByStep[n.step.stepName]}
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
  workerId,
}: {
  node: LaidOutNode;
  isSelected: boolean;
  onSelect: () => void;
  workerId?: string;
}) {
  const step = node.step;
  const renderStatus = effectiveStepStatus(step);
  const v = STEP_STATUS_VISUAL[renderStatus];
  const isPlanned = step.isPlanned === true;
  // Synthetic nodes from journal expansion — read this once so we can
  // shrink the card and dash its border to read as "sub-step of the
  // parent journaled step" rather than another DAG node in the main flow.
  const isActivity = step.metadata?.["journalActivityName"] !== undefined;
  // Parent journaled steps get a small "▷" container indicator so the
  // sub-chain hanging off them reads as belonging to this node and not
  // as a sibling fan-out to peer steps.
  const isJournaledParent = step.metadata?.["isJournaledParent"] === true;

  // Activities are visually subordinate: narrower card, indented inward,
  // dotted border. Same rounded corners + status stripe so they still
  // read as steps, just clearly nested.
  const cardWidth = isActivity ? NODE_W - 32 : NODE_W;
  const cardOffsetX = isActivity ? 16 : 0;

  // Strip + fill use Tailwind CSS classes (compiled to concrete colors) rather
  // than SVG fill attributes with CSS variables — latter don't resolve across
  // all browsers when referenced via hsl(var(--x)).
  const stripClass = classForStatusStrip(renderStatus);
  // Lighter card fill + brighter border so nodes read clearly against the
  // dark base-100 page background. base-300 is the highest of the dark
  // surface tokens, base-content/40 gives a visible (but not loud) edge.
  const cardFillClass = isPlanned ? "fill-base-300/40" : "fill-base-300";
  const borderClass = isSelected
    ? "stroke-primary"
    : isPlanned
      ? "stroke-base-content/30"
      : isActivity
        ? "stroke-base-content/30"
        : "stroke-base-content/40";

  // Dashed border for both planned and activity nodes — repurposes the
  // existing "ghost" treatment to also signal "synthetic / sub-step".
  const dashArray = isPlanned ? "4 3" : isActivity ? "3 2" : undefined;

  return (
    <g transform={`translate(${node.x} ${node.y})`} onClick={onSelect} class="cursor-pointer">
      {/* Card body */}
      <rect
        x={cardOffsetX}
        width={cardWidth}
        height={NODE_H}
        rx={8}
        class={`${cardFillClass} ${borderClass} transition-all`}
        stroke-width={isSelected ? 2 : 1.5}
        stroke-dasharray={dashArray}
      />
      {/* Status stripe on the left */}
      <rect
        x={cardOffsetX}
        y={0}
        width={STRIPE_W + 4}
        height={NODE_H}
        rx={8}
        class={stripClass}
        opacity={isPlanned ? 0.35 : 1}
      />
      {/* Step type glyph. Hidden entirely for plain `single` steps —
          that's the default kind, and labeling every node "SINGLE" was
          pure noise. Journaled parents get "JOURNALED ◆" so the
          sub-chain hanging off them is visually attributed.
          map / sleep / signal still get their label since those are
          the kinds users actually need to distinguish. */}
      {(() => {
        const label = isActivity
          ? "ACTIVITY •"
          : isJournaledParent
            ? "JOURNALED ◆  activities below"
            : step.stepType === "single"
              ? null
              : `${step.stepType.toUpperCase()} ${STEP_TYPE_ICON[step.stepType]}`;
        if (!label) return null;
        return (
          <text
            x={cardOffsetX + STRIPE_W + 16}
            y={NODE_H / 2 - 6}
            class="fill-base-content/50"
            font-family="ui-monospace, monospace"
            font-size={11}
          >
            {label}
          </text>
        );
      })()}
      {/* Step name. Centered vertically when no type label appears above
          (the common `single` case); shifted down when the type label
          is present so they don't overlap. Synthetic activity nodes
          carry their human-readable label on
          `metadata.journalActivityName`; without this fallback they'd
          display the namespaced internal name like
          `research::fetch-sources::0`. */}
      {(() => {
        const hasTypeLabel = isActivity || isJournaledParent || step.stepType !== "single";
        const labelY = hasTypeLabel ? NODE_H / 2 + 9 : NODE_H / 2 + 4;
        return (
          <text
            x={cardOffsetX + STRIPE_W + 16}
            y={labelY}
            class="fill-base-content font-semibold"
            font-family="ui-sans-serif, system-ui"
            font-size={13}
          >
            {truncate(
              (step.metadata?.["journalActivityName"] as string | undefined) ?? step.stepName,
              22,
            )}
          </text>
        );
      })()}
      {/* Status line at the bottom right */}
      <g transform={`translate(${cardOffsetX + cardWidth - 8} ${NODE_H - 8})`}>
        <text
          text-anchor="end"
          class={`${v.textClass} font-medium`}
          font-family="ui-sans-serif, system-ui"
          font-size={10}
        >
          {v.icon} {v.label.toLowerCase()}
          {step.attempt > 1 ? ` · ×${step.attempt}` : ""}
        </text>
      </g>
      {/* Worker chip — bottom-LEFT of the card, below the step name.
          Surfaces "where did this step run?" without forcing a drill-in
          to the step tab's attempts list. Truncated since worker ids are
          UUIDs by default; full id available on hover. */}
      {workerId && (
        <g transform={`translate(${cardOffsetX + STRIPE_W + 16} ${NODE_H - 8})`}>
          <text class="fill-base-content/45" font-family="ui-monospace, monospace" font-size={9}>
            <title>{`Executed on worker ${workerId}`}</title>⚙ {truncate(workerId, 14)}
          </text>
        </g>
      )}
    </g>
  );
}

/** Tailwind-compiled fill utility per status. */
function classForStatusStrip(status: ExtendedStepStatus): string {
  switch (status) {
    case "running":
      return "fill-info";
    case "completed":
    case "compensated":
      return "fill-success";
    case "failed":
    case "compensation_failed":
      return "fill-error";
    case "sleeping":
    case "waiting_for_signal":
    case "upstream_failed":
      return "fill-warning";
    case "skipped":
    case "pending":
    default:
      return "fill-base-content/30";
  }
}

// ---------------------------------------------------------------------------
// Journal expansion — turn each journaled step's activity entries into
// synthetic StepDto children, chained sequentially after the parent.
// ---------------------------------------------------------------------------

/**
 * Sequential expansion: activity[0] depends on the parent journaled step,
 * activity[i] depends on activity[i-1]. Synthetic step names use a
 * `parent::activityName::index` namespace so two journaled steps can
 * reuse the same activityName without colliding. Down-stream user steps
 * still depend on the original parent — the activity chain is a side
 * branch off the parent, not a rewiring of the main DAG.
 */
function expandJournaledSteps(
  steps: ReadonlyArray<StepDto>,
  journalsByStep: Record<string, JournalEntryDto[]>,
): StepDto[] {
  const out: StepDto[] = [];
  for (const s of steps) {
    const journal = journalsByStep[s.stepName];
    const hasJournal = journal && journal.length > 0;
    // Clone the parent step with an isJournaledParent flag in metadata so
    // NodeRect can render a subtle "container" indicator. Without this the
    // parent looks identical to a regular `single` step and the activity
    // sub-chain reads as a flat continuation of the main DAG.
    out.push(
      hasJournal ? { ...s, metadata: { ...(s.metadata ?? {}), isJournaledParent: true } } : s,
    );
    if (!hasJournal) continue;
    let prevSyntheticName = s.stepName;
    for (const entry of journal) {
      const syntheticName = `${s.stepName}::${entry.activityName}::${entry.activityIndex}`;
      out.push(activityToStepDto(entry, syntheticName, prevSyntheticName, s.run));
      prevSyntheticName = syntheticName;
    }
  }
  return out;
}

function activityToStepDto(
  entry: JournalEntryDto,
  syntheticName: string,
  parentName: string,
  run: number,
): StepDto {
  const isFailure = entry.exit?.tag === "Failure";
  const isPending = entry.phase === "pending";
  // Map journal step type → workflow StepType so the existing icon /
  // color machinery (sleep, signal) lights up correctly. "activity" /
  // "compensation" / "child" all fall through to `single`.
  const stepType: StepDto["stepType"] =
    entry.stepType === "sleep" ? "sleep" : entry.stepType === "signal" ? "signal" : "single";
  return {
    stepName: syntheticName,
    run,
    status: isPending
      ? entry.stepType === "sleep"
        ? "sleeping"
        : entry.stepType === "signal"
          ? "waiting_for_signal"
          : "running"
      : isFailure
        ? "failed"
        : "completed",
    stepType,
    dependsOn: [parentName],
    result: entry.exit?.tag === "Success" ? entry.exit.value : undefined,
    error: entry.exit?.tag === "Failure" ? entry.exit.error : undefined,
    completedAt: entry.phase === "completed" ? entry.createdAt : undefined,
    wakeAt: entry.wakeAt,
    attempt: 1,
    metadata: { journalActivityName: entry.activityName, branchPath: entry.branchPath },
  };
}

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

function layout(
  steps: StepDto[],
  orientation: Orientation,
): {
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

  // Horizontal: rank → x, row-within-rank → y.
  // Vertical: rank → y, row-within-rank → x. (Swap the two axes.)
  const horizontal = orientation === "horizontal";
  const rankStep = horizontal ? NODE_W + COL_GAP : NODE_H + COL_GAP;
  const rowStep = horizontal ? NODE_H + ROW_GAP : NODE_W + ROW_GAP;
  const rankSize = horizontal ? NODE_W : NODE_H;
  const rowSize = horizontal ? NODE_H : NODE_W;

  const nodes: LaidOutNode[] = [];
  const nodeByName = new Map<string, LaidOutNode>();
  for (let r = 0; r <= maxRank; r++) {
    const list = byRank.get(r) ?? [];
    // Centre each rank's row band so the diagram is balanced.
    const bandLen = list.length * rowStep - ROW_GAP;
    const totalBand = maxRows * rowStep - ROW_GAP;
    const offset = (totalBand - bandLen) / 2;
    for (let i = 0; i < list.length; i++) {
      const s = list[i]!;
      const rankCoord = PADDING + r * rankStep;
      const rowCoord = PADDING + offset + i * rowStep;
      const n: LaidOutNode = {
        step: s,
        rank: r,
        row: i,
        x: horizontal ? rankCoord : rowCoord,
        y: horizontal ? rowCoord : rankCoord,
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

  const width =
    PADDING * 2 +
    (horizontal
      ? (maxRank + 1) * rankSize + maxRank * COL_GAP
      : maxRows * rowSize + (maxRows - 1) * ROW_GAP);
  const height =
    PADDING * 2 +
    (horizontal
      ? maxRows * rowSize + (maxRows - 1) * ROW_GAP
      : (maxRank + 1) * rankSize + maxRank * COL_GAP);
  return { nodes, edges, width, height };
}

function edgePath(from: LaidOutNode, to: LaidOutNode, orientation: Orientation): string {
  if (orientation === "horizontal") {
    const x1 = from.x + NODE_W;
    const y1 = from.y + NODE_H / 2;
    const x2 = to.x;
    const y2 = to.y + NODE_H / 2;
    const midX = (x1 + x2) / 2;
    return `M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2 - 4} ${y2}`;
  }
  // Vertical: edge from bottom-of-from to top-of-to.
  const x1 = from.x + NODE_W / 2;
  const y1 = from.y + NODE_H;
  const x2 = to.x + NODE_W / 2;
  const y2 = to.y;
  const midY = (y1 + y2) / 2;
  return `M ${x1} ${y1} C ${x1} ${midY}, ${x2} ${midY}, ${x2} ${y2 - 4}`;
}

function truncate(s: string, n: number): string {
  if (s.length <= n) return s;
  return s.slice(0, n - 1) + "…";
}
