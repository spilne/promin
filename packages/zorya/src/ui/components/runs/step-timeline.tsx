import { useState } from "preact/hooks";
import type { RunDto, StepDto } from "../../../server/api-types.ts";
import {
  effectiveStepStatus,
  formatDuration,
  formatRelative,
  STEP_STATUS_VISUAL,
  STEP_TYPE_ICON,
} from "../../lib/format.ts";

interface StepTimelineProps {
  run: RunDto;
  selectedStep?: string;
  onSelectStep?: (stepName: string | undefined) => void;
}

/**
 * Tree + Gantt timeline. Each row has: type icon, indented step name, status
 * pill, duration, and a bar on a shared time axis. Sleep / waiting_for_signal
 * bars use a hatched pattern to distinguish "waiting" from "running". Running
 * / pending steps extend to `now`, so the timeline animates while live.
 *
 * Step hierarchy: dependsOn forms a DAG, but we render a simple indented tree
 * rooted on dependency-less steps. Steps with multiple parents appear once;
 * the deepest-parent indentation wins.
 */
export function StepTimeline({ run, selectedStep, onSelectStep }: StepTimelineProps) {
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  // Origin = earliest of workflow.startedAt and any step startedAt. Covers clock
  // skew and demo data where step timestamps precede workflow.startedAt.
  const wfStart = toMs(run.startedAt ?? run.createdAt);
  const stepStarts = run.steps
    .map((s) => (s.startedAt ? toMs(s.startedAt) : Number.POSITIVE_INFINITY))
    .filter((t) => Number.isFinite(t)) as number[];
  const origin = stepStarts.length > 0 ? Math.min(wfStart, ...stepStarts) : wfStart;

  // End = latest of workflow.completedAt, step startedAt + duration (actual
  // execution end, preferred over completedAt which can be batch-persist time),
  // or step completedAt, or now.
  const stepEnds = run.steps
    .map((s) => {
      if (s.startedAt && s.durationMs !== undefined) return toMs(s.startedAt) + s.durationMs;
      if (s.completedAt) return toMs(s.completedAt);
      return Number.NEGATIVE_INFINITY;
    })
    .filter((t) => Number.isFinite(t)) as number[];
  const wfEnd = run.completedAt ? toMs(run.completedAt) : Date.now();
  const endMs = stepEnds.length > 0 ? Math.max(wfEnd, ...stepEnds) : wfEnd;
  const totalMs = Math.max(1, endMs - origin);

  const byName = new Map<string, StepDto>();
  for (const s of run.steps) byName.set(s.stepName, s);
  const tree = buildTree(run.steps);
  const visibleRows = flatten(tree, collapsed);

  // Piecewise time axis: ranges where only wait/signal steps are active get
  // compressed so a 10-day approval pause doesn't crush the actual work
  // bars to a single pixel. Returns absolute-real-ms → display-ms mapping.
  const axis = buildTimeAxis(run.steps, origin, endMs);

  const toggle = (name: string) => {
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  if (run.steps.length === 0) {
    return (
      <div class="card bg-base-100 shadow">
        <div class="card-body">
          <h3 class="card-title text-base">Timeline</h3>
          <div class="text-base-content/50 py-8 text-center">No steps executed yet</div>
        </div>
      </div>
    );
  }

  // Build ticks against the compressed axis so labels in still-natural
  // regions stay readable; ticks that would land in a compressed region get
  // filtered out (their labels would overlap the squashed segment anyway).
  const ticks = buildAxisTicks(axis);
  const hasCompression = axis.segments.some((s) => s.compressed);

  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4 space-y-3">
        <div class="flex items-center gap-4">
          <h3 class="card-title text-base">Timeline</h3>
          <span class="text-sm text-base-content/60">{formatDuration(totalMs)} total</span>
          <span class="text-sm text-base-content/40">
            · {run.steps.length} {run.steps.length === 1 ? "step" : "steps"}
          </span>
          {hasCompression && (
            <span
              class="text-xs text-warning/80 font-medium"
              title="Idle wait/sleep ranges have been collapsed to keep work steps readable. Bar widths are not to scale across the breaks."
            >
              · time-compressed
            </span>
          )}
          <div class="flex-1" />
          <Legend />
        </div>

        {/* Column header + time axis. Mirror the row's `px-1` so STEP /
            DURATION line up with the data below; without it the headers
            sit 4px to the left of every value. The extra bottom padding
            gives breathing room before the first row. */}
        <div class="flex items-center gap-2 px-1 text-xs text-base-content/50 border-b border-base-content/10 pb-3">
          <div class="w-72 shrink-0 text-center">STEP</div>
          <div class="w-20 shrink-0 text-center">DURATION</div>
          <div class="relative flex-1 h-5">
            {ticks.map((t) => (
              <div
                class="absolute top-0 h-full"
                style={{ left: `${(t.displayMs / axis.totalDisplay) * 100}%` }}
              >
                <div class="w-px h-2 bg-base-content/20" />
                <div class="-translate-x-1/2 mt-0.5">{formatDuration(t.realMs)}</div>
              </div>
            ))}
            {/* Compression band + the real elapsed duration as a label
                above the sliver — without it, the user can't tell whether
                the gap was 5s or 10 days. The label is allowed to overflow
                the band's tiny visual width via whitespace-nowrap; the tick
                filter has already cleared this region of natural ticks so
                there's nothing to collide with. */}
            {axis.segments
              .filter((s) => s.compressed)
              .map((s) => {
                const leftPct = (s.displayStart / axis.totalDisplay) * 100;
                const widthPct = ((s.displayEnd - s.displayStart) / axis.totalDisplay) * 100;
                const real = formatDuration(s.realEnd - s.realStart);
                return (
                  <>
                    <div
                      class="absolute top-0 h-full bg-warning/15 border-x border-dashed border-warning/50"
                      style={{ left: `${leftPct}%`, width: `${widthPct}%` }}
                      title={`Compressed range — ${real} of idle time`}
                    />
                    <div
                      class="absolute top-0 text-[10px] text-warning font-mono whitespace-nowrap pointer-events-none leading-none"
                      style={{
                        left: `${leftPct + widthPct / 2}%`,
                        transform: "translateX(-50%)",
                      }}
                      title={`${real} elapsed`}
                    >
                      ⇥ {real}
                    </div>
                  </>
                );
              })}
          </div>
        </div>

        {/* Rows */}
        <div class="space-y-0.5 pt-1">
          {visibleRows.map((row) => {
            const step = byName.get(row.name);
            if (!step) return null;
            return (
              <StepRow
                step={step}
                depth={row.depth}
                hasChildren={row.hasChildren}
                isCollapsed={collapsed.has(row.name)}
                onToggle={() => toggle(row.name)}
                origin={origin}
                axis={axis}
                isSelected={selectedStep === row.name}
                onSelect={() => onSelectStep?.(selectedStep === row.name ? undefined : row.name)}
              />
            );
          })}
        </div>

        {/* Keyboard cheatsheet */}
        <div class="flex gap-4 text-xs text-base-content/40 pt-2 border-t border-base-content/10">
          <kbd class="kbd kbd-xs">click</kbd>
          <span>Select step</span>
          <kbd class="kbd kbd-xs">▸</kbd>
          <span>Expand / collapse</span>
        </div>
      </div>
    </div>
  );
}

interface StepRowProps {
  step: StepDto;
  depth: number;
  hasChildren: boolean;
  isCollapsed: boolean;
  onToggle: () => void;
  origin: number;
  axis: TimeAxis;
  isSelected: boolean;
  onSelect: () => void;
}

function StepRow({
  step,
  depth,
  hasChildren,
  isCollapsed,
  onToggle,
  origin,
  axis,
  isSelected,
  onSelect,
}: StepRowProps) {
  // Map absolute step start/end through the (possibly compressed) axis.
  // Bars in compressed segments naturally shrink without us doing any
  // per-step special-casing here.
  const startAbs = step.startedAt ? toMs(step.startedAt) : origin;
  const endAbs = stepEndAbs(step, origin);
  const startDisplay = axis.realToDisplay(startAbs);
  const endDisplay = axis.realToDisplay(endAbs);
  const leftPct = (startDisplay / axis.totalDisplay) * 100;
  const widthPct = Math.max(0.5, ((endDisplay - startDisplay) / axis.totalDisplay) * 100);
  const renderStatus = effectiveStepStatus(step);
  const v = STEP_STATUS_VISUAL[renderStatus];
  // Hatch wait-like steps even after they complete — a journaled step that
  // spent 10s in ctx.signal should keep reading as "this was a wait" once
  // resolved, otherwise the chart loses the audit trail.
  const isHatched = isWaitLike(step);
  const retried = step.attempt > 1;

  const metadataSummary =
    step.metadata && Object.keys(step.metadata).length > 0
      ? Object.entries(step.metadata)
          .map(([k, val]) => `${k}=${typeof val === "string" ? val : JSON.stringify(val)}`)
          .join(", ")
      : undefined;
  // Wall-clock span — what users actually want to see for a wait step,
  // since durationMs only counts CPU work and reads as "0ms" for an
  // approval that took 10 days.
  const wallClockMs = endAbs - startAbs;
  const waitLike = isWaitLike(step);
  const tooltip = [
    `${step.stepName} (${v.label})`,
    step.durationMs !== undefined ? `duration ${formatDuration(step.durationMs)}` : "",
    waitLike && wallClockMs > (step.durationMs ?? 0)
      ? `wall-clock ${formatDuration(wallClockMs)}`
      : "",
    step.attempt > 1 ? `attempt ${step.attempt}` : "",
    step.startedAt ? `started ${formatRelative(step.startedAt)}` : "",
    metadataSummary ? `metadata: ${metadataSummary}` : "",
  ]
    .filter(Boolean)
    .join(" · ");

  const isPlanned = step.isPlanned === true;
  return (
    <div
      class={`flex items-center gap-2 h-9 px-1 rounded cursor-pointer transition-colors duration-150 ${
        isSelected ? "bg-primary/10 ring-1 ring-primary/40" : "hover:bg-base-200"
      } ${isPlanned ? "opacity-60" : ""}`}
      onClick={onSelect}
    >
      {/* Step name + indentation + chevron */}
      <div
        class="w-72 shrink-0 flex items-center gap-1 min-w-0"
        style={{ paddingLeft: `${depth * 16}px` }}
      >
        {hasChildren ? (
          <button
            class="w-4 shrink-0 text-base-content/50 hover:text-base-content text-sm"
            onClick={(e) => {
              e.stopPropagation();
              onToggle();
            }}
          >
            {isCollapsed ? "▸" : "▾"}
          </button>
        ) : (
          <span class="w-4 shrink-0" />
        )}
        <span class={`shrink-0 text-sm ${v.textClass}`} title={`${step.stepType} · ${v.label}`}>
          {STEP_TYPE_ICON[step.stepType]}
        </span>
        <span class="truncate text-sm font-mono" title={step.stepName}>
          {step.stepName}
        </span>
        {step.attempt > 1 && (
          <span class="badge badge-sm badge-ghost shrink-0">×{step.attempt}</span>
        )}
      </div>

      {/* Duration — for wait-like steps, prefer the wall-clock span over
          durationMs (which only counts CPU work and reads "0ms" for an
          approval that took 10 days). */}
      <div class="w-20 shrink-0 text-center font-mono text-xs text-base-content/70">
        {isPlanned ? (
          "—"
        ) : waitLike && wallClockMs > (step.durationMs ?? 0) ? (
          <span
            title={`Wall-clock ${formatDuration(wallClockMs)} (CPU ${formatDuration(step.durationMs)})`}
          >
            {formatDuration(wallClockMs)}
          </span>
        ) : (
          formatDuration(step.durationMs)
        )}
      </div>

      {/* Bar */}
      <div class="relative flex-1 h-full" title={tooltip}>
        <div class="absolute inset-y-1 left-0 right-0 bg-base-200/50 rounded" />
        {isPlanned ? (
          // Dashed placeholder for planned steps — shows that the step exists
          // in the DAG but hasn't executed yet.
          <div class="absolute inset-y-1 left-0 right-0 border border-dashed border-base-content/20 rounded" />
        ) : (
          <div
            class={`gantt-bar absolute top-1 bottom-1 rounded flex items-center justify-center overflow-hidden ${v.barClass} ${isHatched ? "gantt-hatched" : ""} ${retried ? "ring-1 ring-warning/70" : ""}`}
            style={{
              left: `${leftPct}%`,
              width: `${widthPct}%`,
            }}
          >
            {/* Render the wall-clock duration inside compressed wait bars so
                a 20-second pause and a 20-day pause stay distinguishable
                even though both occupy the same tiny ~2.5% slice. */}
            {waitLike && wallClockMs >= COMPRESS_MIN_REAL_MS && (
              <span class="text-[10px] font-bold text-base-100 leading-none px-1 whitespace-nowrap">
                {formatDuration(wallClockMs)}
              </span>
            )}
          </div>
        )}
        {retried && (
          <span
            class="absolute top-0 text-[10px] text-warning font-bold"
            style={{ left: `calc(${leftPct}% - 14px)` }}
            title={`${step.attempt} attempts`}
          >
            ↻{step.attempt}
          </span>
        )}
      </div>
    </div>
  );
}

function Legend() {
  const items = [
    { label: "completed", class: "bg-success" },
    { label: "running", class: "bg-info" },
    { label: "failed", class: "bg-error" },
    { label: "waiting", class: "bg-warning gantt-hatched" },
  ];
  return (
    <div class="flex gap-3 text-sm">
      {items.map((i) => (
        <div class="flex items-center gap-1.5">
          <span class={`w-3 h-3 rounded ${i.class}`} />
          <span class="text-base-content/60">{i.label}</span>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tree construction
// ---------------------------------------------------------------------------

interface TreeNode {
  name: string;
  children: TreeNode[];
}

interface FlatRow {
  name: string;
  depth: number;
  hasChildren: boolean;
}

/**
 * Builds a tree from steps. Steps with `dependsOn: []` are roots. A step with
 * multiple parents lives under its first parent (other edges are ignored for
 * layout). Steps are kept in `startedAt` order within each level.
 */
function buildTree(steps: StepDto[]): TreeNode[] {
  const ordered = steps.slice().sort(sortByStart);
  const byName = new Map<string, StepDto>();
  for (const s of ordered) byName.set(s.stepName, s);

  const nodes = new Map<string, TreeNode>();
  for (const s of ordered) nodes.set(s.stepName, { name: s.stepName, children: [] });

  const roots: TreeNode[] = [];
  for (const s of ordered) {
    const node = nodes.get(s.stepName)!;
    const parents = s.dependsOn.filter((p) => byName.has(p));
    if (parents.length === 0) {
      roots.push(node);
    } else {
      const parent = nodes.get(parents[0]!);
      parent?.children.push(node);
    }
  }
  return roots;
}

function flatten(tree: TreeNode[], collapsed: Set<string>, depth = 0): FlatRow[] {
  const out: FlatRow[] = [];
  for (const n of tree) {
    out.push({ name: n.name, depth, hasChildren: n.children.length > 0 });
    if (n.children.length > 0 && !collapsed.has(n.name)) {
      out.push(...flatten(n.children, collapsed, depth + 1));
    }
  }
  return out;
}

function toMs(iso: string): number {
  return new Date(iso).getTime();
}

/**
 * Absolute-timestamp end of a step's visible bar. Uses the LATER of
 * `startedAt + durationMs` and `completedAt`: journaled steps with
 * internal `ctx.sleep` / `ctx.signal` have a `durationMs` that only
 * counts user-code time (often 0ms), but their wall-clock span runs from
 * startedAt to completedAt. Without taking the max, those steps render
 * as a 0-width bar pinned to their start.
 */
function stepEndAbs(step: StepDto, origin: number): number {
  const startMs = step.startedAt ? toMs(step.startedAt) : null;
  const completedMs = step.completedAt ? toMs(step.completedAt) : null;
  const durationEnd =
    startMs !== null && step.durationMs !== undefined ? startMs + step.durationMs : null;

  if (durationEnd !== null && completedMs !== null) return Math.max(durationEnd, completedMs);
  if (completedMs !== null) return completedMs;
  if (durationEnd !== null) return durationEnd;
  if (step.status === "pending") return startMs ?? origin;
  return Date.now();
}

/**
 * True when the step's wall-clock span was dominated by waiting rather
 * than CPU work. Catches both live-active waits (`sleeping`,
 * `waiting_for_signal`) and post-completion "was waiting" cases — a
 * journaled step with `durationMs=0ms` and a 10s span is functionally a
 * wait, even though its current status is `completed`. The 5-second
 * floor avoids classifying every short sleep as a wait worth hiding.
 */
function isWaitLike(step: StepDto): boolean {
  if (step.status === "sleeping" || step.status === "waiting_for_signal") return true;
  if (step.startedAt && step.completedAt && step.durationMs !== undefined) {
    const realSpan = toMs(step.completedAt) - toMs(step.startedAt);
    const idle = realSpan - step.durationMs;
    if (idle >= COMPRESS_MIN_REAL_MS) return true;
  }
  return false;
}

function sortByStart(a: StepDto, b: StepDto): number {
  const ax = a.startedAt ? toMs(a.startedAt) : Number.MAX_SAFE_INTEGER;
  const bx = b.startedAt ? toMs(b.startedAt) : Number.MAX_SAFE_INTEGER;
  return ax - bx;
}

// ---------------------------------------------------------------------------
// Piecewise time axis — keeps wait/sleep idle ranges from crushing the
// timeline.
//
// Strategy:
//   1. Sweep step start/end events to find ranges where ONLY wait-class
//      steps (sleeping / waiting_for_signal) are active. Those ranges are
//      candidates for compression.
//   2. Compress only when the candidate range is at least
//      COMPRESS_MIN_REAL_MS long — short sleeps don't need to be hidden.
//   3. Each compressed range gets a fixed display slice
//      (COMPRESS_DISPLAY_FRACTION × naturally-displayed total), so an idle
//      week and an idle minute take roughly the same on-screen width while
//      the actual work bars stay 1:1.
//
// The returned `realToDisplay` mapping is piecewise linear; bars in
// compressed segments naturally shrink without per-step special-casing.
// ---------------------------------------------------------------------------

const COMPRESS_MIN_REAL_MS = 5_000; // skip compression below 5 seconds idle
// Each compressed range collapses to a tiny fixed slice — small enough that
// 20 seconds and 20 days of approval wait look about the same on screen,
// but big enough to remain interactable. The duration is rendered inside
// the bar so the real time isn't lost, only the proportional area.
const COMPRESS_DISPLAY_FRACTION = 0.025;

interface AxisSegment {
  realStart: number;
  realEnd: number;
  displayStart: number;
  displayEnd: number;
  compressed: boolean;
}

interface TimeAxis {
  segments: AxisSegment[];
  totalDisplay: number;
  realToDisplay(t: number): number;
}

function buildTimeAxis(steps: StepDto[], origin: number, endMs: number): TimeAxis {
  const totalReal = Math.max(1, endMs - origin);

  // Event sweep: track active counts of work vs wait steps so we can spot
  // "all-idle" stretches even when concurrent waits overlap. `isWaitLike`
  // also catches journaled steps that have completed but spent most of
  // their wall-clock time inside ctx.sleep / ctx.signal — pre-fix those
  // showed status="completed" and counted as work, defeating compression.
  type Delta = { wait: number; work: number };
  const events: Array<{ t: number; delta: Delta }> = [];
  for (const s of steps) {
    if (!s.startedAt) continue;
    const start = toMs(s.startedAt);
    const end = stepEndAbs(s, origin);
    const wait = isWaitLike(s);
    events.push({ t: start, delta: { wait: wait ? 1 : 0, work: wait ? 0 : 1 } });
    events.push({ t: end, delta: { wait: wait ? -1 : 0, work: wait ? 0 : -1 } });
  }
  events.sort((a, b) => a.t - b.t);

  const raw: Array<{ realStart: number; realEnd: number; compressed: boolean }> = [];
  let workActive = 0;
  let waitActive = 0;
  let cursor = origin;
  // Idle = no work step actively running. Includes both "wait step is
  // burning the clock" AND "completely empty stretch" — the latter shows
  // up when a journaled step's startedAt got rewritten by a signal-arrival
  // replay, leaving the original wait period unrepresented in the data.
  // Either way the gap should collapse so a 10-day approval pause doesn't
  // dominate the timeline.
  const isIdle = () => workActive === 0;

  for (const e of events) {
    if (e.t > cursor) {
      raw.push({
        realStart: cursor,
        realEnd: e.t,
        compressed: isIdle() && e.t - cursor >= COMPRESS_MIN_REAL_MS,
      });
      cursor = e.t;
    }
    waitActive += e.delta.wait;
    workActive += e.delta.work;
  }
  if (endMs > cursor) {
    raw.push({
      realStart: cursor,
      realEnd: endMs,
      compressed: isIdle() && endMs - cursor >= COMPRESS_MIN_REAL_MS,
    });
  }
  // `waitActive` is no longer load-bearing for the compression decision
  // (we widened to "no work" above), but we keep it computed because the
  // delta records also drive the per-step `isWaitLike` hatching. Reference
  // it once so the linter doesn't flag the unused tracker.
  void waitActive;

  // Fallback for empty / single-segment cases — keep linear mapping.
  if (raw.length === 0) {
    raw.push({ realStart: origin, realEnd: endMs, compressed: false });
  }

  const naturalTotal = raw
    .filter((s) => !s.compressed)
    .reduce((acc, s) => acc + (s.realEnd - s.realStart), 0);
  // When the whole timeline is wait (no work yet — pending approval as the
  // first step), there's nothing to compare against; just go linear.
  const useLinear = naturalTotal <= 0;
  const compressedDisplay = useLinear ? 0 : Math.max(1, naturalTotal * COMPRESS_DISPLAY_FRACTION);

  let displayCursor = 0;
  const segments: AxisSegment[] = raw.map((r) => {
    const real = r.realEnd - r.realStart;
    const display = useLinear || !r.compressed ? real : compressedDisplay;
    const seg: AxisSegment = {
      realStart: r.realStart,
      realEnd: r.realEnd,
      displayStart: displayCursor,
      displayEnd: displayCursor + display,
      compressed: r.compressed && !useLinear,
    };
    displayCursor += display;
    return seg;
  });
  const totalDisplay = displayCursor || totalReal;

  function realToDisplay(t: number): number {
    if (t <= origin) return 0;
    if (t >= endMs) return totalDisplay;
    for (const seg of segments) {
      if (t <= seg.realEnd) {
        const denom = Math.max(1, seg.realEnd - seg.realStart);
        const frac = (t - seg.realStart) / denom;
        return seg.displayStart + (seg.displayEnd - seg.displayStart) * frac;
      }
    }
    return totalDisplay;
  }

  return { segments, totalDisplay, realToDisplay };
}

/**
 * Build evenly-spaced ticks against the *natural* portion of the axis, then
 * project them through `realToDisplay`. Round-interval ticks that fall
 * inside a compressed segment are dropped (labelling the squashed range
 * with "10d / 11d / 12d" would fight the compression). To keep the
 * post-gap timeline labelled, we also emit a tick at the END of every
 * compressed segment so the user sees the "resume time" right after the
 * sliver — without it, after a long approval wait the only label is "0s"
 * at the far left.
 */
function buildAxisTicks(axis: TimeAxis): Array<{ realMs: number; displayMs: number }> {
  if (axis.segments.length === 0 || axis.totalDisplay <= 0) return [];
  const origin = axis.segments[0]!.realStart;
  const endMs = axis.segments[axis.segments.length - 1]!.realEnd;
  const totalMs = endMs - origin;
  if (totalMs <= 0) return [];

  const target = 5;
  const rawStep = totalMs / target;
  const magnitude = Math.pow(10, Math.floor(Math.log10(Math.max(1, rawStep))));
  const normalized = rawStep / magnitude;
  const stepNice = normalized < 1.5 ? 1 : normalized < 3 ? 2 : normalized < 7 ? 5 : 10;
  const step = stepNice * magnitude;

  const ticks: Array<{ realMs: number; displayMs: number }> = [];
  const seen = new Set<number>();
  const push = (realMs: number) => {
    if (seen.has(realMs)) return;
    seen.add(realMs);
    ticks.push({ realMs, displayMs: axis.realToDisplay(origin + realMs) });
  };

  for (let t = 0; t <= totalMs; t += step) {
    const abs = origin + t;
    const inCompressed = axis.segments.some(
      (s) => s.compressed && abs > s.realStart && abs < s.realEnd,
    );
    if (inCompressed) continue;
    push(t);
  }

  // Resume-tick after every compressed segment. Picks the segment's end
  // (i.e. the start of the next natural region) so the user sees real time
  // continuing at the right edge of the sliver.
  for (const seg of axis.segments) {
    if (!seg.compressed) continue;
    push(seg.realEnd - origin);
  }

  ticks.sort((a, b) => a.displayMs - b.displayMs);
  return ticks;
}
