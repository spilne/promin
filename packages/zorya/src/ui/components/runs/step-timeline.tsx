import { useState } from "preact/hooks";
import type { RunDto, StepDto } from "../../../server/api-types.ts";
import {
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

  const ticks = buildTicks(totalMs);

  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4 space-y-3">
        <div class="flex items-center gap-4">
          <h3 class="card-title text-base">Timeline</h3>
          <span class="text-sm text-base-content/60">{formatDuration(totalMs)} total</span>
          <span class="text-sm text-base-content/40">
            · {run.steps.length} {run.steps.length === 1 ? "step" : "steps"}
          </span>
          <div class="flex-1" />
          <Legend />
        </div>

        {/* Column header + time axis */}
        <div class="flex items-center gap-2 text-xs text-base-content/50 border-b border-base-content/10 pb-1">
          <div class="w-72 shrink-0">STEP</div>
          <div class="w-20 shrink-0 text-right pr-2">DURATION</div>
          <div class="relative flex-1 h-5">
            {ticks.map((t) => (
              <div class="absolute top-0 h-full" style={{ left: `${(t / totalMs) * 100}%` }}>
                <div class="w-px h-2 bg-base-content/20" />
                <div class="-translate-x-1/2 mt-0.5">{formatDuration(t)}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Rows */}
        <div class="space-y-0.5">
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
                totalMs={totalMs}
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
  totalMs: number;
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
  totalMs,
  isSelected,
  onSelect,
}: StepRowProps) {
  const start = step.startedAt ? toMs(step.startedAt) - origin : 0;
  // Prefer startedAt + durationMs (actual execution window) over completedAt
  // (persistence timestamp, which can be identical for batch-saved steps).
  const end = stepEndMs(step, origin);
  const width = Math.max(2, end - start);
  const leftPct = (Math.max(0, start) / totalMs) * 100;
  const widthPct = Math.max(0.5, (width / totalMs) * 100);
  const v = STEP_STATUS_VISUAL[step.status];
  const isHatched = step.status === "sleeping" || step.status === "waiting_for_signal";

  const tooltip = [
    `${step.stepName} (${v.label})`,
    step.durationMs !== undefined ? `duration ${formatDuration(step.durationMs)}` : "",
    step.attempt > 1 ? `attempt ${step.attempt}` : "",
    step.startedAt ? `started ${formatRelative(step.startedAt)}` : "",
  ]
    .filter(Boolean)
    .join(" · ");

  return (
    <div
      class={`flex items-center gap-2 h-9 px-1 rounded cursor-pointer transition-colors duration-150 ${
        isSelected ? "bg-primary/10 ring-1 ring-primary/40" : "hover:bg-base-200"
      }`}
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

      {/* Duration */}
      <div class="w-20 shrink-0 text-right pr-2 font-mono text-xs text-base-content/70">
        {formatDuration(step.durationMs)}
      </div>

      {/* Bar */}
      <div class="relative flex-1 h-full" title={tooltip}>
        <div class="absolute inset-y-1 left-0 right-0 bg-base-200/50 rounded" />
        <div
          class={`gantt-bar absolute top-1 bottom-1 rounded ${v.barClass} ${isHatched ? "gantt-hatched" : ""}`}
          style={{
            left: `${leftPct}%`,
            width: `${widthPct}%`,
          }}
        />
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

function stepEndMs(step: StepDto, origin: number): number {
  // Actual execution end = startedAt + durationMs. Fall back to completedAt
  // or "now" (for still-running steps) when duration isn't known.
  if (step.startedAt && step.durationMs !== undefined) {
    return toMs(step.startedAt) + step.durationMs - origin;
  }
  if (step.completedAt) return toMs(step.completedAt) - origin;
  if (step.status === "pending") {
    return step.startedAt ? toMs(step.startedAt) - origin : 0;
  }
  return Date.now() - origin;
}

function sortByStart(a: StepDto, b: StepDto): number {
  const ax = a.startedAt ? toMs(a.startedAt) : Number.MAX_SAFE_INTEGER;
  const bx = b.startedAt ? toMs(b.startedAt) : Number.MAX_SAFE_INTEGER;
  return ax - bx;
}

function buildTicks(totalMs: number): number[] {
  const target = 5;
  const rawStep = totalMs / target;
  const magnitude = Math.pow(10, Math.floor(Math.log10(Math.max(1, rawStep))));
  const normalized = rawStep / magnitude;
  const stepNice = normalized < 1.5 ? 1 : normalized < 3 ? 2 : normalized < 7 ? 5 : 10;
  const step = stepNice * magnitude;
  const ticks: number[] = [];
  for (let t = 0; t <= totalMs; t += step) ticks.push(t);
  return ticks;
}
