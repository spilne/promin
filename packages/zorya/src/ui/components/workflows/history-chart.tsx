// ---------------------------------------------------------------------------
// HistoryChart — per-workflow duration bar chart. Each bar is one run, x
// axis oldest→newest. Two modes:
//   - "total": one bar per run, height = total workflow duration. Fastest
//     read for "is the whole thing getting slower?"
//   - "stacked": per-step segments stacked, so a specific step slowing
//     down shows up as that segment fattening over time.
// Failed runs are tinted red. Hover gives a tooltip with numbers.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type { HistoryResponse, HistoryRunDto } from "../../../server/routes/grid.ts";
import { Skeleton } from "../ui/skeleton.tsx";
import { formatDuration, formatRelative } from "../../lib/format.ts";

interface HistoryChartProps {
  name: string;
  onOpenRun?: (workflowId: string) => void;
}

type Mode = "total" | "stacked";

// Cycled palette for step colors — DaisyUI-safe Tailwind classes. Failed
// runs get tinted red via stroke regardless of step color.
const STEP_COLORS = [
  "fill-info",
  "fill-success",
  "fill-warning",
  "fill-accent",
  "fill-secondary",
  "fill-primary",
];

const CHART_H = 180;
const BAR_GAP = 2;
const PADDING = { top: 10, right: 10, bottom: 28, left: 48 };

export function HistoryChart({ name, onOpenRun }: HistoryChartProps) {
  const [data, setData] = useState<HistoryResponse | undefined>();
  const [error, setError] = useState<string | undefined>();
  const [mode, setMode] = useState<Mode>("total");
  const [limit, setLimit] = useState(50);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const [wrapW, setWrapW] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setData(undefined);
    setError(undefined);
    api
      .getWorkflowHistory(name, limit)
      .then((r) => !cancelled && setData(r))
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, [name, limit]);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const obs = new ResizeObserver((entries) => {
      for (const e of entries) setWrapW(e.contentRect.width);
    });
    obs.observe(el);
    setWrapW(el.clientWidth);
    return () => obs.disconnect();
  }, []);

  const runs = data?.runs ?? [];
  const stepNames = data?.stepNames ?? [];
  const stats = useMemo(() => computeStats(runs), [runs]);

  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4 space-y-3">
        <div class="flex items-center gap-3 flex-wrap">
          <h3 class="card-title text-base">Duration history</h3>
          <span class="text-sm text-base-content/60">
            Last {runs.length} runs · avg {formatDuration(stats.avg)} · p95{" "}
            {formatDuration(stats.p95)}
            {stats.trend !== 0 && (
              <>
                {" "}
                · <TrendBadge value={stats.trend} />
              </>
            )}
          </span>
          <div class="flex-1" />
          <div class="join">
            <button
              class={`btn btn-xs join-item ${mode === "total" ? "btn-primary" : "btn-ghost"}`}
              onClick={() => setMode("total")}
            >
              Total
            </button>
            <button
              class={`btn btn-xs join-item ${mode === "stacked" ? "btn-primary" : "btn-ghost"}`}
              onClick={() => setMode("stacked")}
            >
              Per step
            </button>
          </div>
          <div class="join">
            {[25, 50, 100, 200].map((n) => (
              <button
                class={`btn btn-xs join-item ${limit === n ? "btn-primary" : "btn-ghost"}`}
                onClick={() => setLimit(n)}
              >
                {n}
              </button>
            ))}
          </div>
        </div>

        {error && <div class="alert alert-error text-sm">{error}</div>}

        <div ref={wrapRef} class="relative">
          {!data && !error && <Skeleton w="w-full" h={`h-[${CHART_H}px]`} />}
          {data && runs.length === 0 && (
            <div class="text-center py-8 text-base-content/50 text-sm">No runs yet.</div>
          )}
          {data && runs.length > 0 && wrapW > 0 && (
            <Chart
              runs={runs}
              stepNames={stepNames}
              mode={mode}
              width={wrapW}
              hoverIdx={hoverIdx}
              onHover={setHoverIdx}
              onOpenRun={onOpenRun}
            />
          )}
        </div>

        {mode === "stacked" && stepNames.length > 0 && (
          <div class="flex gap-2 flex-wrap text-xs pt-1 border-t border-base-content/10">
            {stepNames.map((s, i) => (
              <div class="flex items-center gap-1.5">
                <span
                  class={`w-3 h-3 rounded inline-block ${toBgFromFill(STEP_COLORS[i % STEP_COLORS.length]!)}`}
                />
                <span class="font-mono text-base-content/70">{s}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function Chart({
  runs,
  stepNames,
  mode,
  width,
  hoverIdx,
  onHover,
  onOpenRun,
}: {
  runs: readonly HistoryRunDto[];
  stepNames: readonly string[];
  mode: Mode;
  width: number;
  hoverIdx: number | null;
  onHover: (i: number | null) => void;
  onOpenRun?: (id: string) => void;
}) {
  const innerW = Math.max(100, width - PADDING.left - PADDING.right);
  const innerH = CHART_H - PADDING.top - PADDING.bottom;

  const maxY = Math.max(
    1,
    ...runs.map((r) =>
      mode === "total"
        ? (r.totalMs ?? 0)
        : Object.values(r.steps).reduce((s, x) => s + (x.durationMs ?? 0), 0),
    ),
  );

  const barW = Math.max(2, innerW / runs.length - BAR_GAP);
  const yForMs = (ms: number) => (ms / maxY) * innerH;

  // Gridlines at 25/50/75/100%.
  const ticks = [0.25, 0.5, 0.75, 1];

  return (
    <svg width={width} height={CHART_H} viewBox={`0 0 ${width} ${CHART_H}`} class="block w-full">
      {/* Y grid */}
      {ticks.map((t) => {
        const y = PADDING.top + innerH - t * innerH;
        return (
          <g>
            <line
              x1={PADDING.left}
              x2={PADDING.left + innerW}
              y1={y}
              y2={y}
              class="stroke-base-content/10"
              stroke-dasharray="2 3"
            />
            <text
              x={PADDING.left - 6}
              y={y + 3}
              text-anchor="end"
              font-size={10}
              class="fill-base-content/40"
            >
              {formatDuration(Math.round(maxY * t))}
            </text>
          </g>
        );
      })}

      {/* Bars */}
      {runs.map((r, i) => {
        const x = PADDING.left + i * (barW + BAR_GAP);
        const isHover = hoverIdx === i;
        const failed = r.status === "failed" || r.status === "tripwire";
        if (mode === "total") {
          const h = yForMs(r.totalMs ?? 0);
          const y = PADDING.top + innerH - h;
          const fill = failed ? "fill-error" : "fill-info";
          return (
            <g
              onMouseEnter={() => onHover(i)}
              onMouseLeave={() => onHover(null)}
              onClick={() => onOpenRun?.(r.workflowId)}
              class="cursor-pointer"
            >
              <rect
                x={x}
                y={y}
                width={barW}
                height={Math.max(0, h)}
                class={`${fill} transition-opacity ${isHover ? "opacity-100" : "opacity-85"}`}
                rx={1}
              />
            </g>
          );
        }
        // Stacked
        let offset = 0;
        return (
          <g
            onMouseEnter={() => onHover(i)}
            onMouseLeave={() => onHover(null)}
            onClick={() => onOpenRun?.(r.workflowId)}
            class="cursor-pointer"
          >
            {stepNames.map((sn, si) => {
              const s = r.steps[sn];
              const d = s?.durationMs ?? 0;
              if (d <= 0) return null;
              const h = yForMs(d);
              const y = PADDING.top + innerH - offset - h;
              offset += h;
              const color = STEP_COLORS[si % STEP_COLORS.length]!;
              return (
                <rect
                  x={x}
                  y={y}
                  width={barW}
                  height={Math.max(0, h)}
                  class={`${color} transition-opacity ${isHover ? "opacity-100" : "opacity-80"}`}
                />
              );
            })}
            {failed && (
              <rect
                x={x}
                y={PADDING.top}
                width={barW}
                height={innerH}
                class="fill-none stroke-error"
                stroke-width={1}
                pointer-events="none"
              />
            )}
          </g>
        );
      })}

      {/* Hover tooltip */}
      {hoverIdx !== null && runs[hoverIdx] && (
        <Tooltip
          run={runs[hoverIdx]!}
          stepNames={stepNames}
          x={PADDING.left + hoverIdx * (barW + BAR_GAP) + barW}
          containerW={width}
          mode={mode}
        />
      )}

      {/* X axis hint: first + last timestamp */}
      {runs.length > 0 && (
        <g font-size={10} class="fill-base-content/40">
          <text x={PADDING.left} y={CHART_H - 8}>
            {formatRelative(runs[0]!.createdAt)}
          </text>
          <text x={PADDING.left + innerW} y={CHART_H - 8} text-anchor="end">
            {formatRelative(runs[runs.length - 1]!.createdAt)}
          </text>
        </g>
      )}
    </svg>
  );
}

function Tooltip({
  run,
  stepNames,
  x,
  containerW,
  mode,
}: {
  run: HistoryRunDto;
  stepNames: readonly string[];
  x: number;
  containerW: number;
  mode: Mode;
}) {
  // Flip to left of the bar when we're near the right edge.
  const TOOLTIP_W = 220;
  const flipped = x + TOOLTIP_W + 8 > containerW;
  const tx = flipped ? x - TOOLTIP_W - 4 : x + 8;
  const lines =
    mode === "stacked"
      ? stepNames
          .map((s) => ({
            name: s,
            ms: run.steps[s]?.durationMs ?? 0,
            status: run.steps[s]?.status,
          }))
          .filter((l) => l.ms > 0)
      : [];
  const height = 40 + 14 + lines.length * 14;

  return (
    <g>
      <rect
        x={tx}
        y={PADDING.top}
        width={TOOLTIP_W}
        height={Math.min(CHART_H - PADDING.top - 4, height)}
        rx={4}
        class="fill-base-100 stroke-base-content/30"
        stroke-width={1}
      />
      <text
        x={tx + 8}
        y={PADDING.top + 14}
        font-size={11}
        font-family="ui-monospace, monospace"
        class="fill-base-content font-semibold"
      >
        {run.workflowId.slice(0, 18)}…
      </text>
      <text
        x={tx + 8}
        y={PADDING.top + 28}
        font-size={10}
        class={`${run.status === "failed" ? "fill-error" : "fill-base-content/70"}`}
      >
        {run.status}
        {run.totalMs !== undefined ? ` · ${formatDuration(run.totalMs)}` : ""}
      </text>
      <text x={tx + 8} y={PADDING.top + 42} font-size={10} class="fill-base-content/50">
        {formatRelative(run.createdAt)}
      </text>
      {lines.slice(0, 8).map((l, i) => (
        <text
          x={tx + 8}
          y={PADDING.top + 58 + i * 12}
          font-size={10}
          font-family="ui-monospace, monospace"
          class="fill-base-content/70"
        >
          {l.name.slice(0, 18)} · {formatDuration(l.ms)}
        </text>
      ))}
    </g>
  );
}

function TrendBadge({ value }: { value: number }) {
  const rising = value > 0;
  const severe = Math.abs(value) > 0.25;
  const pct = `${rising ? "+" : ""}${Math.round(value * 100)}%`;
  const color = rising ? (severe ? "text-error" : "text-warning") : "text-success";
  const arrow = rising ? "↑" : "↓";
  return (
    <span class={`font-mono ${color}`} title="Median of last-5 vs last-20">
      {arrow} {pct}
    </span>
  );
}

interface HistoryStats {
  avg: number;
  p95: number;
  /** (median of last 5) / (median of last 20) - 1. Positive = slower recently. */
  trend: number;
}

function computeStats(runs: readonly HistoryRunDto[]): HistoryStats {
  const durations = runs.map((r) => r.totalMs ?? 0).filter((d) => d > 0);
  if (durations.length === 0) return { avg: 0, p95: 0, trend: 0 };
  const avg = durations.reduce((s, d) => s + d, 0) / durations.length;
  const sorted = durations.slice().sort((a, b) => a - b);
  const p95 = sorted[Math.floor(sorted.length * 0.95)] ?? sorted[sorted.length - 1]!;
  // Newer runs are at the END of the array (oldest-first from API).
  const tail = (n: number) => durations.slice(-n);
  const recent = median(tail(5));
  const baseline = median(tail(20));
  const trend = baseline > 0 ? recent / baseline - 1 : 0;
  return { avg, p95, trend };
}

function median(xs: readonly number[]): number {
  if (xs.length === 0) return 0;
  const sorted = xs.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1]! + sorted[mid]!) / 2 : sorted[mid]!;
}

/** Tailwind fill-* → bg-* for the legend swatch. */
function toBgFromFill(fillClass: string): string {
  return fillClass.replace(/^fill-/, "bg-");
}
