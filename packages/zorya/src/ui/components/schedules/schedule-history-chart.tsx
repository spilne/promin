// ---------------------------------------------------------------------------
// ScheduleHistoryChart — scatter plot of past fires.
//
// X axis: time (firedAt). Y axis: duration in ms (log-ish via sqrt-scaled).
// Color: workflow status. Each point is one tick — hover for the details.
//
// Why scatter rather than a bar / step chart: a fire is a discrete event, not
// a continuous level, and the two questions you ask about a schedule are:
//   1. "Did fires happen at the cadence we expect?"  → x-axis spacing
//   2. "Are durations stable / failures clustered?"  → y-axis + color
// Scatter answers both at once. Pure SVG, no chart lib.
// ---------------------------------------------------------------------------

import type { ScheduleTickHistoryDto } from "../../../server/routes/schedules.ts";
import type { WorkflowStatus } from "@promin/workflow";
import { WORKFLOW_STATUS_VISUAL, formatDuration, formatRelative } from "../../lib/format.ts";

// SVG `<circle fill>` needs an actual color, not the `bg-*` Tailwind class
// (which sets background-color, no-op for SVG). Map to DaisyUI's CSS
// variables so the chart respects the active theme automatically.
const STATUS_FILL: Record<string, string> = {
  pending: "var(--color-base-content, #999)",
  running: "var(--color-info, #3abff8)",
  suspended: "var(--color-warning, #fbbd23)",
  completed: "var(--color-success, #36d399)",
  failed: "var(--color-error, #f87272)",
  compensating: "var(--color-warning, #fbbd23)",
  tripwire: "var(--color-error, #f87272)",
};
const fillFor = (status: string): string => STATUS_FILL[status] ?? STATUS_FILL.pending!;

interface ScheduleHistoryChartProps {
  history: ScheduleTickHistoryDto[];
  /** Optional extra height; defaults to 200. */
  height?: number;
}

const PAD_LEFT = 40;
const PAD_RIGHT = 16;
const PAD_TOP = 16;
const PAD_BOTTOM = 28;

export function ScheduleHistoryChart({ history, height = 200 }: ScheduleHistoryChartProps) {
  if (history.length === 0) {
    return <div class="text-sm text-base-content/40 p-4 text-center">No fires yet</div>;
  }

  // Reverse so points draw left-to-right (oldest → newest); the table view
  // is newest-first, but a chart reads better in chronological order.
  const points = [...history].reverse().map((h) => {
    const t = h.firedAt ?? h.startedAt;
    return {
      h,
      timeMs: t ? new Date(t).getTime() : 0,
      lagMs: h.lagMs ?? 0,
      durationMs: h.durationMs ?? 0,
    };
  });
  const tMin = points[0]!.timeMs;
  const tMax = points[points.length - 1]!.timeMs || tMin + 1;
  const tSpan = Math.max(tMax - tMin, 1);
  // Sqrt-compressed Y so a single 30s outlier doesn't squash all the
  // 200ms-ish runs to the bottom row. Y axis is "elapsed since firedAt"
  // — both lag (queue wait) and duration (total) live on it, so the
  // scale must cover whichever value is largest across all points.
  const dMax = Math.max(1, ...points.map((p) => Math.sqrt(Math.max(p.durationMs, p.lagMs))));

  const width = 720;
  const innerW = width - PAD_LEFT - PAD_RIGHT;
  const innerH = height - PAD_TOP - PAD_BOTTOM;
  const xFor = (t: number) => PAD_LEFT + ((t - tMin) / tSpan) * innerW;
  const yFor = (d: number) =>
    // Even with d=0, we still want the dot above the axis so it's visible.
    PAD_TOP + innerH - (Math.sqrt(d) / dMax) * (innerH - 8) - 4;

  // Y-axis ticks: 0 / mid / max
  const yTicks = [0, Math.round((dMax * dMax) / 2), Math.round(dMax * dMax)];

  return (
    <div class="overflow-x-auto">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="xMidYMid meet"
        class="w-full"
        style={{ minWidth: "640px", height: `${height}px` }}
      >
        {/* Axes */}
        <line
          x1={PAD_LEFT}
          y1={PAD_TOP}
          x2={PAD_LEFT}
          y2={height - PAD_BOTTOM}
          stroke="currentColor"
          stroke-opacity="0.15"
        />
        <line
          x1={PAD_LEFT}
          y1={height - PAD_BOTTOM}
          x2={width - PAD_RIGHT}
          y2={height - PAD_BOTTOM}
          stroke="currentColor"
          stroke-opacity="0.15"
        />

        {/* Y-axis ticks (duration) */}
        {yTicks.map((d) => {
          const y = yFor(d);
          return (
            <g>
              <line
                x1={PAD_LEFT - 3}
                x2={PAD_LEFT}
                y1={y}
                y2={y}
                stroke="currentColor"
                stroke-opacity="0.3"
              />
              <text
                x={PAD_LEFT - 6}
                y={y + 3}
                text-anchor="end"
                font-size="9"
                fill="currentColor"
                fill-opacity="0.5"
              >
                {d === 0 ? "0" : formatDuration(d)}
              </text>
            </g>
          );
        })}

        {/* X-axis labels — first / last fire, plus midpoint for orientation */}
        {[tMin, tMin + tSpan / 2, tMax].map((t) => {
          const x = xFor(t);
          return (
            <text
              x={x}
              y={height - 10}
              text-anchor={t === tMin ? "start" : t === tMax ? "end" : "middle"}
              font-size="9"
              fill="currentColor"
              fill-opacity="0.5"
            >
              {formatRelative(new Date(t).toISOString())}
            </text>
          );
        })}

        {/* Connecting line — light, just to show cadence visually. Hidden
            when we only have one point (no line to draw). */}
        {points.length > 1 && (
          <polyline
            points={points.map((p) => `${xFor(p.timeMs)},${yFor(p.durationMs)}`).join(" ")}
            fill="none"
            stroke="currentColor"
            stroke-opacity="0.12"
            stroke-width="1"
          />
        )}

        {/* Per-fire glyph — a stacked vertical bar from the X axis up to
            durationMs:
              [axis ── lagMs]   queue wait   (faint neutral)
              [lagMs ── durMs]  execution    (status color)
              dot at durMs                   (status color, solid)
            For in-progress runs (durationMs undefined) we draw only the
            queue segment plus a hollow dot at the lag position so it
            still reads as "this fire is in flight." */}
        {points.map((p) => {
          const v = WORKFLOW_STATUS_VISUAL[p.h.status as WorkflowStatus];
          const cx = xFor(p.timeMs);
          const cyAxis = yFor(0);
          const cyDur = yFor(p.durationMs);
          const cyLag = yFor(p.lagMs);
          const haveLag = p.h.lagMs !== undefined;
          const haveDur = p.h.durationMs !== undefined;
          return (
            <g class="cursor-default">
              {/* Queue-wait segment: axis → lagMs. Always under the
                  execution bar so the visual hierarchy reads "queue at
                  the bottom, execution on top." */}
              {haveLag && (
                <line
                  x1={cx}
                  y1={cyAxis}
                  x2={cx}
                  y2={cyLag}
                  stroke="currentColor"
                  stroke-opacity="0.45"
                  stroke-width="3"
                  stroke-linecap="round"
                />
              )}
              {/* Execution segment: lagMs → durationMs. Only when the run
                  actually finished — for still-running runs the bar
                  stops at the lag tick. */}
              {haveLag && haveDur && (
                <line
                  x1={cx}
                  y1={cyLag}
                  x2={cx}
                  y2={cyDur}
                  stroke={fillFor(p.h.status)}
                  stroke-opacity="0.7"
                  stroke-width="3"
                  stroke-linecap="round"
                />
              )}
              {/* Boundary tick — sits between the queue and execution
                  segments so the join point is clearly visible even when
                  one of them is very short. */}
              {haveLag && (
                <line
                  x1={cx - 5}
                  y1={cyLag}
                  x2={cx + 5}
                  y2={cyLag}
                  stroke="currentColor"
                  stroke-opacity="0.85"
                  stroke-width="1.5"
                />
              )}
              {/* Cap dot — at the duration top for completed runs, or a
                  hollow circle at the lag tick for still-running ones. */}
              {haveDur ? (
                <circle cx={cx} cy={cyDur} r="4" fill={fillFor(p.h.status)} />
              ) : haveLag ? (
                <circle
                  cx={cx}
                  cy={cyLag}
                  r="3.5"
                  fill="none"
                  stroke={fillFor(p.h.status)}
                  stroke-width="1.5"
                />
              ) : (
                <circle cx={cx} cy={cyAxis - 4} r="3" fill={fillFor(p.h.status)} />
              )}
              {/* Hover hit-zone covering the whole glyph + tooltip. */}
              <rect
                x={cx - 6}
                y={Math.min(cyDur, cyLag, cyAxis) - 4}
                width="12"
                height={Math.max(cyAxis, cyDur, cyLag) - Math.min(cyDur, cyLag, cyAxis) + 8}
                fill="transparent"
              >
                <title>
                  #{p.h.tickNumber ?? "?"} · {v?.label ?? p.h.status}
                  {p.h.lagMs !== undefined ? ` · lag ${formatDuration(p.h.lagMs)}` : ""}
                  {p.h.durationMs !== undefined ? ` · dur ${formatDuration(p.h.durationMs)}` : ""}
                  {p.h.firedAt ? ` · ${formatRelative(p.h.firedAt)}` : ""}
                </title>
              </rect>
            </g>
          );
        })}
      </svg>

      {/* Legend — statuses + the lag glyph. Statuses use the actual token
          colors so they line up with the dots; the lag tick is a neutral
          horizontal line so it stays visually distinct from the status
          colors above. */}
      <div class="flex flex-wrap gap-3 px-2 pt-2 text-xs">
        {Array.from(new Set(history.map((h) => h.status))).map((status) => {
          const v = WORKFLOW_STATUS_VISUAL[status as WorkflowStatus];
          return (
            <div class="flex items-center gap-1.5">
              <span class="w-2 h-2 rounded-full" style={{ backgroundColor: fillFor(status) }} />
              <span class="text-base-content/60">{v?.label ?? status}</span>
            </div>
          );
        })}
        {history.some((h) => h.lagMs !== undefined) && (
          <div class="flex items-center gap-1.5" title="Queue wait — startedAt − firedAt">
            <span class="block w-3 h-px bg-base-content/60" />
            <span class="text-base-content/60">Lag</span>
          </div>
        )}
      </div>
    </div>
  );
}
