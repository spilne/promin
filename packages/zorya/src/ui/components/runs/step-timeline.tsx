import type { RunDto, StepDto } from "../../../server/api-types.ts";
import { formatDuration } from "../../lib/format.ts";
import { stepBarColor } from "../../lib/format.ts";

interface StepTimelineProps {
  run: RunDto;
}

/**
 * Gantt-style step timeline. X axis is time, bars are steps.
 *
 * - Each step bar starts at `step.startedAt - run.createdAt` and has width
 *   equal to its observed duration.
 * - Sleep / waiting_for_signal steps use the same layout but a hatched tile
 *   pattern to distinguish "waiting" from "running".
 * - Running / pending steps extend to `now`, so the timeline animates while
 *   the workflow is live.
 */
export function StepTimeline({ run }: StepTimelineProps) {
  const steps = run.steps.slice().sort(sortByStart);
  const origin = toMs(run.startedAt ?? run.createdAt);
  const endMs = run.completedAt ? toMs(run.completedAt) : Date.now();
  const totalMs = Math.max(1, endMs - origin);

  // Build tick marks at round intervals.
  const ticks = buildTicks(totalMs);

  if (steps.length === 0) {
    return (
      <div class="card bg-base-100 shadow">
        <div class="card-body">
          <h3 class="card-title text-base">Timeline</h3>
          <div class="text-base-content/50 py-8 text-center">No steps executed yet</div>
        </div>
      </div>
    );
  }

  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4">
        <div class="flex items-center gap-4 mb-3">
          <h3 class="card-title text-base">Timeline</h3>
          <span class="text-xs text-base-content/60">{formatDuration(totalMs)} total</span>
          <div class="flex-1" />
          <Legend />
        </div>

        {/* Axis */}
        <div class="relative h-5 border-b border-base-content/10 mb-1 ml-40">
          {ticks.map((t) => (
            <div
              class="absolute top-0 h-full text-[10px] text-base-content/50"
              style={{ left: `${(t / totalMs) * 100}%` }}
            >
              <div class="w-px h-2 bg-base-content/20" />
              <div class="-translate-x-1/2">{formatDuration(t)}</div>
            </div>
          ))}
        </div>

        {/* Rows */}
        <div class="space-y-1">
          {steps.map((s) => (
            <StepRow step={s} origin={origin} totalMs={totalMs} />
          ))}
        </div>
      </div>
    </div>
  );
}

interface StepRowProps {
  step: StepDto;
  origin: number;
  totalMs: number;
}

function StepRow({ step, origin, totalMs }: StepRowProps) {
  const start = step.startedAt ? toMs(step.startedAt) - origin : 0;
  const end = step.completedAt
    ? toMs(step.completedAt) - origin
    : step.status === "pending"
      ? start
      : Date.now() - origin;
  const width = Math.max(2, end - start);
  const leftPct = (Math.max(0, start) / totalMs) * 100;
  const widthPct = Math.max(0.5, (width / totalMs) * 100);
  const isHatched = step.status === "sleeping" || step.status === "waiting_for_signal";
  const barClass = stepBarColor[step.status];

  const tooltip = [
    `${step.stepName} (${step.status})`,
    step.durationMs !== undefined ? `duration ${formatDuration(step.durationMs)}` : "",
    step.attempt > 1 ? `attempt ${step.attempt}` : "",
  ]
    .filter(Boolean)
    .join(" · ");

  return (
    <div class="flex items-center gap-2 h-6 group">
      <div
        class="w-40 shrink-0 truncate text-xs font-mono text-base-content/80"
        title={step.stepName}
      >
        {step.stepName}
      </div>
      <div class="relative flex-1 h-full bg-base-200 rounded">
        <div
          class={`absolute top-0.5 bottom-0.5 rounded ${barClass} ${isHatched ? "gantt-hatched" : ""} transition-all`}
          style={{
            left: `${leftPct}%`,
            width: `${widthPct}%`,
          }}
          title={tooltip}
        />
        <div
          class="absolute top-0 h-full flex items-center text-[10px] text-base-content/70 px-1 pointer-events-none"
          style={{
            left: `${Math.min(95, leftPct + widthPct)}%`,
          }}
        >
          {step.durationMs !== undefined ? formatDuration(step.durationMs) : ""}
        </div>
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
    <div class="flex gap-3 text-xs">
      {items.map((i) => (
        <div class="flex items-center gap-1.5">
          <span class={`w-3 h-3 rounded ${i.class}`} />
          <span class="text-base-content/60">{i.label}</span>
        </div>
      ))}
    </div>
  );
}

function toMs(iso: string): number {
  return new Date(iso).getTime();
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
