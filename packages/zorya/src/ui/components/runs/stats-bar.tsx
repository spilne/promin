import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import { formatDuration } from "../../lib/format.ts";
import { Skeleton } from "../ui/skeleton.tsx";

const SKELETON_TITLES = [
  "Total",
  "Completed",
  "Running",
  "Failed",
  "Suspended",
  "Avg",
  "p95",
  "p99",
];

interface StatsBarProps {
  /** When provided, stat cards become buttons that filter the table. */
  onPickStatus?: (status: "completed" | "running" | "failed" | "suspended" | null) => void;
}

export function StatsBar({ onPickStatus }: StatsBarProps = {}) {
  // Metrics aggregate every run in storage: refresh them on a slower cadence
  // than the run list itself.
  const { data } = useFetch(() => api.getMetrics(), [], 15_000);
  const { data: health } = useFetch(() => api.getHealth(), [], 10_000);

  if (!data) {
    return (
      <div class="grid grid-cols-2 gap-2 md:grid-cols-4 xl:grid-cols-8">
        {SKELETON_TITLES.map((t) => (
          <div class="rounded border border-base-content/10 bg-base-300/80 p-3">
            <div class="text-[11px] uppercase tracking-[0.12em] text-base-content/45">{t}</div>
            <div class="pt-2">
              <Skeleton w="w-14" h="h-7" />
            </div>
          </div>
        ))}
      </div>
    );
  }

  const stats = [
    { label: "Total", value: data.total, color: "", filter: null },
    {
      label: "Completed",
      value: data.byStatus.completed,
      color: "text-success",
      filter: "completed" as const,
    },
    {
      label: "Running",
      value: data.byStatus.running,
      color: "text-info",
      filter: "running" as const,
    },
    {
      label: "Failed",
      value: data.byStatus.failed,
      color: "text-error",
      filter: "failed" as const,
    },
    {
      label: "Suspended",
      value: data.byStatus.suspended,
      color: "text-warning",
      filter: "suspended" as const,
    },
  ];

  const durations = [
    { label: "Avg", value: data.avgDurationMs },
    { label: "p95", value: data.p95DurationMs },
    { label: "p99", value: data.p99DurationMs },
  ];

  return (
    <div class="rounded border border-base-content/10 bg-base-300/70 shadow-sm overflow-hidden">
      <div class="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-8">
        {stats.map((s) => {
          const body = (
            <>
              <StatLabel>{s.label}</StatLabel>
              <div class={`mt-2 text-2xl font-semibold leading-none ${s.color}`}>{s.value}</div>
            </>
          );
          // A card that filters the table is a real button (focusable,
          // keyboard-operable); the others are plain cells.
          return s.filter && onPickStatus ? (
            <button
              type="button"
              class={`${CELL_CLASS} block w-full text-left hover:bg-base-100/70 transition-colors`}
              onClick={() => onPickStatus(s.filter)}
              title={`Show ${s.label.toLowerCase()} runs`}
            >
              {body}
            </button>
          ) : (
            <div class={CELL_CLASS}>{body}</div>
          );
        })}
        {durations.map((d) => (
          <div class={CELL_CLASS}>
            <StatLabel>{d.label}</StatLabel>
            <div class="mt-2 text-2xl font-semibold leading-none">{formatDuration(d.value)}</div>
          </div>
        ))}
      </div>
      {health && (
        <div class="flex flex-wrap items-center gap-3 px-4 py-2 text-xs text-base-content/60 border-t border-base-content/10">
          <HealthDot label="API" ok={true} />
          <HealthDot label="Storage" ok={true} />
          <span class="h-3 w-px bg-base-content/15" />
          <span>Updated {new Date().toLocaleTimeString()}</span>
        </div>
      )}
    </div>
  );
}

/** One stat cell: bordered so the grid reads as a table when it wraps. */
const CELL_CLASS = "min-h-[5.75rem] border-b border-r border-base-content/10 p-3 xl:border-b-0";

function StatLabel({ children }: { children: string }) {
  return <div class="text-[11px] uppercase tracking-[0.12em] text-base-content/45">{children}</div>;
}

function HealthDot({ label, ok }: { label: string; ok: boolean }) {
  return (
    <div class="flex items-center gap-1.5">
      <span class={`w-2 h-2 rounded-full ${ok ? "bg-success" : "bg-error"}`} />
      <span>{label}</span>
    </div>
  );
}
