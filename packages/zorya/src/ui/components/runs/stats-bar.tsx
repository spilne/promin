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
  const { data } = useFetch(() => api.getMetrics(), [], 5000);
  const { data: health } = useFetch(() => api.getHealth(), [], 10_000);

  if (!data) {
    return (
      <div class="stats stats-horizontal bg-base-300 shadow w-full">
        {SKELETON_TITLES.map((t) => (
          <div class="stat">
            <div class="stat-title text-sm">{t}</div>
            <div class="pt-1">
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

  return (
    <div class="card bg-base-300 shadow">
      <div class="stats stats-horizontal bg-transparent">
        {stats.map((s) => {
          const clickable = !!s.filter && !!onPickStatus;
          return (
            <div
              class={`stat ${clickable ? "cursor-pointer hover:bg-base-200/50 transition-colors" : ""}`}
              onClick={() => clickable && onPickStatus?.(s.filter)}
            >
              <div class="stat-title text-sm">{s.label}</div>
              <div class={`stat-value text-2xl ${s.color}`}>{s.value}</div>
            </div>
          );
        })}
        <div class="stat">
          <div class="stat-title text-sm">Avg</div>
          <div class="stat-value text-2xl">{formatDuration(data.avgDurationMs)}</div>
        </div>
        <div class="stat">
          <div class="stat-title text-sm">p95</div>
          <div class="stat-value text-2xl">{formatDuration(data.p95DurationMs)}</div>
        </div>
        <div class="stat">
          <div class="stat-title text-sm">p99</div>
          <div class="stat-value text-2xl">{formatDuration(data.p99DurationMs)}</div>
        </div>
      </div>
      {health && (
        <div class="flex items-center gap-4 px-4 py-2 text-xs text-base-content/60 border-t border-base-content/10">
          <HealthDot label="API" ok={true} />
          <HealthDot label="Storage" ok={true} />
          <span class="text-base-content/30">·</span>
          <span>Zorya dashboard · {new Date().toLocaleTimeString()}</span>
        </div>
      )}
    </div>
  );
}

function HealthDot({ label, ok }: { label: string; ok: boolean }) {
  return (
    <div class="flex items-center gap-1.5">
      <span class={`w-2 h-2 rounded-full ${ok ? "bg-success" : "bg-error"}`} />
      <span>{label}</span>
    </div>
  );
}
