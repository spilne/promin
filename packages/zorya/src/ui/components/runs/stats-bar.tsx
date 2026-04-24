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

export function StatsBar() {
  const { data } = useFetch(() => api.getMetrics(), [], 5000);
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
    { label: "Total", value: data.total, color: "" },
    { label: "Completed", value: data.byStatus.completed, color: "text-success" },
    { label: "Running", value: data.byStatus.running, color: "text-info" },
    { label: "Failed", value: data.byStatus.failed, color: "text-error" },
    { label: "Suspended", value: data.byStatus.suspended, color: "text-warning" },
  ];

  return (
    <div class="stats stats-horizontal bg-base-300 shadow w-full">
      {stats.map((s) => (
        <div class="stat">
          <div class="stat-title text-sm">{s.label}</div>
          <div class={`stat-value text-2xl ${s.color}`}>{s.value}</div>
        </div>
      ))}
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
  );
}
