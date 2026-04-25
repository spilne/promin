import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { WorkerDto } from "../../../server/api-types.ts";
import { formatDuration, formatRelative } from "../../lib/format.ts";
import { Skeleton } from "../ui/skeleton.tsx";

interface WorkerGridProps {
  onOpenRun?: (id: string) => void;
}

export function WorkerGrid({ onOpenRun }: WorkerGridProps = {}) {
  const { data, loading, error } = useFetch(() => api.listWorkers(), [], 5000);

  if (loading && !data) {
    return (
      <div class="anim-page p-4 max-w-7xl mx-auto space-y-4">
        <h2 class="text-xl font-semibold">Workers</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
          {Array.from({ length: 2 }).map(() => (
            <div class="card bg-base-100 shadow">
              <div class="card-body p-4 space-y-2">
                <Skeleton w="w-32" h="h-4" />
                <Skeleton w="w-24" h="h-3" />
                <Skeleton w="w-full" h="h-3" />
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }
  if (error) {
    return (
      <div class="p-4 max-w-7xl mx-auto">
        <div class="alert alert-error">{error.message}</div>
      </div>
    );
  }
  const workers = data?.workers ?? [];
  const online = workers.filter((w) => w.status === "online").length;
  const offline = workers.length - online;

  return (
    <div class="anim-page p-4 max-w-7xl mx-auto space-y-4">
      <div class="flex items-center gap-2">
        <h2 class="text-xl font-semibold">Workers</h2>
        <span class="text-base-content/60">
          · {online} online
          {offline > 0 && `, ${offline} offline`}
        </span>
      </div>

      {workers.length === 0 && (
        <div class="card bg-base-100 shadow">
          <div class="card-body py-8 text-center text-base-content/50">No workers registered</div>
        </div>
      )}

      <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
        {workers.map((w) => (
          <WorkerCard worker={w} onOpenRun={onOpenRun} />
        ))}
      </div>
    </div>
  );
}

function WorkerCard({
  worker,
  onOpenRun,
}: {
  worker: WorkerDto;
  onOpenRun?: (id: string) => void;
}) {
  const online = worker.status === "online";
  const labels = worker.labels ?? {};
  const labelEntries = Object.entries(labels);

  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4 space-y-3">
        {/* Header */}
        <div class="flex items-center gap-2 flex-wrap">
          <span class={`w-2.5 h-2.5 rounded-full ${online ? "bg-success" : "bg-error"}`} />
          <span class="font-mono text-sm truncate" title={worker.workerId}>
            {worker.workerId}
          </span>
          {worker.version && (
            <span class="badge badge-sm badge-ghost font-mono">v{worker.version}</span>
          )}
          {worker.runtime && <span class="badge badge-sm badge-ghost">{worker.runtime}</span>}
          <div class="flex-1" />
          {worker.hostname && (
            <span class="text-xs text-base-content/60 font-mono">{worker.hostname}</span>
          )}
        </div>

        {/* Stats row */}
        <div class="grid grid-cols-4 gap-1 text-center">
          <Stat label="Active" value={worker.activeTasks} />
          <Stat label="Done" value={worker.completedCount ?? worker.completedToday} />
          <Stat
            label="Failed"
            value={worker.failedCount ?? 0}
            valueClass={worker.failedCount ? "text-error" : ""}
          />
          <Stat label="Concurrency" value={worker.concurrency ?? "—"} />
        </div>

        {/* Capabilities + workflows + labels */}
        <div class="space-y-1 text-xs">
          {worker.capabilities && worker.capabilities.length > 0 && (
            <TagRow label="Capabilities" tags={worker.capabilities} />
          )}
          {worker.workflowNames && worker.workflowNames.length > 0 && (
            <TagRow label="Workflows" tags={worker.workflowNames} />
          )}
          {worker.namespaces && worker.namespaces.length > 0 && (
            <TagRow label="Namespaces" tags={worker.namespaces} />
          )}
          {labelEntries.length > 0 && (
            <div class="flex items-start gap-2">
              <span class="text-base-content/50 w-24 shrink-0">Labels</span>
              <div class="flex flex-wrap gap-1">
                {labelEntries.map(([k, v]) => (
                  <span class="badge badge-sm badge-ghost font-mono">
                    {k}={v}
                  </span>
                ))}
              </div>
            </div>
          )}
          <div class="flex items-start gap-2 text-base-content/50">
            <span class="w-24 shrink-0">Since</span>
            <span>{formatRelative(worker.startedAt)}</span>
          </div>
          <div class="flex items-start gap-2 text-base-content/50">
            <span class="w-24 shrink-0">Last seen</span>
            <span>{formatRelative(worker.lastHeartbeatAt)}</span>
          </div>
        </div>

        {/* Active runs */}
        {worker.activeRuns && worker.activeRuns.length > 0 && (
          <div class="space-y-1">
            <div class="text-xs font-semibold text-base-content/50 uppercase tracking-wide">
              Active runs
            </div>
            <div class="space-y-0.5">
              {worker.activeRuns.map((r) => (
                <button
                  class="w-full flex items-center gap-2 p-1 hover:bg-base-200 rounded text-left text-xs"
                  onClick={() => onOpenRun?.(r.workflowId)}
                  disabled={!onOpenRun}
                >
                  <span class="w-2 h-2 rounded-full bg-info animate-pulse" />
                  <span class="font-mono truncate flex-1" title={r.workflowId}>
                    {r.workflowId}
                  </span>
                  <span class="text-base-content/60">{r.workflowName}</span>
                  <span class="text-base-content/50">{formatRelative(r.startedAt)}</span>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Recent runs */}
        {worker.recentRuns && worker.recentRuns.length > 0 && (
          <div class="space-y-1">
            <div class="text-xs font-semibold text-base-content/50 uppercase tracking-wide">
              Recent
            </div>
            <div class="space-y-0.5 max-h-32 overflow-y-auto">
              {worker.recentRuns.slice(0, 10).map((r) => (
                <button
                  class="w-full flex items-center gap-2 p-1 hover:bg-base-200 rounded text-left text-xs"
                  onClick={() => onOpenRun?.(r.workflowId)}
                  disabled={!onOpenRun}
                >
                  <span
                    class={`w-2 h-2 rounded-full ${r.status === "completed" ? "bg-success" : "bg-error"}`}
                  />
                  <span class="font-mono truncate flex-1" title={r.workflowId}>
                    {r.workflowId}
                  </span>
                  <span class="text-base-content/60">{r.workflowName}</span>
                  <span class="text-base-content/50 font-mono">{formatDuration(r.durationMs)}</span>
                  <span class="text-base-content/50">{formatRelative(r.at)}</span>
                </button>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function Stat({
  label,
  value,
  valueClass,
}: {
  label: string;
  value: number | string;
  valueClass?: string;
}) {
  return (
    <div class="bg-base-200 rounded p-1.5">
      <div class="text-[10px] uppercase tracking-wider text-base-content/50">{label}</div>
      <div class={`text-lg font-mono ${valueClass ?? ""}`}>{value}</div>
    </div>
  );
}

function TagRow({ label, tags }: { label: string; tags: readonly string[] }) {
  return (
    <div class="flex items-start gap-2">
      <span class="text-base-content/50 w-24 shrink-0">{label}</span>
      <div class="flex flex-wrap gap-1">
        {tags.map((t) => (
          <span class="badge badge-sm badge-ghost font-mono">{t}</span>
        ))}
      </div>
    </div>
  );
}
