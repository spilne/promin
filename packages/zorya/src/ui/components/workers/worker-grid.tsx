import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { WorkerDto } from "../../../server/api-types.ts";
import { formatDuration, formatRelative } from "../../lib/format.ts";
import { Skeleton } from "../ui/skeleton.tsx";
import { Page } from "../ui/page.tsx";
import { Card } from "../ui/card.tsx";

interface WorkerGridProps {
  onOpenRun?: (id: string) => void;
}

export function WorkerGrid({ onOpenRun }: WorkerGridProps = {}) {
  const { data, loading, error } = useFetch(() => api.listWorkers(), [], 5000);

  if (loading && !data) {
    return (
      <Page>
        <h2 class="text-xl font-semibold">Workers</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
          {Array.from({ length: 2 }).map(() => (
            <Card bodyClassName="space-y-2">
              <Skeleton w="w-32" h="h-4" />
              <Skeleton w="w-24" h="h-3" />
              <Skeleton w="w-full" h="h-3" />
            </Card>
          ))}
        </div>
      </Page>
    );
  }
  if (error) {
    return (
      <Page space="none">
        <div class="alert alert-error">{error.message}</div>
      </Page>
    );
  }
  const workers = data?.workers ?? [];
  const online = workers.filter((w) => w.status === "online").length;
  const offline = workers.length - online;

  return (
    <Page>
      <div class="flex items-center gap-2">
        <h2 class="text-xl font-semibold">Workers</h2>
        <span class="text-base-content/60">
          · {online} online
          {offline > 0 && `, ${offline} offline`}
        </span>
      </div>

      {workers.length === 0 && (
        <Card bodyClassName="py-8 text-center text-base-content/50" padding="none">
          No workers registered
        </Card>
      )}

      <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
        {workers.map((w) => (
          <WorkerCard worker={w} onOpenRun={onOpenRun} />
        ))}
      </div>
    </Page>
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
    <Card hover bodyClassName="space-y-3">
      {/* Header — identity row.
            Worker ID is the primary identifier (larger, semibold). Runtime
            + version pair into a small accented badge cluster on the right,
            visually distinct from the ID. Hostname moves to a sub-line so
            it stops competing with the ID for attention. */}
      <div class="flex items-center gap-2 flex-wrap">
        <span class={`w-2.5 h-2.5 rounded-full shrink-0 ${online ? "bg-success" : "bg-error"}`} />
        <span class="font-mono text-base font-semibold truncate" title={worker.workerId}>
          {worker.workerId}
        </span>
        <div class="flex-1" />
        {worker.runtime && (
          <span
            class="badge badge-sm badge-info badge-outline font-mono"
            title={`Runtime: ${worker.runtime}`}
          >
            {worker.runtime}
          </span>
        )}
        {worker.version && (
          <span
            class="badge badge-sm badge-ghost font-mono"
            title={`Worker version: ${worker.version}`}
          >
            v{worker.version}
          </span>
        )}
      </div>
      {worker.hostname && (
        <div class="-mt-1 flex items-center gap-1.5 text-xs text-base-content/60 font-mono">
          <span class="opacity-60">🖥</span>
          <span class="truncate" title={worker.hostname}>
            {worker.hostname}
          </span>
        </div>
      )}

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

      {/* Categories. Each row's badges share a color so users can scan
            "what does this worker offer?" at a glance instead of decoding
            uniform-grey pills. Outline variant keeps the surface dark
            (badge-soft would compete with the card body color). */}
      <div class="space-y-1 text-xs">
        {worker.capabilities && worker.capabilities.length > 0 && (
          <TagRow label="Capabilities" tags={worker.capabilities} variant="primary" />
        )}
        {worker.workflowNames && worker.workflowNames.length > 0 && (
          <TagRow label="Workflows" tags={worker.workflowNames} variant="success" />
        )}
        {worker.namespaces && worker.namespaces.length > 0 && (
          <TagRow label="Namespaces" tags={worker.namespaces} variant="warning" />
        )}
        {labelEntries.length > 0 && (
          <div class="flex items-start gap-2">
            <span class="text-base-content/50 w-24 shrink-0">Labels</span>
            <div class="flex flex-wrap gap-1">
              {labelEntries.map(([k, v]) => (
                <span class="badge badge-sm badge-accent badge-outline font-mono">
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
                class="w-full flex items-center gap-2 p-1 hover:bg-base-100 rounded text-left text-xs"
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
                class="w-full flex items-center gap-2 p-1 hover:bg-base-100 rounded text-left text-xs"
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
    </Card>
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
    <div class="bg-base-100 border border-base-content/10 rounded p-1.5">
      <div class="text-[10px] uppercase tracking-wider text-base-content/50">{label}</div>
      <div class={`text-lg font-mono ${valueClass ?? ""}`}>{value}</div>
    </div>
  );
}

function TagRow({
  label,
  tags,
  variant = "ghost",
}: {
  label: string;
  tags: readonly string[];
  /** DaisyUI badge color variant (without the `badge-` prefix). Default ghost. */
  variant?: "ghost" | "primary" | "success" | "warning" | "info" | "accent";
}) {
  // Outline ensures legibility against the bg-base-200 card surface; solid
  // badges look mushy at this size on dark backgrounds.
  const badgeClass = variant === "ghost" ? "badge-ghost" : `badge-${variant} badge-outline`;
  return (
    <div class="flex items-start gap-2">
      <span class="text-base-content/50 w-24 shrink-0">{label}</span>
      <div class="flex flex-wrap gap-1">
        {tags.map((t) => (
          <span class={`badge badge-sm font-mono ${badgeClass}`}>{t}</span>
        ))}
      </div>
    </div>
  );
}
