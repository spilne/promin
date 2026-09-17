import { useEffect, useState } from "preact/hooks";
import type { WorkflowStatus } from "@promin/workflow";
import { api } from "../../../api/client.ts";
import type { RunHistoryEntryDto } from "../../../../server/routes/run-extras.ts";
import { EmptyState } from "../../ui/empty-state.tsx";
import { StatusBadge } from "../../ui/status-badge.tsx";
import { formatDuration, formatRelative } from "../../../lib/format.ts";

export function HistoryTab({ runId }: { runId: string }) {
  const [data, setData] = useState<RunHistoryEntryDto[] | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    let cancelled = false;
    api
      .getRunHistory(runId)
      .then((r) => !cancelled && setData(r.runs))
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, [runId]);

  if (error) return <div class="alert alert-error text-sm">{error}</div>;
  if (!data) return <div class="text-sm text-base-content/50">Loading…</div>;
  if (data.length === 0) {
    return (
      <EmptyState
        message="No prior runs recorded."
        hint="Every call to startFreshRun on this workflow id shows up here."
      />
    );
  }

  return (
    <div class="space-y-2">
      {data.map((r) => {
        const durationMs =
          r.completedAt && r.startedAt
            ? new Date(r.completedAt).getTime() - new Date(r.startedAt).getTime()
            : undefined;
        return (
          <div class="card bg-base-200">
            <div class="card-body p-3">
              <div class="flex items-center gap-2 flex-wrap">
                <span class="font-mono text-xs">#{r.run}</span>
                {r.version && (
                  <span class="badge badge-sm badge-ghost font-mono">v{r.version}</span>
                )}
                <StatusBadge status={r.status as WorkflowStatus} />
                <div class="flex-1" />
                <span class="text-xs text-base-content/60">{formatDuration(durationMs)}</span>
                <span class="text-xs text-base-content/50">{formatRelative(r.createdAt)}</span>
              </div>
              {r.error && <div class="mt-2 text-xs text-error break-words">{r.error}</div>}
            </div>
          </div>
        );
      })}
    </div>
  );
}
