import { useEffect, useState } from "preact/hooks";
import { api } from "../../../api/client.ts";
import type { RunSummaryDto } from "../../../../server/api-types.ts";
import { EmptyState } from "../../ui/empty-state.tsx";
import { StatusBadge } from "../../ui/status-badge.tsx";
import { formatDuration, formatRelative } from "../../../lib/format.ts";

interface ChildrenTabProps {
  runId: string;
  onOpenRun: (id: string) => void;
}

export function ChildrenTab({ runId, onOpenRun }: ChildrenTabProps) {
  const [data, setData] = useState<RunSummaryDto[] | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    let cancelled = false;
    api
      .getRunChildren(runId)
      .then((r) => !cancelled && setData(r.children))
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
        message="This run has no child workflows."
        hint="Children are workflows spawned with parentWorkflowId set."
      />
    );
  }

  return (
    <div class="space-y-2">
      {data.map((c) => (
        <div
          class="card bg-base-200 hover:bg-base-300 cursor-pointer transition-colors"
          onClick={() => onOpenRun(c.workflowId)}
        >
          <div class="card-body p-3">
            <div class="flex items-center gap-2 flex-wrap">
              <span class="font-mono text-xs truncate max-w-[160px]" title={c.workflowId}>
                {c.workflowId}
              </span>
              <span class="text-sm">{c.workflowName}</span>
              <StatusBadge status={c.status} />
              <div class="flex-1" />
              <span class="text-xs text-base-content/60">{formatDuration(c.totalMs)}</span>
              <span class="text-xs text-base-content/50">{formatRelative(c.createdAt)}</span>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
