// ---------------------------------------------------------------------------
// StepGrid — Airflow-style view of a workflow's last N runs.
// Columns = runs (newest on right), rows = steps, cells coloured by the
// step's effective status in that run.
// ---------------------------------------------------------------------------

import { useEffect, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type { GridResponse } from "../../../server/routes/grid.ts";
import type { ExtendedStepStatus } from "../../../server/api-types.ts";
import { STEP_STATUS_VISUAL, WORKFLOW_STATUS_VISUAL, formatRelative } from "../../lib/format.ts";
import { EmptyState } from "../ui/empty-state.tsx";
import { Skeleton } from "../ui/skeleton.tsx";

interface StepGridProps {
  workflowName: string;
  currentWorkflowId: string;
  onOpenRun: (id: string) => void;
}

export function StepGrid({ workflowName, currentWorkflowId, onOpenRun }: StepGridProps) {
  const [data, setData] = useState<GridResponse | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);
  const [limit, setLimit] = useState(25);

  useEffect(() => {
    let cancelled = false;
    setData(undefined);
    setError(undefined);
    api
      .getWorkflowGrid(workflowName, limit)
      .then((r) => !cancelled && setData(r))
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, [workflowName, limit]);

  if (error) {
    return (
      <div class="card bg-base-100 shadow">
        <div class="card-body">
          <div class="alert alert-error text-sm">{error}</div>
        </div>
      </div>
    );
  }
  if (!data) {
    return (
      <div class="card bg-base-100 shadow">
        <div class="card-body space-y-3">
          <Skeleton w="w-40" h="h-5" />
          <Skeleton w="w-full" h="h-32" />
        </div>
      </div>
    );
  }
  if (data.runs.length === 0 || data.stepNames.length === 0) {
    return (
      <div class="card bg-base-100 shadow">
        <div class="card-body">
          <EmptyState message={`No runs of ${workflowName} yet.`} />
        </div>
      </div>
    );
  }

  // Newest on the right — reverse (list API returns newest first).
  const runs = data.runs.slice().reverse();
  const CELL_W = 20;

  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4 space-y-3">
        <div class="flex items-center gap-3">
          <h3 class="card-title text-base">Grid</h3>
          <span class="text-sm text-base-content/60">
            Last {runs.length} runs of <span class="font-mono">{workflowName}</span>
          </span>
          <div class="flex-1" />
          <div class="join">
            {[10, 25, 50, 100].map((n) => (
              <button
                class={`btn btn-xs join-item ${limit === n ? "btn-primary" : "btn-ghost"}`}
                onClick={() => setLimit(n)}
              >
                {n}
              </button>
            ))}
          </div>
        </div>

        <div class="overflow-auto">
          <table class="border-separate border-spacing-[3px]">
            <thead>
              <tr>
                <th class="text-xs text-base-content/50 font-normal text-left pr-3 sticky left-0 bg-base-100">
                  Step
                </th>
                {runs.map((r) => {
                  const v = WORKFLOW_STATUS_VISUAL[r.status];
                  const active = r.workflowId === currentWorkflowId;
                  return (
                    <th
                      class="p-0 align-bottom"
                      style={{ width: CELL_W, minWidth: CELL_W }}
                      title={`${r.workflowId} · ${v.label} · ${formatRelative(r.createdAt)}`}
                    >
                      <button
                        class={`w-full h-6 rounded-sm transition-opacity ${v.barClass} hover:opacity-80 ${
                          active ? "ring-2 ring-primary" : ""
                        }`}
                        onClick={() => onOpenRun(r.workflowId)}
                      />
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {data.stepNames.map((stepName) => (
                <tr>
                  <td class="text-sm font-mono pr-3 sticky left-0 bg-base-100 whitespace-nowrap">
                    {stepName}
                  </td>
                  {runs.map((r) => {
                    const status = r.steps[stepName] as ExtendedStepStatus | undefined;
                    return (
                      <GridCell stepName={stepName} run={r} status={status} onOpen={onOpenRun} />
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <GridLegend />
      </div>
    </div>
  );
}

function GridCell({
  stepName,
  run,
  status,
  onOpen,
}: {
  stepName: string;
  run: { workflowId: string; createdAt: string };
  status: ExtendedStepStatus | undefined;
  onOpen: (id: string) => void;
}) {
  if (!status) {
    return (
      <td class="p-0" style={{ width: 20, minWidth: 20 }} title={`${stepName} · not present`}>
        <div class="h-5 rounded-sm bg-base-200/40" />
      </td>
    );
  }
  const v = STEP_STATUS_VISUAL[status];
  return (
    <td
      class="p-0"
      style={{ width: 20, minWidth: 20 }}
      title={`${stepName} · ${v.label} · ${run.workflowId}`}
    >
      <button
        class={`w-full h-5 rounded-sm transition-opacity ${v.barClass} hover:opacity-80`}
        onClick={() => onOpen(run.workflowId)}
      />
    </td>
  );
}

function GridLegend() {
  const items: Array<{ label: string; class: string }> = [
    { label: "completed", class: "bg-success" },
    { label: "running", class: "bg-info" },
    { label: "failed", class: "bg-error" },
    { label: "upstream failed", class: "bg-warning/60" },
    { label: "waiting", class: "bg-warning" },
    { label: "pending", class: "bg-base-content/20" },
  ];
  return (
    <div class="flex gap-3 text-xs pt-1 border-t border-base-content/10">
      {items.map((i) => (
        <div class="flex items-center gap-1.5">
          <span class={`w-3 h-3 rounded ${i.class}`} />
          <span class="text-base-content/60">{i.label}</span>
        </div>
      ))}
    </div>
  );
}
