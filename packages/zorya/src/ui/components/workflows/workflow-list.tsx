import { useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { WorkflowDefDto } from "../../../server/routes/workflow-defs.ts";
import type { SparklinesResponse } from "../../../server/routes/grid.ts";
import { Sparkline } from "../ui/sparkline.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { EmptyState } from "../ui/empty-state.tsx";
import { TriggerModal } from "./trigger-modal.tsx";

interface WorkflowListProps {
  onOpenRun: (id: string) => void;
  /** Navigate to the workflow-detail page (DAG, sample input, recent runs). */
  onOpenWorkflow: (name: string) => void;
  /** Navigate to the runs list pre-filtered by this workflow name. */
  onOpenWorkflowRuns?: (name: string) => void;
}

/**
 * Directory of every registered workflow definition. Shows type, step
 * count, recent activity sparkline, and a [Trigger] action per row.
 */
export function WorkflowList({ onOpenRun, onOpenWorkflow, onOpenWorkflowRuns }: WorkflowListProps) {
  const { data, loading, error, refresh } = useFetch(() => api.listWorkflowDefs(), [], 30_000);
  const { data: sparklines } = useFetch<SparklinesResponse>(() => api.getSparklines(14), [], 5000);
  const [triggering, setTriggering] = useState<WorkflowDefDto | undefined>(undefined);

  return (
    <div class="anim-page p-4 max-w-[1400px] mx-auto space-y-4">
      <div class="flex items-end justify-between">
        <div>
          <h2 class="text-xl font-semibold">Workflows</h2>
          <p class="text-xs text-base-content/50">
            {data ? `${data.workflows.length} registered` : "Loading…"}
          </p>
        </div>
        <button class="btn btn-sm btn-ghost gap-1" onClick={() => refresh()}>
          <span>↻</span>
          Refresh
        </button>
      </div>

      {error && <div class="alert alert-error text-sm">{error.message}</div>}

      <div class="card bg-base-100 shadow overflow-hidden">
        <div class="overflow-x-auto">
          <table class="table">
            <thead>
              <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                <th>Name</th>
                <th>Type</th>
                <th>Version</th>
                <th>Steps</th>
                <th>Recent</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {loading && !data && <SkeletonRows rows={6} cols={6} />}
              {data && data.workflows.length === 0 && !loading && (
                <tr>
                  <td colSpan={6}>
                    <EmptyState
                      message="No workflows registered."
                      hint="Pass a `workflows` map to ZoryaServer, or use scanWorkflowsFolder()."
                    />
                  </td>
                </tr>
              )}
              {data?.workflows.map((w) => (
                <tr class="hover:bg-base-200">
                  <td>
                    <button
                      class="btn btn-sm btn-ghost font-mono normal-case"
                      onClick={() => onOpenWorkflow(w.name)}
                    >
                      {w.name}
                    </button>
                  </td>
                  <td class="text-base-content/60">{w.type ?? "—"}</td>
                  <td class="font-mono text-sm text-base-content/60">{w.version ?? "—"}</td>
                  <td class="font-mono text-sm">{w.steps.length}</td>
                  <td>
                    <Sparkline runs={sparklines?.[w.name] ?? []} />
                  </td>
                  <td class="text-right">
                    <div class="flex gap-1 justify-end">
                      {onOpenWorkflowRuns && (
                        <button
                          class="btn btn-sm btn-ghost"
                          onClick={() => onOpenWorkflowRuns(w.name)}
                          title="Jump to the runs list filtered by this workflow"
                        >
                          Runs
                        </button>
                      )}
                      <button
                        class="btn btn-sm btn-ghost"
                        onClick={() => onOpenWorkflow(w.name)}
                        title="Inspect DAG and recent runs"
                      >
                        View
                      </button>
                      <button class="btn btn-sm btn-primary" onClick={() => setTriggering(w)}>
                        Trigger
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {triggering && (
        <TriggerModal
          def={triggering}
          onClose={() => setTriggering(undefined)}
          onTriggered={(id) => {
            setTriggering(undefined);
            onOpenRun(id);
          }}
        />
      )}
    </div>
  );
}
