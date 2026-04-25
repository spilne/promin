// ---------------------------------------------------------------------------
// WorkflowDetail — standalone page showing a workflow definition: DAG graph
// (no run required), step table, sample input, and a list of its recent
// runs. Lets you inspect structure and trigger a run without leaving the
// page.
// ---------------------------------------------------------------------------

import { useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { RunDto, StepDto } from "../../../server/api-types.ts";
import { StepDag } from "../runs/step-dag.tsx";
import { Sparkline } from "../ui/sparkline.tsx";
import { DataList } from "../ui/data-list.tsx";
import { Section } from "../ui/section.tsx";
import { JsonBlock } from "../ui/json-block.tsx";
import { StatusBadge } from "../ui/status-badge.tsx";
import { EmptyState } from "../ui/empty-state.tsx";
import { Skeleton } from "../ui/skeleton.tsx";
import { TriggerModal } from "./trigger-modal.tsx";
import { HistoryChart } from "./history-chart.tsx";
import { STEP_TYPE_ICON, formatDuration, formatRelative } from "../../lib/format.ts";

interface WorkflowDetailProps {
  name: string;
  onBack: () => void;
  onOpenRun: (id: string) => void;
}

export function WorkflowDetail({ name, onBack, onOpenRun }: WorkflowDetailProps) {
  const [triggering, setTriggering] = useState(false);

  const { data: def, error } = useFetch(() => api.getWorkflowDef(name), [name]);
  const { data: runsList } = useFetch(() => api.listRuns({ name, limit: 10 }), [name], 5000);

  // Build a synthetic RunDto so StepDag can render the workflow as planned
  // (no statuses, dashed nodes). Avoids duplicating the SVG layout logic.
  const syntheticRun = useMemo<RunDto | undefined>(() => {
    if (!def) return undefined;
    const steps: StepDto[] = def.steps.map((s) => ({
      stepName: s.name,
      run: 0,
      status: "pending",
      stepType: kindToStepType(s.kind),
      dependsOn: [...s.dependsOn],
      attempt: 0,
      isPlanned: true,
    }));
    const now = new Date().toISOString();
    return {
      workflowId: `(definition) ${name}`,
      workflowName: name,
      workflowType: def.type,
      version: def.version,
      status: "pending",
      run: 0,
      input: def.sampleInput,
      steps,
      createdAt: now,
      updatedAt: now,
    };
  }, [def, name]);

  if (error) {
    return (
      <div class="p-4 max-w-7xl mx-auto">
        <div class="alert alert-error">{error.message}</div>
      </div>
    );
  }
  if (!def || !syntheticRun) {
    return <WorkflowDetailSkeleton onBack={onBack} />;
  }

  const sparkline = runsList?.runs.map((r) => ({ status: r.status, createdAt: r.createdAt })) ?? [];

  return (
    <div class="anim-page p-4 max-w-[1400px] mx-auto space-y-4">
      {/* Header */}
      <div class="flex items-center gap-3 flex-wrap">
        <button class="btn btn-sm btn-ghost" onClick={onBack}>
          ← Workflows
        </button>
        <div class="text-base-content/40">/</div>
        <div class="font-semibold text-lg">{def.name}</div>
        {def.type && <span class="badge badge-sm badge-ghost">{def.type}</span>}
        {def.version && <span class="badge badge-sm badge-ghost font-mono">v{def.version}</span>}
        <span class="text-sm text-base-content/60">
          {def.steps.length} {def.steps.length === 1 ? "step" : "steps"}
        </span>
        <div class="flex-1" />
        <Sparkline runs={sparkline} />
        <button class="btn btn-sm btn-primary" onClick={() => setTriggering(true)}>
          Trigger
        </button>
      </div>

      {/* Duration history chart — operator's first stop when checking for
          degradation of the whole workflow or a specific step. */}
      <HistoryChart name={name} onOpenRun={onOpenRun} />

      <div class="grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_380px] gap-4">
        {/* DAG */}
        <StepDag run={syntheticRun} />

        {/* Right pane: sample input + steps + recent runs */}
        <div class="space-y-4">
          <div class="card bg-base-100 shadow">
            <div class="card-body p-4 space-y-3">
              <DataList
                items={[
                  { label: "Name", value: def.name, valueClass: "font-mono" },
                  { label: "Type", value: def.type, skipEmpty: true },
                  { label: "Version", value: def.version, skipEmpty: true },
                  { label: "Steps", value: def.steps.length },
                ]}
              />
              {def.sampleInput !== undefined && def.sampleInput !== null && (
                <Section title="Sample input">
                  <JsonBlock value={def.sampleInput} />
                </Section>
              )}
            </div>
          </div>

          <div class="card bg-base-100 shadow">
            <div class="card-body p-4 space-y-2">
              <h3 class="text-xs font-semibold text-base-content/50 uppercase tracking-wide">
                Steps
              </h3>
              <ul class="space-y-1 text-sm">
                {def.steps.map((s) => (
                  <li class="flex items-center gap-2">
                    <span class="text-xs text-base-content/50 w-6 text-center" title={s.kind}>
                      {STEP_TYPE_ICON[kindToStepType(s.kind)] ?? "▣"}
                    </span>
                    <span class="font-mono">{s.name}</span>
                    {s.dependsOn.length > 0 && (
                      <span class="text-xs text-base-content/50">← {s.dependsOn.join(", ")}</span>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          </div>

          <RecentRunsCard runs={runsList?.runs ?? []} onOpenRun={onOpenRun} />
        </div>
      </div>

      {triggering && (
        <TriggerModal
          def={def}
          onClose={() => setTriggering(false)}
          onTriggered={(id) => {
            setTriggering(false);
            onOpenRun(id);
          }}
        />
      )}
    </div>
  );
}

function RecentRunsCard({
  runs,
  onOpenRun,
}: {
  runs: ReadonlyArray<{
    workflowId: string;
    status: import("@promin/workflow").WorkflowStatus;
    createdAt: string;
    totalMs?: number;
  }>;
  onOpenRun: (id: string) => void;
}) {
  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4 space-y-2">
        <h3 class="text-xs font-semibold text-base-content/50 uppercase tracking-wide">
          Recent runs
        </h3>
        {runs.length === 0 ? (
          <EmptyState message="No runs yet." pad="py-3" />
        ) : (
          <ul class="space-y-1">
            {runs.slice(0, 10).map((r) => (
              <li>
                <button
                  class="w-full flex items-center gap-2 hover:bg-base-200 rounded p-1.5 text-left"
                  onClick={() => onOpenRun(r.workflowId)}
                >
                  <StatusBadge status={r.status} />
                  <span class="font-mono text-xs truncate flex-1">{r.workflowId}</span>
                  <span class="text-xs text-base-content/50">{formatRelative(r.createdAt)}</span>
                  <span class="font-mono text-xs text-base-content/50">
                    {formatDuration(r.totalMs)}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function WorkflowDetailSkeleton({ onBack }: { onBack: () => void }) {
  return (
    <div class="anim-page p-4 max-w-[1400px] mx-auto space-y-4">
      <div class="flex items-center gap-3">
        <button class="btn btn-sm btn-ghost" onClick={onBack}>
          ← Workflows
        </button>
        <div class="text-base-content/40">/</div>
        <Skeleton w="w-40" h="h-5" />
        <Skeleton w="w-20" h="h-4" />
      </div>
      <div class="grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_380px] gap-4">
        <div class="card bg-base-100 shadow">
          <div class="card-body">
            <Skeleton w="w-32" h="h-5" />
            <Skeleton w="w-full" h="h-48" class="mt-3" />
          </div>
        </div>
        <div class="card bg-base-100 shadow">
          <div class="card-body space-y-2">
            {Array.from({ length: 5 }).map(() => (
              <Skeleton w="w-full" h="h-4" />
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function kindToStepType(kind: string): import("@promin/workflow").StepType {
  switch (kind) {
    case "map":
      return "map";
    case "sleep":
      return "sleep";
    case "signal":
      return "signal";
    default:
      return "single";
  }
}
