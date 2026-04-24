import { useCallback, useEffect, useMemo, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import { useSse } from "../../hooks/use-sse.ts";
import type { RunDto, RunEvent, StepDto } from "../../../server/api-types.ts";
import { StatusBadge, StepStatusBadge } from "../ui/status-badge.tsx";
import { Skeleton } from "../ui/skeleton.tsx";
import { StepTimeline } from "./step-timeline.tsx";
import { StepDag } from "./step-dag.tsx";
import { formatDuration, formatRelative, STEP_STATUS_VISUAL } from "../../lib/format.ts";

interface RunDetailProps {
  id: string;
  onBack: () => void;
}

type RightTab = "overview" | "step" | "payload";
type StepView = "timeline" | "graph";

export function RunDetail({ id, onBack }: RunDetailProps) {
  const [run, setRun] = useState<RunDto | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);
  const [selectedStep, setSelectedStep] = useState<string | undefined>(undefined);
  const [tab, setTab] = useState<RightTab>("overview");
  const [stepView, setStepView] = useState<StepView>("timeline");

  useEffect(() => {
    let cancelled = false;
    api
      .getRun(id)
      .then((r) => !cancelled && setRun(r))
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, [id]);

  useSse<RunEvent>(api.eventsUrl(id), (ev) => {
    setRun((prev) => applyEvent(prev, ev));
  });

  // When a step is selected, flip the right pane to the Step tab.
  useEffect(() => {
    if (selectedStep) setTab("step");
  }, [selectedStep]);

  const cancel = useCallback(async () => {
    if (!confirm("Cancel this run?")) return;
    try {
      await api.cancelRun(id);
      const fresh = await api.getRun(id);
      setRun(fresh);
    } catch (e) {
      alert(`Cancel failed: ${e}`);
    }
  }, [id]);

  const signal = useCallback(async () => {
    const name = prompt("Signal name");
    if (!name) return;
    const payloadRaw = prompt("Payload (JSON, blank for null)") ?? "";
    let payload: unknown = null;
    if (payloadRaw) {
      try {
        payload = JSON.parse(payloadRaw);
      } catch {
        alert("Invalid JSON");
        return;
      }
    }
    try {
      await api.signalRun(id, { signalName: name, payload });
    } catch (e) {
      alert(`Signal failed: ${e}`);
    }
  }, [id]);

  const selected = useMemo(() => {
    if (!run || !selectedStep) return undefined;
    return run.steps.find((s) => s.stepName === selectedStep);
  }, [run, selectedStep]);

  if (error) {
    return (
      <div class="p-4 max-w-7xl mx-auto">
        <div class="alert alert-error">{error}</div>
      </div>
    );
  }
  if (!run) {
    return <RunDetailSkeleton onBack={onBack} />;
  }

  const totalMs = run.completedAt
    ? new Date(run.completedAt).getTime() - new Date(run.createdAt).getTime()
    : Date.now() - new Date(run.createdAt).getTime();

  const terminal = run.status === "completed" || run.status === "failed";

  return (
    <div class="anim-page p-4 max-w-[1400px] mx-auto space-y-4">
      {/* Header */}
      <div class="flex items-center gap-3">
        <button class="btn btn-sm btn-ghost" onClick={onBack}>
          ← Runs
        </button>
        <div class="text-base-content/40">/</div>
        <div class="font-semibold">{run.workflowName}</div>
        <div class="font-mono text-sm text-base-content/60">{run.workflowId}</div>
        <StatusBadge status={run.status} size="md" />
        <div class="text-sm text-base-content/60">{formatDuration(totalMs)} total</div>
        <div class="flex-1" />
        <button class="btn btn-sm btn-outline" onClick={signal}>
          Send signal
        </button>
        {!terminal && (
          <button class="btn btn-sm btn-error btn-outline" onClick={cancel}>
            Cancel
          </button>
        )}
      </div>

      {/* View toggle */}
      <div class="flex items-center gap-2">
        <div class="join">
          <button
            class={`btn btn-sm join-item ${stepView === "timeline" ? "btn-primary" : "btn-ghost"}`}
            onClick={() => setStepView("timeline")}
          >
            Timeline
          </button>
          <button
            class={`btn btn-sm join-item ${stepView === "graph" ? "btn-primary" : "btn-ghost"}`}
            onClick={() => setStepView("graph")}
          >
            Graph
          </button>
        </div>
      </div>

      {/* Main two-column layout */}
      <div class="grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_360px] gap-4">
        {stepView === "timeline" ? (
          <StepTimeline run={run} selectedStep={selectedStep} onSelectStep={setSelectedStep} />
        ) : (
          <StepDag run={run} selectedStep={selectedStep} onSelectStep={setSelectedStep} />
        )}

        <div class="card bg-base-100 shadow self-start">
          <div class="card-body p-0">
            <div class="tabs tabs-lifted px-2 pt-2">
              <a
                class={`tab tab-sm ${tab === "overview" ? "tab-active" : ""}`}
                onClick={() => setTab("overview")}
              >
                Overview
              </a>
              <a
                class={`tab tab-sm ${tab === "step" ? "tab-active" : ""} ${!selected ? "opacity-40" : ""}`}
                onClick={() => selected && setTab("step")}
              >
                Step
              </a>
              <a
                class={`tab tab-sm ${tab === "payload" ? "tab-active" : ""}`}
                onClick={() => setTab("payload")}
              >
                Payload
              </a>
            </div>
            <div class="p-4">
              {tab === "overview" && <OverviewTab run={run} />}
              {tab === "step" && selected && <StepTab step={selected} />}
              {tab === "step" && !selected && (
                <div class="text-base-content/50 text-sm py-6 text-center">
                  Click a step in the timeline to inspect
                </div>
              )}
              {tab === "payload" && <PayloadTab run={run} />}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function OverviewTab({ run }: { run: RunDto }) {
  return (
    <dl class="text-sm grid grid-cols-[auto,1fr] gap-x-3 gap-y-1">
      <dt class="text-base-content/60">ID</dt>
      <dd class="font-mono text-sm break-all">{run.workflowId}</dd>
      <dt class="text-base-content/60">Name</dt>
      <dd>{run.workflowName}</dd>
      {run.workflowType && (
        <>
          <dt class="text-base-content/60">Type</dt>
          <dd>{run.workflowType}</dd>
        </>
      )}
      {run.namespace && (
        <>
          <dt class="text-base-content/60">Namespace</dt>
          <dd>{run.namespace}</dd>
        </>
      )}
      <dt class="text-base-content/60">Run</dt>
      <dd>#{run.run}</dd>
      <dt class="text-base-content/60">Created</dt>
      <dd>{formatRelative(run.createdAt)}</dd>
      {run.startedAt && (
        <>
          <dt class="text-base-content/60">Started</dt>
          <dd>{formatRelative(run.startedAt)}</dd>
        </>
      )}
      {run.completedAt && (
        <>
          <dt class="text-base-content/60">Completed</dt>
          <dd>{formatRelative(run.completedAt)}</dd>
        </>
      )}
    </dl>
  );
}

function StepTab({ step }: { step: StepDto }) {
  const v = STEP_STATUS_VISUAL[step.status];
  return (
    <div class="space-y-3 text-sm">
      <div class="flex items-center gap-2 flex-wrap">
        <span class="font-mono text-sm bg-base-200 px-1.5 py-0.5 rounded">{step.stepName}</span>
        <StepStatusBadge status={step.status} />
      </div>
      <dl class="grid grid-cols-[auto,1fr] gap-x-3 gap-y-1 text-sm">
        <dt class="text-base-content/60">Type</dt>
        <dd>{step.stepType}</dd>
        <dt class="text-base-content/60">Status</dt>
        <dd class={v.textClass}>{v.label}</dd>
        <dt class="text-base-content/60">Attempt</dt>
        <dd>{step.attempt}</dd>
        <dt class="text-base-content/60">Duration</dt>
        <dd>{formatDuration(step.durationMs)}</dd>
        {step.startedAt && (
          <>
            <dt class="text-base-content/60">Started</dt>
            <dd>{formatRelative(step.startedAt)}</dd>
          </>
        )}
        {step.completedAt && (
          <>
            <dt class="text-base-content/60">Completed</dt>
            <dd>{formatRelative(step.completedAt)}</dd>
          </>
        )}
        {step.dependsOn.length > 0 && (
          <>
            <dt class="text-base-content/60">Depends on</dt>
            <dd class="font-mono text-sm">{step.dependsOn.join(", ")}</dd>
          </>
        )}
      </dl>
      {step.result !== undefined && step.result !== null && (
        <div>
          <h4 class="text-xs font-semibold text-base-content/50 uppercase tracking-wide mb-1">
            Result
          </h4>
          <pre class="bg-base-200 p-2 rounded text-sm overflow-x-auto max-h-60">
            {JSON.stringify(step.result, null, 2)}
          </pre>
        </div>
      )}
      {step.error && (
        <div>
          <h4 class="text-xs font-semibold text-error uppercase tracking-wide mb-1">Error</h4>
          <pre class="bg-error/10 p-2 rounded text-sm overflow-x-auto text-error max-h-60">
            {step.error}
          </pre>
        </div>
      )}
    </div>
  );
}

function PayloadTab({ run }: { run: RunDto }) {
  return (
    <div class="space-y-3">
      <div>
        <h4 class="text-xs font-semibold text-base-content/50 uppercase tracking-wide mb-1">
          Input
        </h4>
        <pre class="bg-base-200 p-2 rounded text-sm overflow-x-auto max-h-60">
          {JSON.stringify(run.input, null, 2)}
        </pre>
      </div>
      {run.result !== undefined && run.result !== null && (
        <div>
          <h4 class="text-xs font-semibold text-base-content/50 uppercase tracking-wide mb-1">
            Result
          </h4>
          <pre class="bg-base-200 p-2 rounded text-sm overflow-x-auto max-h-60">
            {JSON.stringify(run.result, null, 2)}
          </pre>
        </div>
      )}
      {run.error && (
        <div>
          <h4 class="text-xs font-semibold text-error uppercase tracking-wide mb-1">Error</h4>
          <pre class="bg-error/10 p-2 rounded text-sm overflow-x-auto text-error max-h-60">
            {run.error}
          </pre>
        </div>
      )}
    </div>
  );
}

function RunDetailSkeleton({ onBack }: { onBack: () => void }) {
  return (
    <div class="anim-page p-4 max-w-[1400px] mx-auto space-y-4">
      <div class="flex items-center gap-3">
        <button class="btn btn-sm btn-ghost" onClick={onBack}>
          ← Runs
        </button>
        <div class="text-base-content/40">/</div>
        <Skeleton w="w-32" h="h-5" />
        <Skeleton w="w-48" h="h-4" />
        <Skeleton w="w-20" h="h-5" />
      </div>
      <div class="grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_360px] gap-4">
        <div class="card bg-base-100 shadow">
          <div class="card-body p-4 space-y-3">
            <Skeleton w="w-40" h="h-5" />
            <Skeleton w="w-full" h="h-4" />
            <div class="space-y-2 pt-2">
              {Array.from({ length: 5 }).map((_, i) => (
                <div class="flex items-center gap-2">
                  <Skeleton w="w-48" h="h-4" />
                  <Skeleton
                    w={i === 0 ? "w-24" : i === 1 ? "w-48" : i === 2 ? "w-32" : "w-40"}
                    h="h-5"
                  />
                </div>
              ))}
            </div>
          </div>
        </div>
        <div class="card bg-base-100 shadow self-start">
          <div class="card-body p-4 space-y-2">
            <Skeleton w="w-24" h="h-5" />
            {Array.from({ length: 6 }).map(() => (
              <Skeleton w="w-full" h="h-3" />
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function applyEvent(prev: RunDto | undefined, ev: RunEvent): RunDto | undefined {
  if (ev.type === "snapshot") return ev.run;
  if (!prev) return prev;
  if (ev.type === "status") return { ...prev, status: ev.status };
  if (ev.type === "step") return { ...prev, steps: mergeStep(prev.steps, ev.stepName, ev.step) };
  return prev;
}

function mergeStep(steps: StepDto[], name: string, next: StepDto): StepDto[] {
  const idx = steps.findIndex((s) => s.stepName === name);
  if (idx === -1) return [...steps, next];
  const copy = steps.slice();
  copy[idx] = next;
  return copy;
}
