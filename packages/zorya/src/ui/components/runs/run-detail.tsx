import { useCallback, useEffect, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import { useSse } from "../../hooks/use-sse.ts";
import type { RunDto, RunEvent, StepDto } from "../../../server/api-types.ts";
import { StatusBadge } from "../ui/status-badge.tsx";
import { StepTimeline } from "./step-timeline.tsx";
import { StepList } from "./step-list.tsx";
import { formatDuration, formatRelative } from "../../lib/format.ts";

interface RunDetailProps {
  id: string;
  onBack: () => void;
}

export function RunDetail({ id, onBack }: RunDetailProps) {
  const [run, setRun] = useState<RunDto | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);

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

  if (error) {
    return (
      <div class="p-4 max-w-7xl mx-auto">
        <div class="alert alert-error">{error}</div>
      </div>
    );
  }
  if (!run) {
    return <div class="p-4 max-w-7xl mx-auto text-base-content/60">Loading run…</div>;
  }

  const totalMs = run.completedAt
    ? new Date(run.completedAt).getTime() - new Date(run.createdAt).getTime()
    : Date.now() - new Date(run.createdAt).getTime();

  return (
    <div class="p-4 max-w-7xl mx-auto space-y-4">
      <div class="flex items-center gap-3">
        <button class="btn btn-sm btn-ghost" onClick={onBack}>
          ← Runs
        </button>
        <div class="text-base-content/40">/</div>
        <div class="font-semibold">{run.workflowName}</div>
        <div class="font-mono text-xs text-base-content/60">{run.workflowId}</div>
        <StatusBadge status={run.status} size="md" />
        <div class="text-sm text-base-content/60">{formatDuration(totalMs)} total</div>
        <div class="flex-1" />
        <button class="btn btn-sm btn-outline" onClick={signal}>
          Send signal
        </button>
        {run.status !== "completed" && run.status !== "failed" && (
          <button class="btn btn-sm btn-error btn-outline" onClick={cancel}>
            Cancel
          </button>
        )}
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div class="lg:col-span-2">
          <StepTimeline run={run} />
        </div>
        <div class="space-y-4">
          <MetaCard run={run} />
          <InputOutputCard run={run} />
        </div>
      </div>

      <StepList steps={run.steps} />
    </div>
  );
}

function MetaCard({ run }: { run: RunDto }) {
  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4">
        <h3 class="card-title text-base">Details</h3>
        <dl class="text-sm grid grid-cols-[auto,1fr] gap-x-3 gap-y-1">
          <dt class="text-base-content/60">ID</dt>
          <dd class="font-mono text-xs break-all">{run.workflowId}</dd>
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
      </div>
    </div>
  );
}

function InputOutputCard({ run }: { run: RunDto }) {
  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4 space-y-2">
        <div>
          <h4 class="text-xs font-semibold text-base-content/60 uppercase tracking-wide mb-1">
            Input
          </h4>
          <pre class="bg-base-200 p-2 rounded text-xs overflow-x-auto">
            {JSON.stringify(run.input, null, 2)}
          </pre>
        </div>
        {run.result !== undefined && run.result !== null && (
          <div>
            <h4 class="text-xs font-semibold text-base-content/60 uppercase tracking-wide mb-1">
              Result
            </h4>
            <pre class="bg-base-200 p-2 rounded text-xs overflow-x-auto">
              {JSON.stringify(run.result, null, 2)}
            </pre>
          </div>
        )}
        {run.error && (
          <div>
            <h4 class="text-xs font-semibold text-error uppercase tracking-wide mb-1">Error</h4>
            <pre class="bg-error/10 p-2 rounded text-xs overflow-x-auto text-error">
              {run.error}
            </pre>
          </div>
        )}
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
