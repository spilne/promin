import { useEffect, useState } from "preact/hooks";
import type { StepDto, StepTaskDto } from "../../../../server/api-types.ts";
import type { AttemptDto } from "../../../../server/routes/run-extras.ts";
import { api } from "../../../api/client.ts";
import { DataList } from "../../ui/data-list.tsx";
import { Section } from "../../ui/section.tsx";
import { JsonBlock } from "../../ui/json-block.tsx";
import { EmptyState } from "../../ui/empty-state.tsx";
import { StepStatusBadge } from "../../ui/status-badge.tsx";
import { formatDuration, formatRelative, STEP_STATUS_VISUAL } from "../../../lib/format.ts";

interface StepTabProps {
  runId: string;
  step: StepDto;
}

export function StepTab({ runId, step }: StepTabProps) {
  const v = STEP_STATUS_VISUAL[step.status];
  const isPlanned = step.isPlanned === true;

  return (
    <div class="space-y-4">
      <div class="flex items-center gap-2 flex-wrap">
        <span class="font-mono text-sm bg-base-200 px-1.5 py-0.5 rounded">{step.stepName}</span>
        <StepStatusBadge status={step.status} />
        {isPlanned && <span class="badge badge-sm badge-ghost">planned</span>}
      </div>

      <DataList
        items={[
          { label: "Type", value: step.stepType },
          { label: "Status", value: <span class={v.textClass}>{v.label}</span> },
          { label: "Attempt", value: step.attempt > 0 ? step.attempt : undefined, skipEmpty: true },
          { label: "Duration", value: formatDuration(step.durationMs) },
          {
            label: "Started",
            value: step.startedAt && formatRelative(step.startedAt),
            skipEmpty: true,
          },
          {
            label: "Completed",
            value: step.completedAt && formatRelative(step.completedAt),
            skipEmpty: true,
          },
          {
            label: "Wake at",
            value: step.wakeAt && formatRelative(step.wakeAt),
            skipEmpty: true,
          },
          { label: "Signal name", value: step.signalName, skipEmpty: true },
          {
            label: "Depends on",
            value: step.dependsOn.length > 0 ? step.dependsOn.join(", ") : undefined,
            skipEmpty: true,
            valueClass: "font-mono text-xs",
          },
        ]}
      />

      {step.compensationStatus && (
        <Section title="Compensation">
          <DataList
            items={[
              { label: "Status", value: step.compensationStatus },
              {
                label: "Compensated at",
                value: step.compensatedAt && formatRelative(step.compensatedAt),
                skipEmpty: true,
              },
            ]}
          />
          {step.compensationError && <JsonBlock value={step.compensationError} variant="error" />}
        </Section>
      )}

      {step.result !== undefined && step.result !== null && (
        <Section title="Result">
          <JsonBlock value={step.result} />
        </Section>
      )}

      {step.error && (
        <Section title="Error">
          <JsonBlock value={step.error} variant="error" />
        </Section>
      )}

      {step.metadata && Object.keys(step.metadata).length > 0 && (
        <Section title="Metadata">
          <JsonBlock value={step.metadata} />
        </Section>
      )}

      {step.tasks && step.tasks.length > 0 && <TasksSection tasks={step.tasks} />}

      {!isPlanned && <AttemptsSection runId={runId} stepName={step.stepName} />}
    </div>
  );
}

function TasksSection({ tasks }: { tasks: StepTaskDto[] }) {
  const statusCounts = tasks.reduce<Record<string, number>>((acc, t) => {
    acc[t.status] = (acc[t.status] ?? 0) + 1;
    return acc;
  }, {});
  const summary = Object.entries(statusCounts)
    .map(([s, n]) => `${n} ${s}`)
    .join(" · ");

  return (
    <Section
      title={`mapOver tasks (${tasks.length})`}
      actions={<span class="text-xs text-base-content/50">{summary}</span>}
    >
      <div class="max-h-60 overflow-y-auto rounded bg-base-200">
        <table class="table table-sm">
          <thead>
            <tr class="text-xs uppercase tracking-wider text-base-content/50">
              <th>#</th>
              <th>Status</th>
              <th>Attempt</th>
            </tr>
          </thead>
          <tbody>
            {tasks.map((t) => (
              <tr>
                <td class="font-mono text-xs">{t.taskIndex}</td>
                <td>
                  <StepStatusBadge status={t.status} />
                </td>
                <td class="text-xs">{t.attempt}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Section>
  );
}

function AttemptsSection({ runId, stepName }: { runId: string; stepName: string }) {
  const [data, setData] = useState<{ supported: boolean; attempts: AttemptDto[] } | undefined>(
    undefined,
  );
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    let cancelled = false;
    api
      .getRunAttempts(runId, stepName)
      .then((r) => !cancelled && setData(r))
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, [runId, stepName]);

  if (error) {
    return (
      <Section title="Attempts">
        <div class="alert alert-error text-sm">{error}</div>
      </Section>
    );
  }
  if (!data) {
    return (
      <Section title="Attempts">
        <div class="text-sm text-base-content/50">Loading…</div>
      </Section>
    );
  }
  if (!data.supported) {
    return (
      <Section title="Attempts">
        <EmptyState
          message="Attempt history not available"
          hint="Storage backend doesn't record StepAttemptStorage."
          pad="py-4"
        />
      </Section>
    );
  }
  if (data.attempts.length === 0) {
    return (
      <Section title="Attempts">
        <EmptyState message="No attempts recorded yet." pad="py-4" />
      </Section>
    );
  }

  return (
    <Section title={`Attempts (${data.attempts.length})`}>
      <div class="space-y-1">
        {data.attempts.map((a) => (
          <div
            class={`rounded p-2 text-sm ${a.status === "failed" ? "bg-error/10" : "bg-base-200"}`}
          >
            <div class="flex items-center gap-2">
              <span class="font-mono text-xs">#{a.attempt}</span>
              <span class="badge badge-sm badge-ghost">{a.type}</span>
              <span
                class={`badge badge-sm ${a.status === "failed" ? "badge-error" : "badge-success"}`}
              >
                {a.status}
              </span>
              <span class="text-xs text-base-content/60">{formatDuration(a.durationMs)}</span>
              {a.workerId && (
                <span class="text-xs text-base-content/60 font-mono">on {a.workerId}</span>
              )}
              <div class="flex-1" />
              <span class="text-xs text-base-content/50">{formatRelative(a.startedAt)}</span>
            </div>
            {a.error && <div class="mt-1 text-xs text-error break-words">{a.error}</div>}
          </div>
        ))}
      </div>
    </Section>
  );
}
