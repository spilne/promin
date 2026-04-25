import { useEffect, useState } from "preact/hooks";
import type { StepDto, StepTaskDto } from "../../../../server/api-types.ts";
import type { AttemptDto, JournalEntryDto } from "../../../../server/routes/run-extras.ts";
import { api } from "../../../api/client.ts";
import { DataList } from "../../ui/data-list.tsx";
import { Section } from "../../ui/section.tsx";
import { JsonBlock } from "../../ui/json-block.tsx";
import { EmptyState } from "../../ui/empty-state.tsx";
import { StepStatusBadge } from "../../ui/status-badge.tsx";
import {
  effectiveStepStatus,
  formatDuration,
  formatRelative,
  STEP_STATUS_VISUAL,
} from "../../../lib/format.ts";

interface StepTabProps {
  runId: string;
  step: StepDto;
}

export function StepTab({ runId, step }: StepTabProps) {
  const renderStatus = effectiveStepStatus(step);
  const v = STEP_STATUS_VISUAL[renderStatus];
  const isPlanned = step.isPlanned === true;

  return (
    <div class="space-y-4">
      <div class="flex items-center gap-2 flex-wrap">
        <span class="font-mono text-sm bg-base-200 px-1.5 py-0.5 rounded">{step.stepName}</span>
        <StepStatusBadge status={renderStatus} />
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
      {/* Journal entries — only renders when there are any. Workflow-level
          StepType doesn't expose a "journal" kind, so we can't gate on
          that; the call is cheap and the component returns null when
          there's nothing to show. */}
      {!isPlanned && <JournalSection runId={runId} stepName={step.stepName} />}
    </div>
  );
}

/**
 * Renders the activity-journal entries for a `.journaled()` step. The
 * outer step shows up as a single node in the DAG / row in the timeline,
 * but its real execution is the chain of `ctx.activity` / `ctx.sleep` /
 * `ctx.signal` checkpoints persisted in the journal — listing them here
 * is the only way to see what actually ran.
 */
function JournalSection({ runId, stepName }: { runId: string; stepName: string }) {
  const [data, setData] = useState<{ supported: boolean; entries: JournalEntryDto[] } | undefined>(
    undefined,
  );
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    let cancelled = false;
    api
      .getRunStepJournal(runId, stepName)
      .then((r) => !cancelled && setData(r))
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, [runId, stepName]);

  if (error) {
    return (
      <Section title="Journal">
        <div class="alert alert-error text-sm">{error}</div>
      </Section>
    );
  }
  // Loading: render nothing rather than a flashing placeholder; most steps
  // aren't journaled and would never show entries anyway, so a loading
  // line for every step would be noise.
  if (!data) return null;
  // Hide the section entirely when the backend doesn't support journals
  // OR there are no entries — only journaled steps surface this section,
  // and only after they've actually run.
  if (!data.supported || data.entries.length === 0) return null;

  return (
    <Section title={`Journal (${data.entries.length})`}>
      <div class="space-y-1">
        {data.entries.map((e) => (
          <JournalRow entry={e} />
        ))}
      </div>
    </Section>
  );
}

function JournalRow({ entry }: { entry: JournalEntryDto }) {
  const isFailure = entry.exit?.tag === "Failure";
  const stepTypeLabel =
    entry.stepType === "sleep"
      ? "💤 sleep"
      : entry.stepType === "signal"
        ? "📡 signal"
        : entry.stepType === "compensation"
          ? "↩ comp"
          : entry.stepType === "child"
            ? "↗ child"
            : "● activity";
  const phaseClass =
    entry.phase === "pending" ? "badge-warning" : isFailure ? "badge-error" : "badge-success";
  return (
    <div class={`rounded p-2 text-sm ${isFailure ? "bg-error/10" : "bg-base-200"}`}>
      <div class="flex items-center gap-2 flex-wrap">
        <span class="font-mono text-xs text-base-content/50">#{entry.activityIndex}</span>
        {entry.branchPath && (
          <span class="badge badge-xs badge-ghost font-mono">br:{entry.branchPath}</span>
        )}
        <span class="text-xs text-base-content/60">{stepTypeLabel}</span>
        <span class="font-mono text-sm">{entry.activityName}</span>
        <span class={`badge badge-sm ${phaseClass}`}>
          {entry.phase}
          {isFailure ? " · failed" : ""}
        </span>
        {entry.payloadHash && (
          <span
            class="badge badge-xs badge-ghost font-mono"
            title={`Payload fingerprint ${entry.payloadHash}`}
          >
            #{entry.payloadHash.slice(0, 8)}
          </span>
        )}
        {entry.wakeAt && (
          <span class="text-xs text-base-content/60">wake {formatRelative(entry.wakeAt)}</span>
        )}
        <div class="flex-1" />
        <span class="text-xs text-base-content/50">{formatRelative(entry.createdAt)}</span>
      </div>
      {entry.exit?.tag === "Success" &&
        entry.exit.value !== undefined &&
        entry.exit.value !== null && (
          <div class="mt-1">
            <JsonBlock value={entry.exit.value} maxH="max-h-32" />
          </div>
        )}
      {entry.exit?.tag === "Failure" && (
        <div class="mt-1 text-xs text-error break-words">{entry.exit.error}</div>
      )}
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
