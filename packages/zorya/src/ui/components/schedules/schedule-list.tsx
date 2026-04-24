import { useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { ScheduleDto } from "../../../server/routes/schedules.ts";
import { formatCountdown, formatDuration, formatRelative } from "../../lib/format.ts";
import { CreateScheduleModal } from "./create-schedule-modal.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";

interface ScheduleListProps {
  onNavigate: (path: string) => void;
}

export function ScheduleList({ onNavigate }: ScheduleListProps) {
  const [showCreate, setShowCreate] = useState(false);
  const [editing, setEditing] = useState<ScheduleDto | undefined>(undefined);
  const { data, loading, error, refresh } = useFetch(() => api.listSchedules(), [], 10_000);

  if (loading && !data) {
    return (
      <div class="anim-page p-4 max-w-7xl mx-auto space-y-4">
        <div>
          <h2 class="text-xl font-semibold">Schedules</h2>
          <p class="text-xs text-base-content/50">Loading…</p>
        </div>
        <div class="card bg-base-100 shadow overflow-hidden">
          <table class="table">
            <thead>
              <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                <th>ID</th>
                <th>Name</th>
                <th>Workflow</th>
                <th>Trigger</th>
                <th>TZ</th>
                <th>Last fire</th>
                <th class="text-right">Ticks</th>
                <th>Status</th>
                <th />
              </tr>
            </thead>
            <tbody>
              <SkeletonRows rows={6} cols={9} />
            </tbody>
          </table>
        </div>
      </div>
    );
  }
  if (error) {
    return (
      <div class="anim-page p-4 max-w-7xl mx-auto">
        <div class="alert alert-error">{error.message}</div>
      </div>
    );
  }

  const configured = data?.configured !== false;
  const schedules = data?.schedules ?? [];

  const togglePause = async (s: ScheduleDto) => {
    try {
      await api.patchSchedule(s.id, { enabled: !s.enabled });
      refresh();
    } catch (e) {
      alert(`Failed: ${e}`);
    }
  };

  const remove = async (s: ScheduleDto) => {
    if (!confirm(`Delete schedule ${s.id}?`)) return;
    try {
      await api.deleteSchedule(s.id);
      refresh();
    } catch (e) {
      alert(`Failed: ${e}`);
    }
  };

  return (
    <div class="anim-page p-4 max-w-7xl mx-auto space-y-4">
      <div class="flex items-end justify-between">
        <div>
          <h2 class="text-xl font-semibold">Schedules</h2>
          <p class="text-xs text-base-content/50">
            {configured
              ? `${data?.total ?? 0} configured · auto-refreshes every 10s`
              : "Scheduler storage not configured on this server"}
          </p>
        </div>
        <div class="flex gap-2">
          {configured && (
            <button class="btn btn-sm btn-primary gap-1" onClick={() => setShowCreate(true)}>
              + New schedule
            </button>
          )}
          <button class="btn btn-sm btn-ghost gap-1" onClick={() => refresh()}>
            <span>↻</span>
            Refresh
          </button>
        </div>
      </div>

      {!configured && (
        <div class="alert alert-warning text-sm">
          <span>
            Pass a <code class="bg-base-300 px-1 rounded">SchedulerStorage</code> to{" "}
            <code class="bg-base-300 px-1 rounded">ZoryaServer</code> to see live schedules here.
          </span>
        </div>
      )}

      {configured && schedules.length === 0 && (
        <div class="card bg-base-100 shadow">
          <div class="card-body py-8 text-center text-base-content/50">
            No schedules yet — click "+ New schedule" to create one.
          </div>
        </div>
      )}

      {schedules.length > 0 && (
        <div class="card bg-base-100 shadow overflow-hidden">
          <div class="overflow-x-auto">
            <table class="table">
              <thead>
                <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                  <th>ID</th>
                  <th>Name</th>
                  <th>Workflow</th>
                  <th>Trigger</th>
                  <th>TZ</th>
                  <th>Last fire</th>
                  <th>Next fire</th>
                  <th class="text-right">Ticks</th>
                  <th>Status</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {schedules.map((s) => {
                  const wfName = (s.metadata?.["workflowName"] as string | undefined) ?? undefined;
                  return (
                    <tr class="hover:bg-base-200">
                      <td class="font-mono text-sm">{s.id}</td>
                      <td>{s.name ?? "—"}</td>
                      <td>
                        {wfName ? (
                          <button
                            class="btn btn-xs btn-ghost font-mono gap-1 normal-case"
                            title={`Show runs of ${wfName}`}
                            onClick={() => onNavigate(`/?name=${encodeURIComponent(wfName)}`)}
                          >
                            <span class="text-primary">↗</span>
                            {wfName}
                          </button>
                        ) : (
                          <span class="text-base-content/40">—</span>
                        )}
                      </td>
                      <td class="font-mono text-sm">{triggerLabel(s)}</td>
                      <td class="text-sm text-base-content/60">{s.timezone ?? "UTC"}</td>
                      <td class="text-sm text-base-content/60">{formatRelative(s.lastFiredAt)}</td>
                      <td class="text-sm text-base-content/60" title={s.nextRunAt ?? ""}>
                        {s.enabled
                          ? s.nextRunAt
                            ? formatCountdown(s.nextRunAt)
                            : "—"
                          : "(paused)"}
                      </td>
                      <td class="font-mono text-sm text-right">{s.tickCount ?? 0}</td>
                      <td>
                        <span
                          class={`badge badge-sm ${s.enabled ? "badge-success" : "badge-ghost"}`}
                        >
                          {s.enabled ? "enabled" : "paused"}
                        </span>
                      </td>
                      <td class="text-right">
                        <div class="flex gap-1 justify-end">
                          <button
                            class="btn btn-sm btn-ghost"
                            onClick={() => setEditing(s)}
                            title="Edit"
                          >
                            Edit
                          </button>
                          <button
                            class="btn btn-sm btn-ghost"
                            onClick={() => togglePause(s)}
                            title={s.enabled ? "Pause" : "Resume"}
                          >
                            {s.enabled ? "Pause" : "Resume"}
                          </button>
                          <button
                            class="btn btn-sm btn-ghost text-error"
                            onClick={() => remove(s)}
                            title="Delete"
                          >
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {showCreate && (
        <CreateScheduleModal
          onClose={() => setShowCreate(false)}
          onCreated={() => {
            setShowCreate(false);
            refresh();
          }}
        />
      )}
      {editing && (
        <CreateScheduleModal
          editing={editing}
          onClose={() => setEditing(undefined)}
          onCreated={() => {
            setEditing(undefined);
            refresh();
          }}
        />
      )}
    </div>
  );
}

function triggerLabel(s: ScheduleDto): string {
  if (s.cron) return s.cron;
  if (s.rrule) return `RRULE ${s.rrule.slice(0, 40)}${s.rrule.length > 40 ? "…" : ""}`;
  if (s.intervalMs !== undefined) return `every ${formatDuration(s.intervalMs)}`;
  return "—";
}
