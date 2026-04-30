import type * as preact from "preact";
import { useEffect, useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type {
  ScheduleDto,
  ScheduleHistoryResponse,
  ScheduleUpcomingResponse,
} from "../../../server/routes/schedules.ts";
import type { WorkflowStatus } from "@promin/workflow";
import { StatusBadge } from "../ui/status-badge.tsx";
import { Page } from "../ui/page.tsx";
import { Pagination } from "../ui/pagination.tsx";
import { Skeleton } from "../ui/skeleton.tsx";
import { JsonBlock } from "../ui/json-block.tsx";
import { ScheduleHistoryChart } from "./schedule-history-chart.tsx";
import { DateTime } from "../ui/date-time.tsx";
import { formatDuration } from "../../lib/format.ts";

const PAGE_SIZE = 20;
const UPCOMING_COUNT = 20;

interface ScheduleDetailProps {
  id: string;
  onBack: () => void;
  onOpenRun: (id: string) => void;
}

export function ScheduleDetail({ id, onBack, onOpenRun }: ScheduleDetailProps) {
  const [page, setPage] = useState(1);
  const [view, setView] = useState<"table" | "plot">("table");

  const {
    data: schedule,
    loading: scheduleLoading,
    error: scheduleError,
  } = useFetch(() => api.getSchedule(id), [id], 10_000);

  const offset = (page - 1) * PAGE_SIZE;
  // Fetch one extra row so we can detect "is there a next page" without an
  // extra count round-trip — when the server returns < limit we know we're
  // on the last page.
  const {
    data: history,
    loading: historyLoading,
    refresh: refreshHistory,
  } = useFetch<ScheduleHistoryResponse>(
    () => api.getScheduleHistory(id, { limit: PAGE_SIZE + 1, offset }),
    [id, offset],
    10_000,
  );

  const { data: upcoming } = useFetch<ScheduleUpcomingResponse>(
    () => api.getScheduleUpcoming(id, { count: UPCOMING_COUNT }),
    [id],
    30_000,
  );

  // Slice off the look-ahead row used to compute hasMore.
  const visibleHistory = useMemo(
    () => (history ? history.history.slice(0, PAGE_SIZE) : []),
    [history],
  );
  const hasMore = (history?.history.length ?? 0) > PAGE_SIZE;
  const totalEstimate = hasMore ? offset + PAGE_SIZE + 1 : offset + visibleHistory.length;

  // Scroll to top whenever the schedule id changes — same UX as the
  // workflow / run detail pages.
  useEffect(() => {
    window.scrollTo({ top: 0, behavior: "instant" as ScrollBehavior });
  }, [id]);

  if (scheduleLoading && !schedule) {
    return (
      <Page>
        <div class="flex items-center gap-2">
          <button class="btn btn-sm btn-ghost" onClick={onBack}>
            ← Back
          </button>
          <Skeleton class="h-6 w-48" />
        </div>
        <Skeleton class="h-32 w-full" />
      </Page>
    );
  }

  if (scheduleError || !schedule) {
    return (
      <Page>
        <button class="btn btn-sm btn-ghost" onClick={onBack}>
          ← Back
        </button>
        <div class="alert alert-error">{scheduleError?.message ?? "Schedule not found"}</div>
      </Page>
    );
  }

  return (
    <Page>
      <div class="flex items-center justify-between gap-2">
        <div class="flex items-center gap-3 min-w-0">
          <button class="btn btn-sm btn-ghost" onClick={onBack}>
            ← Back
          </button>
          <div class="min-w-0">
            <h2 class="text-xl font-semibold font-mono truncate">{schedule.id}</h2>
            <p class="text-xs text-base-content/50 truncate">
              {schedule.name ?? "(no display name)"}
            </p>
          </div>
        </div>
        <div class="flex items-center gap-2 shrink-0">
          <span class={`badge badge-sm ${schedule.enabled ? "badge-success" : "badge-ghost"}`}>
            {schedule.enabled ? "enabled" : "paused"}
          </span>
        </div>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
        <SummaryCell label="Trigger" value={triggerLabel(schedule)} mono />
        <SummaryCell label="Timezone" value={schedule.timezone ?? "UTC"} />
        <SummaryCell label="Namespace" value={schedule.namespace ?? "—"} />
        <SummaryCell
          label="Last fired"
          value={schedule.lastFiredAt ? <RelativePlusUtc iso={schedule.lastFiredAt} /> : "never"}
        />
        <SummaryCell
          label="Next run"
          value={
            !schedule.enabled ? (
              "(paused)"
            ) : schedule.nextRunAt ? (
              <RelativePlusUtc iso={schedule.nextRunAt} countdown />
            ) : (
              "—"
            )
          }
        />
        <SummaryCell label="Total ticks" value={String(schedule.tickCount ?? 0)} mono />
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div class="card surface-card shadow lg:col-span-2">
          <div class="card-body p-0">
            <div class="px-4 pt-4 flex items-center justify-between gap-2">
              <SectionTitle>History</SectionTitle>
              <div class="flex items-center gap-2">
                {/* View switcher: tabular vs scatter plot. Plot is most
                    useful for spotting cadence gaps and duration drift,
                    table is for direct row inspection + actions. */}
                <div class="join">
                  <button
                    class={`btn btn-xs join-item ${view === "table" ? "btn-primary" : "btn-ghost"}`}
                    onClick={() => setView("table")}
                    title="Table view"
                  >
                    Table
                  </button>
                  <button
                    class={`btn btn-xs join-item ${view === "plot" ? "btn-primary" : "btn-ghost"}`}
                    onClick={() => setView("plot")}
                    title="Plot view"
                  >
                    Plot
                  </button>
                </div>
                <button class="btn btn-xs btn-ghost" onClick={() => refreshHistory()}>
                  ↻ Refresh
                </button>
              </div>
            </div>
            {historyLoading && !history ? (
              <div class="p-4 space-y-2">
                {Array.from({ length: 6 }).map(() => (
                  <Skeleton class="h-8 w-full" />
                ))}
              </div>
            ) : visibleHistory.length === 0 ? (
              <div class="p-8 text-center text-base-content/50">No fires yet</div>
            ) : view === "plot" ? (
              <div class="p-4 anim-fade-in">
                <ScheduleHistoryChart history={visibleHistory} />
              </div>
            ) : (
              <div class="overflow-x-auto anim-fade-in">
                <table class="table">
                  <thead>
                    <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                      <th>Tick</th>
                      <th>Status</th>
                      <th>Workflow ID</th>
                      <th>Fired</th>
                      <th
                        class="text-right"
                        title="Time from scheduler fire to worker pickup (queue + dispatch latency)"
                      >
                        Lag
                      </th>
                      <th class="text-right" title="Total elapsed: completedAt − firedAt">
                        Duration
                      </th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {visibleHistory.map((h) => {
                      const when = h.firedAt ?? h.startedAt;
                      return (
                        <tr class="hover:bg-base-200">
                          <td class="font-mono text-sm">#{h.tickNumber ?? "?"}</td>
                          <td>
                            <StatusBadge status={h.status as WorkflowStatus} />
                          </td>
                          <td class="font-mono text-xs truncate max-w-[20rem]">{h.workflowId}</td>
                          <td class="text-sm text-base-content/60">
                            <DateTime iso={when} mode="relative" />
                          </td>
                          <td
                            class="text-sm text-right text-base-content/60"
                            title="startedAt − firedAt"
                          >
                            {h.lagMs !== undefined ? formatDuration(h.lagMs) : "—"}
                          </td>
                          <td
                            class="text-sm text-right text-base-content/60"
                            title="completedAt − firedAt"
                          >
                            {h.durationMs !== undefined ? formatDuration(h.durationMs) : "—"}
                          </td>
                          <td>
                            <button
                              class="btn btn-xs btn-ghost"
                              onClick={() => onOpenRun(h.workflowId)}
                            >
                              Open →
                            </button>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
            <div class="p-3 border-t border-base-300">
              <Pagination
                page={page}
                pageSize={PAGE_SIZE}
                total={totalEstimate}
                onChange={setPage}
                itemsLabel="fires"
              />
            </div>
          </div>
        </div>

        <div class="card surface-card shadow">
          <div class="card-body">
            <SectionTitle>Upcoming</SectionTitle>
            {upcoming === undefined ? (
              <Skeleton class="h-24 w-full" />
            ) : upcoming.upcoming.length === 0 ? (
              <div class="text-sm text-base-content/50">
                {upcoming.exhausted ? "Schedule has no further fires" : "Schedule is paused"}
              </div>
            ) : (
              <ul class="space-y-2">
                {upcoming.upcoming.map((u) => (
                  <li class="card bg-base-100 border border-base-300 px-3 py-2 shadow-sm">
                    <div class="flex items-center gap-2">
                      <span class="font-mono text-xs text-base-content/60">#{u.tickNumber}</span>
                      <DateTime
                        class="text-sm ml-auto"
                        iso={u.scheduledAt}
                        mode="relative"
                        countdown
                      />
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      </div>

      {schedule.metadata && Object.keys(schedule.metadata).length > 0 && (
        <div class="card surface-card shadow">
          <div class="card-body">
            <SectionTitle>Metadata</SectionTitle>
            <JsonBlock value={schedule.metadata} />
          </div>
        </div>
      )}
    </Page>
  );
}

/**
 * Pair of timestamps: bold relative ("5m ago") + faint absolute UTC. The
 * `layout` prop picks between row (absolute follows on the same line, like
 * "5m ago · Apr 26 18:45 UTC") and stacked (absolute on the line below).
 * Pick row when there's horizontal headroom, stacked when vertical.
 */
function RelativePlusUtc({
  iso,
  countdown,
  layout = "row",
}: {
  iso: string;
  countdown?: boolean;
  layout?: "row" | "stacked";
}) {
  if (layout === "row") {
    return (
      <div class="flex items-baseline gap-2 flex-wrap leading-tight">
        <DateTime iso={iso} mode="relative" countdown={countdown} />
        <DateTime iso={iso} mode="utc" class="text-xs text-base-content/50 font-mono" />
      </div>
    );
  }
  return (
    <div class="leading-tight">
      <DateTime iso={iso} mode="relative" countdown={countdown} />
      <DateTime iso={iso} mode="utc" class="block text-xs text-base-content/50 mt-0.5 font-mono" />
    </div>
  );
}

function SectionTitle({ children }: { children: preact.ComponentChildren }) {
  return (
    <h4 class="text-xs font-semibold text-base-content/50 uppercase tracking-wide">{children}</h4>
  );
}

function triggerLabel(s: ScheduleDto): string {
  if (s.cron) return s.cron;
  if (s.rrule) return `RRULE ${s.rrule.slice(0, 40)}${s.rrule.length > 40 ? "…" : ""}`;
  if (s.intervalMs !== undefined) return `every ${formatDuration(s.intervalMs)}`;
  return "—";
}

function SummaryCell({
  label,
  value,
  mono,
  title,
}: {
  label: string;
  value: preact.ComponentChildren;
  mono?: boolean;
  title?: string;
}) {
  return (
    <div class="card surface-card shadow">
      <div class="card-body p-3">
        <div class="text-xs uppercase tracking-wider text-base-content/50">{label}</div>
        <div class={`text-base ${mono ? "font-mono" : ""}`} title={title}>
          {value}
        </div>
      </div>
    </div>
  );
}
