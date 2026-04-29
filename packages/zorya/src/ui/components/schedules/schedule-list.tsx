import { useEffect, useMemo, useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { api } from "../../api/client.ts";
import type { ScheduleDto } from "../../../server/routes/schedules.ts";
import { formatCountdown, formatDuration, formatRelative } from "../../lib/format.ts";
import { confirm, toast } from "../../lib/dialogs.ts";
import { CreateScheduleModal } from "./create-schedule-modal.tsx";
import { ScheduleDrawer } from "./schedule-drawer.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";
import { Pagination } from "../ui/pagination.tsx";
import { Page } from "../ui/page.tsx";

const PAGE_SIZE = 20;
type StatusFilter = "all" | "active" | "cancelled";
type KindFilter = "all" | "workflow" | "agent";
type ScheduleSortCol = "name" | "lastFire" | "nextFire" | "tickCount" | "status";
type ScheduleSortState = { col: ScheduleSortCol; dir: "asc" | "desc" } | null;
// Status maps directly onto the `enabled` flag — cancelled = enabled:false.
// Same shape the chat drawer uses; both surfaces talk about cancelled
// schedules in the same terms.
const STATUS_FILTERS: ReadonlyArray<{ id: StatusFilter; label: string }> = [
  { id: "all", label: "All" },
  { id: "active", label: "Active" },
  { id: "cancelled", label: "Cancelled" },
];
const KIND_FILTERS: ReadonlyArray<{ id: KindFilter; label: string }> = [
  { id: "all", label: "All kinds" },
  { id: "workflow", label: "Workflows" },
  { id: "agent", label: "Agents" },
];

interface ScheduleListProps {
  onNavigate: (path: string) => void;
}

export function ScheduleList({ onNavigate }: ScheduleListProps) {
  const [showCreate, setShowCreate] = useState(false);
  const [editing, setEditing] = useState<ScheduleDto | undefined>(undefined);
  const [peeking, setPeeking] = useState<string | undefined>(undefined);
  const [namespace] = useNamespace();
  const [kindFilter, setKindFilter] = useState<KindFilter>("all");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("active");
  // Both filters push through to storage:
  //  - Status: maps directly to the `enabled` query param
  //    (`active` → enabled:true, `cancelled` → enabled:false, `all` omitted).
  //  - Kind: `agent` becomes a `metadata: { agentTrigger: true }`
  //    containment query; `workflow` needs a client-side post-filter
  //    because storage containment can't express "key is absent".
  // Defaults to `active` so cancelled rows don't crowd the operator
  // view by default — flip the chip to surface them.
  const { data, loading, error, refresh } = useFetch(
    () =>
      api.listSchedules({
        namespace: namespace || undefined,
        ...(statusFilter === "active" && { enabled: true }),
        ...(statusFilter === "cancelled" && { enabled: false }),
        ...(kindFilter === "agent" && { metadata: { agentTrigger: true } }),
      }),
    [namespace, kindFilter, statusFilter],
    10_000,
  );
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState<ScheduleSortState>(null);

  const filtered = useMemo(() => {
    const all = data?.schedules ?? [];
    const q = query.trim().toLowerCase();
    return all.filter((s) => {
      // Kind filter — `agent` is a positive server-side match (already
      // applied at fetch). `workflow` needs a client-side exclusion of
      // agent rows because storage containment can't express "key is
      // absent". `all` passes everything through.
      if (kindFilter === "workflow" && s.metadata?.["agentTrigger"] === true) return false;
      if (!q) return true;
      const wfName = (s.metadata?.["workflowName"] as string | undefined) ?? "";
      const fields = [s.id, s.name ?? "", wfName, s.cron ?? "", s.rrule ?? ""];
      return fields.some((f) => f.toLowerCase().includes(q));
    });
  }, [data, query, kindFilter]);

  const sorted = useMemo(() => {
    if (!sort) return filtered;
    const sign = sort.dir === "asc" ? 1 : -1;
    const copy = [...filtered];
    copy.sort((a, b) => {
      const av = scheduleSortKey(a, sort.col);
      const bv = scheduleSortKey(b, sort.col);
      if (av === undefined && bv === undefined) return 0;
      if (av === undefined) return 1;
      if (bv === undefined) return -1;
      if (av < bv) return -1 * sign;
      if (av > bv) return 1 * sign;
      return 0;
    });
    return copy;
  }, [filtered, sort]);

  const paged = useMemo(
    () => sorted.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE),
    [sorted, page],
  );

  function cycleSort(col: ScheduleSortCol) {
    setSort((cur) => {
      if (!cur || cur.col !== col) return { col, dir: "asc" };
      if (cur.dir === "asc") return { col, dir: "desc" };
      return null;
    });
  }

  useEffect(() => {
    setPage(1);
  }, [query, statusFilter, kindFilter]);

  if (loading && !data) {
    return (
      <Page>
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
      </Page>
    );
  }
  if (error) {
    return (
      <Page space="none">
        <div class="alert alert-error">{error.message}</div>
      </Page>
    );
  }

  const configured = data?.configured !== false;
  const schedules = data?.schedules ?? [];

  // Soft cancel via the `enabled` flag — same primitive the chat drawer
  // uses (`patchSchedule({ enabled: false })`). Both surfaces talk
  // about cancelled schedules in the same terms.
  const togglePause = async (s: ScheduleDto) => {
    try {
      await api.patchSchedule(s.id, { enabled: !s.enabled });
      refresh();
      toast(s.enabled ? "Schedule cancelled" : "Schedule restored", { variant: "success" });
    } catch (e) {
      toast(`Failed: ${e}`, { variant: "error" });
    }
  };

  const emitNow = async (s: ScheduleDto) => {
    try {
      await api.emitSchedule(s.id);
      refresh();
      toast(`Emitting ${s.id}…`, { variant: "success" });
    } catch (e) {
      toast(`Failed: ${e}`, { variant: "error" });
    }
  };

  const remove = async (s: ScheduleDto) => {
    const ok = await confirm({
      title: `Delete schedule ${s.id}?`,
      message: "Prior fire history is preserved; only the schedule definition is removed.",
      confirmLabel: "Delete",
      variant: "danger",
    });
    if (!ok) return;
    try {
      await api.deleteSchedule(s.id);
      refresh();
      toast("Schedule deleted", { variant: "success" });
    } catch (e) {
      toast(`Failed: ${e}`, { variant: "error" });
    }
  };

  return (
    <Page>
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

      {/* Filter row stays mounted whenever the scheduler is configured —
          previously gated on `schedules.length > 0`, which made the chips
          (and Clear button) disappear the moment a filter narrowed to
          zero results, trapping the user in the empty state. */}
      {configured && (
        <div class="flex items-center gap-2 flex-wrap justify-end">
          <div class="join">
            {KIND_FILTERS.map((f) => (
              <button
                class={`btn btn-sm join-item ${kindFilter === f.id ? "btn-primary" : "btn-ghost"}`}
                onClick={() => setKindFilter(f.id)}
                title={
                  f.id === "agent"
                    ? "Schedules created by agents (durable scheduler tool)"
                    : f.id === "workflow"
                      ? "Schedules that trigger a registered workflow"
                      : "All schedule kinds"
                }
              >
                {f.label}
              </button>
            ))}
          </div>
          <div class="join">
            {STATUS_FILTERS.map((f) => (
              <button
                class={`btn btn-sm join-item ${
                  statusFilter === f.id ? "btn-primary" : "btn-ghost"
                }`}
                onClick={() => setStatusFilter(f.id)}
              >
                {f.label}
              </button>
            ))}
          </div>
          <input
            class="input input-bordered input-sm w-full max-w-md font-mono"
            placeholder="Search by id, name, workflow, cron…"
            value={query}
            onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
          />
          {/* Always rendered so adding the first filter doesn't push the
              row left when "Clear" appears — fades via opacity transition. */}
          <button
            class={`btn btn-sm btn-ghost transition-opacity duration-150 ${
              query || statusFilter !== "all" || kindFilter !== "all"
                ? "opacity-100"
                : "opacity-0 pointer-events-none"
            }`}
            aria-hidden={!(query || statusFilter !== "all" || kindFilter !== "all")}
            tabIndex={query || statusFilter !== "all" || kindFilter !== "all" ? 0 : -1}
            onClick={() => {
              setQuery("");
              setStatusFilter("all");
              setKindFilter("all");
            }}
          >
            Clear
          </button>
        </div>
      )}

      {configured && schedules.length === 0 && (
        <div class="card bg-base-100 shadow">
          <div class="card-body py-8 text-center text-base-content/50">
            {kindFilter === "all"
              ? `No schedules yet — click "+ New schedule" to create one.`
              : kindFilter === "agent"
                ? "No agent schedules in this namespace. Agents create them via the durable scheduler tool from chat."
                : "No workflow schedules in this namespace."}
          </div>
        </div>
      )}

      {schedules.length > 0 && filtered.length === 0 && (
        <div class="card bg-base-100 shadow">
          <div class="card-body py-8 text-center text-base-content/50">
            No schedules match the current filters.
          </div>
        </div>
      )}

      {filtered.length > 0 && (
        <div class="card bg-base-100 shadow overflow-hidden">
          <div class="overflow-x-auto">
            <table class="table">
              <thead>
                <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                  <th>ID</th>
                  <SortableTh col="name" label="Name" sort={sort} onClick={cycleSort} />
                  <th>Workflow</th>
                  <th>Trigger</th>
                  <th>TZ</th>
                  <SortableTh col="lastFire" label="Last" sort={sort} onClick={cycleSort} />
                  <SortableTh col="nextFire" label="Next" sort={sort} onClick={cycleSort} />
                  <SortableTh
                    col="tickCount"
                    label="Ticks"
                    sort={sort}
                    onClick={cycleSort}
                    align="right"
                  />
                  <SortableTh col="status" label="Status" sort={sort} onClick={cycleSort} />
                  <th />
                </tr>
              </thead>
              <tbody>
                {paged.map((s) => {
                  const wfName = (s.metadata?.["workflowName"] as string | undefined) ?? undefined;
                  return (
                    <tr class="hover:bg-base-200">
                      <td class="font-mono text-sm">
                        {/* Clicking the id opens the full detail page. The
                            chevron in the actions column is the quick peek
                            (drawer with recent + upcoming). */}
                        <button
                          class="link link-hover font-mono"
                          title={`Open detail page for ${s.id}`}
                          onClick={() => onNavigate(`/schedules/${encodeURIComponent(s.id)}`)}
                        >
                          {s.id}
                        </button>
                      </td>
                      <td>{s.name ?? "—"}</td>
                      <td class="whitespace-nowrap">
                        {wfName ? (
                          <button
                            class="btn btn-xs btn-ghost font-mono normal-case"
                            title={`Show runs of ${wfName}`}
                            onClick={() => onNavigate(`/?name=${encodeURIComponent(wfName)}`)}
                          >
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
                          : "(cancelled)"}
                      </td>
                      <td class="font-mono text-sm text-right">{s.tickCount ?? 0}</td>
                      <td>
                        <span
                          class={`badge badge-sm ${s.enabled ? "badge-success" : "badge-ghost"}`}
                        >
                          {s.enabled ? "active" : "cancelled"}
                        </span>
                      </td>
                      <td class="text-right">
                        <div class="flex gap-1 justify-end">
                          <button
                            class="btn btn-sm btn-square btn-ghost"
                            onClick={() => setEditing(s)}
                            title="Edit"
                            aria-label="Edit"
                          >
                            <svg
                              xmlns="http://www.w3.org/2000/svg"
                              width="14"
                              height="14"
                              viewBox="0 0 24 24"
                              fill="none"
                              stroke="currentColor"
                              stroke-width="2"
                              stroke-linecap="round"
                              stroke-linejoin="round"
                            >
                              <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" />
                              <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" />
                            </svg>
                          </button>
                          <button
                            class="btn btn-sm btn-square btn-ghost"
                            onClick={() => togglePause(s)}
                            title={s.enabled ? "Cancel" : "Restore"}
                            aria-label={s.enabled ? "Cancel" : "Restore"}
                          >
                            {s.enabled ? (
                              <svg
                                xmlns="http://www.w3.org/2000/svg"
                                width="14"
                                height="14"
                                viewBox="0 0 24 24"
                                fill="currentColor"
                              >
                                <rect x="6" y="5" width="4" height="14" rx="1" />
                                <rect x="14" y="5" width="4" height="14" rx="1" />
                              </svg>
                            ) : (
                              <svg
                                xmlns="http://www.w3.org/2000/svg"
                                width="14"
                                height="14"
                                viewBox="0 0 24 24"
                                fill="currentColor"
                              >
                                <path d="M7 5v14l12-7z" />
                              </svg>
                            )}
                          </button>
                          <button
                            class="btn btn-sm btn-square btn-ghost"
                            onClick={() => emitNow(s)}
                            title="Emit now (fire on next poll)"
                            aria-label="Emit now"
                            disabled={!s.enabled}
                          >
                            <svg
                              xmlns="http://www.w3.org/2000/svg"
                              width="14"
                              height="14"
                              viewBox="0 0 24 24"
                              fill="currentColor"
                            >
                              <path d="M13 2L3 14h7l-1 8 10-12h-7l1-8z" />
                            </svg>
                          </button>
                          <button
                            class="btn btn-sm btn-square btn-ghost text-error"
                            onClick={() => remove(s)}
                            title="Delete"
                            aria-label="Delete"
                          >
                            <svg
                              xmlns="http://www.w3.org/2000/svg"
                              width="14"
                              height="14"
                              viewBox="0 0 24 24"
                              fill="none"
                              stroke="currentColor"
                              stroke-width="2"
                              stroke-linecap="round"
                              stroke-linejoin="round"
                            >
                              <polyline points="3 6 5 6 21 6" />
                              <path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6" />
                              <path d="M10 11v6" />
                              <path d="M14 11v6" />
                              <path d="M9 6V4a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2v2" />
                            </svg>
                          </button>
                          <button
                            class="btn btn-sm btn-square btn-ghost"
                            onClick={() => setPeeking(s.id)}
                            title="Peek (recent + upcoming)"
                            aria-label="Peek"
                          >
                            ›
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

      {filtered.length > 0 && (
        <Pagination
          page={page}
          pageSize={PAGE_SIZE}
          total={filtered.length}
          onChange={setPage}
          itemsLabel="schedules"
        />
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
      {peeking && (
        <ScheduleDrawer
          scheduleId={peeking}
          onClose={() => setPeeking(undefined)}
          onOpenDetail={(id) => {
            setPeeking(undefined);
            onNavigate(`/schedules/${encodeURIComponent(id)}`);
          }}
          onOpenRun={(id) => {
            setPeeking(undefined);
            onNavigate(`/runs/${encodeURIComponent(id)}`);
          }}
        />
      )}
    </Page>
  );
}

function triggerLabel(s: ScheduleDto): string {
  if (s.cron) return s.cron;
  if (s.rrule) return `RRULE ${s.rrule.slice(0, 40)}${s.rrule.length > 40 ? "…" : ""}`;
  if (s.intervalMs !== undefined) return `every ${formatDuration(s.intervalMs)}`;
  return "—";
}

function scheduleSortKey(s: ScheduleDto, col: ScheduleSortCol): number | string | undefined {
  switch (col) {
    case "name":
      return s.name ?? s.id;
    case "lastFire":
      return s.lastFiredAt ? new Date(s.lastFiredAt).getTime() : undefined;
    case "nextFire":
      return s.enabled && s.nextRunAt ? new Date(s.nextRunAt).getTime() : undefined;
    case "tickCount":
      return s.tickCount ?? 0;
    case "status":
      return s.enabled ? "active" : "cancelled";
  }
}

function SortableTh({
  col,
  label,
  sort,
  onClick,
  align,
}: {
  col: ScheduleSortCol;
  label: string;
  sort: ScheduleSortState;
  onClick: (col: ScheduleSortCol) => void;
  align?: "right";
}) {
  const active = sort?.col === col;
  const indicator = active ? (sort!.dir === "asc" ? "▲" : "▼") : "↕";
  return (
    <th
      class={`cursor-pointer select-none hover:text-base-content ${
        align === "right" ? "text-right" : ""
      }`}
      onClick={() => onClick(col)}
    >
      <span class="inline-flex items-center gap-1">
        {label}
        <span class={`text-[0.6rem] ${active ? "opacity-100" : "opacity-20"}`}>{indicator}</span>
      </span>
    </th>
  );
}
