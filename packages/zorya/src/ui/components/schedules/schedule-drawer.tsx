import { useEffect, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type {
  ScheduleHistoryResponse,
  ScheduleUpcomingResponse,
} from "../../../server/routes/schedules.ts";
import type { WorkflowStatus } from "@promin/workflow";
import { StatusBadge } from "../ui/status-badge.tsx";
import { formatCountdown, formatDuration, formatRelative } from "../../lib/format.ts";

interface ScheduleDrawerProps {
  scheduleId: string;
  onClose: () => void;
  onOpenDetail: (id: string) => void;
  onOpenRun: (id: string) => void;
}

const RECENT_LIMIT = 5;
const UPCOMING_LIMIT = 5;

export function ScheduleDrawer({
  scheduleId,
  onClose,
  onOpenDetail,
  onOpenRun,
}: ScheduleDrawerProps) {
  const [history, setHistory] = useState<ScheduleHistoryResponse | undefined>(undefined);
  const [upcoming, setUpcoming] = useState<ScheduleUpcomingResponse | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);

  // Single load on open. Drawer is meant for a quick peek — refresh comes
  // from re-opening, not from a poll loop, so we don't burn fetches while
  // the user is just hovering.
  useEffect(() => {
    let cancelled = false;
    Promise.all([
      api.getScheduleHistory(scheduleId, { limit: RECENT_LIMIT }),
      api.getScheduleUpcoming(scheduleId, { count: UPCOMING_LIMIT }),
    ])
      .then(([h, u]) => {
        if (cancelled) return;
        setHistory(h);
        setUpcoming(u);
      })
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, [scheduleId]);

  // Esc closes — matches the modal pattern elsewhere.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <>
      {/* Backdrop — click to dismiss. Above modal-z so it sits over the list,
          below the dialog-host so confirmation dialogs from other actions
          still surface. */}
      <div class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in" onClick={onClose} aria-hidden />
      <aside
        class="fixed top-0 right-0 h-screen w-full max-w-md bg-base-100 shadow-2xl
               z-40 flex flex-col anim-drawer-in"
        role="dialog"
        aria-label={`Schedule ${scheduleId}`}
      >
        <header class="flex items-start justify-between gap-2 p-4 border-b border-base-300">
          <div class="min-w-0">
            <div class="text-xs text-base-content/50 uppercase tracking-wider">Schedule</div>
            <div class="font-mono text-lg truncate">{scheduleId}</div>
          </div>
          <div class="flex gap-1 shrink-0">
            <button
              class="btn btn-sm btn-primary"
              onClick={() => onOpenDetail(scheduleId)}
              title="Open full detail page"
            >
              Open detail
            </button>
            <button class="btn btn-sm btn-ghost" onClick={onClose} aria-label="Close" title="Close">
              ✕
            </button>
          </div>
        </header>

        <div class="overflow-y-auto flex-1 p-4 space-y-5">
          {error && <div class="alert alert-error text-sm">{error}</div>}

          <section>
            <h4 class="text-xs font-semibold uppercase tracking-wider text-base-content/60 mb-2">
              Recent fires
            </h4>
            {history === undefined ? (
              <div class="text-sm text-base-content/40">Loading…</div>
            ) : history.history.length === 0 ? (
              <div class="text-sm text-base-content/40">No fires yet</div>
            ) : (
              <ul class="space-y-1">
                {history.history.map((h) => {
                  // Prefer the dispatcher-stamped firedAt (the moment the
                  // trigger actually went out); fall back to startedAt for
                  // legacy rows whose metadata predates the firedAt write.
                  const when = h.firedAt ?? h.startedAt;
                  return (
                    <li
                      class="card surface-card surface-card-hover px-3 py-2 cursor-pointer"
                      onClick={() => onOpenRun(h.workflowId)}
                    >
                      <div class="flex items-center gap-2">
                        <StatusBadge status={h.status as WorkflowStatus} size="sm" />
                        <span class="font-mono text-xs text-base-content/70">
                          #{h.tickNumber ?? "?"}
                        </span>
                        <span class="text-sm text-base-content/60 ml-auto" title={when ?? ""}>
                          {formatRelative(when)}
                        </span>
                      </div>
                      <div class="text-xs text-base-content/50 mt-1 flex justify-between gap-2">
                        <span class="truncate font-mono">{h.workflowId}</span>
                        {h.durationMs !== undefined && <span>{formatDuration(h.durationMs)}</span>}
                      </div>
                    </li>
                  );
                })}
              </ul>
            )}
          </section>

          <section>
            <h4 class="text-xs font-semibold uppercase tracking-wider text-base-content/60 mb-2">
              Upcoming
            </h4>
            {upcoming === undefined ? (
              <div class="text-sm text-base-content/40">Loading…</div>
            ) : upcoming.upcoming.length === 0 ? (
              <div class="text-sm text-base-content/40">
                {upcoming.exhausted ? "Schedule has no further fires" : "Schedule is paused"}
              </div>
            ) : (
              <ul class="space-y-1">
                {upcoming.upcoming.map((u) => (
                  <li class="card surface-card px-3 py-2">
                    <div class="flex items-center gap-2">
                      <span class="font-mono text-xs text-base-content/60">#{u.tickNumber}</span>
                      <span class="text-sm ml-auto" title={u.scheduledAt}>
                        {formatCountdown(u.scheduledAt)}
                      </span>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </section>
        </div>
      </aside>
    </>
  );
}
