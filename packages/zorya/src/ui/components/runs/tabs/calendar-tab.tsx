// ---------------------------------------------------------------------------
// CalendarTab — GitHub-contributions-style heatmap. Days on the X axis
// (weeks as columns), days-of-week on Y. Cell colour reflects the worst
// outcome that day (any failure = red, else all success = green, else
// lighter shades).
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { api } from "../../../api/client.ts";
import type { SparklinesResponse } from "../../../../server/routes/grid.ts";
import type { WorkflowStatus } from "@promin/workflow";
import { EmptyState } from "../../ui/empty-state.tsx";

interface CalendarTabProps {
  workflowName: string;
}

const WEEKS_BACK = 20;

export function CalendarTab({ workflowName }: CalendarTabProps) {
  const [data, setData] = useState<SparklinesResponse | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    let cancelled = false;
    // Ask for enough recent runs to populate the heatmap. 50 / name × 20
    // weeks × (runs/day) should cover most demo workloads.
    api
      .getSparklines(50)
      .then((r) => !cancelled && setData(r))
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, [workflowName]);

  const grid = useMemo(() => {
    if (!data) return undefined;
    return buildGrid(data[workflowName] ?? [], WEEKS_BACK);
  }, [data, workflowName]);

  if (error) return <div class="alert alert-error text-sm">{error}</div>;
  if (!grid) return <div class="text-sm text-base-content/50">Loading…</div>;

  const runsTotal = (data?.[workflowName] ?? []).length;
  if (runsTotal === 0) {
    return <EmptyState message={`No recent runs of ${workflowName}.`} />;
  }

  return (
    <div class="space-y-3">
      <div class="text-sm text-base-content/60">
        Last {WEEKS_BACK} weeks · {runsTotal} runs
      </div>
      <div class="flex gap-1">
        <div class="flex flex-col justify-around text-[10px] text-base-content/40 pr-1">
          <span>M</span>
          <span>W</span>
          <span>F</span>
        </div>
        <div class="flex gap-[2px]">
          {grid.columns.map((col) => (
            <div class="flex flex-col gap-[2px]">
              {col.map((cell) => (
                <div class={`w-3 h-3 rounded-[2px] ${cell.class}`} title={cell.tooltip} />
              ))}
            </div>
          ))}
        </div>
      </div>
      <div class="flex items-center gap-1 text-xs text-base-content/50">
        <span>Less</span>
        <div class="w-3 h-3 rounded-[2px] bg-base-200" />
        <div class="w-3 h-3 rounded-[2px] bg-success/30" />
        <div class="w-3 h-3 rounded-[2px] bg-success/60" />
        <div class="w-3 h-3 rounded-[2px] bg-success" />
        <span class="mx-1">·</span>
        <div class="w-3 h-3 rounded-[2px] bg-error/60" />
        <div class="w-3 h-3 rounded-[2px] bg-error" />
        <span>Any fail</span>
      </div>
    </div>
  );
}

interface GridCell {
  class: string;
  tooltip: string;
}

function buildGrid(
  runs: ReadonlyArray<{ status: WorkflowStatus; createdAt: string }>,
  weeks: number,
): { columns: GridCell[][] } {
  // Bucket runs by YYYY-MM-DD in local time.
  const byDay = new Map<string, { total: number; failed: number; completed: number }>();
  for (const r of runs) {
    const d = new Date(r.createdAt);
    const key = `${d.getFullYear()}-${d.getMonth() + 1}-${d.getDate()}`;
    const cur = byDay.get(key) ?? { total: 0, failed: 0, completed: 0 };
    cur.total += 1;
    if (r.status === "failed") cur.failed += 1;
    if (r.status === "completed") cur.completed += 1;
    byDay.set(key, cur);
  }

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  // Start on Sunday of the week `weeks-1` ago.
  const start = new Date(today);
  start.setDate(start.getDate() - start.getDay() - (weeks - 1) * 7);

  const columns: GridCell[][] = [];
  for (let w = 0; w < weeks; w++) {
    const col: GridCell[] = [];
    for (let dow = 0; dow < 7; dow++) {
      const d = new Date(start);
      d.setDate(d.getDate() + w * 7 + dow);
      if (d > today) {
        col.push({ class: "bg-transparent", tooltip: "" });
        continue;
      }
      const key = `${d.getFullYear()}-${d.getMonth() + 1}-${d.getDate()}`;
      const bucket = byDay.get(key);
      const label = d.toDateString();
      if (!bucket || bucket.total === 0) {
        col.push({ class: "bg-base-200", tooltip: `${label} · no runs` });
      } else if (bucket.failed > 0) {
        const intensity = bucket.failed / bucket.total;
        col.push({
          class: intensity > 0.5 ? "bg-error" : "bg-error/60",
          tooltip: `${label} · ${bucket.failed}/${bucket.total} failed`,
        });
      } else {
        const bucketTotal = bucket.total;
        const cls =
          bucketTotal >= 5 ? "bg-success" : bucketTotal >= 2 ? "bg-success/60" : "bg-success/30";
        col.push({ class: cls, tooltip: `${label} · ${bucketTotal} ok` });
      }
    }
    columns.push(col);
  }
  return { columns };
}
