import type { WorkflowStatus } from "@promin/workflow";
import { WORKFLOW_STATUS_VISUAL } from "../../lib/format.ts";

interface SparklineProps {
  /** Recent runs, newest first (matches storage.listWorkflows order). */
  runs: ReadonlyArray<{ status: WorkflowStatus; createdAt: string }>;
  /** Total slot count. Missing slots render as empty. Default 14. */
  slots?: number;
  /** Bar width in px. Default 4. */
  barW?: number;
  /** Total height in px. Default 18. */
  height?: number;
}

/**
 * Compact run-outcome history — one thin vertical bar per recent run,
 * coloured by status. Newest on the right so a trailing edge of red
 * reads as "recently broken" at a glance.
 */
export function Sparkline({ runs, slots = 14, barW = 4, height = 18 }: SparklineProps) {
  // API returns newest-first; flip so newest sits on the right.
  const ordered = runs.slice(0, slots).reverse();
  const pad = slots - ordered.length;
  const cells: Array<{ status?: WorkflowStatus; key: string }> = [];
  for (let i = 0; i < pad; i++) cells.push({ key: `pad-${i}` });
  ordered.forEach((r, i) => cells.push({ status: r.status, key: `r-${i}-${r.createdAt}` }));

  return (
    <div class="flex items-end gap-[2px]" style={{ height: `${height}px` }} title={tooltip(runs)}>
      {cells.map((c) => {
        const color = c.status ? WORKFLOW_STATUS_VISUAL[c.status].barClass : "bg-base-content/10";
        return (
          <div
            class={`rounded-sm ${color}`}
            style={{ width: `${barW}px`, height: `${height}px` }}
          />
        );
      })}
    </div>
  );
}

function tooltip(runs: ReadonlyArray<{ status: WorkflowStatus }>): string {
  if (runs.length === 0) return "No recent runs";
  const counts: Partial<Record<WorkflowStatus, number>> = {};
  for (const r of runs) counts[r.status] = (counts[r.status] ?? 0) + 1;
  const parts = Object.entries(counts).map(([s, n]) => `${n} ${s}`);
  return `Last ${runs.length} runs: ${parts.join(", ")}`;
}
