import { useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { WorkflowStatus } from "@promin/workflow";
import type { RunListQuery } from "../../../server/api-types.ts";
import { StatsBar } from "./stats-bar.tsx";
import { StatusBadge } from "../ui/status-badge.tsx";
import { formatDuration, formatRelative, WORKFLOW_STATUS_VISUAL } from "../../lib/format.ts";

interface RunListProps {
  onOpen: (id: string) => void;
}

const STATUS_FILTERS: Array<WorkflowStatus | "all"> = [
  "all",
  "running",
  "suspended",
  "pending",
  "completed",
  "failed",
];

export function RunList({ onOpen }: RunListProps) {
  const [name, setName] = useState("");
  const [status, setStatus] = useState<WorkflowStatus | "all">("all");

  const query: RunListQuery = {
    name: name || undefined,
    status: status === "all" ? undefined : status,
    limit: 100,
  };
  const { data, loading, error, refresh } = useFetch(
    () => api.listRuns(query),
    [name, status],
    5000,
  );

  return (
    <div class="anim-page p-4 max-w-[1400px] mx-auto space-y-4">
      <div class="flex items-end justify-between">
        <div>
          <h2 class="text-xl font-semibold">Runs</h2>
          <p class="text-sm text-base-content/50">Live · auto-refreshes every 5s</p>
        </div>
        <button class="btn btn-sm btn-ghost gap-1" onClick={() => refresh()}>
          <span>↻</span>
          Refresh
        </button>
      </div>

      <StatsBar />

      {/* Filter bar (chip-style) */}
      <div class="flex items-center gap-2 flex-wrap">
        <div class="join">
          {STATUS_FILTERS.map((s) => {
            const active = status === s;
            const label = s === "all" ? "All" : WORKFLOW_STATUS_VISUAL[s as WorkflowStatus].label;
            return (
              <button
                class={`btn btn-sm join-item ${active ? "btn-primary" : "btn-ghost"}`}
                onClick={() => setStatus(s)}
              >
                {label}
              </button>
            );
          })}
        </div>
        <div class="h-5 w-px bg-base-content/20 mx-1" />
        <div class="relative">
          <input
            type="text"
            placeholder="Filter by name…"
            class="input input-bordered input-sm w-56 pl-7"
            value={name}
            onInput={(e) => setName((e.target as HTMLInputElement).value)}
          />
          <span class="absolute left-2 top-1/2 -translate-y-1/2 text-base-content/40 text-sm">
            ⌕
          </span>
        </div>
        {(name || status !== "all") && (
          <button
            class="btn btn-sm btn-ghost"
            onClick={() => {
              setName("");
              setStatus("all");
            }}
          >
            Clear
          </button>
        )}
      </div>

      {error && (
        <div class="alert alert-error text-sm">
          <span>Failed to load: {error.message}</span>
        </div>
      )}

      <div class="card bg-base-100 shadow overflow-hidden">
        <div class="overflow-x-auto">
          <table class="table">
            <thead>
              <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                <th>ID</th>
                <th>Name</th>
                <th>Type</th>
                <th>Status</th>
                <th>Started</th>
                <th>Duration</th>
              </tr>
            </thead>
            <tbody>
              {loading && !data && (
                <tr>
                  <td colSpan={6} class="text-center py-8 text-base-content/50">
                    Loading…
                  </td>
                </tr>
              )}
              {data && data.runs.length === 0 && !loading && (
                <tr>
                  <td colSpan={6} class="text-center py-12">
                    <div class="text-base-content/50">No runs match the current filters</div>
                  </td>
                </tr>
              )}
              {data?.runs.map((r) => (
                <tr class="hover:bg-base-200 cursor-pointer" onClick={() => onOpen(r.workflowId)}>
                  <td class="font-mono text-sm">{r.workflowId}</td>
                  <td>{r.workflowName}</td>
                  <td class="text-base-content/60">{r.workflowType ?? "—"}</td>
                  <td>
                    <StatusBadge status={r.status} />
                  </td>
                  <td class="text-base-content/60">{formatRelative(r.createdAt)}</td>
                  <td class="font-mono text-sm">{formatDuration(r.totalMs)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
