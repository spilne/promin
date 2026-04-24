import { useState } from "preact/hooks";
import { useFetch } from "../../hooks/use-fetch.ts";
import { api } from "../../api/client.ts";
import type { WorkflowStatus } from "@promin/workflow";
import type { RunListQuery } from "../../../server/api-types.ts";
import { StatsBar } from "./stats-bar.tsx";
import { StatusBadge } from "../ui/status-badge.tsx";
import { formatDuration, formatRelative } from "../../lib/format.ts";

interface RunListProps {
  onOpen: (id: string) => void;
}

const STATUSES: Array<WorkflowStatus | ""> = [
  "",
  "pending",
  "running",
  "suspended",
  "completed",
  "failed",
];

export function RunList({ onOpen }: RunListProps) {
  const [name, setName] = useState("");
  const [status, setStatus] = useState<WorkflowStatus | "">("");
  const query: RunListQuery = {
    name: name || undefined,
    status: status || undefined,
    limit: 100,
  };
  const { data, loading, error, refresh } = useFetch(
    () => api.listRuns(query),
    [name, status],
    5000,
  );

  return (
    <div class="p-4 max-w-7xl mx-auto space-y-4">
      <StatsBar />

      <div class="flex gap-3 items-center">
        <h2 class="text-xl font-semibold">Runs</h2>
        <div class="flex-1" />
        <input
          type="text"
          placeholder="Name"
          class="input input-bordered input-sm w-48"
          value={name}
          onInput={(e) => setName((e.target as HTMLInputElement).value)}
        />
        <select
          class="select select-bordered select-sm"
          value={status}
          onChange={(e) => setStatus((e.target as HTMLSelectElement).value as WorkflowStatus | "")}
        >
          {STATUSES.map((s) => (
            <option value={s}>{s || "all statuses"}</option>
          ))}
        </select>
        <button class="btn btn-sm btn-ghost" onClick={() => refresh()}>
          ↻ Refresh
        </button>
      </div>

      {error && (
        <div class="alert alert-error">
          <span>Failed to load: {error.message}</span>
        </div>
      )}

      <div class="card bg-base-100 shadow overflow-hidden">
        <div class="overflow-x-auto">
          <table class="table table-sm">
            <thead>
              <tr class="bg-base-200">
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
                  <td colSpan={6} class="text-center py-8 text-base-content/50">
                    No runs yet
                  </td>
                </tr>
              )}
              {data?.runs.map((r) => (
                <tr class="hover:bg-base-200 cursor-pointer" onClick={() => onOpen(r.workflowId)}>
                  <td class="font-mono text-xs">{r.workflowId}</td>
                  <td>{r.workflowName}</td>
                  <td class="text-base-content/60">{r.workflowType ?? "—"}</td>
                  <td>
                    <StatusBadge status={r.status} />
                  </td>
                  <td class="text-base-content/60">{formatRelative(r.createdAt)}</td>
                  <td class="font-mono text-xs">{formatDuration(r.totalMs)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
