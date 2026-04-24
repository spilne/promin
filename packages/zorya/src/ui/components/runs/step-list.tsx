import type { StepDto } from "../../../server/api-types.ts";
import { StepStatusBadge } from "../ui/status-badge.tsx";
import { formatDuration, formatRelative } from "../../lib/format.ts";

interface StepListProps {
  steps: StepDto[];
}

export function StepList({ steps }: StepListProps) {
  if (steps.length === 0) {
    return (
      <div class="card bg-base-100 shadow">
        <div class="card-body py-6 text-center text-base-content/50">No steps</div>
      </div>
    );
  }
  return (
    <div class="card bg-base-100 shadow overflow-hidden">
      <div class="overflow-x-auto">
        <table class="table table-sm">
          <thead>
            <tr class="bg-base-200">
              <th>Name</th>
              <th>Type</th>
              <th>Status</th>
              <th>Duration</th>
              <th>Attempt</th>
              <th>Started</th>
            </tr>
          </thead>
          <tbody>
            {steps.map((s) => (
              <tr class="hover:bg-base-200">
                <td class="font-mono text-xs">{s.stepName}</td>
                <td class="text-xs text-base-content/60">{s.stepType}</td>
                <td>
                  <StepStatusBadge status={s.status} />
                </td>
                <td class="font-mono text-xs">{formatDuration(s.durationMs)}</td>
                <td class="font-mono text-xs">{s.attempt}</td>
                <td class="text-xs text-base-content/60">{formatRelative(s.startedAt)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
