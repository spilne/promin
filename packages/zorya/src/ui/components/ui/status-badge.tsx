import type { WorkflowStatus, StepStatus } from "@promin/workflow";
import { statusColor, stepStatusColor, statusIcon } from "../../lib/format.ts";

interface StatusBadgeProps {
  status: WorkflowStatus;
  size?: "sm" | "md";
}

export function StatusBadge({ status, size = "sm" }: StatusBadgeProps) {
  const sizeClass = size === "sm" ? "badge-sm" : "";
  return (
    <span class={`badge ${sizeClass} ${statusColor[status]} gap-1`}>
      <span>{statusIcon(status)}</span>
      {status}
    </span>
  );
}

interface StepStatusBadgeProps {
  status: StepStatus;
}

export function StepStatusBadge({ status }: StepStatusBadgeProps) {
  return (
    <span class={`badge badge-sm ${stepStatusColor[status]} gap-1`}>
      <span>{statusIcon(status)}</span>
      {status}
    </span>
  );
}
