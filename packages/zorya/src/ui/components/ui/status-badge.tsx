import type { WorkflowStatus, StepStatus } from "@promin/workflow";
import { WORKFLOW_STATUS_VISUAL, STEP_STATUS_VISUAL } from "../../lib/format.ts";

interface StatusBadgeProps {
  status: WorkflowStatus;
  size?: "sm" | "md";
}

export function StatusBadge({ status, size = "sm" }: StatusBadgeProps) {
  const v = WORKFLOW_STATUS_VISUAL[status];
  const sizeClass = size === "sm" ? "badge-sm" : "";
  return (
    <span class={`badge ${sizeClass} ${v.badgeClass} gap-1`}>
      <span>{v.icon}</span>
      {v.label}
    </span>
  );
}

interface StepStatusBadgeProps {
  status: StepStatus;
}

export function StepStatusBadge({ status }: StepStatusBadgeProps) {
  const v = STEP_STATUS_VISUAL[status];
  return (
    <span class={`badge badge-sm ${v.badgeClass} gap-1`}>
      <span>{v.icon}</span>
      {v.label}
    </span>
  );
}
