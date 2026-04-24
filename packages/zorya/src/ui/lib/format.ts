import type { WorkflowStatus, StepStatus } from "@promin/workflow";

export function formatDuration(ms?: number): string {
  if (ms === undefined || ms === null) return "—";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)}s`;
  const minutes = Math.floor(ms / 60_000);
  const seconds = Math.round((ms % 60_000) / 1000);
  return `${minutes}m ${seconds}s`;
}

export function formatRelative(iso?: string): string {
  if (!iso) return "—";
  const then = new Date(iso).getTime();
  const now = Date.now();
  const diff = now - then;
  if (diff < 5_000) return "just now";
  if (diff < 60_000) return `${Math.floor(diff / 1000)}s ago`;
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
  return `${Math.floor(diff / 86_400_000)}d ago`;
}

export const statusColor: Record<WorkflowStatus, string> = {
  pending: "badge-ghost",
  running: "badge-info",
  suspended: "badge-warning",
  completed: "badge-success",
  failed: "badge-error",
  compensating: "badge-warning",
};

export const stepStatusColor: Record<StepStatus, string> = {
  pending: "badge-ghost",
  running: "badge-info",
  completed: "badge-success",
  failed: "badge-error",
  skipped: "badge-ghost",
  sleeping: "badge-warning",
  waiting_for_signal: "badge-warning",
  compensated: "badge-success",
  compensation_failed: "badge-error",
};

export const stepBarColor: Record<StepStatus, string> = {
  pending: "bg-base-content/20",
  running: "bg-info",
  completed: "bg-success",
  failed: "bg-error",
  skipped: "bg-base-content/20",
  sleeping: "bg-warning",
  waiting_for_signal: "bg-warning",
  compensated: "bg-success",
  compensation_failed: "bg-error",
};

export function statusIcon(status: WorkflowStatus | StepStatus): string {
  switch (status) {
    case "completed":
    case "compensated":
      return "✓";
    case "failed":
    case "compensation_failed":
      return "✕";
    case "running":
      return "↻";
    case "suspended":
    case "sleeping":
    case "waiting_for_signal":
      return "⏸";
    case "pending":
      return "·";
    case "skipped":
      return "↷";
    default:
      return "·";
  }
}
