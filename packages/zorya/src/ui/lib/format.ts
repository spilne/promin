import type { WorkflowStatus, StepStatus, StepType } from "@promin/workflow";

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

// ---------------------------------------------------------------------------
// Status / step visual registries — single source of truth.
//
// Every component that shows a status should read from here instead of
// hard-coding colours or icons.
// ---------------------------------------------------------------------------

export interface StatusVisual {
  /** Short glyph shown inside badges or next to rows. */
  icon: string;
  /** DaisyUI badge class. */
  badgeClass: string;
  /** Tailwind bg class for Gantt bar / dots. */
  barClass: string;
  /** Tailwind text class for inline text. */
  textClass: string;
  /** Human-readable label for chips/filters. */
  label: string;
}

export const WORKFLOW_STATUS_VISUAL: Record<WorkflowStatus, StatusVisual> = {
  pending: {
    icon: "·",
    badgeClass: "badge-ghost",
    barClass: "bg-base-content/20",
    textClass: "text-base-content/60",
    label: "Queued",
  },
  running: {
    icon: "↻",
    badgeClass: "badge-info",
    barClass: "bg-info",
    textClass: "text-info",
    label: "Executing",
  },
  suspended: {
    icon: "⏸",
    badgeClass: "badge-warning",
    barClass: "bg-warning",
    textClass: "text-warning",
    label: "Suspended",
  },
  completed: {
    icon: "✓",
    badgeClass: "badge-success",
    barClass: "bg-success",
    textClass: "text-success",
    label: "Completed",
  },
  failed: {
    icon: "✕",
    badgeClass: "badge-error",
    barClass: "bg-error",
    textClass: "text-error",
    label: "Failed",
  },
  compensating: {
    icon: "↺",
    badgeClass: "badge-warning",
    barClass: "bg-warning",
    textClass: "text-warning",
    label: "Compensating",
  },
  tripwire: {
    icon: "⚠",
    badgeClass: "badge-error",
    barClass: "bg-error",
    textClass: "text-error",
    label: "Tripwire",
  },
};

export const STEP_STATUS_VISUAL: Record<StepStatus, StatusVisual> = {
  pending: {
    icon: "·",
    badgeClass: "badge-ghost",
    barClass: "bg-base-content/20",
    textClass: "text-base-content/60",
    label: "Queued",
  },
  running: {
    icon: "↻",
    badgeClass: "badge-info",
    barClass: "bg-info",
    textClass: "text-info",
    label: "Running",
  },
  completed: {
    icon: "✓",
    badgeClass: "badge-success",
    barClass: "bg-success",
    textClass: "text-success",
    label: "Completed",
  },
  failed: {
    icon: "✕",
    badgeClass: "badge-error",
    barClass: "bg-error",
    textClass: "text-error",
    label: "Failed",
  },
  skipped: {
    icon: "↷",
    badgeClass: "badge-ghost",
    barClass: "bg-base-content/20",
    textClass: "text-base-content/50",
    label: "Skipped",
  },
  sleeping: {
    icon: "⏸",
    badgeClass: "badge-warning",
    barClass: "bg-warning",
    textClass: "text-warning",
    label: "Sleeping",
  },
  waiting_for_signal: {
    icon: "⏳",
    badgeClass: "badge-warning",
    barClass: "bg-warning",
    textClass: "text-warning",
    label: "Waiting",
  },
  compensated: {
    icon: "↺",
    badgeClass: "badge-success",
    barClass: "bg-success",
    textClass: "text-success",
    label: "Compensated",
  },
  compensation_failed: {
    icon: "✕",
    badgeClass: "badge-error",
    barClass: "bg-error",
    textClass: "text-error",
    label: "Comp. failed",
  },
};

export const STEP_TYPE_ICON: Record<StepType, string> = {
  single: "▣",
  map: "⋮⋮",
  sleep: "⏱",
  signal: "⚑",
};
