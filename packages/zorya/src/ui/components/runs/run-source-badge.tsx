// ---------------------------------------------------------------------------
// RunSourceBadge — small visual marker for what kicked a run off.
//
// Used in the runs list and run detail header. Glyph + label, optionally
// linked to a deeper view (e.g., schedule detail page for "schedule" runs).
// ---------------------------------------------------------------------------

import type { RunSource } from "@promin/workflow";

interface SourceVisual {
  icon: string;
  label: string;
  /** DaisyUI badge color class — kept consistent with status-badge.tsx. */
  badgeClass: string;
}

const SOURCE_VISUAL: Record<RunSource, SourceVisual> = {
  schedule: { icon: "⏱", label: "Schedule", badgeClass: "badge-info" },
  manual: { icon: "👆", label: "Manual", badgeClass: "badge-ghost" },
  api: { icon: "⇆", label: "API", badgeClass: "badge-ghost" },
  webhook: { icon: "🔗", label: "Webhook", badgeClass: "badge-ghost" },
  parent: { icon: "↳", label: "Sub-run", badgeClass: "badge-ghost" },
  agent: { icon: "🧠", label: "Agent", badgeClass: "badge-accent" },
};

interface RunSourceBadgeProps {
  source?: RunSource;
  sourceId?: string;
  /** When set, the badge renders as a button that calls this on click. */
  onClick?: () => void;
  /** Show the producer id alongside the source label. Default: false (compact). */
  showId?: boolean;
}

export function RunSourceBadge({ source, sourceId, onClick, showId }: RunSourceBadgeProps) {
  if (!source) {
    // Older rows without a typed source — render nothing. The header
    // already has plenty of other badges, and a faint placeholder ends up
    // looking like noise next to the saturated status badge.
    return null;
  }
  const v = SOURCE_VISUAL[source];
  const inner = (
    <>
      <span aria-hidden>{v.icon}</span>
      <span>{v.label}</span>
      {showId && sourceId && (
        <span class="font-mono text-[10px] opacity-70 truncate max-w-[10rem]">{sourceId}</span>
      )}
    </>
  );
  if (onClick) {
    return (
      <button
        class={`badge badge-sm ${v.badgeClass} gap-1 cursor-pointer hover:opacity-80`}
        onClick={onClick}
        title={sourceId ? `${v.label} · ${sourceId}` : v.label}
      >
        {inner}
      </button>
    );
  }
  return (
    <span
      class={`badge badge-sm ${v.badgeClass} gap-1`}
      title={sourceId ? `${v.label} · ${sourceId}` : v.label}
    >
      {inner}
    </span>
  );
}

export const RUN_SOURCE_OPTIONS: Array<{ id: "all" | RunSource; label: string }> = [
  { id: "all", label: "All sources" },
  { id: "schedule", label: "Schedule" },
  { id: "manual", label: "Manual" },
  { id: "api", label: "API" },
  { id: "webhook", label: "Webhook" },
  { id: "parent", label: "Sub-run" },
  { id: "agent", label: "Agent" },
];
