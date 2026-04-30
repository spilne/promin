// ---------------------------------------------------------------------------
// DateTime — single-source date renderer for the dashboard.
//
// Shows time in UTC (the canonical timestamp) by default and reveals the
// viewer's local-zone equivalent on hover. The visible vs. tooltip split
// follows the dashboard convention: server-side state is UTC, the user's
// frame of reference is their local clock.
//
// Supports three display modes — pick the one that fits the surface:
//   "utc"      → "Apr 26, 18:45 UTC" (compact absolute, default)
//   "relative" → "5m ago" / "in 30s" using the existing format helpers
//   "datetime" → "2026-04-26 18:45:32 UTC" (verbose, for forensic detail)
//
// Tooltip always carries BOTH zones so a user can scan a single row of
// timestamps without losing the local-clock anchor.
// ---------------------------------------------------------------------------

import { formatCountdown, formatRelative } from "../../lib/format.ts";

export type DateTimeMode = "utc" | "relative" | "datetime";

interface DateTimeProps {
  /** ISO 8601 string. `undefined` / null renders an em-dash. */
  iso?: string | null;
  /** Default: `"utc"`. */
  mode?: DateTimeMode;
  /** Extra class on the wrapper `<time>`. */
  class?: string;
  /**
   * Show the future-tense `formatCountdown` ("in 30s") instead of the
   * past-tense `formatRelative` when `mode="relative"`. Default: false
   * (auto-picks based on whether the timestamp is in the past or future).
   */
  countdown?: boolean;
}

/**
 * Render an ISO timestamp with a UTC/local-aware tooltip. The visible
 * label depends on `mode`; the tooltip is always the same shape:
 *
 *   UTC:   2026-04-26 18:45:32
 *   Local: Sun, Apr 26, 11:45:32 AM PDT
 */
export function DateTime({ iso, mode = "utc", class: klass, countdown }: DateTimeProps) {
  if (!iso) return <span class={`text-base-content/40 ${klass ?? ""}`}>—</span>;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) {
    return <span class={`text-base-content/40 ${klass ?? ""}`}>—</span>;
  }
  const visible = (() => {
    if (mode === "relative") {
      // Auto-pick relative vs. countdown based on whether `iso` is in the
      // past or future, unless the caller pinned the direction. Avoids
      // showing "just now" for any timestamp seconds in the future
      // (formatRelative's negative-diff edge case).
      const inFuture = d.getTime() > Date.now();
      return countdown || inFuture ? formatCountdown(iso) : formatRelative(iso);
    }
    if (mode === "datetime") return formatUtcDateTime(d);
    return formatUtcCompact(d);
  })();
  // DaisyUI's `.tooltip` shows on hover/focus with no native-title delay
  // (the OS-managed `title=` tooltip waits ~1.5s before appearing). Wrap
  // a `<time>` so screen readers + the dateTime attribute still work.
  // Newlines turn into actual line breaks via `whitespace-pre` on the
  // bubble's content.
  const tip = `UTC:   ${formatUtcDateTime(d)}\nLocal: ${formatLocalFull(d)}`;
  return (
    <span
      class={`tooltip tooltip-top before:whitespace-pre before:text-left before:font-mono before:text-xs ${klass ?? ""}`}
      data-tip={tip}
    >
      <time dateTime={iso}>{visible}</time>
    </span>
  );
}

// ---------------------------------------------------------------------------
// Formatters — kept local so the component is the single seam touching
// `Intl.DateTimeFormat`. Other call sites should use <DateTime/> rather
// than re-rolling these.
// ---------------------------------------------------------------------------

const UTC_COMPACT = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

const UTC_DATETIME = new Intl.DateTimeFormat("en-US", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

const LOCAL_FULL = new Intl.DateTimeFormat(undefined, {
  weekday: "short",
  year: "numeric",
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  timeZoneName: "short",
});

function formatUtcCompact(d: Date): string {
  // "Apr 26, 18:45 UTC" — Intl gives us "Apr 26, 06:45 PM" with hour12,
  // or "Apr 26, 18:45" with hour12=false. Append the zone suffix so it's
  // unambiguous when shown next to local-format dates elsewhere.
  return `${UTC_COMPACT.format(d).replace(",", ",")} UTC`;
}

function formatUtcDateTime(d: Date): string {
  // "04/26/2026, 18:45:32" → reshape to "2026-04-26 18:45:32" (ISO-ish,
  // no T, no zone) for the tooltip's first line. Easier to scan than the
  // raw `.toISOString()` and matches the convention the rest of the
  // dashboard uses for human-readable absolute times.
  const parts = UTC_DATETIME.formatToParts(d);
  const get = (t: Intl.DateTimeFormatPartTypes) => parts.find((p) => p.type === t)?.value ?? "";
  return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")}:${get("second")}`;
}

function formatLocalFull(d: Date): string {
  return LOCAL_FULL.format(d);
}
