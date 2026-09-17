// ---------------------------------------------------------------------------
// useGraphOrientation — picks horizontal vs vertical for an SVG graph
// based on the measured container width.
//
// Shared by every graph view (workflow step DAG, agent run-trace graph).
// Default is horizontal — most panels are wide enough for a short DAG, and
// holding the first paint horizontal avoids a visible flip while the
// container width is still being measured. We switch to vertical only once
// a real width has arrived AND the horizontal layout overflows it by more
// than a hysteresis margin (so a 1px overshoot doesn't flap the layout).
// ---------------------------------------------------------------------------

import { useLayoutEffect, useRef, useState, type MutableRef } from "preact/hooks";
import type { GraphOrientation } from "../lib/graph-layout.ts";

// A container can be a few pixels narrower than the horizontal layout
// (scrollbar, padding rounding) without it being worth flipping to a
// taller vertical layout. Empirically 24px swallows the common cases.
const ORIENTATION_HYSTERESIS_PX = 24;

export interface GraphOrientationResult {
  /** Attach to the scroll container the SVG lives in. */
  readonly containerRef: MutableRef<HTMLDivElement | null>;
  readonly orientation: GraphOrientation;
}

/**
 * @param horizontalWidth width of the diagram laid out horizontally
 * @param verticalWidth width of the diagram laid out vertically
 */
export function useGraphOrientation(args: {
  horizontalWidth: number;
  verticalWidth: number;
}): GraphOrientationResult {
  const containerRef = useRef<HTMLDivElement | null>(null);
  // `null` = not yet measured — don't pick an orientation until a real
  // width arrives, otherwise the first paint flips horizontal → vertical
  // → horizontal as the measurement settles.
  const [containerW, setContainerW] = useState<number | null>(null);

  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    // Measure synchronously before paint so the first painted frame is
    // already in the correct orientation.
    setContainerW(el.clientWidth || null);
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        if (entry.contentRect.width > 0) setContainerW(entry.contentRect.width);
      }
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const orientation: GraphOrientation =
    containerW != null &&
    args.horizontalWidth > containerW + ORIENTATION_HYSTERESIS_PX &&
    args.verticalWidth <= args.horizontalWidth
      ? "vertical"
      : "horizontal";

  return { containerRef, orientation };
}
