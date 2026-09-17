// ---------------------------------------------------------------------------
// Page — shared top-level wrapper for every dashboard page.
//
// Centralizes max-width, horizontal centering, top/side/bottom padding, and
// the page-in animation so every page reads at the same width and the
// content doesn't visibly jump when switching tabs in the sidebar.
//
// Some pages (e.g. error states) want a tighter `space-y` rhythm or none at
// all — pass `space="none"` to opt out of the default vertical gap.
// ---------------------------------------------------------------------------

import type { ComponentChildren } from "preact";

export interface PageProps {
  children: ComponentChildren;
  /** Vertical spacing between direct children. `default` = space-y-4. */
  space?: "default" | "none";
  /** Extra classes applied to the wrapper. */
  className?: string;
}

const SPACE_CLASS: Record<NonNullable<PageProps["space"]>, string> = {
  default: "space-y-4",
  none: "",
};

export function Page({ children, space = "default", className = "" }: PageProps) {
  return (
    <div
      class={`anim-page pt-8 px-4 pb-4 max-w-[1400px] mx-auto ${SPACE_CLASS[space]} ${className}`}
    >
      {children}
    </div>
  );
}
