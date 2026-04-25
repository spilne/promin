// ---------------------------------------------------------------------------
// Card — shared elevated-surface wrapper for content blocks.
//
// Uses the `surface-card` CSS rule (base-300 mixed with white in OKLCH) so
// cards stand out from the page bg on dark themes — DaisyUI doesn't ship a
// brighter base token, so we have a custom one. A border + soft shadow
// finish the elevation; `hover` opt-in brightens the surface and sharpens
// the border so interactive cards (worker tiles) feel clickable without
// requiring per-call boilerplate.
//
// Most callers want the default body padding (`p-4`); pass `padding="none"`
// when wrapping content that already provides its own (e.g. tables that
// carry their own border + body).
// ---------------------------------------------------------------------------

import type { ComponentChildren } from "preact";

export interface CardProps {
  children: ComponentChildren;
  /** Body padding. `default` = p-4. */
  padding?: "default" | "none";
  /** When true, brightens on hover + slightly raises shadow. */
  hover?: boolean;
  /** Extra classes on the outer card. */
  className?: string;
  /** Extra classes on the inner body. Useful for `space-y` rhythm. */
  bodyClassName?: string;
}

const PADDING_CLASS: Record<NonNullable<CardProps["padding"]>, string> = {
  default: "p-4",
  none: "",
};

export function Card({
  children,
  padding = "default",
  hover = false,
  className = "",
  bodyClassName = "",
}: CardProps) {
  const hoverClass = hover
    ? "surface-card-hover hover:shadow-md hover:border-base-content/25 transition-shadow"
    : "";
  return (
    <div
      class={`card surface-card border border-base-content/15 shadow-sm ${hoverClass} ${className}`}
    >
      <div class={`card-body ${PADDING_CLASS[padding]} ${bodyClassName}`}>{children}</div>
    </div>
  );
}
