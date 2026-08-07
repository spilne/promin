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

export interface PageHeaderProps {
  title: string;
  eyebrow?: string;
  description?: ComponentChildren;
  meta?: ComponentChildren;
  actions?: ComponentChildren;
}

const SPACE_CLASS: Record<NonNullable<PageProps["space"]>, string> = {
  default: "space-y-4",
  none: "",
};

export function Page({ children, space = "default", className = "" }: PageProps) {
  return (
    <div
      class={`anim-page pt-6 px-4 pb-4 max-w-[1480px] mx-auto ${SPACE_CLASS[space]} ${className}`}
    >
      {children}
    </div>
  );
}

export function PageHeader({ title, eyebrow, description, meta, actions }: PageHeaderProps) {
  return (
    <div class="flex flex-col gap-3 border-b border-base-content/10 pb-4 md:flex-row md:items-end md:justify-between">
      <div class="min-w-0">
        {eyebrow && (
          <div class="mb-1 text-[11px] font-medium uppercase tracking-[0.14em] text-primary/80">
            {eyebrow}
          </div>
        )}
        <div class="flex flex-wrap items-center gap-3">
          <h2 class="text-2xl font-semibold tracking-tight leading-tight">{title}</h2>
          {meta && <div class="flex items-center gap-2 text-xs text-base-content/55">{meta}</div>}
        </div>
        {description && <div class="mt-1 text-sm text-base-content/55">{description}</div>}
      </div>
      {actions && <div class="flex shrink-0 flex-wrap items-center gap-2">{actions}</div>}
    </div>
  );
}
