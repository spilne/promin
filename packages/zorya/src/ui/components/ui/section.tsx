import type { ComponentChildren } from "preact";

interface SectionProps {
  title: string;
  /** Optional right-side content (buttons, counts). */
  actions?: ComponentChildren;
  children: ComponentChildren;
  /** Extra class on the wrapper. */
  class?: string;
}

/**
 * Consistent block header: `<h4>TITLE</h4>` in uppercase muted text with
 * optional right-aligned actions row. Used inside right-pane tabs for
 * Input / Result / Error / Payload / etc. blocks.
 */
export function Section({ title, actions, children, class: klass = "" }: SectionProps) {
  return (
    <div class={`space-y-1 ${klass}`}>
      <div class="flex items-center">
        <h4 class="text-xs font-semibold text-base-content/50 uppercase tracking-wide">{title}</h4>
        <div class="flex-1" />
        {actions}
      </div>
      {children}
    </div>
  );
}
