import type { ComponentChildren } from "preact";

export interface TabDef<V extends string> {
  id: V;
  label: string;
  /** When false, tab is visible but not clickable and dimmed. */
  enabled?: boolean;
  /** Optional badge/count to show next to the label. */
  badge?: string | number;
}

interface TabsProps<V extends string> {
  tabs: ReadonlyArray<TabDef<V>>;
  active: V;
  onChange: (id: V) => void;
  /** "lifted" | "boxed" | "bordered" — passed through to DaisyUI. */
  variant?: "lifted" | "boxed" | "bordered";
  /** Tailwind size modifier. */
  size?: "xs" | "sm" | "md";
  /** Extra class on the wrapper. */
  class?: string;
}

export function Tabs<V extends string>({
  tabs,
  active,
  onChange,
  // `variant` kept for API compat — lifted is the only style we ship now,
  // since DaisyUI's tabs-lifted didn't read well against the dark theme.
  variant: _variant = "lifted",
  size = "sm",
  class: klass = "",
}: TabsProps<V>) {
  const padX = size === "xs" ? "px-2" : size === "md" ? "px-4" : "px-3";
  const padY = size === "xs" ? "py-1" : size === "md" ? "py-2" : "py-1.5";
  const fontSize = size === "xs" ? "text-xs" : size === "md" ? "text-base" : "text-sm";

  // Folder-style: selected tab gets L/T/R borders, no bottom (so it visually
  // merges with the panel below). The container has a bottom border that the
  // selected tab "cuts" with a -mb-px overlay. Unselected tabs are dim with
  // a hairline bottom border so they sit on the same baseline.
  return (
    <div class={`flex gap-0 border-b border-base-content/30 ${klass}`}>
      {tabs.map((t) => {
        const enabled = t.enabled !== false;
        const isActive = active === t.id;
        const base = `${padX} ${padY} ${fontSize} -mb-px transition-colors`;
        const stateClass = isActive
          ? "border-l border-t border-r border-base-content/30 rounded-t bg-base-100 text-base-content font-medium"
          : "border-b border-transparent text-base-content/55 hover:text-base-content/85";
        const disabledClass = enabled ? "cursor-pointer" : "opacity-40 cursor-not-allowed";
        return (
          <a
            class={`${base} ${stateClass} ${disabledClass}`}
            onClick={() => enabled && onChange(t.id)}
          >
            {t.label}
            {t.badge !== undefined && t.badge !== "" && (
              <span class="ml-1 badge badge-sm badge-ghost">{t.badge}</span>
            )}
          </a>
        );
      })}
    </div>
  );
}

interface TabPanelProps {
  children: ComponentChildren;
  class?: string;
}

export function TabPanel({ children, class: klass = "p-4" }: TabPanelProps) {
  return <div class={klass}>{children}</div>;
}
