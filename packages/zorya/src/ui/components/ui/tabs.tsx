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
  variant = "lifted",
  size = "sm",
  class: klass = "",
}: TabsProps<V>) {
  const variantClass =
    variant === "lifted" ? "tabs-lifted" : variant === "boxed" ? "tabs-boxed" : "tabs-bordered";
  const sizeClass = `tab-${size}`;
  return (
    <div class={`tabs ${variantClass} ${klass}`}>
      {tabs.map((t) => {
        const enabled = t.enabled !== false;
        return (
          <a
            class={`tab ${sizeClass} ${active === t.id ? "tab-active" : ""} ${!enabled ? "opacity-40 cursor-not-allowed" : ""}`}
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
