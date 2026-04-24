import type { ComponentChildren } from "preact";

export interface DataListItem {
  label: string;
  value: ComponentChildren;
  /** Hide the item entirely when value is null/undefined/empty string. Default true. */
  skipEmpty?: boolean;
  /** Extra class on the <dd>. Useful for mono-width keys / ids. */
  valueClass?: string;
}

interface DataListProps {
  items: DataListItem[];
  /** Extra class on the wrapper. */
  class?: string;
}

/**
 * Key/value rendering. A dozen places in the run detail were hand-rolling
 * the same `<dl class="grid grid-cols-[auto,1fr] …">` block; this
 * centralises the pattern.
 */
export function DataList({ items, class: klass = "" }: DataListProps) {
  const visible = items.filter((i) => ((i.skipEmpty ?? true) ? !isEmpty(i.value) : true));
  return (
    <dl class={`grid grid-cols-[auto,1fr] gap-x-3 gap-y-1 text-sm ${klass}`}>
      {visible.map((i) => (
        <>
          <dt class="text-base-content/60">{i.label}</dt>
          <dd class={i.valueClass ?? ""}>{i.value}</dd>
        </>
      ))}
    </dl>
  );
}

function isEmpty(v: ComponentChildren): boolean {
  return v === undefined || v === null || v === "" || v === false;
}
