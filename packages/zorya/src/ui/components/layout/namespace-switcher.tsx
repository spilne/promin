// ---------------------------------------------------------------------------
// NamespaceSwitcher — Vercel-style team picker at the top of the sidebar.
// Single button trigger (icon + current scope + chevron) opening a floating
// panel with search + the discovered namespace list. Writes to the
// `useNamespace` global store so every page re-queries with the new scope.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";

const ALL_LABEL = "All namespaces";

export function NamespaceSwitcher() {
  const [current, setCurrent] = useNamespace();
  const [namespaces, setNamespaces] = useState<string[]>([]);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const searchRef = useRef<HTMLInputElement | null>(null);

  // Refresh the namespace list whenever the dropdown opens — cheap, and it
  // picks up newly-triggered runs without requiring a full page reload.
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    api
      .listWorkflowNames()
      .then((r) => {
        if (cancelled) return;
        setNamespaces(r.namespaces ?? []);
      })
      .catch(() => {
        // Ignore — show whatever we have and let the user try again.
      });
    return () => {
      cancelled = true;
    };
  }, [open]);

  // Also seed the list on mount so the trigger shows a valid "current" even
  // before the user opens the panel.
  useEffect(() => {
    api
      .listWorkflowNames()
      .then((r) => setNamespaces(r.namespaces ?? []))
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (!open) return;
    requestAnimationFrame(() => searchRef.current?.focus());
    const handler = (e: MouseEvent) => {
      const target = e.target as Node | null;
      if (target && wrapRef.current?.contains(target)) return;
      setOpen(false);
      setQuery("");
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    const list = namespaces.slice().sort();
    if (!q) return list;
    return list.filter((n) => n.toLowerCase().includes(q));
  }, [namespaces, query]);

  const pick = (value: string) => {
    setCurrent(value);
    setOpen(false);
    setQuery("");
  };

  const label = current || ALL_LABEL;

  return (
    <div ref={wrapRef} class="relative px-3 py-2 border-b border-base-content/10">
      <button
        type="button"
        class="w-full flex items-center gap-2 px-2 py-1.5 rounded-md border border-base-content/15 bg-base-200/40 hover:bg-base-200 transition-colors"
        onClick={() => setOpen((v) => !v)}
        title="Switch namespace"
      >
        <span
          class={`w-5 h-5 rounded flex items-center justify-center text-[10px] font-bold shrink-0 ${
            current ? "bg-primary text-primary-content" : "bg-base-content/20 text-base-content/60"
          }`}
        >
          {current ? current.slice(0, 2).toUpperCase() : "∗"}
        </span>
        <span class="flex-1 text-sm font-medium truncate text-left" title={label}>
          {label}
        </span>
        <span class="text-base-content/40 text-xs">▾</span>
      </button>

      {open && (
        <div class="absolute left-3 right-3 z-30 mt-1 bg-base-100 border border-base-content/30 rounded-md shadow-lg overflow-hidden">
          <div class="p-1.5 border-b border-base-content/15">
            <input
              ref={searchRef}
              class="input input-bordered input-xs w-full"
              placeholder="Search namespaces…"
              value={query}
              onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
              onKeyDown={(e) => {
                if (e.key === "Escape") {
                  setOpen(false);
                  setQuery("");
                }
              }}
            />
          </div>
          <ul class="max-h-64 overflow-y-auto py-1">
            <NamespaceItem
              label={ALL_LABEL}
              active={!current}
              onClick={() => pick("")}
              icon="∗"
              iconClass="bg-base-content/20 text-base-content/60"
            />
            {filtered.length === 0 && !!query && (
              <li class="px-3 py-2 text-xs text-base-content/50 italic">No matches</li>
            )}
            {filtered.map((ns) => (
              <NamespaceItem
                label={ns}
                active={ns === current}
                onClick={() => pick(ns)}
                icon={ns.slice(0, 2).toUpperCase()}
                iconClass="bg-primary text-primary-content"
              />
            ))}
            {namespaces.length === 0 && (
              <li class="px-3 py-2 text-xs text-base-content/50 italic">No namespaces seen yet</li>
            )}
          </ul>
        </div>
      )}
    </div>
  );
}

function NamespaceItem({
  label,
  active,
  onClick,
  icon,
  iconClass,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
  icon: string;
  iconClass: string;
}) {
  return (
    <li
      class={`px-2 py-1.5 mx-1 rounded cursor-pointer flex items-center gap-2 text-sm ${
        active ? "bg-base-200 text-primary font-medium" : "hover:bg-base-200/60"
      }`}
      onClick={onClick}
    >
      <span
        class={`w-5 h-5 rounded flex items-center justify-center text-[10px] font-bold shrink-0 ${iconClass}`}
      >
        {icon}
      </span>
      <span class="flex-1 truncate">{label}</span>
      {active && <span class="text-primary">✓</span>}
    </li>
  );
}
