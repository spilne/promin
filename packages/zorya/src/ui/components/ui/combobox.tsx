// ---------------------------------------------------------------------------
// Combobox — styled dropdown with optional search. Drop-in replacement for
// <select>; same `value` + `onChange` shape but with a custom panel, click-
// outside dismiss, keyboard nav, and a search input that filters long lists.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useRef, useState } from "preact/hooks";

export interface ComboboxOption {
  /** Stored value. */
  value: string;
  /** Display label. Defaults to value. */
  label?: string;
  /** Optional sub-label (rendered dimmed). */
  hint?: string;
}

interface ComboboxProps {
  value: string;
  onChange: (value: string) => void;
  /**
   * Static option list. When `loadOptions` is also provided it acts as
   * the initial / "fallback" set shown before any async load resolves
   * (useful so the trigger label can still render the selected value).
   */
  options: ReadonlyArray<ComboboxOption>;
  /**
   * Async loader. When set, the panel calls this on open + on each
   * keystroke (debounced) and shows the returned options instead of
   * filtering `options` client-side. Backend search.
   */
  loadOptions?: (query: string) => Promise<ReadonlyArray<ComboboxOption>>;
  /** Debounce window for `loadOptions` (ms). Default 200. */
  loadDebounceMs?: number;
  /** Placeholder when value is empty. */
  placeholder?: string;
  /**
   * Show search input above the options. Default: auto-on when options
   * count exceeds 6 OR `loadOptions` is supplied.
   */
  searchable?: boolean;
  /** Width / sizing class (passed through). */
  class?: string;
  size?: "xs" | "sm" | "md";
  disabled?: boolean;
}

export function Combobox({
  value,
  onChange,
  options,
  loadOptions,
  loadDebounceMs = 200,
  placeholder = "Select…",
  searchable,
  class: klass = "w-40",
  size = "sm",
  disabled = false,
}: ComboboxProps) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [highlight, setHighlight] = useState(0);
  const [loaded, setLoaded] = useState<ReadonlyArray<ComboboxOption>>(options);
  const [loading, setLoading] = useState(false);
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const searchRef = useRef<HTMLInputElement | null>(null);
  // Token guards stale async results — only the latest load is allowed
  // to update state. Prevents a slow earlier query from flashing back
  // after a faster later query has already resolved.
  const loadTokenRef = useRef(0);

  const showSearch = searchable ?? (options.length > 6 || !!loadOptions);
  const sizeClass = size === "xs" ? "input-xs h-7" : size === "md" ? "input-md" : "input-sm";

  // Async loading path: fire on open and on every query change (debounced).
  useEffect(() => {
    if (!loadOptions || !open) return;
    const myToken = ++loadTokenRef.current;
    setLoading(true);
    const handle = setTimeout(async () => {
      try {
        const result = await loadOptions(query);
        if (loadTokenRef.current === myToken) {
          setLoaded(result);
          setLoading(false);
        }
      } catch {
        if (loadTokenRef.current === myToken) {
          setLoaded([]);
          setLoading(false);
        }
      }
    }, loadDebounceMs);
    return () => clearTimeout(handle);
  }, [loadOptions, open, query, loadDebounceMs]);

  // Resolved options shown in the panel:
  //   - async mode: whatever the loader returned for the current query
  //   - static mode: the prop list filtered by the typed query
  const filtered = useMemo(() => {
    if (loadOptions) return loaded;
    const q = query.trim().toLowerCase();
    if (!q) return options;
    return options.filter((o) => {
      const label = (o.label ?? o.value).toLowerCase();
      const hint = o.hint?.toLowerCase() ?? "";
      return label.includes(q) || hint.includes(q);
    });
  }, [options, query, loadOptions, loaded]);

  // Selected label — shown on the trigger when closed.
  const selected = options.find((o) => o.value === value);
  const triggerLabel = selected?.label ?? selected?.value ?? "";

  // Close on outside click. Using mousedown so the close beats a re-open
  // click on a sibling combobox.
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      const target = e.target as Node | null;
      if (target && wrapRef.current?.contains(target)) return;
      setOpen(false);
      setQuery("");
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  // Focus the search box when the panel opens. Keeps highlight in range as
  // the filter changes underneath.
  useEffect(() => {
    if (open) {
      setHighlight(0);
      if (showSearch) requestAnimationFrame(() => searchRef.current?.focus());
    }
  }, [open, showSearch]);

  useEffect(() => {
    if (highlight >= filtered.length) setHighlight(Math.max(0, filtered.length - 1));
  }, [filtered, highlight]);

  const choose = (opt: ComboboxOption) => {
    onChange(opt.value);
    setOpen(false);
    setQuery("");
  };

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === "Escape") {
      e.preventDefault();
      setOpen(false);
      setQuery("");
      return;
    }
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setHighlight((h) => Math.min(filtered.length - 1, h + 1));
      return;
    }
    if (e.key === "ArrowUp") {
      e.preventDefault();
      setHighlight((h) => Math.max(0, h - 1));
      return;
    }
    if (e.key === "Enter") {
      e.preventDefault();
      const opt = filtered[highlight];
      if (opt) choose(opt);
    }
  };

  return (
    <div ref={wrapRef} class={`relative ${klass}`}>
      <button
        type="button"
        class={`input input-bordered ${sizeClass} w-full flex items-center pr-7 cursor-pointer ${
          disabled ? "opacity-50 cursor-not-allowed" : ""
        }`}
        disabled={disabled}
        onClick={() => !disabled && setOpen((v) => !v)}
      >
        <span class={`flex-1 truncate text-left ${triggerLabel ? "" : "text-base-content/40"}`}>
          {triggerLabel || placeholder}
        </span>
        <span class="absolute right-2 text-base-content/50 pointer-events-none text-xs">▾</span>
      </button>

      {open && (
        <div
          class="absolute left-0 z-30 mt-1 w-full bg-base-100 border border-base-content/30 rounded-md shadow-lg overflow-hidden"
          onKeyDown={onKeyDown}
        >
          {showSearch && (
            <div class="p-1.5 border-b border-base-content/15">
              <input
                ref={searchRef}
                class="input input-bordered input-xs w-full"
                placeholder="Search…"
                value={query}
                onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
                onKeyDown={onKeyDown}
              />
            </div>
          )}
          <ul class="max-h-64 overflow-y-auto py-1">
            {loading && filtered.length === 0 && (
              <li class="px-3 py-2 text-xs text-base-content/50 italic">Loading…</li>
            )}
            {!loading && filtered.length === 0 && (
              <li class="px-3 py-2 text-xs text-base-content/50 italic">No matches</li>
            )}
            {filtered.map((opt, i) => {
              const isSelected = opt.value === value;
              const isHighlighted = i === highlight;
              return (
                <li
                  class={`px-3 py-1.5 text-sm cursor-pointer flex items-center gap-2 ${
                    isHighlighted ? "bg-base-200" : ""
                  } ${isSelected ? "text-primary font-medium" : ""}`}
                  onMouseEnter={() => setHighlight(i)}
                  onClick={() => choose(opt)}
                >
                  <span class="flex-1 truncate">{opt.label ?? opt.value}</span>
                  {opt.hint && (
                    <span class="text-xs text-base-content/50 truncate">{opt.hint}</span>
                  )}
                  {isSelected && <span class="text-primary">✓</span>}
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </div>
  );
}
