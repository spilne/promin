// ---------------------------------------------------------------------------
// SmartSearchInput — single-input filter bar for the runs table.
//
// Wraps a plain text input with an inline suggestion dropdown that fires
// only while the cursor is inside a `name:` / `type:` / `namespace:`
// clause. Other clause types (`version:`, `id:`, `key=value`, free text)
// pass through unchanged — there's no useful suggestion pool for them.
//
// Cursor handling is intentionally simple: we read `selectionStart` from
// the input and locate the enclosing whitespace-delimited token. Quoted
// values still tokenize as a single token (the parser's tokenize function
// already preserves them) but the autocomplete dropdown only opens when
// the cursor sits in the unquoted partial after `field:`.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import type { JSX } from "preact";

export type SuggestField = "name" | "type" | "namespace";

interface Props {
  value: string;
  onChange: (next: string) => void;
  /** Fired on Enter when no suggestion is highlighted. */
  onSubmit: () => void;
  /** Fired when the input loses focus and the value differs from `appliedValue`. */
  onBlurCommit?: () => void;
  pools: {
    name: ReadonlyArray<string>;
    type: ReadonlyArray<string>;
    namespace: ReadonlyArray<string>;
  };
  placeholder?: string;
  /** Tooltip text. */
  title?: string;
  className?: string;
  /** Optional clear button hook — rendered inside the input on the right. */
  onClearInput?: () => void;
}

interface ActiveToken {
  start: number;
  end: number;
  field?: SuggestField;
  /** Raw partial value after `field:`. Empty string when user just typed `field:`. */
  partial: string;
}

const MAX_SUGGESTIONS = 8;

export function SmartSearchInput({
  value,
  onChange,
  onSubmit,
  onBlurCommit,
  pools,
  placeholder,
  title,
  className,
  onClearInput,
}: Props) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [cursor, setCursor] = useState(0);
  const [open, setOpen] = useState(false);
  const [highlight, setHighlight] = useState(0);

  const activeToken = useMemo(() => getActiveToken(value, cursor), [value, cursor]);

  const suggestions = useMemo(() => {
    if (!activeToken.field) return [];
    const pool = pools[activeToken.field];
    if (!pool || pool.length === 0) return [];
    const partial = activeToken.partial.toLowerCase();
    const matches = partial ? pool.filter((v) => v.toLowerCase().includes(partial)) : [...pool];
    return matches.slice(0, MAX_SUGGESTIONS);
  }, [activeToken, pools]);

  // Reset highlight when the suggestion list changes shape so a stale index
  // doesn't point past the new end.
  useEffect(() => {
    setHighlight(0);
  }, [suggestions.length, activeToken.field]);

  const dropdownVisible = open && suggestions.length > 0 && activeToken.field !== undefined;

  function applySuggestion(suggested: string) {
    if (!activeToken.field) return;
    const replacement = `${activeToken.field}:${quoteIfNeeded(suggested)}`;
    let next = value.slice(0, activeToken.start) + replacement + value.slice(activeToken.end);
    let nextCursor = activeToken.start + replacement.length;
    // Trailing space when we're at the end of the string — lets the user
    // immediately start the next clause without manually adding a space.
    if (nextCursor === next.length) {
      next += " ";
      nextCursor += 1;
    }
    onChange(next);
    setCursor(nextCursor);
    setOpen(false);
    // Restore focus + cursor on the next frame after the controlled value
    // has flushed; setting selectionRange too eagerly fights with the
    // upcoming React render.
    requestAnimationFrame(() => {
      const el = inputRef.current;
      if (!el) return;
      el.focus();
      el.setSelectionRange(nextCursor, nextCursor);
    });
  }

  function handleKeyDown(e: JSX.TargetedKeyboardEvent<HTMLInputElement>) {
    // Standard search-bar pattern: Enter and Tab both apply the highlighted
    // suggestion while the dropdown is open. To submit raw text instead
    // (e.g. `type:web` literally when "webhook" is in the list), press Esc
    // first to dismiss the dropdown, then Enter.
    if (dropdownVisible) {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setHighlight((h) => Math.min(h + 1, suggestions.length - 1));
        return;
      }
      if (e.key === "ArrowUp") {
        e.preventDefault();
        setHighlight((h) => Math.max(h - 1, 0));
        return;
      }
      if (e.key === "Enter" || e.key === "Tab") {
        e.preventDefault();
        applySuggestion(suggestions[highlight]!);
        return;
      }
      if (e.key === "Escape") {
        e.preventDefault();
        setOpen(false);
        return;
      }
    }
    if (e.key === "Enter") {
      e.preventDefault();
      setOpen(false);
      onSubmit();
    }
  }

  // Track cursor moves that don't generate input events (arrow keys, mouse
  // clicks). Keeping cursor state up to date is what drives the active-token
  // computation.
  function syncCursor() {
    const el = inputRef.current;
    if (el) setCursor(el.selectionStart ?? el.value.length);
  }

  return (
    <div class={`relative ${className ?? ""}`}>
      <input
        ref={inputRef}
        type="text"
        class="input input-bordered input-sm w-full font-mono pr-10"
        placeholder={placeholder}
        title={title}
        value={value}
        onInput={(e) => {
          const el = e.target as HTMLInputElement;
          onChange(el.value);
          setCursor(el.selectionStart ?? el.value.length);
          setOpen(true);
        }}
        onKeyDown={handleKeyDown}
        onKeyUp={syncCursor}
        onClick={syncCursor}
        onFocus={() => setOpen(true)}
        onBlur={() => {
          // Delay the close so a mousedown-then-mouseup on a suggestion item
          // gets a chance to fire `applySuggestion` first. The dropdown items
          // also call preventDefault on mousedown which keeps the input
          // focused, but the timeout is the belt-and-suspenders fallback.
          setTimeout(() => {
            setOpen(false);
            onBlurCommit?.();
          }, 120);
        }}
      />
      {onClearInput && value && (
        <button
          type="button"
          class="btn btn-xs btn-ghost btn-circle absolute right-1 top-1/2 -translate-y-1/2"
          aria-label="Clear search input"
          // mousedown so the input doesn't blur first and trigger
          // `onBlurCommit` before the clear lands.
          onMouseDown={(e) => {
            e.preventDefault();
            onClearInput();
          }}
        >
          ×
        </button>
      )}

      {dropdownVisible && (
        <ul
          class="absolute z-50 mt-1 w-full max-h-64 overflow-auto rounded-md border border-base-content/10 bg-base-100 shadow-lg anim-fade-in"
          role="listbox"
        >
          <li class="px-3 py-1 text-[0.7rem] uppercase tracking-wider text-base-content/40 border-b border-base-content/10">
            {activeToken.field}
            {activeToken.partial ? ` · matching "${activeToken.partial}"` : " · all"}
          </li>
          {suggestions.map((s, i) => {
            const active = i === highlight;
            return (
              <li
                role="option"
                aria-selected={active}
                class={`px-3 py-1.5 cursor-pointer font-mono text-sm ${
                  active ? "bg-primary/15 text-primary" : "hover:bg-base-200"
                }`}
                onMouseDown={(e) => {
                  e.preventDefault();
                  applySuggestion(s);
                }}
                onMouseEnter={() => setHighlight(i)}
              >
                {highlightMatch(s, activeToken.partial)}
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

/**
 * Walk outward from `cursor` to the surrounding whitespace boundaries, then
 * decide whether the resulting token is a `field:value` pair we offer
 * suggestions for. Quoted segments inside the token are kept verbatim — the
 * partial value we hand to the matcher includes the quote characters when
 * the user is typing inside a quoted string.
 */
function getActiveToken(text: string, cursor: number): ActiveToken {
  let start = Math.min(cursor, text.length);
  let end = start;
  while (start > 0 && !/\s/.test(text[start - 1]!)) start--;
  while (end < text.length && !/\s/.test(text[end]!)) end++;
  const content = text.slice(start, end);
  const colon = content.indexOf(":");
  const eq = content.indexOf("=");
  // Same precedence rule as the parser: `:` wins when both are present and
  // appears first. Otherwise `key=value` (metadata) takes over and we
  // don't suggest.
  if (colon > 0 && (eq < 0 || colon < eq)) {
    const fieldName = content.slice(0, colon).toLowerCase();
    if (isSuggestField(fieldName)) {
      const rawPartial = content.slice(colon + 1);
      // Strip surrounding quotes for matching purposes — re-quote on insert.
      const partial = rawPartial.startsWith('"') ? rawPartial.replace(/^"|"$/g, "") : rawPartial;
      return { start, end, field: fieldName, partial };
    }
  }
  return { start, end, partial: content };
}

function isSuggestField(s: string): s is SuggestField {
  return s === "name" || s === "type" || s === "namespace";
}

function quoteIfNeeded(s: string): string {
  if (s === "") return '""';
  if (/[\s"]/.test(s)) return `"${s.replace(/"/g, '\\"')}"`;
  return s;
}

/**
 * Render the suggestion text with the matched substring underlined so users
 * can see why an item showed up. Pure visual — falls back to the raw string
 * when the partial is empty (initial dropdown for a `field:` with no value
 * yet).
 */
function highlightMatch(text: string, partial: string): JSX.Element | string {
  if (!partial) return text;
  const lower = text.toLowerCase();
  const idx = lower.indexOf(partial.toLowerCase());
  if (idx < 0) return text;
  return (
    <>
      {text.slice(0, idx)}
      <span class="font-semibold underline underline-offset-2">
        {text.slice(idx, idx + partial.length)}
      </span>
      {text.slice(idx + partial.length)}
    </>
  );
}
