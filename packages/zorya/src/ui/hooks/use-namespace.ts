// ---------------------------------------------------------------------------
// useNamespace — tiny global store for the currently-selected namespace.
//
// Backed by localStorage so the choice survives refreshes, with a custom
// event bus so every subscriber re-renders when the value changes. The
// alternative (React context) would require threading a Provider through
// App, which is overkill for a single dashboard-wide scope.
// ---------------------------------------------------------------------------

import { useEffect, useState } from "preact/hooks";

const STORAGE_KEY = "zorya_namespace";
const EVENT_NAME = "zorya:namespace-change";

function read(): string {
  try {
    return localStorage.getItem(STORAGE_KEY) ?? "";
  } catch {
    return "";
  }
}

function write(value: string): void {
  try {
    if (value) localStorage.setItem(STORAGE_KEY, value);
    else localStorage.removeItem(STORAGE_KEY);
  } catch {
    // Storage unavailable (private mode, quota) — fall through silently.
  }
  window.dispatchEvent(new CustomEvent(EVENT_NAME, { detail: value }));
}

/**
 * Read + write the dashboard-wide namespace scope. Empty string means "all
 * namespaces". Pages pass the returned value into their API queries; the
 * hook re-renders on cross-component updates via the event bus.
 */
export function useNamespace(): [string, (value: string) => void] {
  const [value, setValue] = useState<string>(() => read());

  useEffect(() => {
    const onChange = (e: Event) => {
      const custom = e as CustomEvent<string>;
      setValue(custom.detail ?? read());
    };
    window.addEventListener(EVENT_NAME, onChange);
    // Cross-tab: the `storage` event fires when another tab writes.
    const onStorage = (e: StorageEvent) => {
      if (e.key === STORAGE_KEY) setValue(e.newValue ?? "");
    };
    window.addEventListener("storage", onStorage);
    return () => {
      window.removeEventListener(EVENT_NAME, onChange);
      window.removeEventListener("storage", onStorage);
    };
  }, []);

  return [value, write];
}
