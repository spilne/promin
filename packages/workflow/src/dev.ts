// ---------------------------------------------------------------------------
// @promin/workflow/dev — opt-in developer tooling.
//
// Today: `instrumentNonDeterminism()` patches non-deterministic globals
// (Date.now, Math.random, setTimeout, fetch) so a stray call from inside a
// journaled step body surfaces as a warning or throw, instead of quietly
// producing divergent behaviour between fresh-run and replay. ESLint catches
// direct bare-await but misses indirect calls (e.g. a helper function that
// uses Date.now); this catches what static analysis can't.
//
// Never import this module in production. It mutates globals.
// ---------------------------------------------------------------------------

import { isInJournaledBody, currentJournaledStepName } from "./lib/durable/journaled-body-scope.ts";

export type InstrumentMode = "warn" | "strict";

export interface InstrumentNonDeterminismOptions {
  /**
   * - `"warn"` (default) — `console.warn` on each call. Code keeps running.
   * - `"strict"` — throw an Error instead. Useful in CI to fail fast.
   */
  mode?: InstrumentMode;
  /**
   * Function called for each non-deterministic call inside a body. Overrides
   * the default warn/throw. Return nothing — the original global runs after.
   */
  onCall?: (event: {
    readonly api: string;
    readonly stepName: string;
    readonly mode: InstrumentMode;
  }) => void;
}

export interface InstrumentationHandle {
  /** Restore every patched global. Safe to call multiple times. */
  stop(): void;
}

interface Patch {
  readonly target: object;
  readonly key: string;
  readonly original: unknown;
}

/**
 * Wrap Date.now / Math.random / setTimeout / fetch so they report when
 * invoked inside a journaled step body. Returns a handle with `.stop()` to
 * restore the originals.
 */
export function instrumentNonDeterminism(
  options: InstrumentNonDeterminismOptions = {},
): InstrumentationHandle {
  const mode = options.mode ?? "warn";
  const onCall =
    options.onCall ??
    ((event) => {
      const message =
        `[@promin/workflow] non-deterministic call ${event.api} inside ` +
        `journaled step "${event.stepName}". Wrap it in ctx.activity(...) ` +
        `so the value is recorded in the journal and replay stays deterministic.`;
      if (event.mode === "strict") throw new Error(message);
      console.warn(message);
    });

  const patches: Patch[] = [];

  const report = (api: string): void => {
    if (!isInJournaledBody()) return;
    const stepName = currentJournaledStepName() ?? "<unknown>";
    onCall({ api, stepName, mode });
  };

  // Date.now
  const origDateNow = Date.now.bind(Date);
  Date.now = function instrumentedDateNow() {
    report("Date.now");
    return origDateNow();
  };
  patches.push({ target: Date, key: "now", original: origDateNow });

  // Math.random
  const origRandom = Math.random.bind(Math);
  Math.random = function instrumentedRandom() {
    report("Math.random");
    return origRandom();
  };
  patches.push({ target: Math, key: "random", original: origRandom });

  // setTimeout
  const g = globalThis as unknown as Record<string, unknown>;
  const origSetTimeout = g.setTimeout;
  if (typeof origSetTimeout === "function") {
    g.setTimeout = function instrumentedSetTimeout(...args: unknown[]) {
      report("setTimeout");
      return (origSetTimeout as (...args: unknown[]) => unknown).apply(globalThis, args);
    };
    patches.push({ target: g, key: "setTimeout", original: origSetTimeout });
  }

  // fetch
  const origFetch = g.fetch;
  if (typeof origFetch === "function") {
    g.fetch = function instrumentedFetch(...args: unknown[]) {
      report("fetch");
      return (origFetch as (...args: unknown[]) => unknown).apply(globalThis, args);
    };
    patches.push({ target: g, key: "fetch", original: origFetch });
  }

  let stopped = false;
  return {
    stop() {
      if (stopped) return;
      stopped = true;
      for (const p of patches) {
        (p.target as Record<string, unknown>)[p.key] = p.original;
      }
    },
  };
}

// Re-export the low-level scope helpers for tests and power users who want
// to build their own checks on top of the same context.
export { isInJournaledBody, currentJournaledStepName };
