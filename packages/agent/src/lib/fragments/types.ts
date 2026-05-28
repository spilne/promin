// ---------------------------------------------------------------------------
// Fragment registry — small, operator-curated markdown blocks that role
// recipes compose into their system prompt. Always-on; concatenated at
// resolve time. Distinct from skills (which are load-on-demand).
//
// Synchronous on purpose: fragments are short static markdown strings —
// no I/O needed at resolve time. `resolveLocalAgent` reads them inline.
// ---------------------------------------------------------------------------

/**
 * Look up the prompt fragments referenced by a recipe's `systemPrompt.layers`.
 * Implementations are typically a thin wrapper around a `Record<string,string>`.
 */
export interface FragmentRegistry {
  /** Return the fragment body for `key`, or `undefined` if absent. */
  get(key: string): string | undefined;
  /** Enumerate every registered fragment (key + body). */
  list(): ReadonlyArray<{ readonly key: string; readonly content: string }>;
}
