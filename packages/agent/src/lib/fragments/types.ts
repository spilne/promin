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
 *
 * Mutators (`set` / `delete`) are sync so a Postgres-backed adapter would
 * implement them as a write-through cache over an async store rather than
 * making `resolveSystemPrompt` async. For the in-memory case used by the
 * demo, the operations are direct Map writes.
 */
export interface FragmentRegistry {
  /** Return the fragment body for `key`, or `undefined` if absent. */
  get(key: string): string | undefined;
  /** Enumerate every registered fragment (key + body). */
  list(): ReadonlyArray<{ readonly key: string; readonly content: string }>;
  /** Create or replace a fragment. Sync; used by the scanner and CRUD routes. */
  set(key: string, content: string): void;
  /** Remove a fragment. No-op when not present. */
  delete(key: string): void;
}
