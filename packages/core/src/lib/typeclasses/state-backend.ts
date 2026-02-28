// ---------------------------------------------------------------------------
// StateBackend<K, V> — pluggable keyed mutable state
// Used by stateful stream operators (e.g. statefulMap, windows).
// ---------------------------------------------------------------------------

export interface StateBackend<K, V> {
  /** Get the value for a key. Returns undefined if not found. */
  get(key: K): Promise<V | undefined>;

  /** Set the value for a key. */
  put(key: K, value: V): Promise<void>;

  /** Delete a key. */
  delete(key: K): Promise<void>;

  /** Get all keys. */
  keys(): Promise<K[]>;

  /** Get all entries. */
  entries(): Promise<[K, V][]>;

  /** Checkpoint current state (for crash recovery). No-op for in-memory. */
  checkpoint(params: { name: string }): Promise<void>;

  /** Restore state from a checkpoint. No-op for in-memory. */
  restore(params: { name: string }): Promise<void>;

  /** Clear all state. */
  clear(): Promise<void>;
}
