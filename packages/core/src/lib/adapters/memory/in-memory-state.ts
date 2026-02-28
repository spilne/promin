// ---------------------------------------------------------------------------
// InMemoryState<K, V> — Map-backed StateBackend
// ---------------------------------------------------------------------------

import type { StateBackend } from "../../typeclasses/state-backend.ts";

export class InMemoryState<K, V> implements StateBackend<K, V> {
  private store = new Map<K, V>();
  private checkpoints = new Map<string, Map<K, V>>();

  async get(key: K): Promise<V | undefined> {
    return this.store.get(key);
  }

  async put(key: K, value: V): Promise<void> {
    this.store.set(key, value);
  }

  async delete(key: K): Promise<void> {
    this.store.delete(key);
  }

  async keys(): Promise<K[]> {
    return [...this.store.keys()];
  }

  async entries(): Promise<[K, V][]> {
    return [...this.store.entries()];
  }

  async checkpoint(params: { name: string }): Promise<void> {
    this.checkpoints.set(params.name, new Map(this.store));
  }

  async restore(params: { name: string }): Promise<void> {
    const saved = this.checkpoints.get(params.name);
    if (saved) {
      this.store = new Map(saved);
    }
  }

  async clear(): Promise<void> {
    this.store.clear();
  }

  /** Test helper: current size. */
  get size(): number {
    return this.store.size;
  }
}
