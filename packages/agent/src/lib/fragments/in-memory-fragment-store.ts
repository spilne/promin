// ---------------------------------------------------------------------------
// `InMemoryFragmentStore` — reference implementation used in tests and for
// hosts that don't want durable persistence. Real backends (SQLite, PG)
// pass `fragmentStoreTestSuite`.
// ---------------------------------------------------------------------------

import type { FragmentStore } from "./types.ts";

export class InMemoryFragmentStore implements FragmentStore {
  private readonly fragments = new Map<string, string>();

  async loadAll(): Promise<ReadonlyArray<{ readonly key: string; readonly content: string }>> {
    return [...this.fragments.entries()].map(([key, content]) => ({ key, content }));
  }

  async set(key: string, content: string): Promise<void> {
    this.fragments.set(key, content);
  }

  async delete(key: string): Promise<void> {
    this.fragments.delete(key);
  }
}
