// ---------------------------------------------------------------------------
// `InMemoryFragmentRegistry` — reference implementation. Hosts construct one
// at boot, optionally seeded from a curated library; the scanner and the
// CRUD routes both mutate it via `set` / `delete`. resolveSystemPrompt reads
// it synchronously.
// ---------------------------------------------------------------------------

import type { FragmentRegistry } from "./types.ts";

export class InMemoryFragmentRegistry implements FragmentRegistry {
  private readonly fragments: Map<string, string>;

  constructor(seed: Readonly<Record<string, string>> = {}) {
    this.fragments = new Map(Object.entries(seed));
  }

  get(key: string): string | undefined {
    return this.fragments.get(key);
  }

  list(): ReadonlyArray<{ readonly key: string; readonly content: string }> {
    return [...this.fragments.entries()].map(([key, content]) => ({ key, content }));
  }

  set(key: string, content: string): void {
    this.fragments.set(key, content);
  }

  delete(key: string): void {
    this.fragments.delete(key);
  }
}
