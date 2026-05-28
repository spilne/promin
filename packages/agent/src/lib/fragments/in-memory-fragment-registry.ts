// ---------------------------------------------------------------------------
// `InMemoryFragmentRegistry` — reference implementation. A frozen
// `Record<string, string>` keyed by layer name. Hosts construct one at
// boot with their curated fragment library and pass it into
// `resolveLocalAgent` deps; the resolver concatenates referenced layers
// into the system prompt.
// ---------------------------------------------------------------------------

import type { FragmentRegistry } from "./types.ts";

export class InMemoryFragmentRegistry implements FragmentRegistry {
  private readonly fragments: Readonly<Record<string, string>>;

  constructor(fragments: Readonly<Record<string, string>>) {
    this.fragments = { ...fragments };
  }

  get(key: string): string | undefined {
    return this.fragments[key];
  }

  list(): ReadonlyArray<{ readonly key: string; readonly content: string }> {
    return Object.entries(this.fragments).map(([key, content]) => ({ key, content }));
  }
}
