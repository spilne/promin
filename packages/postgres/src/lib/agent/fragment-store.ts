// ---------------------------------------------------------------------------
// PostgresFragmentStore — `FragmentStore` over the `fragment_store` table.
// Mirrors `SqliteFragmentStore`. Only operator-authored fragments live
// here; file-scanned ones stay in the in-memory FragmentRegistry.
// ---------------------------------------------------------------------------

import { eq } from "drizzle-orm";
import type { FragmentStore } from "@promin/agent";
import type { DrizzleDb } from "../drizzle-db.ts";
import { fragmentStore } from "../schema.ts";

export interface PostgresFragmentStoreConfig {
  readonly db: DrizzleDb;
  /** Optional clock for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class PostgresFragmentStore implements FragmentStore {
  private readonly db: DrizzleDb;
  private readonly clock: () => number;

  constructor(config: PostgresFragmentStoreConfig) {
    this.db = config.db;
    this.clock = config.now ?? (() => Date.now());
  }

  async loadAll(): Promise<ReadonlyArray<{ readonly key: string; readonly content: string }>> {
    const rows = await this.db.select().from(fragmentStore);
    return rows.map((r) => ({ key: r.key, content: r.content }));
  }

  async set(key: string, content: string): Promise<void> {
    const now = this.clock();
    await this.db
      .insert(fragmentStore)
      .values({ key, content, updatedAt: now })
      .onConflictDoUpdate({
        target: fragmentStore.key,
        set: { content, updatedAt: now },
      });
  }

  async delete(key: string): Promise<void> {
    await this.db.delete(fragmentStore).where(eq(fragmentStore.key, key));
  }
}
