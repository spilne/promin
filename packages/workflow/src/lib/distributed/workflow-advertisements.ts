// ---------------------------------------------------------------------------
// WorkflowAdvertisements — registry of workflows advertised by connected
// workers. Populates dashboards and start routing without requiring the
// host server to own workflow code directly.
//
// Workers register on connect (`upsert`) with their workflow definitions
// (name, version, DAG steps, optional sampleInput) and re-advertise
// periodically. The server reads `distinct()` to know which workflows (and
// versions) some live worker can run; the dashboard reads it to populate
// the Workflows page.
//
// Lifecycle:
//   - upsert is called on worker startup and then periodically
//   - an advertisement not refreshed within the registry's `ttlMs` expires:
//     `list` / `distinct` leave it out, and `prune` deletes it — so a
//     worker that died without `remove` stops being advertised
//   - remove is called on worker graceful shutdown
//   - in-memory backends lose state on restart (workers re-advertise within
//     one interval); persistent backends (SQLite, Postgres) survive restarts
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

export interface AdvertisedWorkflow {
  name: string;
  version?: string;
  steps: ReadonlyArray<{
    name: string;
    kind: string;
    dependsOn: readonly string[];
    /** Capabilities this step requires (informational). */
    needs?: readonly string[];
    /** Dispatch priority — higher runs first (informational). */
    priority?: number;
  }>;
  sampleInput?: unknown;
}

export interface AdvertisementEntry {
  workerId: string;
  workflows: AdvertisedWorkflow[];
  advertisedAt: Date;
}

/**
 * Default advertisement lifetime: an advertisement not refreshed for this
 * long is treated as gone. Workers re-advertise well within it.
 */
export const DEFAULT_ADVERTISEMENT_TTL_MS = 60_000;

export interface WorkflowAdvertisementRegistry {
  /**
   * Replace the advertisement for a worker and stamp `advertisedAt` from the
   * registry's clock. Called on worker startup and on every re-advertise.
   */
  upsert(params: { workerId: string; workflows: AdvertisedWorkflow[] }): Promise<void>;
  /** Remove a worker's advertisement. Called on worker shutdown / death. */
  remove(workerId: string): Promise<void>;
  /** Every live advertisement entry (refreshed within the registry's TTL). */
  list(): Promise<AdvertisementEntry[]>;
  /**
   * Distinct live workflows across all workers, one row per (name, version).
   * When several workers advertise the same (name, version), the most
   * recent advertisement (`advertisedAt`) wins. Sorted by name. Used to
   * route starts and to populate the dashboard's Workflows page.
   */
  distinct(): Promise<AdvertisedWorkflow[]>;
  /** Delete expired advertisements; resolves with how many workers' entries went. */
  prune(): Promise<number>;
}

export interface InMemoryWorkflowAdvertisementRegistryConfig {
  /** Time source for `advertisedAt` and expiry. Default: `SystemWallClock`. */
  clock?: WallClock;
  /**
   * An advertisement not refreshed for this long expires. Default:
   * `DEFAULT_ADVERTISEMENT_TTL_MS`; `Infinity` keeps them until `remove`.
   */
  ttlMs?: number;
}

export class InMemoryWorkflowAdvertisementRegistry implements WorkflowAdvertisementRegistry {
  private readonly byWorker = new Map<string, AdvertisementEntry>();
  private readonly clock: WallClock;
  private readonly ttlMs: number;

  constructor(config: InMemoryWorkflowAdvertisementRegistryConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
    this.ttlMs = config.ttlMs ?? DEFAULT_ADVERTISEMENT_TTL_MS;
  }

  async upsert(params: { workerId: string; workflows: AdvertisedWorkflow[] }): Promise<void> {
    const { workerId, workflows } = params;
    this.byWorker.set(workerId, { workerId, workflows, advertisedAt: this.clock.now() });
  }

  async remove(workerId: string): Promise<void> {
    this.byWorker.delete(workerId);
  }

  async list(): Promise<AdvertisementEntry[]> {
    return this.live();
  }

  async distinct(): Promise<AdvertisedWorkflow[]> {
    // Dedupe on (name, version). Walk entries oldest first so the most
    // recent advertisement overwrites older ones; Map insertion order
    // would keep a re-upserted worker at its first position.
    const entries = this.live().sort((a, b) => a.advertisedAt.getTime() - b.advertisedAt.getTime());
    const byKey = new Map<string, AdvertisedWorkflow>();
    for (const entry of entries) {
      for (const wf of entry.workflows) {
        byKey.set(`${wf.name}@${wf.version ?? ""}`, wf);
      }
    }
    return [...byKey.values()].sort((a, b) => a.name.localeCompare(b.name));
  }

  async prune(): Promise<number> {
    const cutoff = this.clock.currentTimeMs() - this.ttlMs;
    let pruned = 0;
    for (const [workerId, entry] of this.byWorker) {
      if (entry.advertisedAt.getTime() <= cutoff) {
        this.byWorker.delete(workerId);
        pruned += 1;
      }
    }
    return pruned;
  }

  private live(): AdvertisementEntry[] {
    const cutoff = this.clock.currentTimeMs() - this.ttlMs;
    return [...this.byWorker.values()].filter((e) => e.advertisedAt.getTime() > cutoff);
  }
}
