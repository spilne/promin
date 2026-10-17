// ---------------------------------------------------------------------------
// WorkflowAdvertisements — registry of workflows advertised by connected
// workers. Populates dashboards and dispatch logic without requiring the
// host server to own workflow code directly.
//
// Workers register on connect (`upsert`) with their full workflow
// definitions (name, version, DAG steps, optional sampleInput). The
// coordinator/dispatch layer reads `distinct()` to build stub workflows
// for trigger requests; the dashboard reads it to populate the
// Workflows page.
//
// Lifecycle:
//   - upsert is called on worker startup + on heartbeat refresh
//   - remove is called on worker graceful shutdown
//   - in-memory backends lose state on restart (workers re-advertise on
//     reconnect); persistent backends (SQLite, Postgres) survive restarts
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";

export interface AdvertisedWorkflow {
  name: string;
  version?: string;
  steps: ReadonlyArray<{
    name: string;
    kind: string;
    dependsOn: readonly string[];
    /**
     * Capabilities this step requires. Forwarded by step-mode workers so
     * the coordinator can route the dispatched task to a worker that
     * actually advertises the matching capabilities.
     */
    needs?: readonly string[];
    /** Dispatch priority — higher runs first (queue-level default: 5). */
    priority?: number;
  }>;
  sampleInput?: unknown;
}

export interface AdvertisementEntry {
  workerId: string;
  workflows: AdvertisedWorkflow[];
  advertisedAt: Date;
}

export interface WorkflowAdvertisementRegistry {
  /**
   * Replace the advertisement for a worker and stamp `advertisedAt` from the
   * registry's clock. Called on worker startup and heartbeat refresh.
   */
  upsert(params: { workerId: string; workflows: AdvertisedWorkflow[] }): Promise<void>;
  /** Remove a worker's advertisement. Called on worker shutdown / death. */
  remove(workerId: string): Promise<void>;
  /** List every outstanding advertisement entry. */
  list(): Promise<AdvertisementEntry[]>;
  /**
   * Distinct workflows across all workers, one row per (name, version).
   * When several workers advertise the same (name, version), the most
   * recent advertisement (`advertisedAt`) wins. Sorted by name. Used to
   * populate dispatch lookups and the dashboard's Workflows page.
   */
  distinct(): Promise<AdvertisedWorkflow[]>;
}

export interface InMemoryWorkflowAdvertisementRegistryConfig {
  /** Time source for `advertisedAt`. Default: `SystemWallClock`. */
  clock?: WallClock;
}

export class InMemoryWorkflowAdvertisementRegistry implements WorkflowAdvertisementRegistry {
  private readonly byWorker = new Map<string, AdvertisementEntry>();
  private readonly clock: WallClock;

  constructor(config: InMemoryWorkflowAdvertisementRegistryConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
  }

  async upsert(params: { workerId: string; workflows: AdvertisedWorkflow[] }): Promise<void> {
    const { workerId, workflows } = params;
    this.byWorker.set(workerId, { workerId, workflows, advertisedAt: this.clock.now() });
  }

  async remove(workerId: string): Promise<void> {
    this.byWorker.delete(workerId);
  }

  async list(): Promise<AdvertisementEntry[]> {
    return [...this.byWorker.values()];
  }

  async distinct(): Promise<AdvertisedWorkflow[]> {
    // Dedupe on (name, version). Walk entries oldest first so the most
    // recent advertisement overwrites older ones; Map insertion order
    // would keep a re-upserted worker at its first position.
    const entries = [...this.byWorker.values()].sort(
      (a, b) => a.advertisedAt.getTime() - b.advertisedAt.getTime(),
    );
    const byKey = new Map<string, AdvertisedWorkflow>();
    for (const entry of entries) {
      for (const wf of entry.workflows) {
        byKey.set(`${wf.name}@${wf.version ?? ""}`, wf);
      }
    }
    return [...byKey.values()].sort((a, b) => a.name.localeCompare(b.name));
  }
}
