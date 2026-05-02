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
  /** Replace the advertisement for a worker. Called on worker startup. */
  upsert(workerId: string, workflows: AdvertisedWorkflow[]): Promise<void>;
  /** Remove a worker's advertisement. Called on worker shutdown / death. */
  remove(workerId: string): Promise<void>;
  /** List every outstanding advertisement entry. */
  list(): Promise<AdvertisementEntry[]>;
  /**
   * Distinct workflows across all workers, one row per (name, version).
   * Used to populate dispatch lookups and the dashboard's Workflows page.
   */
  distinct(): Promise<AdvertisedWorkflow[]>;
}

export class InMemoryWorkflowAdvertisementRegistry implements WorkflowAdvertisementRegistry {
  private readonly byWorker = new Map<string, AdvertisementEntry>();

  async upsert(workerId: string, workflows: AdvertisedWorkflow[]): Promise<void> {
    this.byWorker.set(workerId, { workerId, workflows, advertisedAt: new Date() });
  }

  async remove(workerId: string): Promise<void> {
    this.byWorker.delete(workerId);
  }

  async list(): Promise<AdvertisementEntry[]> {
    return [...this.byWorker.values()];
  }

  async distinct(): Promise<AdvertisedWorkflow[]> {
    // Dedupe on (name, version); last advertisement wins (newer replaces older)
    const byKey = new Map<string, AdvertisedWorkflow>();
    for (const entry of this.byWorker.values()) {
      for (const wf of entry.workflows) {
        byKey.set(`${wf.name}@${wf.version ?? ""}`, wf);
      }
    }
    return [...byKey.values()].sort((a, b) => a.name.localeCompare(b.name));
  }
}
