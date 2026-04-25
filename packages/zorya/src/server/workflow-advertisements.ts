// ---------------------------------------------------------------------------
// WorkflowAdvertisements — server-side registry of workflows advertised by
// connected workers. Populates the Workflows page without requiring the
// zorya server to own workflow code directly.
//
// Workers post to POST /api/advertisements on connect with their workflow
// definitions (name, version, DAG steps, optional sampleInput). The server
// stores these in-memory (losing them on restart is fine — workers re-
// advertise when they reconnect). Entries expire when a worker stops
// heartbeating.
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
   * Used to populate /api/workflows/definitions.
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
