// ---------------------------------------------------------------------------
// TriggerService — encapsulates the "create a run from a name + input" step
// that the dashboard's POST /api/runs/trigger/:name endpoint needs.
//
// In split mode (no in-process workflow code, only a remote worker pool)
// triggering means: pre-create a `pending` storage row so the dashboard
// sees the run instantly, then enqueue a start that connected workers
// poll and execute. ZoryaServer wires a TriggerService when a worker
// protocol with a WorkflowStartQueue is configured and no explicit
// `trigger` fn is supplied.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "@promin/workflow";
import type { RunTrigger } from "../routes/runs.ts";
import type { WorkflowStartQueue } from "../workflow-starts.ts";
import type { WorkflowAdvertisementRegistry } from "../workflow-advertisements.ts";

export interface TriggerServiceDeps {
  storage: WorkflowStorage;
  /**
   * Pending workflow-start queue. When provided, triggers enqueue here and
   * workers poll. Without it the trigger returns the workflowId but
   * nothing executes (so the run sits in `pending` forever).
   */
  workflowStarts?: WorkflowStartQueue;
  /**
   * Worker advertisements — consulted when the caller doesn't specify a
   * `version`. Without this fallback, triggering a versioned workflow
   * without an explicit version stores `version: undefined` on the row
   * and the worker (whose primary def carries a version) throws
   * WorkflowVersionMismatchError on first step.
   */
  advertisements?: WorkflowAdvertisementRegistry;
}

export class TriggerService {
  constructor(private readonly deps: TriggerServiceDeps) {}

  /**
   * Pre-create the storage row with the requested version and enqueue a
   * start. Returns the workflowId so the caller can navigate / poll.
   */
  readonly trigger: RunTrigger = async (name, input, options) => {
    const workflowId = options?.workflowId ?? crypto.randomUUID();
    const version = options?.version ?? (await this.resolveDefaultVersion(name));
    await this.deps.storage.createWorkflow({
      workflowId,
      workflowName: name,
      input,
      workflowType: options?.workflowType,
      namespace: options?.namespace,
      metadata: options?.metadata,
      version,
    });
    if (this.deps.workflowStarts) {
      await this.deps.workflowStarts.enqueue({
        workflowId,
        workflowName: name,
        input,
        ...(options?.namespace !== undefined && { namespace: options.namespace }),
        metadata: options?.metadata,
        version,
      });
    }
    return { workflowId };
  };

  /**
   * Pick the default version for a workflow when the caller didn't supply
   * one. Uses the highest advertised version seen across workers; falls
   * back to `undefined` when nothing is advertised (engine then skips the
   * version-mismatch check).
   */
  private async resolveDefaultVersion(name: string): Promise<string | undefined> {
    if (!this.deps.advertisements) return undefined;
    const distinct = await this.deps.advertisements.distinct();
    const versions = distinct
      .filter((a) => a.name === name && !!a.version)
      .map((a) => a.version as string);
    if (versions.length === 0) return undefined;
    // Lexicographic sort is good enough for numeric tags ("1", "2", "10")
    // as long as callers use numeric padding for double-digit versions;
    // semver-style picks are a follow-up if it bites.
    versions.sort();
    return versions[versions.length - 1];
  }
}
