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

export interface TriggerServiceDeps {
  storage: WorkflowStorage;
  /**
   * Pending workflow-start queue. When provided, triggers enqueue here and
   * workers poll. Without it the trigger returns the workflowId but
   * nothing executes (so the run sits in `pending` forever).
   */
  workflowStarts?: WorkflowStartQueue;
}

export class TriggerService {
  constructor(private readonly deps: TriggerServiceDeps) {}

  /**
   * Pre-create the storage row with the requested version and enqueue a
   * start. Returns the workflowId so the caller can navigate / poll.
   */
  readonly trigger: RunTrigger = async (name, input, options) => {
    const workflowId = options?.workflowId ?? crypto.randomUUID();
    await this.deps.storage.createWorkflow({
      workflowId,
      workflowName: name,
      input,
      workflowType: options?.workflowType,
      namespace: options?.namespace,
      metadata: options?.metadata,
      version: options?.version,
    });
    if (this.deps.workflowStarts) {
      await this.deps.workflowStarts.enqueue({
        workflowId,
        workflowName: name,
        input,
        metadata: options?.metadata,
        version: options?.version,
      });
    }
    return { workflowId };
  };
}
