// ---------------------------------------------------------------------------
// QueuedWorkflows — job-board mode. This process doesn't run workflows; it
// just pre-creates the storage row and enqueues a "please-start" record.
// Workflow-mode workers (ZoryaWorker { mode: "workflow" }) poll the queue
// and execute the entire workflow end-to-end in their own process.
//
// Owns:
//   - WorkflowStartQueue — the pending-start records workers claim
//   - Optional WorkflowAdvertisementRegistry — used to resolve default
//     versions and to drive `canHandle` (so unknown workflows don't sit
//     forever in the queue waiting for a worker that will never come)
// ---------------------------------------------------------------------------

import type { Workflow, WorkflowStorage } from "@promin/workflow";
import { InMemoryWorkflowStartQueue, type WorkflowStartQueue } from "../../workflow-starts.ts";
import {
  InMemoryWorkflowAdvertisementRegistry,
  type WorkflowAdvertisementRegistry,
} from "../../workflow-advertisements.ts";
import { ZoryaWorkflows, type TriggerOptions, type TriggerResult } from "./zorya-workflows.ts";

export interface QueuedWorkflowsConfig {
  storage: WorkflowStorage;
  /** Workflow-start queue. Auto-creates an in-memory one if omitted. */
  workflowStarts?: WorkflowStartQueue;
  /** Shared advertisements registry. Auto-creates an in-memory one if omitted. */
  advertisements?: WorkflowAdvertisementRegistry;
  /**
   * Optional in-process definitions — present in hybrid setups where this
   * layer also knows about local workflows (rare; usually empty).
   */
  definitions?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /**
   * When true, canHandle returns true for any name (optimistic mode). Use
   * this when the queue is the terminal layer in a chain and you'd rather
   * have unknown triggers sit pending than throw. Default: false (only
   * accept workflows that appear in advertisements or definitions).
   */
  acceptAny?: boolean;
  /** Optional fallback layer. */
  fallback?: ZoryaWorkflows;
}

export class QueuedWorkflows extends ZoryaWorkflows {
  readonly storage: WorkflowStorage;
  readonly workflowStarts: WorkflowStartQueue;
  override readonly advertisements: WorkflowAdvertisementRegistry;
  private readonly acceptAny: boolean;

  constructor(config: QueuedWorkflowsConfig) {
    const advertisements = config.advertisements ?? new InMemoryWorkflowAdvertisementRegistry();
    super({
      advertisements,
      ...(config.definitions && { definitions: config.definitions }),
      ...(config.fallback && { fallback: config.fallback }),
    });
    this.storage = config.storage;
    this.workflowStarts = config.workflowStarts ?? new InMemoryWorkflowStartQueue();
    this.advertisements = advertisements;
    this.acceptAny = config.acceptAny ?? false;
  }

  protected async canHandle(name: string): Promise<boolean> {
    if (this.acceptAny) return true;
    if (this.definitions && name in this.definitions) return true;
    const distinct = await this.advertisements.distinct();
    return distinct.some((a) => a.name === name);
  }

  protected async dispatch(
    name: string,
    input: unknown,
    opts?: TriggerOptions,
  ): Promise<TriggerResult> {
    const workflowId = opts?.workflowId ?? crypto.randomUUID();
    const version = opts?.version ?? (await this.resolveDefaultVersion(name));

    const result = await this.storage.createWorkflow({
      workflowId,
      workflowName: name,
      input,
      ...(opts?.workflowType !== undefined && { workflowType: opts.workflowType }),
      ...(opts?.namespace !== undefined && { namespace: opts.namespace }),
      ...(opts?.metadata !== undefined && { metadata: opts.metadata }),
      ...(opts?.runSource !== undefined && { runSource: opts.runSource }),
      ...(opts?.runSourceId !== undefined && { runSourceId: opts.runSourceId }),
      ...(version !== undefined && { version }),
    });
    if (!result.created && isTerminal(result.existing.status)) {
      await this.storage.startFreshRun(workflowId);
    }

    await this.workflowStarts.enqueue({
      workflowId,
      workflowName: name,
      input,
      ...(opts?.namespace !== undefined && { namespace: opts.namespace }),
      ...(opts?.metadata !== undefined && { metadata: opts.metadata }),
      ...(version !== undefined && { version }),
    });

    return { workflowId };
  }

  override async rerun(workflowId: string): Promise<void> {
    const state = await this.storage.loadWorkflow(workflowId);
    if (!state) {
      if (this.fallback) return this.fallback.rerun(workflowId);
      throw new Error(`rerun: workflow "${workflowId}" not found`);
    }
    await this.storage.startFreshRun(workflowId);
    await this.workflowStarts.enqueue({
      workflowId,
      workflowName: state.workflowName,
      input: state.input,
      ...(state.namespace !== undefined && { namespace: state.namespace }),
      ...(state.metadata !== undefined && { metadata: state.metadata }),
      ...(state.version !== undefined && { version: state.version }),
    });
  }

  /** Default version resolution: highest advertised version for this name. */
  private async resolveDefaultVersion(name: string): Promise<string | undefined> {
    const distinct = await this.advertisements.distinct();
    const versions = distinct
      .filter((a) => a.name === name && !!a.version)
      .map((a) => a.version as string);
    if (versions.length === 0) return undefined;
    versions.sort();
    return versions[versions.length - 1];
  }
}

function isTerminal(status: string): boolean {
  return status === "completed" || status === "failed" || status === "cancelled";
}
