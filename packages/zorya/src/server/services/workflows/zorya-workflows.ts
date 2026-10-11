// ---------------------------------------------------------------------------
// ZoryaWorkflows — abstract base for the workflow service layer.
//
// Owns: storage, optional definitions / advertisements catalogs, dispatch
// (trigger + rerun), and lifecycle (start/stop) for any background loops a
// concrete subclass needs (sleep scanner, coordinator sweep, recovery).
//
// Subclasses use the template-method pattern: implement `canHandle` +
// `dispatch` (and overrides for rerun / lifecycle as needed) and the base
// handles the optional fallback chain. `trigger` is final on the base —
// subclasses don't need to remember to delegate.
//
// Composition: an instance can carry `fallback?: ZoryaWorkflows` so triggers
// for workflows it can't handle locally walk the chain. `start()` / `stop()`
// cascade through the fallback.
// ---------------------------------------------------------------------------

import type { CreateWorkflowResult, Workflow, WorkflowStorage, RunSource } from "@promin/workflow";
import type { WorkflowAdvertisementRegistry } from "../../workflow-advertisements.ts";
import { UnknownWorkflowError, WorkflowIdConflictError } from "./errors.ts";

export interface TriggerOptions {
  readonly workflowId?: string;
  readonly workflowType?: string;
  readonly namespace?: string;
  readonly metadata?: Record<string, unknown>;
  readonly version?: string;
  readonly runSource?: RunSource;
  readonly runSourceId?: string;
}

export interface TriggerResult {
  readonly workflowId: string;
  /**
   * True when a run with this `workflowId` already existed: the trigger
   * started nothing and answers with that run (any status). Re-running an
   * ended run is an explicit `rerun`.
   */
  readonly existing?: boolean;
}

/**
 * Own-property lookup of a workflow definition by name, so prototype keys
 * (`constructor`, `toString`, ...) never resolve to a definition.
 */
export function findDefinition(
  definitions: Readonly<Record<string, Workflow<unknown, unknown>>> | undefined,
  name: string,
): Workflow<unknown, unknown> | undefined {
  return definitions && Object.hasOwn(definitions, name) ? definitions[name] : undefined;
}

/**
 * Interpret `createWorkflow`'s result for a trigger. `undefined` means the
 * row was created and the caller starts it. An existing run of the same
 * workflow is answered as-is (`existing: true`) — a redelivered schedule
 * tick or a retried trigger with the same id must not run the body again.
 * An existing run of another workflow throws `WorkflowIdConflictError`.
 */
export function existingRunResult(params: {
  workflowName: string;
  workflowId: string;
  created: CreateWorkflowResult;
}): TriggerResult | undefined {
  const { created } = params;
  if (created.created) return undefined;
  if (created.existing.workflowName !== params.workflowName) {
    throw new WorkflowIdConflictError({
      workflowId: params.workflowId,
      existingWorkflowName: created.existing.workflowName,
      requestedWorkflowName: params.workflowName,
    });
  }
  return { workflowId: params.workflowId, existing: true };
}

export interface ZoryaWorkflowsBaseConfig {
  definitions?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  advertisements?: WorkflowAdvertisementRegistry;
  fallback?: ZoryaWorkflows;
}

export abstract class ZoryaWorkflows {
  /** Storage backing all run CRUD/query routes. */
  abstract readonly storage: WorkflowStorage;

  /** Optional in-process workflow definitions, keyed by name. */
  readonly definitions?: Readonly<Record<string, Workflow<unknown, unknown>>>;

  /** Optional worker-advertisement registry — present on remote-capable layers. */
  readonly advertisements?: WorkflowAdvertisementRegistry;

  /** Optional fallback layer. trigger() walks here when this layer can't handle. */
  protected readonly fallback?: ZoryaWorkflows;

  private started = false;

  constructor(config: ZoryaWorkflowsBaseConfig = {}) {
    if (config.definitions) this.definitions = config.definitions;
    if (config.advertisements) this.advertisements = config.advertisements;
    if (config.fallback) this.fallback = config.fallback;
  }

  /** Subclass: do you know this workflow name? */
  protected abstract canHandle(name: string): boolean | Promise<boolean>;

  /** Subclass: actually dispatch. Only called when canHandle returned true. */
  protected abstract dispatch(
    name: string,
    input: unknown,
    opts?: TriggerOptions,
  ): Promise<TriggerResult>;

  /**
   * Start a fresh run of an ended workflow under the same id. Callers check
   * that the run exists and has ended. Default: not supported.
   */
  async rerun(workflowId: string): Promise<void> {
    if (this.fallback) return this.fallback.rerun(workflowId);
    throw new Error(
      `rerun(${workflowId}) — no layer in the chain implements rerun for this workflow`,
    );
  }

  /**
   * Start a workflow by name. Walks the fallback chain when this layer
   * doesn't know the workflow. Final on the base — subclasses override
   * `canHandle` / `dispatch` instead.
   */
  async trigger(name: string, input: unknown, opts?: TriggerOptions): Promise<TriggerResult> {
    if (await this.canHandle(name)) {
      return this.dispatch(name, input, opts);
    }
    if (this.fallback) return this.fallback.trigger(name, input, opts);
    throw new UnknownWorkflowError(name);
  }

  /**
   * Start background loops + run any one-shot startup work (e.g. recovery).
   * Bottom-up: fallback starts first so the outer layer can rely on its
   * inner state. Idempotent.
   */
  async start(): Promise<void> {
    if (this.started) return;
    if (this.fallback) await this.fallback.start();
    await this.onStart();
    this.started = true;
  }

  /**
   * Stop background loops. Top-down: outer layer stops first so it doesn't
   * push work onto a stopping inner layer. Idempotent.
   */
  async stop(): Promise<void> {
    if (!this.started) return;
    await this.onStop();
    if (this.fallback) await this.fallback.stop();
    this.started = false;
  }

  /** Subclass hook for start lifecycle. Default: no-op. */
  protected async onStart(): Promise<void> {}

  /** Subclass hook for stop lifecycle. Default: no-op. */
  protected async onStop(): Promise<void> {}
}
