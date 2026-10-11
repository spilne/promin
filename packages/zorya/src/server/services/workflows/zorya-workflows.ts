// ---------------------------------------------------------------------------
// ZoryaWorkflows — abstract base for the workflow service layer.
//
// Owns: storage, optional definitions / advertisements catalogs, dispatch
// (trigger + rerun), and lifecycle (start/stop) for any background loops a
// concrete subclass needs (wake-up scanners, orphan recovery).
//
// Two execution modes: `LocalWorkflows` drives runs in this process;
// `QueuedWorkflows` queues them for workers that host the code and drive
// whole runs with the engine against shared storage.
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

import { WorkflowLockError } from "@promin/workflow";
import type {
  CreateWorkflowResult,
  RunSource,
  Workflow,
  WorkflowStorage,
  WorkflowVersionRegistry,
} from "@promin/workflow";
import type { WorkflowAdvertisementRegistry } from "../../workflow-advertisements.ts";
import { UnknownWorkflowError, WorkflowIdConflictError } from "./errors.ts";
import { ZoryaClusterConfigError, type ZoryaCluster } from "./cluster.ts";

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

/**
 * `fromStep` and every step that depends on it, directly or transitively —
 * the steps a resume from `fromStep` resets. Throws for a step the
 * workflow doesn't have.
 */
export function stepsFrom(params: {
  readonly steps: ReadonlyArray<{ readonly name: string; readonly dependsOn: readonly string[] }>;
  readonly fromStep: string;
  readonly workflowName: string;
}): string[] {
  const { steps, fromStep } = params;
  if (!steps.some((s) => s.name === fromStep)) {
    throw new Error(
      `workflow "${params.workflowName}" has no step "${fromStep}" ` +
        `(steps: ${steps.map((s) => s.name).join(", ")})`,
    );
  }
  const reset = new Set<string>([fromStep]);
  for (let grew = true; grew; ) {
    grew = false;
    for (const step of steps) {
      if (!reset.has(step.name) && step.dependsOn.some((d) => reset.has(d))) {
        reset.add(step.name);
        grew = true;
      }
    }
  }
  return [...reset];
}

/**
 * Reset `stepNames` of `workflowId` while holding its run lock, so a run
 * someone is driving is never rewound under them: refuses with
 * `WorkflowLockError` when the lock is held.
 */
export async function resetStepsUnderLock(params: {
  readonly storage: WorkflowStorage;
  readonly workflowId: string;
  readonly stepNames: readonly string[];
}): Promise<void> {
  const { storage, workflowId, stepNames } = params;
  if (!storage.resetSteps) throw new Error("resume needs a storage that implements resetSteps");
  const lock = await storage.tryLock({ workflowId, lockDurationMs: 30_000 });
  if (!lock.acquired) {
    throw new WorkflowLockError({
      workflowId,
      message: `Workflow "${workflowId}" is being driven right now; resume it once it stops`,
    });
  }
  const guard = lock.token !== undefined ? { fenceToken: lock.token } : undefined;
  try {
    await storage.resetSteps({ workflowId, stepNames });
  } finally {
    await storage.releaseLock({ workflowId, guard }).catch(() => {});
  }
}

export interface ZoryaWorkflowsBaseConfig {
  definitions?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  advertisements?: WorkflowAdvertisementRegistry;
  versionRegistry?: WorkflowVersionRegistry;
  fallback?: ZoryaWorkflows;
}

export abstract class ZoryaWorkflows {
  /** Storage backing all run CRUD/query routes. */
  abstract readonly storage: WorkflowStorage;

  /** Optional in-process workflow definitions, keyed by name. */
  readonly definitions?: Readonly<Record<string, Workflow<unknown, unknown>>>;

  /** Optional worker-advertisement registry — present on remote-capable layers. */
  readonly advertisements?: WorkflowAdvertisementRegistry;

  /**
   * The version registry this layer routes new runs by (promoted version)
   * and resolves stored versions through. ZoryaServer manages it on
   * `/api/workflows/:name/versions/*`, so a promote changes dispatch.
   */
  readonly versionRegistry?: WorkflowVersionRegistry;

  /** Optional fallback layer. trigger() walks here when this layer can't handle. */
  readonly fallback?: ZoryaWorkflows;

  /** The layer whose fallback this is, if any. */
  private outer?: ZoryaWorkflows;
  private started = false;

  constructor(config: ZoryaWorkflowsBaseConfig = {}) {
    if (config.definitions) this.definitions = config.definitions;
    if (config.advertisements) this.advertisements = config.advertisements;
    if (config.versionRegistry) this.versionRegistry = config.versionRegistry;
    if (config.fallback) {
      if (config.fallback.outer) {
        throw new Error("ZoryaWorkflows: a layer can be the fallback of one layer only");
      }
      this.fallback = config.fallback;
      config.fallback.outer = this;
    }
  }

  /** Every layer of the chain from this one down, in trigger order. */
  layers(): ZoryaWorkflows[] {
    return [this, ...(this.fallback?.layers() ?? [])];
  }

  /**
   * The layer whose runs of `name` are: the first layer of the whole chain,
   * from the top, that handles the name (the one its triggers reach), or
   * undefined when none does. Only the owner drives, wakes and recovers a
   * run — whichever direction the chain is stacked in.
   */
  protected async ownerOf(name: string): Promise<ZoryaWorkflows | undefined> {
    for (const layer of this.chain()) {
      if (await layer.canHandle(name)) return layer;
    }
    return undefined;
  }

  /**
   * Throw `ZoryaClusterConfigError` unless every layer from this one down
   * runs with the same `cluster` (or none does): leadership is per
   * deployment, so a chain can't mix a clustered and a single-instance
   * layer, or two clusters. Subclasses call it at the end of their
   * constructor.
   */
  protected assertOneCluster(): void {
    const clusters = this.layers().map((l) => (l as { cluster?: ZoryaCluster }).cluster);
    const first = clusters[0];
    const same = clusters.every(
      (c) => c?.instanceId === first?.instanceId && c?.leases === first?.leases,
    );
    if (!same) {
      throw new ZoryaClusterConfigError(
        "ZoryaWorkflows: every layer of a chain must be given the same `cluster` (or none)",
      );
    }
  }

  /** Every layer of the chain this layer belongs to, from the top. */
  protected chain(): ZoryaWorkflows[] {
    return this.outer ? this.outer.chain() : this.layers();
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
   * Re-drive a run from `fromStep`: that step and everything downstream of
   * it are reset (their results dropped, the run's terminal status undone)
   * and the run continues from there under its own run number — never a
   * fresh run. Refused with `WorkflowLockError` while someone drives the
   * run. Resolves once the run has been handed to its driver. Default:
   * the fallback's, else not supported.
   */
  async resume(params: { workflowId: string; fromStep: string }): Promise<void> {
    if (this.fallback) return this.fallback.resume(params);
    throw new Error(
      `resume(${params.workflowId}) — no layer in the chain implements resume for this workflow`,
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
