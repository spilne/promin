// ---------------------------------------------------------------------------
// DistributedWorkflowRunner — WorkflowRunner backed by a StepQueue.
//
// Implements the same WorkflowRunner interface as DefaultWorkflowRunner but
// delegates step execution to remote workers via a StepQueue instead of
// running step bodies in-process. The orchestration loop (DAG ready-set,
// lock, retry, compensation) still runs here; only step bodies are remote.
//
// Also runs a background leader-elected sweep loop (startLoop / stopLoop)
// that detects dead workers and re-enqueues their claimed steps.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import type { WorkflowState, WorkflowRunEvent } from "../durable/workflow-state.ts";
import type {
  Workflow,
  WorkflowDAG,
  StepDefinition,
  WorkflowStatusInfo,
  WorkflowHandle,
} from "../durable/durable-pipeline.ts";
import { LosslessJsonCodec } from "@promin/core";
import type {
  IWorkflowVersionRegistry,
  WorkflowVersionRegistry,
} from "../durable/workflow-version-registry.ts";
import {
  createWorkflowRunner,
  type WorkflowRunner,
  type WorkflowRunnerRunParams,
  type WorkflowRunSafeError,
  type RecoveryResult,
  type RecoveryStrategy,
} from "../durable/workflow-runner.ts";
import { WorkflowLockError } from "../durable/durable-pipeline-error.ts";
import type { StepQueue } from "./step-queue.ts";
import type { WorkerRegistry } from "./worker-registry.ts";
import type { LeaderElection } from "./leader-election.ts";
import { SingleLeader } from "./leader-election.ts";
import { StepQueueExecutor } from "./step-queue-executor.ts";

export interface DistributedRunnerConfig {
  /** Workflow storage for state persistence. */
  storage: WorkflowStorage;
  /** Step queue for dispatching tasks to workers. */
  stepQueue: StepQueue;
  /**
   * How often to run the dead-worker sweep (ms). Default: 1000.
   */
  pollIntervalMs?: number;
  /** Worker registry for health monitoring. Optional — without it, no dead detection. */
  workerRegistry?: WorkerRegistry;
  /** How long before a worker is considered dead (ms). Default: 30000. */
  workerTimeoutMs?: number;
  /**
   * How long a retired / dead worker row is kept before the sweep loop's
   * `gc()` reaps it (ms). Keeps gracefully-stopped workers visible to the
   * dashboard + run forensics for a window. Default: 7 days.
   */
  workerRetentionMs?: number;
  /** Leader election — ensures only one instance runs the sweep. Default: SingleLeader (always wins). */
  leaderElection?: LeaderElection;
  /**
   * Optional workflow registry. When provided, `run({ name, ... })`
   * resolves the definition by name via the registry.
   */
  registry?: WorkflowVersionRegistry | IWorkflowVersionRegistry;
  /**
   * How often the StepQueueExecutor polls storage while waiting for a step
   * to complete. Default: 500ms.
   */
  stepPollIntervalMs?: number;
}

/** @deprecated Use DistributedRunnerConfig */
export type CoordinatorConfig = DistributedRunnerConfig;

/**
 * Run worker `gc()` every Nth dead-worker sweep rather than every tick —
 * the retention window is days, so a reap scan every poll buys nothing.
 * At the default 1s poll this is roughly once a minute.
 */
const WORKER_GC_EVERY_N_TICKS = 60;

export class DistributedWorkflowRunner implements WorkflowRunner {
  readonly storage: WorkflowStorage;
  private readonly innerRunner: WorkflowRunner;
  private readonly stepQueue: StepQueue;
  private readonly pollIntervalMs: number;
  private readonly workerRegistry?: WorkerRegistry;
  private readonly workerTimeoutMs: number;
  private readonly workerRetentionMs: number;
  /** Dead-worker sweep counter — drives the throttled `gc()` cadence. */
  private workerSweepCount = 0;
  private readonly leaderElection: LeaderElection;
  private running = false;
  private isLeader = false;
  private runningWorkflows = new Map<string, Promise<unknown>>();
  private waiters = new Map<
    string,
    { resolve: (v: unknown) => void; reject: (e: unknown) => void }[]
  >();

  constructor(config: DistributedRunnerConfig) {
    this.storage = config.storage;
    this.stepQueue = config.stepQueue;
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.workerRegistry = config.workerRegistry;
    this.workerTimeoutMs = config.workerTimeoutMs ?? 30_000;
    this.workerRetentionMs = config.workerRetentionMs ?? 7 * 24 * 60 * 60 * 1000;
    this.leaderElection = config.leaderElection ?? new SingleLeader();

    const executor = new StepQueueExecutor({
      stepQueue: config.stepQueue,
      storage: config.storage,
      pollIntervalMs: config.stepPollIntervalMs ?? config.pollIntervalMs ?? 500,
      staleTimeoutMs: config.workerTimeoutMs ?? 30_000,
    });

    this.innerRunner = createWorkflowRunner({
      storage: config.storage,
      registry: config.registry,
      stepExecutor: executor,
    });
  }

  // ---------------------------------------------------------------------------
  // WorkflowRunner interface
  // ---------------------------------------------------------------------------

  async run(params: WorkflowRunnerRunParams): Promise<unknown> {
    const { workflowId } = params;
    await this._submit(params);
    return this._waitForResult(workflowId);
  }

  async runSafe(
    params: WorkflowRunnerRunParams,
  ): Promise<{ data: unknown; error: null } | { data: null; error: WorkflowRunSafeError }> {
    try {
      const data = await this.run(params);
      return { data, error: null };
    } catch (error) {
      return { data: null, error: error as WorkflowRunSafeError };
    }
  }

  async start<Input = unknown, Output = unknown>(params: {
    readonly workflow: Workflow<Input, Output>;
    readonly workflowId: string;
    readonly input: Input;
  }): Promise<WorkflowHandle<Output>> {
    await this._submit(params);
    return this.handle<Output>(params.workflowId);
  }

  handle<Output = unknown>(workflowId: string): WorkflowHandle<Output> {
    return this.innerRunner.handle<Output>(workflowId);
  }

  resume<Input = unknown, Output = unknown>(params: {
    readonly workflow: Workflow<Input, Output>;
    readonly workflowId: string;
    readonly fromStep: string;
  }): Promise<Output> {
    return this.innerRunner.resume<Input, Output>(params);
  }

  subscribe(
    workflowId: string,
    options?: { signal?: AbortSignal; pollIntervalMs?: number },
  ): AsyncIterable<WorkflowRunEvent> {
    return this.innerRunner.subscribe(workflowId, options);
  }

  getStatus(
    workflowId: string,
    params?: { readonly includeStepResults?: boolean },
  ): Promise<WorkflowStatusInfo<unknown> | null> {
    return this.innerRunner.getStatus(workflowId, params);
  }

  /**
   * Load the workflow's current persistent state. Pairs with the
   * deprecated `WorkflowCoordinator.status` contract — new callers should
   * prefer `getStatus()` (richer info) or `storage.loadWorkflow()`.
   */
  status(workflowId: string): Promise<WorkflowState | null> {
    return this.storage.loadWorkflow(workflowId);
  }

  recover(strategy: RecoveryStrategy): Promise<RecoveryResult> {
    return this.innerRunner.recover(strategy);
  }

  // ---------------------------------------------------------------------------
  // Distributed-specific: submit fire-and-forget (used by trigger services)
  // ---------------------------------------------------------------------------

  async submit<Input>(
    params:
      | { workflow: Workflow<Input, unknown>; workflowId: string; input: Input }
      | { name: string; version?: string; workflowId: string; input: Input },
  ): Promise<void> {
    await this._submit(params as WorkflowRunnerRunParams);
  }

  // ---------------------------------------------------------------------------
  // Lifecycle — start/stop the background dead-worker sweep loop
  // ---------------------------------------------------------------------------

  async startLoop(): Promise<void> {
    this.running = true;

    while (this.running) {
      this.isLeader = await this.leaderElection.tryAcquire();

      if (this.isLeader) {
        if (this.runningWorkflows.size === 0) {
          await this._recoverActiveWorkflows();
        }
        await this._tickDeadWorkers();
      }

      await new Promise((r) => setTimeout(r, this.pollIntervalMs));
    }

    if (this.isLeader) {
      await this.leaderElection.release();
      this.isLeader = false;
    }
  }

  async stopLoop(): Promise<void> {
    this.running = false;
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private async _submit(params: WorkflowRunnerRunParams): Promise<void> {
    const { workflowId, input } = params;
    if (this.runningWorkflows.has(workflowId)) return;

    let workflow: Workflow<unknown, unknown>;
    if ("workflow" in params) {
      workflow = params.workflow as Workflow<unknown, unknown>;
    } else {
      workflow = (await this._resolveByName(params.name, (params as any).version)) as Workflow<
        unknown,
        unknown
      >;
    }

    // Pre-create in storage with DAG embedded in metadata so crash-recovery
    // can rebuild the stub without the original definition object.
    await this.storage.createWorkflow({
      workflowId,
      workflowName: workflow.name,
      input,
      version: workflow.version,
      metadata: { ...((workflow as any).metadata ?? {}), _dag: workflow.dag },
    });

    const p = this.innerRunner
      .run({ workflow, workflowId, input })
      .then((r) => this._resolveWaiters(workflowId, r))
      .catch((e) => {
        if (e instanceof WorkflowLockError) return;
        this._rejectWaiters(workflowId, e);
      })
      .finally(() => this.runningWorkflows.delete(workflowId));
    this.runningWorkflows.set(workflowId, p);
  }

  /**
   * Wait for a previously-submitted workflow to complete (or fail). Pairs
   * with `submit({...})` for the deprecated submit-then-wait flow that
   * the `WorkflowCoordinator` interface still describes; new code should
   * use `run({...})` which submits + waits in one call.
   */
  waitForResult<Output>(workflowId: string): Promise<Output> {
    return this._waitForResult<Output>(workflowId);
  }

  private _waitForResult<Output>(workflowId: string): Promise<Output> {
    return this.storage.loadWorkflow(workflowId).then((state) => {
      if (state?.status === "completed") return state.result as Output;
      if (state?.status === "failed") throw new Error(state.error ?? "Workflow failed");

      return new Promise<Output>((resolve, reject) => {
        if (!this.waiters.has(workflowId)) this.waiters.set(workflowId, []);
        this.waiters.get(workflowId)!.push({
          resolve: resolve as (v: unknown) => void,
          reject,
        });
      });
    });
  }

  private async _resolveByName(
    name: string,
    version?: string,
  ): Promise<Workflow<unknown, unknown>> {
    const registry = (this.innerRunner as any).registry as
      | WorkflowVersionRegistry
      | IWorkflowVersionRegistry
      | undefined;
    if (!registry) {
      throw new Error(
        `DistributedWorkflowRunner.run({ name }) requires \`registry\` on the config. ` +
          `Pass a WorkflowVersionRegistry or use the { workflow } shape.`,
      );
    }
    const def = await registry.resolve(name, version);
    if (!def) {
      const allNames = await registry.names();
      throw new Error(
        `No workflow "${name}"${version ? ` version "${version}"` : ""} in registry. ` +
          `Registered: ${(allNames as string[]).join(", ") || "(none)"}.`,
      );
    }
    return def;
  }

  private async _tickDeadWorkers(): Promise<void> {
    if (this.workerRegistry) {
      const dead = await this.workerRegistry.detectDead(this.workerTimeoutMs);
      for (const worker of dead) {
        await this.stepQueue.requeueStuck({ claimedBy: worker.workerId });
      }
      // Reap worker rows past the retention window. Throttled — see
      // WORKER_GC_EVERY_N_TICKS — so retired / dead rows stay visible
      // for the window, then go.
      this.workerSweepCount += 1;
      if (this.workerSweepCount % WORKER_GC_EVERY_N_TICKS === 0) {
        await this.workerRegistry.gc({ retainMs: this.workerRetentionMs });
      }
    }
    await this.stepQueue.requeueStuck({ staleTimeoutMs: this.workerTimeoutMs });
  }

  private async _recoverActiveWorkflows(): Promise<void> {
    for (const status of ["pending", "running", "suspended"] as const) {
      let offset = 0;
      const pageSize = 100;
      while (true) {
        const page = await this.storage.listWorkflows({ status, limit: pageSize, offset });
        for (const state of page) {
          if (this.runningWorkflows.has(state.workflowId)) continue;
          const dag = state.metadata?._dag as WorkflowDAG | undefined;
          if (!dag) continue;
          const stub = buildStubWorkflow(dag, state.workflowName ?? dag.name, state.version);
          await this._submit({ workflow: stub, workflowId: state.workflowId, input: state.input });
        }
        if (page.length < pageSize) break;
        offset += pageSize;
      }
    }
  }

  private _resolveWaiters(workflowId: string, result: unknown): void {
    const waiters = this.waiters.get(workflowId) ?? [];
    for (const w of waiters) w.resolve(result);
    this.waiters.delete(workflowId);
  }

  private _rejectWaiters(workflowId: string, error: Error): void {
    const waiters = this.waiters.get(workflowId) ?? [];
    for (const w of waiters) w.reject(error);
    this.waiters.delete(workflowId);
  }
}

/**
 * Build a minimal Workflow stub from a persisted DAG. Step `execute`
 * functions are unreachable — the distributed runner delegates all step
 * bodies to `StepQueueExecutor`. Used for crash recovery and by trigger
 * handlers that build from an advertised DAG without holding the full definition.
 */
export function buildStubWorkflow(
  dag: WorkflowDAG,
  name: string,
  version?: string,
): Workflow<unknown, unknown> {
  const steps: StepDefinition[] = dag.steps.map((node) => ({
    name: node.name,
    dependsOn: [...node.dependsOn],
    kind: node.kind as StepDefinition["kind"],
    execute: () => {
      throw new Error(
        `unreachable: stub workflow step "${node.name}" should never be executed in-process`,
      );
    },
    codec: LosslessJsonCodec,
    needs: node.needs,
    priority: node.priority,
  }));

  return {
    name,
    version,
    dag,
    _definition: {
      steps,
      onVersionMismatch: "strict",
    },
  };
}

export function createDistributedWorkflowRunner(
  config: DistributedRunnerConfig,
): DistributedWorkflowRunner {
  return new DistributedWorkflowRunner(config);
}

// ---------------------------------------------------------------------------
// Backward-compat aliases
// ---------------------------------------------------------------------------

/** @deprecated Use DistributedWorkflowRunner */
export interface WorkflowCoordinator {
  submit<Input>(params: {
    workflow: Workflow<Input, unknown>;
    workflowId: string;
    input: Input;
  }): Promise<void>;
  submit<Input>(params: {
    name: string;
    workflowId: string;
    input: Input;
    version?: string;
  }): Promise<void>;
  status(workflowId: string): Promise<WorkflowState | null>;
  waitForResult<Output>(workflowId: string): Promise<Output>;
  /** @deprecated Use startLoop() */
  startLoop(): Promise<void>;
  /** @deprecated Use stopLoop() */
  stopLoop(): Promise<void>;
}

/** @deprecated Use DistributedWorkflowRunner */
export const DefaultCoordinator = DistributedWorkflowRunner;

/** @deprecated Use createDistributedWorkflowRunner */
export function createCoordinator(config: CoordinatorConfig): DistributedWorkflowRunner {
  return new DistributedWorkflowRunner(config);
}
