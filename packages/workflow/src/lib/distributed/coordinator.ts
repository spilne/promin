// ---------------------------------------------------------------------------
// WorkflowCoordinator — submits workflows and orchestrates step dispatch
//
// The coordinator holds a WorkflowRunner backed by StepQueueExecutor.
// Each submitted workflow runs as a background Promise: the runner computes
// the DAG ready-set, delegates step bodies to StepQueueExecutor (which
// enqueues + polls storage), and fires waiters on completion.
//
// The start() loop handles leader election and dead-worker detection only —
// the DAG loop lives inside WorkflowRunner now.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import type { WorkflowState } from "../durable/workflow-state.ts";
import type { Workflow, WorkflowDAG, StepDefinition } from "../durable/durable-pipeline.ts";
import { LosslessJsonCodec } from "@promin/core";
import type {
  IWorkflowVersionRegistry,
  WorkflowVersionRegistry,
} from "../durable/workflow-version-registry.ts";
import { createWorkflowRunner, type WorkflowRunner } from "../durable/workflow-runner.ts";
import { WorkflowLockError } from "../durable/durable-pipeline-error.ts";
import type { StepQueue } from "./step-queue.ts";
import type { WorkerRegistry } from "./worker-registry.ts";
import type { LeaderElection } from "./leader-election.ts";
import { SingleLeader } from "./leader-election.ts";
import { StepQueueExecutor } from "./step-queue-executor.ts";

export interface CoordinatorConfig {
  /** Workflow storage for state persistence. */
  storage: WorkflowStorage;
  /** Step queue for dispatching tasks to workers. */
  stepQueue: StepQueue;
  /**
   * How often to check for dead workers and recover active workflows (ms).
   * Default: 1000.
   */
  pollIntervalMs?: number;
  /** Worker registry for health monitoring. Optional — without it, no dead detection. */
  workerRegistry?: WorkerRegistry;
  /** How long before a worker is considered dead (ms). Default: 30000. */
  workerTimeoutMs?: number;
  /** Leader election — ensures only one coordinator runs. Default: SingleLeader (always wins). */
  leaderElection?: LeaderElection;
  /**
   * Optional workflow registry. When provided, `submit({ name, ... })`
   * resolves the definition by name (latest version) via the registry.
   * Lets submitters stay decoupled from workflow definitions — only the
   * coordinator process has to know how to build them.
   */
  registry?: WorkflowVersionRegistry | IWorkflowVersionRegistry;
  /**
   * How often the StepQueueExecutor polls storage while waiting for a step
   * to complete. Default: 500ms.
   */
  stepPollIntervalMs?: number;
}

/** Submit a workflow by passing its definition directly. */
export interface DirectSubmit<Input> {
  workflow: Workflow<Input, unknown>;
  workflowId: string;
  input: Input;
}

/** Submit a workflow by name — requires `registry` on the coordinator. */
export interface NamedSubmit<Input> {
  name: string;
  workflowId: string;
  input: Input;
  /** Optional version override. Defaults to the registry's latest. */
  version?: string;
}

export interface WorkflowCoordinator {
  /** Submit a workflow for distributed execution — by definition. */
  submit<Input>(params: DirectSubmit<Input>): Promise<void>;
  /** Submit a workflow for distributed execution — by registered name. */
  submit<Input>(params: NamedSubmit<Input>): Promise<void>;

  /** Get current workflow state. */
  status(workflowId: string): Promise<WorkflowState | null>;

  /** Wait for a workflow to complete. Returns the final result. */
  waitForResult<Output>(workflowId: string): Promise<Output>;

  /** Run the coordination loop (leader election + dead-worker detection). */
  start(): Promise<void>;

  /** Stop the coordination loop gracefully. */
  stop(): Promise<void>;
}

export class DefaultCoordinator implements WorkflowCoordinator {
  private readonly storage: WorkflowStorage;
  private readonly stepQueue: StepQueue;
  private readonly runner: WorkflowRunner;
  private readonly pollIntervalMs: number;
  private readonly workerRegistry?: WorkerRegistry;
  private readonly workerTimeoutMs: number;
  private readonly leaderElection: LeaderElection;
  private readonly registry?: WorkflowVersionRegistry | IWorkflowVersionRegistry;
  private running = false;
  private isLeader = false;
  private runningWorkflows = new Map<string, Promise<unknown>>();
  private waiters = new Map<
    string,
    { resolve: (v: unknown) => void; reject: (e: unknown) => void }[]
  >();

  constructor(config: CoordinatorConfig) {
    this.storage = config.storage;
    this.stepQueue = config.stepQueue;
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.workerRegistry = config.workerRegistry;
    this.workerTimeoutMs = config.workerTimeoutMs ?? 30_000;
    this.leaderElection = config.leaderElection ?? new SingleLeader();
    this.registry = config.registry;

    const executor = new StepQueueExecutor({
      stepQueue: config.stepQueue,
      storage: config.storage,
      pollIntervalMs: config.stepPollIntervalMs ?? config.pollIntervalMs ?? 500,
      staleTimeoutMs: config.workerTimeoutMs ?? 30_000,
    });

    this.runner = createWorkflowRunner({
      storage: config.storage,
      registry: config.registry,
      stepExecutor: executor,
    });
  }

  submit<Input>(params: DirectSubmit<Input>): Promise<void>;
  submit<Input>(params: NamedSubmit<Input>): Promise<void>;
  async submit<Input>(params: DirectSubmit<Input> | NamedSubmit<Input>): Promise<void> {
    const workflow =
      "workflow" in params
        ? params.workflow
        : ((await this.resolveByName(params.name, params.version)) as Workflow<Input, unknown>);
    const { workflowId, input } = params;

    if (this.runningWorkflows.has(workflowId)) return;

    // Pre-create in storage with DAG embedded in metadata so crash-recovery
    // can rebuild the stub workflow without the original definition object.
    await this.storage.createWorkflow({
      workflowId,
      workflowName: workflow.name,
      input,
      version: workflow.version,
      metadata: { ...((workflow as any).metadata ?? {}), _dag: workflow.dag },
    });

    const p = this.runner
      .run({ workflow, workflowId, input })
      .then((r) => this.resolveWaiters(workflowId, r))
      .catch((e) => {
        if (e instanceof WorkflowLockError) return;
        this.rejectWaiters(workflowId, e);
      })
      .finally(() => this.runningWorkflows.delete(workflowId));
    this.runningWorkflows.set(workflowId, p);
  }

  async status(workflowId: string): Promise<WorkflowState | null> {
    return this.storage.loadWorkflow(workflowId);
  }

  async waitForResult<Output>(workflowId: string): Promise<Output> {
    const state = await this.storage.loadWorkflow(workflowId);
    if (state?.status === "completed") return state.result as Output;
    if (state?.status === "failed") throw new Error(state.error ?? "Workflow failed");

    return new Promise<Output>((resolve, reject) => {
      if (!this.waiters.has(workflowId)) this.waiters.set(workflowId, []);
      this.waiters.get(workflowId)!.push({
        resolve: resolve as (v: unknown) => void,
        reject,
      });
    });
  }

  async start(): Promise<void> {
    this.running = true;

    while (this.running) {
      this.isLeader = await this.leaderElection.tryAcquire();

      if (this.isLeader) {
        if (this.runningWorkflows.size === 0) {
          await this.recoverActiveWorkflows();
        }
        await this.tickDeadWorkers();
      }

      await new Promise((r) => setTimeout(r, this.pollIntervalMs));
    }

    if (this.isLeader) {
      await this.leaderElection.release();
      this.isLeader = false;
    }
  }

  async stop(): Promise<void> {
    this.running = false;
  }

  private async resolveByName(name: string, version?: string): Promise<Workflow<unknown, unknown>> {
    if (!this.registry) {
      throw new Error(
        `coordinator.submit({ name }) requires \`registry\` on CoordinatorConfig. ` +
          `Pass a WorkflowVersionRegistry or use the { workflow } shape.`,
      );
    }
    const def = await this.registry.resolve(name, version);
    if (!def) {
      const allNames = await this.registry.names();
      throw new Error(
        `No workflow "${name}"${version ? ` version "${version}"` : ""} in registry. Registered: ${(allNames as string[]).join(", ") || "(none)"}.`,
      );
    }
    return def;
  }

  private async tickDeadWorkers(): Promise<void> {
    if (this.workerRegistry) {
      const dead = await this.workerRegistry.detectDead(this.workerTimeoutMs);
      for (const worker of dead) {
        await this.stepQueue.requeueStuck({ claimedBy: worker.workerId });
      }
    }
    await this.stepQueue.requeueStuck({ staleTimeoutMs: this.workerTimeoutMs });
  }

  private async recoverActiveWorkflows(): Promise<void> {
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
          await this.submit({ workflow: stub, workflowId: state.workflowId, input: state.input });
        }
        if (page.length < pageSize) break;
        offset += pageSize;
      }
    }
  }

  private resolveWaiters(workflowId: string, result: unknown): void {
    const waiters = this.waiters.get(workflowId) ?? [];
    for (const w of waiters) w.resolve(result);
    this.waiters.delete(workflowId);
  }

  private rejectWaiters(workflowId: string, error: Error): void {
    const waiters = this.waiters.get(workflowId) ?? [];
    for (const w of waiters) w.reject(error);
    this.waiters.delete(workflowId);
  }
}

/**
 * Build a minimal Workflow stub from a persisted DAG for crash recovery.
 * Step execute functions are unreachable — the coordinator delegates all
 * step bodies to StepQueueExecutor, which enqueues + polls storage.
 */
function buildStubWorkflow(
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

export function createCoordinator(config: CoordinatorConfig): WorkflowCoordinator {
  return new DefaultCoordinator(config);
}
