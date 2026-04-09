// ---------------------------------------------------------------------------
// WorkflowCoordinator — submits workflows and dispatches steps to queues
//
// The coordinator computes the DAG ready-set and enqueues steps.
// Workers poll their queue, execute, and checkpoint results.
// The coordinator detects completions and enqueues the next batch.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import type { WorkflowState } from "../durable/workflow-state.ts";
import type { WorkflowDefinition, WorkflowDAG } from "../durable/durable-pipeline.ts";
import { computeReadySet } from "../durable/workflow-dag.ts";
import type { StepQueue } from "./step-queue.ts";
import type { WorkerRegistry } from "./worker-registry.ts";
import type { LeaderElection } from "./leader-election.ts";
import { SingleLeader } from "./leader-election.ts";

export interface CoordinatorConfig {
  /** Workflow storage for state persistence. */
  storage: WorkflowStorage;
  /** Step queue for dispatching tasks to workers. */
  stepQueue: StepQueue;
  /**
   * Route step names to queue names.
   * Unmatched steps go to "default".
   *
   * @example
   * ```ts
   * routing: {
   *   "transcribe": "gpu",
   *   "summarize": "ai",
   * }
   * ```
   */
  routing?: Record<string, string>;
  /** Default queue name for unrouted steps. Default: "default". */
  defaultQueue?: string;
  /** How often to check for completed steps and enqueue next batch (ms). Default: 1000. */
  pollIntervalMs?: number;
  /** Worker registry for health monitoring. Optional — without it, no dead detection. */
  workerRegistry?: WorkerRegistry;
  /** How long before a worker is considered dead (ms). Default: 30000. */
  workerTimeoutMs?: number;
  /** Leader election — ensures only one coordinator runs. Default: SingleLeader (always wins). */
  leaderElection?: LeaderElection;
}

export interface WorkflowCoordinator {
  /** Submit a workflow for distributed execution. */
  submit<Input>(params: {
    workflow: WorkflowDefinition<Input, unknown>;
    workflowId: string;
    input: Input;
  }): Promise<void>;

  /** Get current workflow state. */
  status(workflowId: string): Promise<WorkflowState | null>;

  /** Wait for a workflow to complete. Returns the final result. */
  waitForResult<Output>(workflowId: string): Promise<Output>;

  /** Run the coordination loop (enqueue ready steps, watch completions). */
  start(): Promise<void>;

  /** Stop the coordination loop gracefully. */
  stop(): Promise<void>;
}

export class DefaultCoordinator implements WorkflowCoordinator {
  private readonly storage: WorkflowStorage;
  private readonly stepQueue: StepQueue;
  private readonly routing: Record<string, string>;
  private readonly defaultQueue: string;
  private readonly pollIntervalMs: number;
  private readonly workerRegistry?: WorkerRegistry;
  private readonly workerTimeoutMs: number;
  private readonly leaderElection: LeaderElection;
  private running = false;
  private isLeader = false;
  private dags = new Map<string, WorkflowDAG>();
  private enqueued = new Map<string, Set<string>>(); // workflowId → set of enqueued step names
  private waiters = new Map<
    string,
    { resolve: (v: unknown) => void; reject: (e: unknown) => void }[]
  >();

  constructor(config: CoordinatorConfig) {
    this.storage = config.storage;
    this.stepQueue = config.stepQueue;
    this.routing = config.routing ?? {};
    this.defaultQueue = config.defaultQueue ?? "default";
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.workerRegistry = config.workerRegistry;
    this.workerTimeoutMs = config.workerTimeoutMs ?? 30_000;
    this.leaderElection = config.leaderElection ?? new SingleLeader();
  }

  async submit<Input>(params: {
    workflow: WorkflowDefinition<Input, unknown>;
    workflowId: string;
    input: Input;
  }): Promise<void> {
    const { workflow, workflowId, input } = params;
    const dag = workflow.dag;

    // Create workflow in storage — persist DAG in metadata for recovery
    await this.storage.createWorkflow({
      workflowId,
      workflowName: workflow.name,
      input,
      metadata: { ...((workflow as any).metadata ?? {}), _dag: dag },
    });

    // Store the DAG for coordination
    this.dags.set(workflowId, dag);

    // Enqueue initial ready steps
    await this.enqueueReady(workflowId, input);
  }

  async status(workflowId: string): Promise<WorkflowState | null> {
    return this.storage.loadWorkflow(workflowId);
  }

  async waitForResult<Output>(workflowId: string): Promise<Output> {
    // Check if already complete
    const state = await this.storage.loadWorkflow(workflowId);
    if (state?.status === "completed") return state.result as Output;
    if (state?.status === "failed") throw new Error(state.error ?? "Workflow failed");

    // Wait via promise
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
      // Leader election — only one coordinator runs at a time
      this.isLeader = await this.leaderElection.tryAcquire();

      if (this.isLeader) {
        // First tick as leader — recover active workflows
        if (this.dags.size === 0) {
          await this.recoverActiveWorkflows();
        }
        await this.tick();
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

  // ---------------------------------------------------------------------------
  // Coordination loop
  // ---------------------------------------------------------------------------

  private async tick(): Promise<void> {
    // Detect dead workers and re-enqueue their stuck tasks
    if (this.workerRegistry) {
      const dead = await this.workerRegistry.detectDead(this.workerTimeoutMs);
      for (const worker of dead) {
        await this.reEnqueueStuckTasks(worker.workerId);
      }
    }

    // Catch-all: requeue any task stuck in 'running' longer than the timeout.
    // This handles workers that crashed before registering or heartbeating.
    await this.stepQueue.requeueStuck({ staleTimeoutMs: this.workerTimeoutMs });

    // Check all tracked workflows for completed steps
    for (const [workflowId] of this.dags) {
      const state = await this.storage.loadWorkflow(workflowId);
      if (!state) continue;

      if (state.status === "completed") {
        this.resolveWaiters(workflowId, state.result);
        this.dags.delete(workflowId);
        continue;
      }

      if (state.status === "failed") {
        this.rejectWaiters(workflowId, new Error(state.error ?? "Workflow failed"));
        this.dags.delete(workflowId);
        continue;
      }

      // Enqueue any newly ready steps
      await this.enqueueReady(workflowId, state.input);
    }
  }

  private async enqueueReady(workflowId: string, input: unknown): Promise<void> {
    const dag = this.dags.get(workflowId);
    if (!dag) return;

    const state = await this.storage.loadWorkflow(workflowId);
    if (!state) return;

    // Track already-enqueued steps for this workflow
    if (!this.enqueued.has(workflowId)) this.enqueued.set(workflowId, new Set());
    const enqueuedSteps = this.enqueued.get(workflowId)!;

    const completed = new Set<string>();
    const running = new Set<string>();

    for (const [stepName, stepState] of Object.entries(state.steps)) {
      if (stepState.status === "completed") completed.add(stepName);
      if (stepState.status === "running" || stepState.status === "failed") running.add(stepName);
    }

    // Treat enqueued-but-not-yet-completed steps as running
    for (const name of enqueuedSteps) {
      if (!completed.has(name)) running.add(name);
    }

    const dagNodes = dag.steps.map((s) => ({
      name: s.name,
      dependsOn: [...s.dependsOn],
    }));

    const ready = computeReadySet({ nodes: dagNodes, completed, running });

    // Build results map from completed steps
    const prevResults: Record<string, unknown> = {};
    for (const [stepName, stepState] of Object.entries(state.steps)) {
      if (stepState.status === "completed") {
        prevResults[stepName] = stepState.result;
      }
    }

    for (const stepName of ready) {
      if (enqueuedSteps.has(stepName)) continue;

      const queue = this.routing[stepName] ?? this.defaultQueue;
      await this.stepQueue.enqueue({
        workflowId,
        stepName,
        queue,
        input,
        prevResults,
      });
      enqueuedSteps.add(stepName);
    }

    // Check if all steps are complete
    if (completed.size === dag.steps.length) {
      const lastStep = dag.steps[dag.steps.length - 1];
      const finalResult = lastStep ? prevResults[lastStep.name] : undefined;
      await this.storage.completeWorkflow(workflowId, finalResult);
      this.enqueued.delete(workflowId);
    }
  }

  private async recoverActiveWorkflows(): Promise<void> {
    // Reload running/suspended workflows in pages to handle large counts
    for (const status of ["running", "suspended"] as const) {
      let offset = 0;
      const pageSize = 100;
      while (true) {
        const page = await this.storage.listWorkflows({ status, limit: pageSize, offset });
        for (const state of page) {
          if (this.dags.has(state.workflowId)) continue;
          const dag = state.metadata?._dag as WorkflowDAG | undefined;
          if (!dag) continue;
          this.dags.set(state.workflowId, dag);
        }
        if (page.length < pageSize) break;
        offset += pageSize;
      }
    }
  }

  private async reEnqueueStuckTasks(deadWorkerId: string): Promise<void> {
    const requeued = await this.stepQueue.requeueStuck({ claimedBy: deadWorkerId });
    if (requeued > 0) {
      // The next tick will re-evaluate ready-sets and re-enqueue naturally
      // since stuck tasks are now back to "pending"
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

export function createCoordinator(config: CoordinatorConfig): WorkflowCoordinator {
  return new DefaultCoordinator(config);
}
