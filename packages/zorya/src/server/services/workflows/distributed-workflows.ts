// ---------------------------------------------------------------------------
// DistributedWorkflows — coordinator mode. This process owns the DAG state
// machine and dispatches individual steps to remote step-mode workers via a
// shared step queue. Workers claim tasks whose `needs` are satisfied by
// their declared capabilities (per-step capability routing).
//
// Owns:
//   - DistributedWorkflowRunner (which itself owns step-queue execution +
//     dead-worker sweep loop + leader election)
//   - WorkflowAdvertisementRegistry — workers POST their advertised DAGs
//     here on connect; we resolve names → DAGs through this registry
// ---------------------------------------------------------------------------

import type {
  LeaderElection,
  RecoveryStrategy,
  StepQueue,
  Workflow,
  WorkerRegistry,
  WorkflowDAG,
  WorkflowStorage,
} from "@promin/workflow";
import {
  buildStubWorkflow,
  DistributedWorkflowRunner,
  createDistributedWorkflowRunner,
} from "@promin/workflow";
import {
  InMemoryWorkflowAdvertisementRegistry,
  type AdvertisedWorkflow,
  type WorkflowAdvertisementRegistry,
} from "../../workflow-advertisements.ts";
import { ZoryaWorkflows, type TriggerOptions, type TriggerResult } from "./zorya-workflows.ts";

export interface DistributedWorkflowsConfig {
  storage: WorkflowStorage;
  stepQueue: StepQueue;
  workerRegistry?: WorkerRegistry;
  leaderElection?: LeaderElection;
  /** Optional in-process definitions for hybrid setups (rarely needed). */
  definitions?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /** Shared advertisements registry. Auto-creates an in-memory one if omitted. */
  advertisements?: WorkflowAdvertisementRegistry;
  /** Recovery strategy run once on start(). */
  recovery?: RecoveryStrategy;
  /** Coordinator sweep cadence (ms). Default 1000. */
  pollIntervalMs?: number;
  /** Worker dead-timeout (ms). Default 30000. */
  workerTimeoutMs?: number;
  /** Optional fallback layer. */
  fallback?: ZoryaWorkflows;
}

export class DistributedWorkflows extends ZoryaWorkflows {
  readonly storage: WorkflowStorage;
  override readonly advertisements: WorkflowAdvertisementRegistry;
  readonly stepQueue: StepQueue;
  readonly workerRegistry?: WorkerRegistry;
  private readonly runner: DistributedWorkflowRunner;
  private readonly recovery?: RecoveryStrategy;
  private loop?: Promise<void>;

  constructor(config: DistributedWorkflowsConfig) {
    const advertisements = config.advertisements ?? new InMemoryWorkflowAdvertisementRegistry();
    super({
      advertisements,
      ...(config.definitions && { definitions: config.definitions }),
      ...(config.fallback && { fallback: config.fallback }),
    });
    this.storage = config.storage;
    this.stepQueue = config.stepQueue;
    this.advertisements = advertisements;
    if (config.workerRegistry) this.workerRegistry = config.workerRegistry;
    if (config.recovery) this.recovery = config.recovery;

    const runnerConfig: Parameters<typeof createDistributedWorkflowRunner>[0] = {
      storage: config.storage,
      stepQueue: config.stepQueue,
    };
    if (config.workerRegistry) runnerConfig.workerRegistry = config.workerRegistry;
    if (config.leaderElection) runnerConfig.leaderElection = config.leaderElection;
    if (config.pollIntervalMs !== undefined) runnerConfig.pollIntervalMs = config.pollIntervalMs;
    if (config.workerTimeoutMs !== undefined) runnerConfig.workerTimeoutMs = config.workerTimeoutMs;
    this.runner = createDistributedWorkflowRunner(runnerConfig);
  }

  protected async canHandle(name: string): Promise<boolean> {
    if (this.definitions && name in this.definitions) return true;
    const adv = await this.findAdvertised(name);
    return adv !== undefined;
  }

  protected async dispatch(
    name: string,
    input: unknown,
    opts?: TriggerOptions,
  ): Promise<TriggerResult> {
    const workflowId = opts?.workflowId ?? crypto.randomUUID();

    // Prefer in-process definition when present (rare in distributed mode);
    // otherwise resolve from advertisements + build a stub.
    const localDef = this.definitions?.[name];
    let workflow: Workflow<unknown, unknown>;
    let resolvedVersion = opts?.version;

    if (localDef) {
      workflow = localDef;
      if (resolvedVersion === undefined) resolvedVersion = localDef.version;
    } else {
      const advertised = await this.findAdvertised(name, opts?.version);
      if (!advertised) {
        throw new Error(
          `DistributedWorkflows: no advertisement for "${name}"${
            opts?.version ? ` version "${opts.version}"` : ""
          } — at least one worker must advertise the DAG`,
        );
      }
      const dag: WorkflowDAG = {
        name: advertised.name,
        steps: advertised.steps.map((s) => ({
          name: s.name,
          kind: s.kind as WorkflowDAG["steps"][number]["kind"],
          dependsOn: [...s.dependsOn],
          ...(s.needs !== undefined && { needs: s.needs }),
          ...(s.priority !== undefined && { priority: s.priority }),
        })),
      };
      if (resolvedVersion === undefined) resolvedVersion = advertised.version;
      workflow = buildStubWorkflow(dag, advertised.name, resolvedVersion);
    }

    // Pre-create the storage row so typed fields land (namespace, metadata,
    // runSource) and the dashboard sees a `pending` row immediately.
    await this.storage.createWorkflow({
      workflowId,
      workflowName: name,
      input,
      ...(opts?.workflowType !== undefined && { workflowType: opts.workflowType }),
      ...(opts?.namespace !== undefined && { namespace: opts.namespace }),
      ...(opts?.metadata !== undefined && { metadata: opts.metadata }),
      ...(opts?.runSource !== undefined && { runSource: opts.runSource }),
      ...(opts?.runSourceId !== undefined && { runSourceId: opts.runSourceId }),
      ...(resolvedVersion !== undefined && { version: resolvedVersion }),
    });

    await this.runner.submit({ workflow, workflowId, input });
    return { workflowId };
  }

  override async rerun(workflowId: string): Promise<void> {
    const state = await this.storage.loadWorkflow(workflowId);
    if (!state) {
      if (this.fallback) return this.fallback.rerun(workflowId);
      throw new Error(`rerun: workflow "${workflowId}" not found`);
    }
    await this.storage.startFreshRun(workflowId);
    // Re-dispatch with the same id; runner.submit is idempotent on workflowId
    // (it sees the existing row and resumes).
    await this.dispatch(state.workflowName, state.input, {
      workflowId,
      ...(state.version !== undefined && { version: state.version }),
    });
  }

  protected override async onStart(): Promise<void> {
    if (this.recovery) await this.runner.recover(this.recovery);
    // startLoop runs forever; fire-and-forget.
    this.loop = this.runner.startLoop().catch(() => {});
  }

  protected override async onStop(): Promise<void> {
    await this.runner.stopLoop();
    if (this.loop) await this.loop;
    this.loop = undefined;
  }

  private async findAdvertised(
    name: string,
    version?: string,
  ): Promise<AdvertisedWorkflow | undefined> {
    const all = await this.advertisements.distinct();
    if (version !== undefined) {
      return all.find((a) => a.name === name && a.version === version);
    }
    const matches = all.filter((a) => a.name === name);
    const unversioned = matches.find((a) => !a.version);
    if (unversioned) return unversioned;
    return matches.sort((a, b) => (a.version ?? "").localeCompare(b.version ?? "")).pop();
  }
}
