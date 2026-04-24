// ---------------------------------------------------------------------------
// ZoryaWorker — worker process that advertises its workflow definitions to
// the Zorya server and runs them locally with storage writes going over
// the remote wire.
//
// This is the "workflow worker" shape — each assignment drives the whole
// workflow orchestration in the worker's own process (Temporal's workflow-
// worker model). Storage is remote so the server dashboard always sees
// current state, but step bodies execute with access to the worker's local
// runtime (native libs, filesystem, secrets, etc.).
//
// Task-level step dispatch (server pushes individual steps to workers) is a
// future layer on top — see promin-32k2.
// ---------------------------------------------------------------------------

import {
  createSleepScanner,
  createWorkflowRunner,
  type SleepScanner,
  type Workflow,
  type WorkflowRunner,
} from "@promin/workflow";
import type { ZoryaClient } from "./zorya-client.ts";

export interface ZoryaWorkerConfig {
  client: ZoryaClient;
  /** Workflow definitions this worker will execute. Advertised on start. */
  workflows: ReadonlyArray<Workflow<unknown, unknown>>;
  /** Stable worker id. Default: random UUID. */
  workerId?: string;
  /** Capability tags stored on the WorkerRegistry entry. */
  capabilities?: readonly string[];
  /** Max concurrent runs — advisory metadata today. Default 10. */
  concurrency?: number;
  /** Heartbeat interval in ms. Default 5_000. */
  heartbeatIntervalMs?: number;
  /** Optional sample-input lookup for dashboard trigger forms. */
  sampleInput?: (workflowName: string) => unknown;
  /** Optional metadata stored on the WorkerRegistry entry. */
  metadata?: Record<string, unknown>;
  /**
   * When true (default), the worker starts a SleepScanner so ctx.sleep
   * inside journaled steps actually resumes.
   */
  resumeSuspendedRuns?: boolean;
}

export class ZoryaWorker {
  readonly workerId: string;
  readonly client: ZoryaClient;
  readonly runner: WorkflowRunner;
  private readonly config: ZoryaWorkerConfig;
  private readonly byName: Map<string, Workflow<unknown, unknown>>;
  private heartbeatHandle?: ReturnType<typeof setInterval>;
  private sleepScanner?: SleepScanner;
  private started = false;

  constructor(config: ZoryaWorkerConfig) {
    this.config = config;
    this.client = config.client;
    this.workerId = config.workerId ?? crypto.randomUUID();
    this.byName = new Map(config.workflows.map((w) => [w.name, w]));
    this.runner = createWorkflowRunner({ storage: config.client.storage });
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;

    await this.client.advertise(this.workerId, this.config.workflows, this.config.sampleInput);
    await this.client.workerRegistry.register({
      workerId: this.workerId,
      capabilities: this.config.capabilities ?? [],
      concurrency: this.config.concurrency ?? 10,
      metadata: {
        ...(this.config.metadata ?? {}),
        workflowNames: this.config.workflows.map((w) => w.name),
      },
    });

    const hbMs = this.config.heartbeatIntervalMs ?? 5_000;
    this.heartbeatHandle = setInterval(() => {
      this.client.workerRegistry.heartbeat(this.workerId).catch(() => {
        // Transient errors: worker stays up and retries next interval.
      });
    }, hbMs);

    if (this.config.resumeSuspendedRuns !== false) {
      this.sleepScanner = createSleepScanner({
        storage: this.client.storage,
        runner: this.runner,
        scanIntervalMs: 2_000,
        resolveWorkflow: (name) => this.byName.get(name),
      });
      void this.sleepScanner.start();
    }
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    if (this.heartbeatHandle !== undefined) clearInterval(this.heartbeatHandle);
    this.heartbeatHandle = undefined;
    await this.sleepScanner?.stop();
    await this.client.workerRegistry.deregister(this.workerId).catch(() => {});
    await this.client.unadvertise(this.workerId).catch(() => {});
  }

  /**
   * Run one workflow instance using this worker's runner. Storage writes
   * go over the wire so server dashboards see progress live. Returns the
   * terminal workflow result (or rejects on failure, same semantics as
   * `runner.run`).
   */
  run(params: {
    workflow: string | Workflow<unknown, unknown>;
    workflowId?: string;
    input?: unknown;
  }): Promise<unknown> {
    const def =
      typeof params.workflow === "string" ? this.byName.get(params.workflow) : params.workflow;
    if (!def) {
      return Promise.reject(new Error(`Unknown workflow "${String(params.workflow)}"`));
    }
    const id = params.workflowId ?? `${def.name}-${Date.now().toString(36)}-${randomSuffix()}`;
    return this.runner.run({ workflow: def, workflowId: id, input: params.input });
  }
}

function randomSuffix(): string {
  return Math.random().toString(36).slice(2, 8);
}
