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
  InProcessStepExecutor,
  type FairnessPolicy,
  type SleepScanner,
  type StepTask,
  type Workflow,
  type WorkflowRunner,
} from "@promin/workflow";
import type { ZoryaClient } from "./zorya-client.ts";
import { WorkerControlSocket } from "./worker-control-socket.ts";

export interface ZoryaWorkerConfig {
  client: ZoryaClient;
  /** Workflow definitions this worker will execute. Advertised on start. */
  workflows: ReadonlyArray<Workflow<unknown, unknown>>;
  /** Stable worker id. Default: random UUID. */
  workerId?: string;
  /** Capability tags — server routes tasks whose `needs ⊆ capabilities`. */
  capabilities?: readonly string[];
  /** Max concurrent runs — advisory metadata today. Default 10. */
  concurrency?: number;
  /** Heartbeat interval in ms. Default 5_000. */
  heartbeatIntervalMs?: number;
  /** Optional sample-input lookup for dashboard trigger forms. */
  sampleInput?: (workflowName: string) => unknown;
  /**
   * Application version, surfaced on the Workers page so operators can
   * spot stragglers during a rolling deploy.
   */
  version?: string;
  /**
   * Free-form labels surfaced on the Workers page. Use for routing
   * ("region=us-east"), ownership ("team=payments"), deployment tags, etc.
   */
  labels?: Record<string, string>;
  /**
   * Namespaces this worker serves. Servers running multi-tenant can scope
   * task dispatch to matching workers.
   */
  namespaces?: readonly string[];
  /**
   * Raw metadata — merged with auto-detected fields (hostname, pid,
   * runtime). Anything passed here overrides the auto-detected defaults.
   */
  metadata?: Record<string, unknown>;
  /**
   * When true (default), the worker starts a SleepScanner so ctx.sleep
   * inside journaled steps actually resumes.
   */
  resumeSuspendedRuns?: boolean;
  /**
   * When true (default), the worker polls the server for pending
   * workflow-start requests (e.g. dashboard "Trigger" button) and runs
   * any whose workflowName is in this worker's advertised list. Disable
   * for workers that should never act on dashboard-issued starts.
   */
  pollWorkflowStarts?:
    | boolean
    | {
        /** Poll interval in ms. Default 1_000. */
        intervalMs?: number;
        /** Max claims per poll. Default 10. */
        limit?: number;
      };
  /**
   * Dispatch mode:
   * - `"workflow"` (default) — claim whole workflow runs from the server's
   *   workflow-start queue and execute the entire orchestration locally.
   *   Pairs with a server in default (non-coordinator) mode.
   * - `"step"` — claim individual step tasks from the server's step queue
   *   and execute one step body per claim. Pairs with a server running
   *   `coordination: { enabled: true }`. The coordinator owns the workflow
   *   state machine and dispatches ready steps; this worker only runs the
   *   step bodies it claims.
   *
   * Step-mode workers do NOT poll the workflow-start queue and do NOT run
   * the SleepScanner — both are coordinator-side concerns. Mix-and-match
   * (workflow-mode + step-mode workers against the same server) is not
   * supported in v1: the trigger flow chooses one path.
   */
  mode?: "workflow" | "step";
  /**
   * Step-mode poll cadence + claim batch.
   */
  stepPolling?: {
    /** Poll interval when no tasks available. Default 250ms. */
    intervalMs?: number;
    /** Max claims per poll. Default 5. */
    limit?: number;
    /** Heartbeat cadence per running step. Default 5_000ms. */
    heartbeatMs?: number;
    /** Fairness policy passed to `stepQueue.claim`. Default `"strict-priority"`. */
    fairness?: FairnessPolicy;
  };
  /**
   * Opt in to the persistent worker → server WebSocket. When enabled the
   * worker keeps a long-lived connection to `${url}/ws/worker` and exposes
   * `worker.control` for downstream features:
   *   - agent event streaming (promin-o8dj)
   *   - workflow query handlers (promin-i0wi)
   *   - server-pushed step dispatch (future)
   *
   * Defaults to `false` for back-compat — workers that don't need any of
   * those features stay HTTP-only. Set to `true` once the server has been
   * upgraded with the matching `/ws/worker` endpoint (Zorya 0.5+).
   */
  controlSocket?:
    | boolean
    | {
        /** Initial reconnect delay. Default 250ms. */
        reconnectDelayMs?: number;
        /** Max reconnect delay. Default 30_000ms. */
        maxReconnectDelayMs?: number;
      };
}

export class ZoryaWorker {
  readonly workerId: string;
  readonly client: ZoryaClient;
  readonly runner: WorkflowRunner;
  /**
   * Persistent WS to the Zorya server. Defined when `config.controlSocket`
   * is truthy. Public so feature integrations (agent streaming, query
   * handlers) can register command handlers + send frames without
   * subclassing the worker.
   */
  readonly control?: WorkerControlSocket;
  private readonly config: ZoryaWorkerConfig;
  /** name → primary Workflow (the one passed in `config.workflows`). */
  private readonly byName: Map<string, Workflow<unknown, unknown>>;
  /**
   * name → (version → Workflow). Includes each primary def's
   * previousVersions. The `versionless` sentinel covers defs built
   * without an explicit `version` (the runtime treats those as
   * "no version mismatch check" rather than version "1").
   */
  private readonly byNameAndVersion: Map<
    string,
    Map<string | typeof VERSIONLESS, Workflow<unknown, unknown>>
  >;
  private heartbeatHandle?: ReturnType<typeof setInterval>;
  private startsPollHandle?: ReturnType<typeof setInterval>;
  private stepPollHandle?: ReturnType<typeof setInterval>;
  private sleepScanner?: SleepScanner;
  private started = false;
  /** taskId → cleanup for an in-flight step heartbeat timer. */
  private readonly inFlightSteps = new Map<string, () => void>();
  // Activity tracked so the dashboard can show what each worker is doing.
  private readonly activeRuns = new Map<string, { workflowName: string; startedAt: number }>();
  private completedCount = 0;
  private failedCount = 0;
  /** Last terminal run outcomes — bounded ring buffer fed into metadata. */
  private readonly recentRuns: Array<{
    workflowId: string;
    workflowName: string;
    status: "completed" | "failed";
    durationMs: number;
    at: string;
  }> = [];
  private readonly RECENT_RUNS_CAP = 20;

  constructor(config: ZoryaWorkerConfig) {
    this.config = config;
    this.client = config.client;
    this.workerId = config.workerId ?? crypto.randomUUID();
    this.byName = new Map(config.workflows.map((w) => [w.name, w]));
    this.byNameAndVersion = buildVersionIndex(config.workflows);
    this.runner = createWorkflowRunner({ storage: config.client.storage });
    if (config.controlSocket) {
      const csCfg = typeof config.controlSocket === "object" ? config.controlSocket : {};
      this.control = new WorkerControlSocket({
        url: this.client.url,
        workerId: this.workerId,
        capabilities: this.config.capabilities,
        apiKey: this.client.apiKey,
        reconnectDelayMs: csCfg.reconnectDelayMs,
        maxReconnectDelayMs: csCfg.maxReconnectDelayMs,
      });
    }
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;

    // Open the persistent control socket first so any feature wiring done
    // before start() (a downstream caller registering a command handler)
    // is in place when the server's first ping arrives.
    this.control?.start();

    await this.client.advertise(this.workerId, this.config.workflows, this.config.sampleInput);
    await this.client.workerRegistry.register({
      workerId: this.workerId,
      capabilities: this.config.capabilities ?? [],
      concurrency: this.config.concurrency ?? 10,
      metadata: this.buildMetadata(),
    });

    const hbMs = this.config.heartbeatIntervalMs ?? 5_000;
    this.heartbeatHandle = setInterval(() => {
      // Re-register rather than plain heartbeat so activity counters +
      // active-run list refresh on every tick. register() is idempotent
      // at the storage level; it replaces the entry.
      this.client.workerRegistry
        .register({
          workerId: this.workerId,
          capabilities: this.config.capabilities ?? [],
          concurrency: this.config.concurrency ?? 10,
          metadata: this.buildMetadata(),
        })
        .catch(() => {
          // Transient errors: worker stays up and retries next interval.
        });
    }, hbMs);

    const stepMode = this.config.mode === "step";

    // SleepScanner is a workflow-mode concern: in step-mode the coordinator
    // owns workflow-level orchestration so the worker shouldn't be poking
    // the same storage with a competing scanner.
    if (!stepMode && this.config.resumeSuspendedRuns !== false) {
      this.sleepScanner = createSleepScanner({
        storage: this.client.storage,
        runner: this.runner,
        scanIntervalMs: 2_000,
        resolveWorkflow: (name) => this.byName.get(name),
      });
      void this.sleepScanner.start();
    }

    if (stepMode) {
      const cfg = this.config.stepPolling ?? {};
      const intervalMs = cfg.intervalMs ?? 250;
      const limit = cfg.limit ?? 5;
      this.stepPollHandle = setInterval(() => {
        void this.drainPendingStepTasks(limit).catch(() => {
          // Transient — next tick retries.
        });
      }, intervalMs);
    } else {
      const poll = this.config.pollWorkflowStarts;
      if (poll !== false) {
        const intervalMs = (typeof poll === "object" && poll?.intervalMs) || 1_000;
        const limit = (typeof poll === "object" && poll?.limit) || 10;
        this.startsPollHandle = setInterval(() => {
          void this.drainPendingStarts(limit).catch(() => {
            // Transient — next tick retries.
          });
        }, intervalMs);
      }
    }
  }

  /**
   * Claim any pending workflow-starts the server has queued for workflows
   * we advertise, run them, and ack each on completion (success or
   * failure — the storage row carries the actual outcome).
   */
  private async drainPendingStarts(limit: number): Promise<void> {
    const specs = this.workflowSpecs();
    if (specs.length === 0) return;
    const claims = await this.client.claimWorkflowStarts({
      workflowSpecs: specs,
      workerId: this.workerId,
      limit,
    });
    for (const claim of claims) {
      const def = this.resolveWorkflow(claim.workflowName, claim.version);
      if (!def) {
        // Server thought we could serve this name+version but our local
        // index disagrees. Ack so we don't loop, but don't pretend we ran.
        await this.client.completeWorkflowStart(claim.id).catch(() => {});
        continue;
      }
      // Fire-and-forget: don't block the poll loop on a long workflow.
      void this.run({ workflow: def, workflowId: claim.workflowId, input: claim.input })
        .catch(() => {
          // Failure is recorded in storage by the runner.
        })
        .finally(() => {
          void this.client.completeWorkflowStart(claim.id).catch(() => {});
        });
    }
  }

  /**
   * (name, versions) tuples this worker advertises for the claim filter.
   * A workflow that was built without an explicit `version` advertises
   * `versions: []` — the runtime skips the version-mismatch check for
   * those, so they can run any pinned-version start.
   */
  private workflowSpecs(): Array<{ name: string; versions: readonly string[] }> {
    const out: Array<{ name: string; versions: readonly string[] }> = [];
    for (const [name, byVersion] of this.byNameAndVersion) {
      const explicit: string[] = [];
      let hasVersionless = false;
      for (const key of byVersion.keys()) {
        if (key === VERSIONLESS) hasVersionless = true;
        else explicit.push(key);
      }
      // Versionless trumps explicit versions: the worker has a def that
      // skips the version check entirely, so the queue can hand it any
      // version-pinned start without mismatch.
      out.push({ name, versions: hasVersionless ? [] : explicit });
    }
    return out;
  }

  /**
   * Pick the right Workflow def for a claim. Prefer an exact version match;
   * fall back to a versionless def, then the primary registration.
   */
  private resolveWorkflow(
    name: string,
    version: string | undefined,
  ): Workflow<unknown, unknown> | undefined {
    const byVersion = this.byNameAndVersion.get(name);
    if (!byVersion) return undefined;
    if (version) {
      const exact = byVersion.get(version);
      if (exact) return exact;
    }
    return byVersion.get(VERSIONLESS) ?? this.byName.get(name);
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    if (this.heartbeatHandle !== undefined) clearInterval(this.heartbeatHandle);
    this.heartbeatHandle = undefined;
    if (this.startsPollHandle !== undefined) clearInterval(this.startsPollHandle);
    this.startsPollHandle = undefined;
    if (this.stepPollHandle !== undefined) clearInterval(this.stepPollHandle);
    this.stepPollHandle = undefined;
    for (const cleanup of this.inFlightSteps.values()) cleanup();
    this.inFlightSteps.clear();
    await this.sleepScanner?.stop();
    await this.control?.stop();
    await this.client.workerRegistry.deregister(this.workerId).catch(() => {});
    await this.client.unadvertise(this.workerId).catch(() => {});
  }

  /**
   * Claim individual step tasks from the server's step queue and execute
   * them locally. Used in `mode: "step"` against a server running with
   * `coordination: { enabled: true }`.
   */
  private async drainPendingStepTasks(limit: number): Promise<void> {
    const fairness = this.config.stepPolling?.fairness ?? "strict-priority";
    const tasks = await this.client.stepQueue.claim({
      capabilities: this.config.capabilities ?? [],
      limit,
      fairness,
      filter: (t) => this.canHandleStep(t),
    });
    for (const task of tasks) {
      void this.executeStepTask(task).catch(() => {
        // Failure already written to storage + queue inside executeStepTask;
        // swallowing here keeps the poll loop alive on transient post-fail
        // errors (RPC blip while writing the failure outcome itself).
      });
    }
  }

  private canHandleStep(task: StepTask): boolean {
    // StepTask doesn't carry workflowName, so we filter by step-name
    // presence across every advertised workflow. Capability subset matching
    // happens server-side via `stepQueue.claim`.
    return this.findWorkflowForStep(task) !== undefined;
  }

  private async executeStepTask(task: StepTask): Promise<void> {
    const def = this.findWorkflowForStep(task);
    if (!def) {
      const error = `No advertised workflow contains step "${task.stepName}" on worker ${this.workerId}`;
      await this.failStepTask(task, error, 0);
      return;
    }

    const heartbeatMs = this.config.stepPolling?.heartbeatMs ?? 5_000;
    const heartbeat = setInterval(() => {
      void this.client.stepQueue.heartbeat({ taskId: task.id }).catch(() => {});
    }, heartbeatMs);
    const cleanup = () => clearInterval(heartbeat);
    this.inFlightSteps.set(task.id, cleanup);

    const startedAt = new Date();
    const startMs = Date.now();
    try {
      const executor = new InProcessStepExecutor(def, { storage: this.client.storage });
      const stepDef = def._definition.steps.find((s) => s.name === task.stepName)!;
      const result = await executor.executeStep({
        workflowId: task.workflowId,
        stepName: task.stepName,
        input: task.input,
        prevResults: task.prevResults,
        attempt: task.attempt,
        needs: stepDef.needs ?? task.needs,
        priority: stepDef.priority ?? task.priority,
        version: def.version,
      });
      const durationMs = Date.now() - startMs;

      if (result.ok) {
        // Write the encoded result to storage first so the coordinator's
        // poll on workflow state sees the step terminal before the queue
        // task is acknowledged. The runner also expects encoded values
        // in storage and decodes them when building prevResults for the
        // next step.
        await this.client.storage.saveStepResult({
          workflowId: task.workflowId,
          stepName: task.stepName,
          result: result.result,
          durationMs,
          startedAt,
          metadata: result.metadata,
        });
        await this.client.stepQueue.complete({
          taskId: task.id,
          result: result.result,
          durationMs,
        });
      } else {
        await this.failStepTask(task, result.error, durationMs, startedAt);
      }
    } catch (err) {
      const durationMs = Date.now() - startMs;
      const tag = (err as { _tag?: string } | undefined)?._tag;
      if (tag === "WorkflowSuspendedError") {
        // Journaled step suspended (sleep / signal). The journal entry was
        // already written to storage by `runJournaledStep`; don't fail or
        // complete the queue task — let its lease expire so it gets
        // requeued when the journal entry resumes. The coordinator's
        // SleepScanner-equivalent (or signal delivery) will trigger the
        // resume.
        return;
      }
      const msg = err instanceof Error ? err.message : String(err);
      await this.failStepTask(task, msg, durationMs, startedAt);
    } finally {
      cleanup();
      this.inFlightSteps.delete(task.id);
    }
  }

  private findWorkflowForStep(task: StepTask): Workflow<unknown, unknown> | undefined {
    // Prefer an exact-version match across all advertised defs that
    // contain this step name; fall back to versionless / primary.
    for (const [, byVersion] of this.byNameAndVersion) {
      if (task.version) {
        const exact = byVersion.get(task.version);
        if (exact && exact._definition.steps.some((s) => s.name === task.stepName)) return exact;
      }
      const versionless = byVersion.get(VERSIONLESS);
      if (versionless && versionless._definition.steps.some((s) => s.name === task.stepName)) {
        return versionless;
      }
    }
    for (const wf of this.byName.values()) {
      if (wf._definition.steps.some((s) => s.name === task.stepName)) return wf;
    }
    return undefined;
  }

  private async failStepTask(
    task: StepTask,
    error: string,
    durationMs: number,
    startedAt: Date = new Date(),
  ): Promise<void> {
    await this.client.storage
      .saveStepFailure({
        workflowId: task.workflowId,
        stepName: task.stepName,
        error,
        durationMs,
        startedAt,
      })
      .catch(() => {});
    await this.client.stepQueue.fail({ taskId: task.id, error, durationMs }).catch(() => {});
  }

  /**
   * Assemble the metadata record sent on register. Combines user-provided
   * fields with auto-detected runtime info (hostname, pid, runtime
   * version). User-supplied metadata wins on key collisions.
   */
  private buildMetadata(): Record<string, unknown> {
    const cfg = this.config;
    const auto: Record<string, unknown> = {
      workflowNames: cfg.workflows.map((w) => w.name),
      workflowVersions: Array.from(
        new Set(cfg.workflows.map((w) => w.version).filter((v): v is string => !!v)),
      ),
      capabilities: cfg.capabilities ?? [],
      concurrency: cfg.concurrency ?? 10,
      startedAt: new Date().toISOString(),
      runtime: detectRuntime(),
      // Live activity counters — refreshed on every register() call from
      // the heartbeat tick. Gives the dashboard "what is this worker doing
      // right now" without attributing runs to workers in storage.
      activeRuns: [...this.activeRuns.entries()].map(([id, rec]) => ({
        workflowId: id,
        workflowName: rec.workflowName,
        startedAt: new Date(rec.startedAt).toISOString(),
      })),
      activeCount: this.activeRuns.size,
      completedCount: this.completedCount,
      failedCount: this.failedCount,
      recentRuns: [...this.recentRuns],
    };
    if (typeof process !== "undefined" && typeof process.pid === "number") {
      auto.pid = process.pid;
    }
    const hostname = tryGetHostname();
    if (hostname) auto.hostname = hostname;
    if (cfg.version) auto.version = cfg.version;
    if (cfg.labels && Object.keys(cfg.labels).length > 0) auto.labels = cfg.labels;
    if (cfg.namespaces && cfg.namespaces.length > 0) auto.namespaces = [...cfg.namespaces];
    return { ...auto, ...(cfg.metadata ?? {}) };
  }

  /**
   * Run one workflow instance using this worker's runner. Storage writes
   * go over the wire so server dashboards see progress live. Tracks the
   * run in `activeRuns` so the dashboard's Workers page shows what each
   * worker is doing right now.
   */
  async run(params: {
    workflow: string | Workflow<unknown, unknown>;
    workflowId?: string;
    input?: unknown;
  }): Promise<unknown> {
    const def =
      typeof params.workflow === "string" ? this.byName.get(params.workflow) : params.workflow;
    if (!def) {
      throw new Error(`Unknown workflow "${String(params.workflow)}"`);
    }
    const id = params.workflowId ?? `${def.name}-${Date.now().toString(36)}-${randomSuffix()}`;
    this.activeRuns.set(id, { workflowName: def.name, startedAt: Date.now() });
    const startedAt = Date.now();
    try {
      const result = await this.runner.run({ workflow: def, workflowId: id, input: params.input });
      this.completedCount += 1;
      this.recordRecent(id, def.name, "completed", Date.now() - startedAt);
      return result;
    } catch (err) {
      this.failedCount += 1;
      this.recordRecent(id, def.name, "failed", Date.now() - startedAt);
      throw err;
    } finally {
      this.activeRuns.delete(id);
    }
  }

  private recordRecent(
    workflowId: string,
    workflowName: string,
    status: "completed" | "failed",
    durationMs: number,
  ): void {
    this.recentRuns.unshift({
      workflowId,
      workflowName,
      status,
      durationMs,
      at: new Date().toISOString(),
    });
    if (this.recentRuns.length > this.RECENT_RUNS_CAP) {
      this.recentRuns.length = this.RECENT_RUNS_CAP;
    }
  }
}

function randomSuffix(): string {
  return Math.random().toString(36).slice(2, 8);
}

/**
 * Sentinel for workflows built without an explicit `version`. The runtime
 * doesn't substitute a default — it simply skips the version-mismatch
 * check — so we keep these in their own bucket rather than pretending
 * they're version "1".
 */
const VERSIONLESS = Symbol("versionless");

/**
 * Walk every advertised workflow + its previousVersions and build a
 * `name → version → def` lookup.
 */
function buildVersionIndex(
  workflows: ReadonlyArray<Workflow<unknown, unknown>>,
): Map<string, Map<string | typeof VERSIONLESS, Workflow<unknown, unknown>>> {
  const out = new Map<string, Map<string | typeof VERSIONLESS, Workflow<unknown, unknown>>>();
  const add = (def: Workflow<unknown, unknown>) => {
    let inner = out.get(def.name);
    if (!inner) {
      inner = new Map();
      out.set(def.name, inner);
    }
    inner.set(def.version ?? VERSIONLESS, def);
  };
  for (const wf of workflows) {
    add(wf);
    // previousVersions live on `_definition` (runtime internals), not on
    // the public Workflow shape — that's where the runner reads them too.
    const prev = wf._definition.previousVersions;
    if (prev) {
      for (const p of prev) add(p);
    }
  }
  return out;
}

function detectRuntime(): string {
  const bunVersion = (globalThis as { Bun?: { version?: string } }).Bun?.version;
  if (bunVersion) return `bun-${bunVersion}`;
  const nodeVersion = (globalThis as { process?: { versions?: { node?: string } } }).process
    ?.versions?.node;
  if (nodeVersion) return `node-${nodeVersion}`;
  return "unknown";
}

function tryGetHostname(): string | undefined {
  try {
    // Dynamic import kept in a try/catch so browser builds that bundle this
    // module don't blow up at load time.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const os = require("node:os") as { hostname?: () => string };
    return os.hostname?.();
  } catch {
    return undefined;
  }
}
