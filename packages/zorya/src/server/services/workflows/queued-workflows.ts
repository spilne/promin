// ---------------------------------------------------------------------------
// QueuedWorkflows — worker-driven runs. This process doesn't run workflow
// code: it creates the run row and queues a start; a worker that hosts the
// workflow (and the run's version) claims the start and drives the WHOLE
// run with the engine (`createWorkflowRunner`) against the shared, fenced
// storage. Zorya schedules, queues, observes, wakes and recovers runs:
//
//   - wake-ups: leader-elected sleep and signal scanners find runs whose
//     sleep is due or whose signal arrived (a child run's end included) and
//     queue them again for a hosting worker, with the run's stored version
//   - recovery: a leader-elected sweep queues every run nobody drives
//     (pending / running / compensating, no live lock, quiet for a grace
//     period). A run whose start was lost before it reached the queue is
//     one of those, so it is recovered the same way. Runs another worker
//     drives hold a live lock and are never touched.
//   - the start queue keeps one record per run, so a wake-up, a sweep or a
//     retried trigger for a run already queued changes nothing
//   - advertisements expire unless workers refresh them; workers the
//     registry declares dead lose theirs at once
//
// Owns the start queue, the advertisement registry and the worker
// registry; ZoryaServer mounts the worker protocol over them and shows the
// registry on the Workers page.
// ---------------------------------------------------------------------------

import {
  SystemWallClock,
  isCancelledRun,
  latestWorkflowVersion,
  type WallClock,
  type Workflow,
  type WorkflowStorage,
  type WorkflowVersionRegistry,
} from "@promin/workflow";
import {
  InMemoryWorkerRegistry,
  createSignalScanner,
  createSleepScanner,
  scannerLeaderKey,
  type SignalScanner,
  type SleepScanner,
  type WorkerRegistry,
} from "@promin/workflow/distributed";
import { InMemoryWorkflowStartQueue, type WorkflowStartQueue } from "@promin/workflow/distributed";
import {
  InMemoryWorkflowAdvertisementRegistry,
  type WorkflowAdvertisementRegistry,
} from "@promin/workflow/distributed";
import { RunCancelledError, UnknownWorkflowVersionError } from "./errors.ts";
import { assertSharedStores, leaderElectionFor, type ZoryaCluster } from "./cluster.ts";
import { LeaderLoop, forEachOrphanedRun, redeliverChildEndedSignals } from "./run-maintenance.ts";
import {
  ZoryaWorkflows,
  existingRunResult,
  findDefinition,
  resetStepsUnderLock,
  stepsFrom,
  type TriggerOptions,
  type TriggerResult,
} from "./zorya-workflows.ts";

/** Where a background failure of `QueuedWorkflows` came from. */
export type QueuedWorkflowsErrorSource = "sleep-wake" | "signal-wake" | "maintenance";

export interface QueuedWorkflowsErrorEvent {
  readonly source: QueuedWorkflowsErrorSource;
  readonly error: unknown;
  /** The run concerned, when the failure is about one. */
  readonly workflowId?: string;
}

export interface QueuedWorkflowsConfig {
  storage: WorkflowStorage;
  /** Start queue workers claim from. Default: in-memory (single instance only). */
  workflowStarts?: WorkflowStartQueue;
  /** Workers' advertisements. Default: in-memory (single instance only). */
  advertisements?: WorkflowAdvertisementRegistry;
  /**
   * The worker registry workers register and heartbeat into (`/rpc/worker`);
   * the Workers page lists it. Default: in-memory (single instance only).
   */
  workerRegistry?: WorkerRegistry;
  /**
   * Promoted versions: a trigger without a version starts the active
   * (promoted) version when a live worker advertises it, else the newest
   * advertised version.
   */
  versionRegistry?: WorkflowVersionRegistry;
  /**
   * Optional in-process definitions — present in hybrid setups where this
   * layer also knows about local workflows (rare; usually empty).
   */
  definitions?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /**
   * When true, canHandle returns true for any name (optimistic mode): a
   * trigger for a workflow no live worker advertises is queued and waits
   * for one. Default: false (only accept workflows that appear in live
   * advertisements or definitions).
   */
  acceptAny?: boolean;
  /** Optional fallback layer. */
  fallback?: ZoryaWorkflows;
  /**
   * Several Zorya instances share the stores: wake-ups and recovery run on
   * one leader at a time, and every store must be shared (in-memory ones
   * are refused).
   */
  cluster?: ZoryaCluster;
  /** Time source for the scanners and recovery. Default: `SystemWallClock`. */
  clock?: WallClock;
  /** Wake-up scan cadence (ms). `0` disables a scanner. Defaults: 1000 each. */
  wakeups?: { sleepScanIntervalMs?: number; signalScanIntervalMs?: number };
  /**
   * Orphan recovery and housekeeping. `intervalMs` (default 5000) is the
   * sweep cadence; `orphanGraceMs` (default 10 000) leaves runs that
   * changed more recently alone; `workerTimeoutMs` (default 30 000)
   * declares a worker dead (its advertisements are dropped).
   * `false` disables it.
   */
  maintenance?: false | { intervalMs?: number; orphanGraceMs?: number; workerTimeoutMs?: number };
  /** Background failures. Default: `console.error`. */
  onError?: (event: QueuedWorkflowsErrorEvent) => void;
}

/** What one maintenance sweep did. */
export interface QueuedMaintenanceReport {
  /** Orphaned runs found. */
  readonly orphans: number;
  /** Of those, how many were not queued yet and got a new start. */
  readonly requeued: number;
  /** Workers whose advertisements were dropped (dead or expired). */
  readonly expiredWorkers: number;
  /** Lost child-ended wake-ups delivered again. */
  readonly childWakes: number;
  /**
   * Set when the sweep did not run: another instance holds the
   * maintenance leadership (or this one is stopping). The counts are 0.
   */
  readonly skipped?: "not_leader";
}

const DEFAULT_SCAN_MS = 1_000;
const DEFAULT_MAINTENANCE_MS = 5_000;
const DEFAULT_ORPHAN_GRACE_MS = 10_000;
const DEFAULT_WORKER_TIMEOUT_MS = 30_000;
const WORKER_RETENTION_MS = 24 * 60 * 60 * 1000;
/** How often the sweep looks for parked parents whose child-ended wake-up was lost. */
const CHILD_WAKE_CHECK_MS = 30_000;

export class QueuedWorkflows extends ZoryaWorkflows {
  readonly storage: WorkflowStorage;
  readonly workflowStarts: WorkflowStartQueue;
  override readonly advertisements: WorkflowAdvertisementRegistry;
  readonly workerRegistry: WorkerRegistry;
  private readonly acceptAny: boolean;
  private readonly clock: WallClock;
  private readonly onError: (event: QueuedWorkflowsErrorEvent) => void;
  private readonly sleepScanner?: SleepScanner;
  private readonly signalScanner?: SignalScanner;
  private readonly maintenance?: LeaderLoop;
  private readonly orphanGraceMs: number;
  private readonly workerTimeoutMs: number;
  /** The last maintenance sweep's outcome (tests, diagnostics). */
  lastMaintenance?: QueuedMaintenanceReport;
  private lastChildWakeCheck = Number.NEGATIVE_INFINITY;

  constructor(config: QueuedWorkflowsConfig) {
    const advertisements = config.advertisements ?? new InMemoryWorkflowAdvertisementRegistry();
    super({
      advertisements,
      ...(config.versionRegistry && { versionRegistry: config.versionRegistry }),
      ...(config.definitions && { definitions: config.definitions }),
      ...(config.fallback && { fallback: config.fallback }),
      ...(config.cluster && { cluster: config.cluster }),
    });
    this.storage = config.storage;
    this.clock = config.clock ?? SystemWallClock;
    this.workflowStarts =
      config.workflowStarts ?? new InMemoryWorkflowStartQueue({ clock: this.clock });
    this.advertisements = advertisements;
    this.workerRegistry =
      config.workerRegistry ?? new InMemoryWorkerRegistry({ clock: this.clock });
    this.acceptAny = config.acceptAny ?? false;
    this.onError = config.onError ?? defaultOnError;
    if (config.cluster) {
      assertSharedStores({
        owner: "QueuedWorkflows",
        stores: {
          storage: this.storage,
          workflowStarts: this.workflowStarts,
          advertisements: this.advertisements,
          workerRegistry: this.workerRegistry,
          versionRegistry: this.versionRegistry,
          leases: config.cluster.leases,
        },
      });
    }
    this.assertOneCluster();

    const sleepMs = config.wakeups?.sleepScanIntervalMs ?? DEFAULT_SCAN_MS;
    if (sleepMs > 0) {
      this.sleepScanner = createSleepScanner({
        storage: this.storage,
        clock: this.clock,
        scanIntervalMs: sleepMs,
        wake: (run) => this.wake(run),
        leaderElection: leaderElectionFor({
          cluster: config.cluster,
          key: scannerLeaderKey({ scanner: "sleep", namespace: "zorya-worker-runs" }),
          intervalMs: sleepMs,
        }),
        onError: (workflowId, error) =>
          this.report({ source: "sleep-wake", error, ...runIdOf(workflowId) }),
      });
    }
    const signalMs = config.wakeups?.signalScanIntervalMs ?? DEFAULT_SCAN_MS;
    if (signalMs > 0) {
      this.signalScanner = createSignalScanner({
        storage: this.storage,
        clock: this.clock,
        scanIntervalMs: signalMs,
        wake: (run) => this.wake(run),
        leaderElection: leaderElectionFor({
          cluster: config.cluster,
          key: scannerLeaderKey({ scanner: "signal", namespace: "zorya-worker-runs" }),
          intervalMs: signalMs,
        }),
        onError: (workflowId, error) =>
          this.report({ source: "signal-wake", error, ...runIdOf(workflowId) }),
      });
    }

    const maintenance = config.maintenance === false ? undefined : (config.maintenance ?? {});
    this.orphanGraceMs = maintenance?.orphanGraceMs ?? DEFAULT_ORPHAN_GRACE_MS;
    this.workerTimeoutMs = maintenance?.workerTimeoutMs ?? DEFAULT_WORKER_TIMEOUT_MS;
    if (maintenance) {
      const intervalMs = maintenance.intervalMs ?? DEFAULT_MAINTENANCE_MS;
      this.maintenance = new LeaderLoop({
        name: "zorya-worker-runs-maintenance",
        intervalMs,
        clock: this.clock,
        leaderElection: leaderElectionFor({
          cluster: config.cluster,
          key: "worker-runs/maintenance",
          intervalMs,
        }),
        tick: async () => {
          this.lastMaintenance = await this.sweep();
        },
        onError: (error) => this.report({ source: "maintenance", error }),
      });
    }
  }

  protected async canHandle(name: string): Promise<boolean> {
    if (this.acceptAny) return true;
    if (this.definitions && Object.hasOwn(this.definitions, name)) return true;
    const distinct = await this.advertisements.distinct();
    return distinct.some((a) => a.name === name);
  }

  protected async dispatch(
    name: string,
    input: unknown,
    opts?: TriggerOptions,
  ): Promise<TriggerResult> {
    const workflowId = opts?.workflowId ?? crypto.randomUUID();
    const version = await this.resolveVersion({ name, requested: opts?.version });

    const created = await this.storage.createWorkflow({
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
    // An existing run is answered as-is. One still pending may have lost
    // its start (a crash between the create and the enqueue): queue it
    // again — a no-op when its start is still there.
    const existing = existingRunResult({ workflowName: name, workflowId, created });
    if (existing) {
      if (!created.created && created.existing.status === "pending") {
        await this.enqueue({ run: created.existing });
      }
      return existing;
    }
    await this.enqueue({
      run: {
        workflowId,
        workflowName: name,
        input,
        ...(opts?.metadata !== undefined && { metadata: opts.metadata }),
        ...(version !== undefined && { version }),
      },
    });
    return { workflowId };
  }

  override async rerun(workflowId: string): Promise<void> {
    const state = await this.storage.loadWorkflow(workflowId);
    if (!state) {
      if (this.fallback) return this.fallback.rerun(workflowId);
      throw new Error(`rerun: workflow "${workflowId}" not found`);
    }
    await this.storage.startFreshRun({ workflowId, expectedRun: state.run });
    await this.enqueue({ run: state });
  }

  /**
   * Re-drive a worker-driven run from `fromStep`: the step and everything
   * downstream (by the DAG a live worker advertises for the run's version)
   * are reset under the run lock, and the run is queued for a worker that
   * hosts that version. Not a fresh run.
   */
  override async resume(params: { workflowId: string; fromStep: string }): Promise<void> {
    const state = await this.storage.loadWorkflow(params.workflowId);
    if (!state) {
      if (this.fallback) return this.fallback.resume(params);
      throw new Error(`resume: workflow "${params.workflowId}" not found`);
    }
    if (isCancelledRun(state)) throw new RunCancelledError(params.workflowId);
    const advertised = (await this.advertisements.distinct()).find(
      (a) =>
        a.name === state.workflowName &&
        (state.version === undefined || a.version === undefined || a.version === state.version),
    );
    const local = this.definitions
      ? findDefinition(this.definitions, state.workflowName)
      : undefined;
    const steps = advertised?.steps ?? local?._definition.steps;
    if (!steps) {
      if (this.fallback) return this.fallback.resume(params);
      throw new Error(
        `resume: no live worker advertises "${state.workflowName}"` +
          `${state.version !== undefined ? ` version "${state.version}"` : ""}`,
      );
    }
    const stepNames = stepsFrom({
      steps,
      fromStep: params.fromStep,
      workflowName: state.workflowName,
    });
    await resetStepsUnderLock({ storage: this.storage, workflowId: params.workflowId, stepNames });
    await this.enqueue({ run: state });
  }

  /**
   * Run one maintenance sweep now (tests, operators). With maintenance
   * enabled it takes the cluster's maintenance leadership first, and when
   * another instance holds it returns `{ skipped: "not_leader" }` without
   * sweeping, so two instances never sweep at once.
   */
  async maintain(): Promise<QueuedMaintenanceReport> {
    if (this.maintenance && !(await this.maintenance.acquire())) {
      return { orphans: 0, requeued: 0, expiredWorkers: 0, childWakes: 0, skipped: "not_leader" };
    }
    return this.sweep();
  }

  /** One maintenance sweep, under a leadership already held. */
  private async sweep(): Promise<QueuedMaintenanceReport> {
    let requeued = 0;
    const stopped = () => this.maintenance?.stopping ?? false;
    const keepGoing = async () => (this.maintenance ? this.maintenance.stillLeading() : true);
    const orphans = await forEachOrphanedRun({
      storage: this.storage,
      clock: this.clock,
      graceMs: this.orphanGraceMs,
      stopped,
      keepGoing,
      visit: async (run) => {
        if (!(await this.owns(run.workflowName))) return;
        if (await this.enqueue({ run })) requeued++;
      },
    });

    // Lost child-ended wake-ups, every `CHILD_WAKE_CHECK_MS` (a wake-up is
    // rarely lost, and each candidate costs a child load).
    let childWakes = 0;
    const now = this.clock.currentTimeMs();
    if (now - this.lastChildWakeCheck >= CHILD_WAKE_CHECK_MS) {
      this.lastChildWakeCheck = now;
      childWakes = await redeliverChildEndedSignals({ storage: this.storage, stopped, keepGoing });
    }

    const dead = await this.workerRegistry.detectDead(this.workerTimeoutMs);
    for (const worker of dead) await this.advertisements.remove(worker.workerId);
    const expired = await this.advertisements.prune();
    await this.workerRegistry.gc({ retainMs: WORKER_RETENTION_MS });
    return { orphans, requeued, expiredWorkers: dead.length + expired, childWakes };
  }

  protected override async onStart(): Promise<void> {
    if (this.sleepScanner) void this.sleepScanner.start();
    if (this.signalScanner) void this.signalScanner.start();
    this.maintenance?.start();
  }

  protected override async onStop(): Promise<void> {
    await Promise.all([
      this.sleepScanner?.stop(),
      this.signalScanner?.stop(),
      this.maintenance?.stop(),
    ]);
  }

  /** Queue a start for `run`; true when a new record was created. */
  private async enqueue(params: {
    readonly run: {
      readonly workflowId: string;
      readonly workflowName: string;
      readonly input: unknown;
      readonly version?: string;
      readonly metadata?: Record<string, unknown>;
    };
    readonly keepAvailableAt?: boolean;
  }): Promise<boolean> {
    const { run } = params;
    const { created } = await this.workflowStarts.enqueue({
      workflowId: run.workflowId,
      workflowName: run.workflowName,
      input: run.input,
      ...(run.metadata !== undefined && { metadata: run.metadata }),
      ...(run.version !== undefined && { version: run.version }),
      ...(params.keepAvailableAt && { keepAvailableAt: true }),
    });
    return created;
  }

  /** A due or signalled run: queue it for a worker unless another layer drives it. */
  private async wake(run: {
    readonly workflowId: string;
    readonly workflowName: string;
    readonly input: unknown;
    readonly version?: string;
    readonly reason: "sleep" | "signal-timeout" | "signal";
  }): Promise<void> {
    if (!(await this.owns(run.workflowName))) return;
    // A timer due by this instance's clock keeps a worker's hold (it set
    // the delay because the timer isn't due yet by its own); a delivered
    // signal is claimable at once.
    await this.enqueue({ run, keepAvailableAt: run.reason !== "signal" });
  }

  /**
   * True when runs of `name` are this layer's to wake and recover: it is the
   * chain's owner of the name, or no layer handles the name (a run whose
   * workers are all gone right now still waits for one).
   */
  private async owns(name: string): Promise<boolean> {
    const owner = await this.ownerOf(name);
    return owner === undefined || owner === this;
  }

  /**
   * The version a trigger starts. A requested version must be one some
   * live worker advertises (or a local definition's); without one, the
   * promoted version when it is advertised, else the newest advertised
   * version (numeric / semver order), else none.
   */
  private async resolveVersion(params: {
    readonly name: string;
    readonly requested: string | undefined;
  }): Promise<string | undefined> {
    const { name, requested } = params;
    const advertised = (await this.advertisements.distinct())
      .filter((a) => a.name === name && a.version !== undefined)
      .map((a) => a.version as string);
    const local = this.definitions ? findDefinition(this.definitions, name)?.version : undefined;
    const available = local !== undefined ? [...advertised, local] : advertised;
    if (requested !== undefined) {
      if (this.acceptAny || available.includes(requested)) return requested;
      throw new UnknownWorkflowVersionError({ workflowName: name, version: requested, available });
    }
    const active = await this.versionRegistry?.findActive?.(name);
    if (active && available.includes(active.version)) return active.version;
    return latestWorkflowVersion(available);
  }

  private report(event: QueuedWorkflowsErrorEvent): void {
    try {
      this.onError(event);
    } catch {
      // A throwing error hook must not take a background loop down.
    }
  }
}

function runIdOf(workflowId: string): { workflowId?: string } {
  return workflowId.startsWith("(") ? {} : { workflowId };
}

function defaultOnError(event: QueuedWorkflowsErrorEvent): void {
  console.error(
    `[zorya] ${event.source} failed${event.workflowId ? ` (run ${event.workflowId})` : ""}:`,
    event.error,
  );
}
