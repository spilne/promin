// ---------------------------------------------------------------------------
// LocalWorkflows — in-process execution. The host process drives every run
// of the workflows it defines with its own runner. Used for monoliths,
// tests, and as the fast-path layer in hybrid (Local + Queued) chains.
//
// Owns:
//   - a WorkflowRunner (createWorkflowRunner or any other) for execution
//   - sleep and signal scanners that resume suspended runs in-process
//   - periodic orphan recovery: runs nobody drives (no live lock, quiet for
//     a grace period) are resumed here, so a run whose driver died — this
//     process before a restart, or a peer instance — never stays stuck
//   - an optional one-shot startup strategy (e.g. fail runs that are too old)
//
// One resolver picks the definition for a (name, version) everywhere —
// trigger, scanners, recovery and rerun — so a run is always resumed with
// the code of the version it was started on. With a `cluster`, scanners and
// recovery run on one leader instance at a time.
// ---------------------------------------------------------------------------

import type {
  RecoveryStrategy,
  WallClock,
  Workflow,
  WorkflowRunner,
  WorkflowStorage,
  WorkflowVersionRegistry,
} from "@promin/workflow";
import {
  createSignalScanner,
  createSleepScanner,
  scannerLeaderKey,
  type SignalScanner,
  type SleepScanner,
} from "@promin/workflow/distributed";
import { SystemWallClock, isCancelledRun, recoverWorkflows } from "@promin/workflow";
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

/** Where a background failure of `LocalWorkflows` came from. */
export type LocalWorkflowsErrorSource = "sleep-scanner" | "signal-scanner" | "recovery";

export interface LocalWorkflowsErrorEvent {
  readonly source: LocalWorkflowsErrorSource;
  readonly error: unknown;
  readonly workflowId?: string;
}

export interface LocalWorkflowsConfig {
  storage: WorkflowStorage;
  runner: WorkflowRunner;
  /** Workflow definitions keyed by name. */
  definitions: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /** Optional strategy applied once on start (e.g. `failStale`); runs with a live lock are skipped. */
  recovery?: RecoveryStrategy;
  /**
   * Periodic orphan recovery: every `intervalMs` (default 10 000) the
   * leader resumes runs of these workflows that nobody drives and that
   * have not changed for `graceMs` (default 10 000), at most `concurrency`
   * (default 10) at once. `false` disables it.
   */
  orphanRecovery?: false | { intervalMs?: number; graceMs?: number; concurrency?: number };
  /** Sleep scanner cadence (ms). Default 2000. Pass 0 to disable. */
  sleepScanIntervalMs?: number;
  /** Signal scanner cadence (ms). Default 2000. Pass 0 to disable. */
  signalScanIntervalMs?: number;
  /** Optional fallback for workflows this layer doesn't know. */
  fallback?: ZoryaWorkflows;
  /**
   * Optional version registry. A trigger without a version starts the
   * promoted (`findActive`) version; a run of a version the local
   * definition doesn't carry (neither its own nor a `previousVersions`
   * entry) resolves through `registry.resolve`.
   */
  versionRegistry?: WorkflowVersionRegistry;
  /**
   * Several Zorya instances share the stores: scanners and recovery run on
   * one leader at a time, and every store must be shared.
   */
  cluster?: ZoryaCluster;
  /** Time source for scanners and recovery. Default: `SystemWallClock`. */
  clock?: WallClock;
  /** Background failures. Default: `console.error`. */
  onError?: (event: LocalWorkflowsErrorEvent) => void;
}

const DEFAULT_SCAN_MS = 2_000;
const DEFAULT_RECOVERY_MS = 10_000;
const DEFAULT_ORPHAN_GRACE_MS = 10_000;
const DEFAULT_RECOVERY_CONCURRENCY = 10;
/** How often recovery looks for parked parents whose child-ended wake-up was lost. */
const CHILD_WAKE_CHECK_MS = 30_000;

export class LocalWorkflows extends ZoryaWorkflows {
  readonly storage: WorkflowStorage;
  declare readonly definitions: Readonly<Record<string, Workflow<unknown, unknown>>>;
  private readonly runner: WorkflowRunner;
  private readonly recovery?: RecoveryStrategy;
  private readonly sleepScanner?: SleepScanner;
  private readonly signalScanner?: SignalScanner;
  private readonly orphanLoop?: LeaderLoop;
  private readonly orphanGraceMs: number;
  private readonly recoveryConcurrency: number;
  private readonly clock: WallClock;
  private readonly onError: (event: LocalWorkflowsErrorEvent) => void;
  /** Runs resumed by orphan recovery and still in flight. */
  private readonly recovering = new Map<string, Promise<void>>();
  private lastChildWakeCheck = Number.NEGATIVE_INFINITY;

  constructor(config: LocalWorkflowsConfig) {
    super({
      definitions: config.definitions,
      ...(config.versionRegistry && { versionRegistry: config.versionRegistry }),
      ...(config.fallback && { fallback: config.fallback }),
      ...(config.cluster && { cluster: config.cluster }),
    });
    this.storage = config.storage;
    this.runner = config.runner;
    if (config.recovery !== undefined) this.recovery = config.recovery;
    this.clock = config.clock ?? SystemWallClock;
    this.onError = config.onError ?? defaultOnError;
    if (config.cluster) {
      assertSharedStores({
        owner: "LocalWorkflows",
        stores: {
          storage: this.storage,
          versionRegistry: this.versionRegistry,
          leases: config.cluster.leases,
        },
      });
    }
    this.assertOneCluster();
    // Scanners resume only the runs this layer owns: in a chain the same
    // names can reach another layer (a worker-driven layer above or below).
    const resolveWorkflow = async (run: { workflowName: string; version?: string }) =>
      (await this.ownerOf(run.workflowName)) === this ? this.resolveRun(run) : undefined;

    const sleepMs = config.sleepScanIntervalMs ?? DEFAULT_SCAN_MS;
    if (sleepMs > 0) {
      this.sleepScanner = createSleepScanner({
        storage: this.storage,
        runner: this.runner,
        clock: this.clock,
        scanIntervalMs: sleepMs,
        resolveWorkflow,
        leaderElection: leaderElectionFor({
          cluster: config.cluster,
          key: scannerLeaderKey({ scanner: "sleep", namespace: "zorya-local" }),
          intervalMs: sleepMs,
        }),
        onError: (workflowId, error) => this.report({ source: "sleep-scanner", workflowId, error }),
      });
    }

    // Delivered signals (and child-run ends, delivered as reserved
    // signals) only resume a run through this scanner: `deliverSignal`
    // records the value and nothing more.
    const signalMs = config.signalScanIntervalMs ?? DEFAULT_SCAN_MS;
    if (signalMs > 0) {
      this.signalScanner = createSignalScanner({
        storage: this.storage,
        runner: this.runner,
        clock: this.clock,
        scanIntervalMs: signalMs,
        resolveWorkflow,
        leaderElection: leaderElectionFor({
          cluster: config.cluster,
          key: scannerLeaderKey({ scanner: "signal", namespace: "zorya-local" }),
          intervalMs: signalMs,
        }),
        onError: (workflowId, error) =>
          this.report({ source: "signal-scanner", workflowId, error }),
      });
    }

    const orphan = config.orphanRecovery === false ? undefined : (config.orphanRecovery ?? {});
    this.orphanGraceMs = orphan?.graceMs ?? DEFAULT_ORPHAN_GRACE_MS;
    this.recoveryConcurrency = Math.max(1, orphan?.concurrency ?? DEFAULT_RECOVERY_CONCURRENCY);
    if (orphan) {
      const intervalMs = orphan.intervalMs ?? DEFAULT_RECOVERY_MS;
      this.orphanLoop = new LeaderLoop({
        name: "zorya-local-recovery",
        intervalMs,
        clock: this.clock,
        leaderElection: leaderElectionFor({
          cluster: config.cluster,
          key: "local/recovery",
          intervalMs,
        }),
        tick: async () => {
          await this.sweepOrphans();
        },
        onError: (error) => this.report({ source: "recovery", error }),
      });
    }
  }

  protected canHandle(name: string): boolean {
    return Object.hasOwn(this.definitions, name);
  }

  protected async dispatch(
    name: string,
    input: unknown,
    opts?: TriggerOptions,
  ): Promise<TriggerResult> {
    const def =
      opts?.version !== undefined
        ? await this.resolveRun({ workflowName: name, version: opts.version })
        : await this.resolveActive(name);
    if (!def) {
      if (opts?.version !== undefined) {
        throw new UnknownWorkflowVersionError({
          workflowName: name,
          version: opts.version,
          available: localVersions(findDefinition(this.definitions, name)),
        });
      }
      throw new Error(`LocalWorkflows.dispatch: missing definition for "${name}"`);
    }

    const workflowId = opts?.workflowId ?? crypto.randomUUID();
    const version = opts?.version ?? def.version;
    // The definition's own type and metadata, as the runner would record them.
    const workflowType = opts?.workflowType ?? def._definition.type;
    const metadata = opts?.metadata ?? def._definition.metadata;

    // Create the row first, with the typed fields (namespace, metadata,
    // runSource) the runner's own create doesn't take. An id that already
    // exists is answered from storage: a redelivered schedule tick or a
    // retried trigger never runs the body again, and an id of another
    // workflow is a conflict. A fresh run of an ended one is `rerun`.
    const created = await this.storage.createWorkflow({
      workflowId,
      workflowName: name,
      input,
      ...(workflowType !== undefined && { workflowType }),
      ...(opts?.namespace !== undefined && { namespace: opts.namespace }),
      ...(metadata !== undefined && { metadata }),
      ...(opts?.runSource !== undefined && { runSource: opts.runSource }),
      ...(opts?.runSourceId !== undefined && { runSourceId: opts.runSourceId }),
      ...(version !== undefined && { version }),
    });
    const existing = existingRunResult({ workflowName: name, workflowId, created });
    if (existing) return existing;

    void this.runner.runSafe({ workflow: def, workflowId, input });
    return { workflowId };
  }

  override async rerun(workflowId: string): Promise<void> {
    const state = await this.storage.loadWorkflow(workflowId);
    if (!state) {
      if (this.fallback) return this.fallback.rerun(workflowId);
      throw new Error(`rerun: workflow "${workflowId}" not found`);
    }
    const def = await this.resolveRun(state);
    if (!def) {
      if (this.fallback) return this.fallback.rerun(workflowId);
      throw new Error(
        `rerun: no in-process definition for "${state.workflowName}"` +
          `${state.version !== undefined ? ` version "${state.version}"` : ""} (run ${workflowId})`,
      );
    }
    // `force` makes the runner archive the ended run and start a fresh one
    // under the run's lock — one run-number bump, fenced against any other
    // driver. Resolves once the run holds the lock; it goes on in the
    // background.
    await this.runner.start({ workflow: def, workflowId, input: state.input, force: true });
  }

  /**
   * Re-drive a run of these workflows from `fromStep` in this process, with
   * the definition of the run's version: the step and everything
   * downstream are reset under the run lock, then the run continues (not a
   * fresh run). Resolves once the run is handed to the runner.
   */
  override async resume(params: { workflowId: string; fromStep: string }): Promise<void> {
    const state = await this.storage.loadWorkflow(params.workflowId);
    if (!state) throw new Error(`resume: workflow "${params.workflowId}" not found`);
    if (isCancelledRun(state)) throw new RunCancelledError(params.workflowId);
    const def =
      (await this.ownerOf(state.workflowName)) === this ? await this.resolveRun(state) : undefined;
    if (!def) {
      if (this.fallback) return this.fallback.resume(params);
      throw new Error(
        `resume: no in-process definition for "${state.workflowName}"` +
          `${state.version !== undefined ? ` version "${state.version}"` : ""}`,
      );
    }
    const stepNames = stepsFrom({
      steps: def._definition.steps,
      fromStep: params.fromStep,
      workflowName: def.name,
    });
    await resetStepsUnderLock({ storage: this.storage, workflowId: params.workflowId, stepNames });
    void this.runner.runSafe({
      workflow: def,
      workflowId: params.workflowId,
      input: state.input,
      force: true,
    });
  }

  /**
   * Resume every orphaned run of these workflows once (tests, operators).
   * Resolves once each has been handed to the runner; resumed runs go on
   * in the background, at most `concurrency` at once. With orphan recovery
   * enabled it takes the cluster's recovery leadership first, and when
   * another instance holds it returns `{ resumed: 0, skipped: "not_leader" }`
   * without sweeping.
   */
  async recoverOrphans(): Promise<{ resumed: number; skipped?: "not_leader" }> {
    if (this.orphanLoop && !(await this.orphanLoop.acquire())) {
      return { resumed: 0, skipped: "not_leader" };
    }
    return { resumed: await this.sweepOrphans() };
  }

  /** One orphan sweep, under a leadership already held; the runs resumed. */
  private async sweepOrphans(): Promise<number> {
    let resumed = 0;
    const stopped = () => this.orphanLoop?.stopping ?? false;
    const keepGoing = async () => (this.orphanLoop ? this.orphanLoop.stillLeading() : true);
    await forEachOrphanedRun({
      storage: this.storage,
      clock: this.clock,
      graceMs: this.orphanGraceMs,
      stopped,
      keepGoing,
      visit: async (run) => {
        if (this.recovering.has(run.workflowId)) return;
        if ((await this.ownerOf(run.workflowName)) !== this) return;
        const def = await this.resolveRun(run);
        if (!def) return;
        while (this.recovering.size >= this.recoveryConcurrency) {
          await Promise.race(this.recovering.values());
        }
        const done = this.runner
          .runSafe({ workflow: def, workflowId: run.workflowId, input: run.input })
          .then(() => undefined)
          .finally(() => this.recovering.delete(run.workflowId));
        this.recovering.set(run.workflowId, done);
        resumed++;
      },
    });
    // Lost child-ended wake-ups (rarely lost, so not every sweep).
    const now = this.clock.currentTimeMs();
    if (now - this.lastChildWakeCheck >= CHILD_WAKE_CHECK_MS) {
      this.lastChildWakeCheck = now;
      await redeliverChildEndedSignals({ storage: this.storage, stopped, keepGoing });
    }
    return resumed;
  }

  protected override async onStart(): Promise<void> {
    if (this.recovery) {
      // Runs with a live lock are neither terminated nor resumed.
      await recoverWorkflows({
        strategy: this.recovery,
        storage: this.storage,
        registry: this.recoveryRegistry(),
        clock: this.clock,
        resume: (run) => this.runner.runSafe(run),
      });
    }
    if (this.sleepScanner) void this.sleepScanner.start();
    if (this.signalScanner) void this.signalScanner.start();
    this.orphanLoop?.start();
  }

  protected override async onStop(): Promise<void> {
    await Promise.all([
      this.sleepScanner?.stop(),
      this.signalScanner?.stop(),
      this.orphanLoop?.stop(),
    ]);
    await Promise.all(this.recovering.values());
  }

  /** The definition a new run of `name` starts with: the promoted version, else the local one. */
  private async resolveActive(name: string): Promise<Workflow<unknown, unknown> | undefined> {
    if (!this.canHandle(name)) return undefined;
    if (this.versionRegistry?.findActive) {
      const active = await this.versionRegistry.findActive(name);
      if (active) {
        const resolved = await this.resolveRun({ workflowName: name, version: active.version });
        if (resolved) return resolved;
      }
    }
    return findDefinition(this.definitions, name);
  }

  /**
   * The definition that drives a run of `workflowName` at `version`: the
   * local definition when it is that version (or carries it in
   * `previousVersions`, which the runner drains with), else the version
   * registry's. Undefined for workflows this layer doesn't define.
   */
  private async resolveRun(run: {
    readonly workflowName: string;
    readonly version?: string;
  }): Promise<Workflow<unknown, unknown> | undefined> {
    const local = findDefinition(this.definitions, run.workflowName);
    if (!local) return undefined;
    if (run.version === undefined || localVersions(local).includes(run.version)) return local;
    if (local.version === undefined) return local;
    return this.versionRegistry?.resolve({ name: run.workflowName, version: run.version });
  }

  /** Read-only registry view over `resolveRun`, for `recoverWorkflows`. */
  private recoveryRegistry(): WorkflowVersionRegistry {
    const readOnly = async (): Promise<never> => {
      throw new Error("LocalWorkflows recovery registry is read-only");
    };
    return {
      resolve: ({ name, version }) =>
        this.resolveRun({ workflowName: name, ...(version !== undefined && { version }) }),
      versions: async (name) => localVersions(findDefinition(this.definitions, name)),
      latest: async (name) => findDefinition(this.definitions, name)?.version,
      names: async () => Object.keys(this.definitions),
      register: readOnly,
      deregister: readOnly,
    };
  }

  private report(event: LocalWorkflowsErrorEvent): void {
    try {
      this.onError(event);
    } catch {
      // A throwing error hook must not take a background loop down.
    }
  }
}

/** The versions a local definition can drive: its own and its `previousVersions`. */
function localVersions(def: Workflow<unknown, unknown> | undefined): string[] {
  if (!def) return [];
  const out: string[] = [];
  if (def.version !== undefined) out.push(def.version);
  for (const prev of def._definition.previousVersions ?? []) {
    if (prev.version !== undefined) out.push(prev.version);
  }
  return out;
}

function defaultOnError(event: LocalWorkflowsErrorEvent): void {
  // Unknown definitions are expected in hybrid chains: the fallback layer's
  // runs show up in the same storage.
  if (String(event.error).includes("no definition for workflow")) return;
  console.error(
    `[zorya] ${event.source} failed${event.workflowId && !event.workflowId.startsWith("(") ? ` (run ${event.workflowId})` : ""}:`,
    event.error,
  );
}
