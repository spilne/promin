// ---------------------------------------------------------------------------
// LocalWorkflows — in-process execution. The host process is both the brain
// (DAG state machine) and the body (step execution). Used for monoliths,
// tests, and as the fast-path layer in hybrid (Local + Queued/Distributed)
// chains.
//
// Owns:
//   - a WorkflowRunner (createWorkflowRunner or any other) for execution
//   - In-process sleep scanner so suspended runs resume after their
//     wakeAt passes (without this, ctx.sleep is a no-op-on-restart)
//   - Optional one-shot recovery on start (cancel/fail stale, resume
//     orphaned pending / running / compensating runs)
// ---------------------------------------------------------------------------

import type {
  WorkflowVersionRegistry,
  RecoveryStrategy,
  WallClock,
  Workflow,
  WorkflowRunner,
  WorkflowStorage,
} from "@promin/workflow";
import {
  createSignalScanner,
  createSleepScanner,
  type SignalScanner,
  type SleepScanner,
} from "@promin/workflow/distributed";
import { SystemWallClock, recoverWorkflows } from "@promin/workflow";
import { ZoryaWorkflows, type TriggerOptions, type TriggerResult } from "./zorya-workflows.ts";

export interface LocalWorkflowsConfig {
  storage: WorkflowStorage;
  runner: WorkflowRunner;
  /** Workflow definitions keyed by name. */
  definitions: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /** Optional recovery strategy run once on start(). */
  recovery?: RecoveryStrategy;
  /** Sleep scanner cadence (ms). Default 2000. Pass 0 to disable. */
  sleepScanIntervalMs?: number;
  /** Signal scanner cadence (ms). Default 2000. Pass 0 to disable. */
  signalScanIntervalMs?: number;
  /** Optional fallback for workflows this layer doesn't know. */
  fallback?: ZoryaWorkflows;
  /**
   * Optional version registry. When provided, `dispatch()` consults
   * `registry.findActive(name)` first and uses that version's definition
   * instead of the local `definitions[name]` mapping. Falls back to the
   * local mapping when nothing's been promoted (preserves the existing
   * "use the default-export" behaviour for unversioned workflows).
   */
  versionRegistry?: import("@promin/workflow").WorkflowVersionRegistry;
  /** Time source for start-up recovery. Default: `SystemWallClock`. */
  clock?: WallClock;
}

export class LocalWorkflows extends ZoryaWorkflows {
  readonly storage: WorkflowStorage;
  declare readonly definitions: Readonly<Record<string, Workflow<unknown, unknown>>>;
  private readonly runner: WorkflowRunner;
  private readonly recovery?: RecoveryStrategy;
  private readonly sleepScanner?: SleepScanner;
  private readonly signalScanner?: SignalScanner;
  private readonly versionRegistry?: import("@promin/workflow").WorkflowVersionRegistry;
  private readonly clock: WallClock;

  constructor(config: LocalWorkflowsConfig) {
    super({
      definitions: config.definitions,
      ...(config.fallback && { fallback: config.fallback }),
    });
    this.storage = config.storage;
    this.runner = config.runner;
    if (config.recovery !== undefined) this.recovery = config.recovery;
    if (config.versionRegistry !== undefined) this.versionRegistry = config.versionRegistry;
    this.clock = config.clock ?? SystemWallClock;

    const scanIntervalMs = config.sleepScanIntervalMs ?? 2_000;
    if (scanIntervalMs > 0) {
      this.sleepScanner = createSleepScanner({
        storage: this.storage,
        runner: this.runner,
        scanIntervalMs,
        resolveWorkflow: (name) => this.definitions[name],
      });
    }

    // Mirror for delivered signals: an `approve:<id>` or any custom
    // `ctx.signal(...)` wait gets resumed when a signal row matches the
    // suspended step's name. Without this, `storage.deliverSignal` would
    // append the row but the workflow would stay suspended until a caller
    // explicitly completed the journal entry and re-ran it.
    const signalIntervalMs = config.signalScanIntervalMs ?? 2_000;
    if (signalIntervalMs > 0) {
      this.signalScanner = createSignalScanner({
        storage: this.storage,
        runner: this.runner,
        scanIntervalMs: signalIntervalMs,
        resolveWorkflow: (name) => this.definitions[name],
      });
    }
  }

  /**
   * Resolve a workflow name to a definition. Prefers the active version
   * registered in `versionRegistry` when set; otherwise returns the
   * local `definitions[name]` mapping (the existing default-export path).
   */
  private async resolveDefinition(name: string): Promise<Workflow<unknown, unknown> | undefined> {
    if (this.versionRegistry?.findActive) {
      const active = await this.versionRegistry.findActive(name);
      if (active) {
        const resolved = await this.versionRegistry.resolve({ name, version: active.version });
        if (resolved) return resolved;
      }
    }
    return this.definitions[name];
  }

  protected canHandle(name: string): boolean {
    return name in this.definitions;
  }

  protected async dispatch(
    name: string,
    input: unknown,
    opts?: TriggerOptions,
  ): Promise<TriggerResult> {
    const def = await this.resolveDefinition(name);
    if (!def) throw new Error(`LocalWorkflows.dispatch: missing definition for "${name}"`);

    const workflowId = opts?.workflowId ?? crypto.randomUUID();

    // Pre-create the row when the caller wants typed fields
    // (namespace / metadata / runSource) to land — without this they're
    // lost since runner.run's internal createWorkflow doesn't get them.
    if (opts?.namespace || opts?.metadata || opts?.runSource || opts?.workflowType) {
      const result = await this.storage.createWorkflow({
        workflowId,
        workflowName: name,
        input,
        ...(opts.workflowType !== undefined && { workflowType: opts.workflowType }),
        ...(opts.namespace !== undefined && { namespace: opts.namespace }),
        ...(opts.metadata !== undefined && { metadata: opts.metadata }),
        ...(opts.runSource !== undefined && { runSource: opts.runSource }),
        ...(opts.runSourceId !== undefined && { runSourceId: opts.runSourceId }),
        version: opts.version ?? def.version,
      });
      // Cross-boot collision: deterministic ids (e.g. scheduler ticks) can
      // hit a terminal-state row. Reset so this fresh fire reports its own
      // latency instead of inheriting the prior run's timestamps.
      if (!result.created && isTerminal(result.existing.status)) {
        await this.storage.startFreshRun({ workflowId });
      }
    }

    void this.runner.runSafe({ workflow: def, workflowId, input });
    return { workflowId };
  }

  override async rerun(workflowId: string): Promise<void> {
    const state = await this.storage.loadWorkflow(workflowId);
    if (!state) {
      if (this.fallback) return this.fallback.rerun(workflowId);
      throw new Error(`rerun: workflow "${workflowId}" not found`);
    }
    const def = this.definitions[state.workflowName];
    if (!def) {
      if (this.fallback) return this.fallback.rerun(workflowId);
      throw new Error(
        `rerun: no in-process definition for "${state.workflowName}" (run ${workflowId})`,
      );
    }
    await this.storage.startFreshRun({ workflowId });
    void this.runner.runSafe({
      workflow: def,
      workflowId,
      input: state.input,
      force: true,
    });
  }

  protected override async onStart(): Promise<void> {
    if (this.recovery) {
      await this.runRecovery(this.recovery);
    }
    // Sleep scanner runs forever; fire-and-forget so start() returns.
    if (this.sleepScanner) void this.sleepScanner.start();
    if (this.signalScanner) void this.signalScanner.start();
  }

  /**
   * Apply `strategy` with the runner's recovery sweep (`recoverWorkflows`,
   * the one behind `runner.recover`), resolving definitions from this
   * layer's `definitions` instead of a runner registry, so the host doesn't
   * have to also register every workflow on the runner just to enable
   * resume. Stale runs are terminated first; then every pending / running /
   * compensating run nobody is driving is resumed through `runner.runSafe`
   * (keyset-paged `listOrphanedRuns` when the storage has it, at most
   * `resumeRecent({ concurrent })` in flight). Runs whose workflow this
   * layer doesn't define are left alone.
   */
  private async runRecovery(strategy: RecoveryStrategy): Promise<void> {
    await recoverWorkflows({
      strategy,
      storage: this.storage,
      registry: definitionsRegistry(this.definitions),
      clock: this.clock,
      resume: (run) => this.runner.runSafe(run),
    });
  }

  protected override async onStop(): Promise<void> {
    if (this.sleepScanner) await this.sleepScanner.stop();
    if (this.signalScanner) await this.signalScanner.stop();
  }
}

/**
 * Read-only registry view over a name-keyed definition map: every version
 * of a name resolves to its one local definition.
 */
function definitionsRegistry(
  definitions: Readonly<Record<string, Workflow<unknown, unknown>>>,
): WorkflowVersionRegistry {
  const readOnly = async (): Promise<never> => {
    throw new Error("LocalWorkflows recovery registry is read-only");
  };
  return {
    resolve: async ({ name }) => (Object.hasOwn(definitions, name) ? definitions[name] : undefined),
    versions: async (name) => {
      const version = Object.hasOwn(definitions, name) ? definitions[name]!.version : undefined;
      return version !== undefined ? [version] : [];
    },
    latest: async (name) =>
      Object.hasOwn(definitions, name) ? definitions[name]!.version : undefined,
    names: async () => Object.keys(definitions),
    register: readOnly,
    deregister: readOnly,
  };
}

function isTerminal(status: string): boolean {
  return status === "completed" || status === "failed" || status === "tripwire";
}
