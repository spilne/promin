// ---------------------------------------------------------------------------
// LocalWorkflows — in-process execution. The host process is both the brain
// (DAG state machine) and the body (step execution). Used for monoliths,
// tests, and as the fast-path layer in hybrid (Local + Queued/Distributed)
// chains.
//
// Owns:
//   - DefaultWorkflowRunner (or any WorkflowRunner) for execution
//   - In-process sleep scanner so suspended runs resume after their
//     wakeAt passes (without this, ctx.sleep is a no-op-on-restart)
//   - Optional one-shot recovery on start (cancel/fail stale, resume
//     orphaned)
// ---------------------------------------------------------------------------

import type {
  RecoveryStrategy,
  SignalScanner,
  SleepScanner,
  Workflow,
  WorkflowRunner,
  WorkflowStorage,
} from "@promin/workflow";
import { DefaultSignalScanner, DefaultSleepScanner } from "@promin/workflow";
import { ZoryaWorkflows, type TriggerOptions, type TriggerResult } from "./zorya-workflows.ts";

/**
 * Local mirror of RecoveryStrategy's internal opts shape (not exported by
 * @promin/workflow). Kept here so runRecovery can read its fields without
 * a structural cast at every site.
 */
interface RecoveryOpts {
  readonly staleThresholdMs: number | undefined;
  readonly staleStatuses: ReadonlyArray<"pending" | "running" | "suspended">;
  readonly staleAction:
    | { readonly kind: "cancel" }
    | { readonly kind: "fail"; readonly error: string };
  readonly resumeRecent: boolean;
  readonly resumeConcurrency: number;
}

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
  versionRegistry?: import("@promin/workflow").IWorkflowVersionRegistry;
}

export class LocalWorkflows extends ZoryaWorkflows {
  readonly storage: WorkflowStorage;
  declare readonly definitions: Readonly<Record<string, Workflow<unknown, unknown>>>;
  private readonly runner: WorkflowRunner;
  private readonly recovery?: RecoveryStrategy;
  private readonly sleepScanner?: SleepScanner;
  private readonly signalScanner?: SignalScanner;
  private readonly versionRegistry?: import("@promin/workflow").IWorkflowVersionRegistry;

  constructor(config: LocalWorkflowsConfig) {
    super({
      definitions: config.definitions,
      ...(config.fallback && { fallback: config.fallback }),
    });
    this.storage = config.storage;
    this.runner = config.runner;
    if (config.recovery !== undefined) this.recovery = config.recovery;
    if (config.versionRegistry !== undefined) this.versionRegistry = config.versionRegistry;

    const scanIntervalMs = config.sleepScanIntervalMs ?? 2_000;
    if (scanIntervalMs > 0) {
      this.sleepScanner = new DefaultSleepScanner({
        storage: this.storage,
        runner: this.runner,
        scanIntervalMs,
        resolveWorkflow: (name, version) => this.resolveDefinition(name, version),
      });
    }

    // Mirror for delivered signals: an `approve:<id>` or any custom
    // `ctx.signal(...)` wait gets resumed when a signal row matches the
    // suspended step's name. Without this, `storage.deliverSignal` would
    // append the row but the workflow would stay suspended until a caller
    // explicitly completed the journal entry and re-ran it.
    const signalIntervalMs = config.signalScanIntervalMs ?? 2_000;
    if (signalIntervalMs > 0) {
      this.signalScanner = new DefaultSignalScanner({
        storage: this.storage,
        runner: this.runner,
        scanIntervalMs: signalIntervalMs,
        resolveWorkflow: (name, version) => this.resolveDefinition(name, version),
      });
    }
  }

  /**
   * Resolve a workflow name to a definition. When a stored version is passed
   * (scanner/recovery path), resolve that exact version. Fresh dispatches
   * prefer the active registry version, then the local definitions map.
   */
  private async resolveDefinition(
    name: string,
    version?: string,
  ): Promise<Workflow<unknown, unknown> | undefined> {
    if (version !== undefined && this.versionRegistry) {
      const resolved = await this.versionRegistry.resolve(name, version);
      if (resolved) return resolved;
    }
    if (this.versionRegistry?.findActive) {
      const active = await this.versionRegistry.findActive(name);
      if (active) {
        const resolved = await this.versionRegistry.resolve(name, active.version);
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
        await this.storage.startFreshRun(workflowId);
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
    await this.storage.startFreshRun(workflowId);
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
   * Recovery using local definitions. Two phases:
   *   1. Stale termination — delegated to `runner.recover` (uses storage
   *      only; no registry needed for the stale phase).
   *   2. Resume recent — done here, walking storage and looking up
   *      definitions on this layer instead of going through the runner's
   *      registry. This way the host doesn't have to also register every
   *      workflow on the runner just to enable resume.
   */
  private async runRecovery(strategy: RecoveryStrategy): Promise<void> {
    const opts = (strategy as unknown as { _opts: RecoveryOpts })._opts;

    // Phase 1 — stale termination via runner.recover with resume disabled.
    if (opts.staleThresholdMs !== undefined) {
      const stalePart = {
        _opts: {
          ...opts,
          resumeRecent: false,
        },
      } as unknown as RecoveryStrategy;
      await this.runner.recover(stalePart);
    }

    // Phase 2 — resume recent using local definitions.
    if (opts.resumeRecent) {
      const PAGE = 200;
      const concurrency = opts.resumeConcurrency ?? 10;
      for (const status of ["pending", "running"] as const) {
        let offset = 0;
        while (true) {
          const page = await this.storage.listWorkflows({ status, limit: PAGE, offset });
          if (page.length === 0) break;
          for (let i = 0; i < page.length; i += concurrency) {
            const batch = page.slice(i, i + concurrency);
            for (const wf of batch) {
              const def = this.definitions[wf.workflowName];
              if (!def) continue; // unknown to this layer; nothing to resume
              void this.runner.runSafe({
                workflow: def,
                workflowId: wf.workflowId,
                input: wf.input,
              });
            }
            if (i + concurrency < page.length) {
              await new Promise<void>((r) => setTimeout(r, 0));
            }
          }
          if (page.length < PAGE) break;
          offset += PAGE;
        }
      }
    }
  }

  protected override async onStop(): Promise<void> {
    if (this.sleepScanner) await this.sleepScanner.stop();
    if (this.signalScanner) await this.signalScanner.stop();
  }
}

function isTerminal(status: string): boolean {
  return status === "completed" || status === "failed" || status === "tripwire";
}
