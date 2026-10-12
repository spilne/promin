// ---------------------------------------------------------------------------
// ZoryaEvals — eval suites, durable eval runs and promotion gates.
//
//   - Suites live in an `EvalSuiteRegistry` (a `VersionedRegistry`): every
//     edit is a new immutable version, promoted like agent recipes.
//   - A run measures one suite version against one agent recipe version
//     (a draft version included). It is a run of the eval-run workflow,
//     defined here on the host's in-process workflows, so it shows in the
//     runs list and survives a restart (orphan recovery resumes it; the
//     cases it finished are not run again). Case results land in the
//     `EvalRunStore` as each case finishes, which is what progress reads.
//   - A gate (per agent, in an `EvalGateRegistry`) lets a version become
//     active only once the gate's suite scored at least `minScore` on it;
//     the agent routes ask `checkGate` before a promote.
//
// The run store is written by the workflow; when the workflow ends another
// way (cancelled, failed, marked failed by an operator, swept by
// retention), the run row still says `running` — `getRun` / `listRuns`
// reconcile it from the workflow's status as they read it.
// ---------------------------------------------------------------------------

import type { Agent, ApprovalDecision, LLMProvider, RegisteredAgent } from "@promin/agent";
import {
  InMemoryEvalGateRegistry,
  agentTarget,
  checkEvalGate,
  resolveEvalOptions,
  resolveEvalSuite,
  runEvalCase,
  totalsOf,
  type EvalCase,
  type EvalCaseResult,
  type EvalDatasetStore,
  type EvalGateRegistry,
  type EvalGateVerdict,
  type EvalRunQuery,
  type EvalRunRecord,
  type EvalRunStore,
  type EvalRunTotals,
  type EvalSuiteInput,
  type EvalSuiteRegistry,
  type EvalTarget,
  type RegisteredEvalGate,
  type RegisteredEvalSuite,
  type ResolvedEvalSuite,
  type StoredEvalRun,
} from "@promin/evals";
import {
  SystemWallClock,
  isCancelledRun,
  type WallClock,
  type Workflow,
  type WorkflowStatusSnapshot,
} from "@promin/workflow";
import type { ZoryaAgents } from "../agents/zorya-agents.ts";
import type { LocalWorkflows } from "../workflows/local-workflows.ts";
import {
  EVAL_RUN_WORKFLOW_NAME,
  createEvalRunWorkflow,
  type EvalRunPlan,
  type EvalRunWorkflowInput,
  type EvalRunWorkflowOutput,
} from "./eval-run-workflow.ts";

/** Default cap on a run's concurrent cases. */
export const DEFAULT_EVAL_MAX_CONCURRENCY = 4;

/** A suite registered at `start()` (a changed body becomes the active version). */
export type EvalSuiteSeed = EvalSuiteInput & { readonly id: string };

export interface ZoryaEvalsConfig {
  /** Eval suites. Pass the SQL backend's `*EvalSuiteRegistry` to keep them. */
  suites: EvalSuiteRegistry;
  /** Run history and progress. */
  runs: EvalRunStore;
  /** Promotion gates by agent id. Default: in memory. */
  gates?: EvalGateRegistry;
  /** Stored datasets suites may name (`{ kind: "stored" }`). */
  datasets?: EvalDatasetStore;
  /** Judge LLMs `llmJudge` scorers name by `judgeRef`. */
  judges?: Readonly<Record<string, LLMProvider>>;
  /**
   * In-process workflows the eval-run workflow is defined on. Runs start,
   * recover and show in the runs list through them — use the instance the
   * server mounts.
   */
  workflows: LocalWorkflows;
  /** The agents service: its registry names versions, its `resolve` builds the agent under test. */
  agents: ZoryaAgents;
  /** Most cases one run evaluates at once; a suite asking for more gets this. Default 4. */
  maxConcurrency?: number;
  /** Suites registered at `start()`. */
  seed?: ReadonlyArray<EvalSuiteSeed>;
  clock?: WallClock;
}

/** Why `startRun` refused. */
export type StartEvalRunRefusal =
  | { readonly error: "suite_not_found"; readonly message: string }
  | { readonly error: "agent_not_found"; readonly message: string }
  | { readonly error: "agent_disabled"; readonly message: string }
  | { readonly error: "invalid_suite"; readonly message: string }
  | { readonly error: "run_id_conflict"; readonly message: string };

/** A gate and how a version fares against it. */
export interface EvalGateCheck {
  readonly gate: RegisteredEvalGate;
  readonly verdict: EvalGateVerdict;
}

export class ZoryaEvals {
  readonly suites: EvalSuiteRegistry;
  readonly runs: EvalRunStore;
  readonly gates: EvalGateRegistry;
  readonly datasets?: EvalDatasetStore;
  readonly workflows: LocalWorkflows;
  readonly agents: ZoryaAgents;
  /** The eval-run workflow, defined on `workflows` under `EVAL_RUN_WORKFLOW_NAME`. */
  readonly workflow: Workflow<EvalRunWorkflowInput, EvalRunWorkflowOutput>;

  private readonly judges?: Readonly<Record<string, LLMProvider>>;
  private readonly maxConcurrency: number;
  private readonly seed: ReadonlyArray<EvalSuiteSeed>;
  private readonly clock: WallClock;
  /** Live aborts of the runs this process drives, by run id. */
  private readonly aborts = new Map<string, AbortController>();
  /** Case attempts begun in this process and not yet run. */
  private readonly attempts = new Set<string>();

  constructor(config: ZoryaEvalsConfig) {
    this.suites = config.suites;
    this.runs = config.runs;
    this.gates = config.gates ?? new InMemoryEvalGateRegistry();
    if (config.datasets) this.datasets = config.datasets;
    if (config.judges) this.judges = config.judges;
    this.workflows = config.workflows;
    this.agents = config.agents;
    this.maxConcurrency = resolveEvalOptions({
      concurrency: config.maxConcurrency ?? DEFAULT_EVAL_MAX_CONCURRENCY,
    }).concurrency;
    this.seed = config.seed ?? [];
    this.clock = config.clock ?? SystemWallClock;
    this.workflow = createEvalRunWorkflow({
      steps: {
        plan: (p) => this.plan(p),
        runCase: (p) => this.runCase(p),
        beginCase: () => this.beginCase(),
        finish: (p) => this.finishRun(p),
        release: (p) => this.release(p),
      },
    });
    // File scans and remote re-syncs keep a gated agent's new versions as drafts.
    config.agents.canAutoPromote = (p) => this.canAutoPromote(p);
    // Internal: runs start through the evals routes only, after their checks.
    config.workflows.define(this.workflow as Workflow<unknown, unknown>, { internal: true });
  }

  async start(): Promise<void> {
    for (const { id, ...suite } of this.seed) {
      await this.suites.register({ id, ...suite, promote: true });
    }
  }

  /**
   * Nothing to stop: a run in flight is left to its workflow, which the
   * next process resumes (aborting it here would fail it instead).
   */
  async stop(): Promise<void> {}

  /**
   * Start a durable run of `suiteId` (its active version unless named)
   * against `agentId` (its active version unless named — any version,
   * drafts included). Answers once the run is under way.
   */
  async startRun(params: {
    readonly suiteId: string;
    readonly suiteVersion?: string;
    readonly agentId: string;
    readonly agentVersion?: string;
    /** Tenant scope cases run in. `resourceId` default: `eval-<runId>`, one per run. */
    readonly scope: { readonly namespaceId: string; readonly resourceId?: string };
    /** Run id; an id that already has a run of the same suite and agent answers with it. */
    readonly runId?: string;
  }): Promise<{ readonly runId: string; readonly existing?: true } | StartEvalRunRefusal> {
    const suite = await this.suites.get({
      id: params.suiteId,
      ...(params.suiteVersion !== undefined && { version: params.suiteVersion }),
    });
    if (!suite) {
      return {
        error: "suite_not_found",
        message: `Eval suite "${params.suiteId}${params.suiteVersion ? `@${params.suiteVersion}` : ""}" not registered.`,
      };
    }
    const recipe = await this.agents.registry.get({
      id: params.agentId,
      ...(params.agentVersion !== undefined && { version: params.agentVersion }),
    });
    if (!recipe) {
      return {
        error: "agent_not_found",
        message: `Agent "${params.agentId}${params.agentVersion ? `@${params.agentVersion}` : ""}" not registered.`,
      };
    }
    if (!recipe.enabled) {
      return { error: "agent_disabled", message: `Agent "${recipe.id}" is disabled.` };
    }
    let cases: EvalCase[];
    let resolved: ResolvedEvalSuite;
    try {
      resolved = this.resolve(suite);
      cases = await collectCases(resolved);
    } catch (err) {
      return { error: "invalid_suite", message: errorMessage(err) };
    }

    const runId = params.runId ?? `eval-${crypto.randomUUID()}`;
    const input: EvalRunWorkflowInput = {
      suiteId: suite.id,
      suiteVersion: suite.version,
      agentId: recipe.id,
      agentVersion: recipe.version,
      scope: {
        namespaceId: params.scope.namespaceId,
        resourceId: params.scope.resourceId ?? `eval-${runId}`,
      },
    };
    const started = await this.workflows.trigger(EVAL_RUN_WORKFLOW_NAME, input, {
      workflowId: runId,
      namespace: input.scope.namespaceId,
      runSource: "api",
      metadata: {
        suiteId: suite.id,
        suiteVersion: suite.version,
        agentId: recipe.id,
        agentVersion: recipe.version,
      },
      internal: true,
    });
    if (started.existing === true) {
      const existing = await this.workflows.storage.loadWorkflow(started.workflowId);
      const stored = existing?.input as Partial<EvalRunWorkflowInput> | undefined;
      if (
        existing?.workflowName !== EVAL_RUN_WORKFLOW_NAME ||
        stored?.suiteId !== input.suiteId ||
        stored.suiteVersion !== input.suiteVersion ||
        stored.agentId !== input.agentId ||
        stored.agentVersion !== input.agentVersion
      ) {
        return {
          error: "run_id_conflict",
          message: `Run "${started.workflowId}" exists for another suite or agent.`,
        };
      }
      return { runId: started.workflowId, existing: true };
    }
    // Recorded here too (the workflow's plan step does it again, idempotently)
    // so progress reads find the run from the moment this answers.
    await this.runs.start(this.startParams({ runId, input, resolved, totalCases: cases.length }));
    return { runId };
  }

  /** A run with its case results, reconciled with its workflow. Null when unknown. */
  async getRun(runId: string): Promise<StoredEvalRun | null> {
    const run = await this.runs.get(runId);
    if (run === null || run.status !== "running") return run;
    return (await this.reconcile(run)) ? this.runs.get(runId) : run;
  }

  /** Runs newest first (no case results), each reconciled with its workflow. */
  async listRuns(query: EvalRunQuery = {}): Promise<EvalRunRecord[]> {
    const runs = await this.runs.list(query);
    let changed = false;
    for (const run of runs) {
      if (run.status === "running" && (await this.reconcile(run))) changed = true;
    }
    return changed ? this.runs.list(query) : runs;
  }

  /**
   * Cancel a run: its workflow is cancelled, its in-flight cases in this
   * process are aborted, and the run is recorded `cancelled`.
   */
  async cancelRun(runId: string): Promise<"cancelled" | "not_found" | "ended"> {
    const outcome = await this.cancelIfRunning(runId);
    if (outcome !== "cancelled") return outcome;
    // Another ending (the run's own finish) may have won the race.
    return (await this.runs.get(runId))?.status === "cancelled" ? "cancelled" : "ended";
  }

  private async cancelIfRunning(runId: string): Promise<"cancelled" | "not_found" | "ended"> {
    const run = await this.runs.get(runId);
    if (run === null) return "not_found";
    if (run.status !== "running") return "ended";
    const status = await this.workflows.storage.loadWorkflowStatus(runId);
    if (status !== null && !isEnded(status)) {
      await this.workflows.storage.cancelWorkflow({ workflowId: runId });
    }
    this.aborts.get(runId)?.abort(new Error("eval run cancelled"));
    this.aborts.delete(runId);
    await this.runs.finish({
      runId,
      status: "cancelled",
      finishedAt: this.clock.currentTimeMs(),
      totals: totalsOf(run.caseResults),
    });
    return "cancelled";
  }

  /** The gate on `agentId` and how `version` fares against it; null when the agent has none. */
  async checkGate(params: {
    readonly agentId: string;
    readonly version: string;
  }): Promise<EvalGateCheck | null> {
    const gate = await this.gates.get({ id: params.agentId });
    if (!gate || !gate.enabled) return null;
    const activeSuite = await this.suites.get({ id: gate.suiteId });
    const verdict = await checkEvalGate({
      gate,
      runs: this.runs,
      agentId: params.agentId,
      version: params.version,
      activeSuiteVersion: activeSuite?.version ?? null,
    });
    return { gate, verdict };
  }

  /**
   * Whether a new, never evaluated version of `id` may become active
   * without review: false while `id` has an enabled promotion gate. The
   * paths that publish without a request to refuse (file scans, remote
   * deployment re-syncs) keep such a version as a draft.
   */
  async canAutoPromote(params: { readonly id: string }): Promise<boolean> {
    const gate = await this.gates.get({ id: params.id });
    return !gate || !gate.enabled;
  }

  // ---- Workflow steps ------------------------------------------------------

  private async plan(params: { runId: string; input: EvalRunWorkflowInput }): Promise<EvalRunPlan> {
    const { runId, input } = params;
    const suite = await this.requireSuite(input);
    const resolved = this.resolve(suite);
    const cases = await collectCases(resolved);
    await this.runs.start(this.startParams({ runId, input, resolved, totalCases: cases.length }));
    return {
      cases,
      concurrency: Math.min(resolved.concurrency ?? 1, this.maxConcurrency),
    };
  }

  private async runCase(params: {
    runId: string;
    input: EvalRunWorkflowInput;
    caseIndex: number;
    evalCase: EvalCase;
    attempt: string;
  }): Promise<EvalCaseResult> {
    const { runId, input } = params;
    const abort = this.abortFor(runId);
    // `attempt` was minted by this case's begin step. Not minted in this
    // process: the case started before a restart and its target may have
    // run (tools with side effects included), so unless the suite says its
    // cases are idempotent it is recorded as interrupted, not run again.
    const freshAttempt = this.attempts.delete(params.attempt);
    // A cancel from another process lands on the workflow, not on our abort.
    const status = await this.workflows.storage.loadWorkflowStatus(runId);
    if (status !== null && isCancelledRun(status)) abort.abort(new Error("eval run cancelled"));
    if (abort.signal.aborted) throw abort.signal.reason;

    const suite = await this.requireSuite(input);
    const resolved = this.resolve(suite);
    const recipe = await this.agents.registry.get({
      id: input.agentId,
      version: input.agentVersion,
    });
    if (!recipe) throw new Error(`agent "${input.agentId}@${input.agentVersion}" is gone`);
    const interrupted = !freshAttempt && suite.idempotentCases !== true;
    const result = await runEvalCase({
      evalCase: params.evalCase,
      target: interrupted
        ? interruptedTarget({ id: recipe.id, version: recipe.version })
        : agentTarget({
            id: recipe.id,
            version: recipe.version,
            agent: () => this.materialize({ recipe, scope: input.scope }),
            invoke: {
              // Nobody can decide an approval inside an eval run.
              onApprovalRequired: refuseApproval,
              ...(suite.allowedTools !== undefined && { allowedTools: suite.allowedTools }),
            },
            clock: this.clock,
          }),
      scorers: resolved.scorers,
      ...(resolved.samplesPerCase !== undefined && { samplesPerCase: resolved.samplesPerCase }),
      ...(resolved.passThreshold !== undefined && { passThreshold: resolved.passThreshold }),
      ...(resolved.caseTimeoutMs !== undefined && { caseTimeoutMs: resolved.caseTimeoutMs }),
      signal: abort.signal,
      clock: this.clock,
    });
    // An aborted case is not a result: the run is being cancelled.
    if (abort.signal.aborted) throw abort.signal.reason;
    await this.runs.recordCase({ runId, caseIndex: params.caseIndex, result });
    return result;
  }

  private beginCase(): string {
    const attempt = crypto.randomUUID();
    this.attempts.add(attempt);
    return attempt;
  }

  private release(params: { runId: string }): void {
    this.aborts.delete(params.runId);
  }

  private async finishRun(params: {
    runId: string;
    caseResults: ReadonlyArray<EvalCaseResult>;
  }): Promise<EvalRunTotals> {
    const totals = totalsOf(params.caseResults);
    await this.runs.finish({
      runId: params.runId,
      status: "completed",
      finishedAt: this.clock.currentTimeMs(),
      totals,
    });
    this.aborts.delete(params.runId);
    return totals;
  }

  // ---- Internals -----------------------------------------------------------

  private resolve(suite: RegisteredEvalSuite): ResolvedEvalSuite {
    // Operator-authored suites must not read files off this host.
    return resolveEvalSuite({
      suite,
      deps: {
        allowJsonl: false,
        ...(this.datasets !== undefined && { datasetStore: this.datasets }),
        ...(this.judges !== undefined && { judges: this.judges }),
      },
    });
  }

  private async requireSuite(input: EvalRunWorkflowInput): Promise<RegisteredEvalSuite> {
    const suite = await this.suites.get({ id: input.suiteId, version: input.suiteVersion });
    if (!suite) throw new Error(`eval suite "${input.suiteId}@${input.suiteVersion}" is gone`);
    return suite;
  }

  private startParams(params: {
    runId: string;
    input: EvalRunWorkflowInput;
    resolved: ResolvedEvalSuite;
    totalCases: number;
  }) {
    const { input } = params;
    return {
      runId: params.runId,
      targetId: input.agentId,
      targetVersion: input.agentVersion,
      datasetId: params.resolved.dataset.id,
      suiteId: input.suiteId,
      suiteVersion: input.suiteVersion,
      samplesPerCase: params.resolved.samplesPerCase ?? 1,
      totalCases: params.totalCases,
      startedAt: this.clock.currentTimeMs(),
      namespaceId: input.scope.namespaceId,
      metadata: { resourceId: input.scope.resourceId },
    };
  }

  private async materialize(params: {
    recipe: RegisteredAgent;
    scope: { namespaceId: string; resourceId: string };
  }): Promise<Agent> {
    const agent = await this.agents.resolve(params.recipe, params.scope);
    return agent.withScope(params.scope);
  }

  private abortFor(runId: string): AbortController {
    let abort = this.aborts.get(runId);
    if (abort === undefined) {
      abort = new AbortController();
      this.aborts.set(runId, abort);
    }
    return abort;
  }

  /**
   * End a `running` run row whose workflow has ended without finishing it
   * (failed, cancelled, marked failed) or is gone. True when it changed.
   */
  private async reconcile(run: EvalRunRecord): Promise<boolean> {
    const status = await this.workflows.storage.loadWorkflowStatus(run.runId);
    if (status !== null && !isEnded(status)) return false;
    if (status?.status === "completed") return false; // its finish step wrote the row
    const cancelled = status !== null && isCancelledRun(status);
    this.aborts.get(run.runId)?.abort(new Error("eval run ended"));
    this.aborts.delete(run.runId);
    const full = await this.runs.get(run.runId);
    await this.runs.finish({
      runId: run.runId,
      status: cancelled ? "cancelled" : "failed",
      finishedAt: this.clock.currentTimeMs(),
      ...(full !== null && { totals: totalsOf(full.caseResults) }),
      ...(!cancelled && {
        error: status === null ? "the run's workflow is gone" : (status.error ?? "the run failed"),
      }),
    });
    return true;
  }
}

/** Every approval-gated tool call in an eval run is refused. */
async function refuseApproval(): Promise<ApprovalDecision> {
  return { approved: false, reason: "requires approval; not available in eval runs" };
}

/** A target for a case a restart cut short: every sample is a failed run. */
function interruptedTarget(params: { id: string; version: string }): EvalTarget {
  return {
    id: params.id,
    version: params.version,
    run: async () => ({
      output: undefined,
      metrics: { latencyMs: 0 },
      error:
        "interrupted: a restart cut the case short; not re-run (the suite's cases are not idempotent)",
    }),
  };
}

function isEnded(status: WorkflowStatusSnapshot): boolean {
  return (
    status.status === "completed" || status.status === "failed" || status.status === "tripwire"
  );
}

async function collectCases(resolved: ResolvedEvalSuite): Promise<EvalCase[]> {
  const cases: EvalCase[] = [];
  for await (const evalCase of resolved.dataset.cases()) cases.push(evalCase);
  return cases;
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
