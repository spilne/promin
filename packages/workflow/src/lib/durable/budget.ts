// ---------------------------------------------------------------------------
// Budgets — per-run / per-step token and cost caps.
//
// A step body reports what it spent with `yield* ctx.reportUsage({...})`.
// Each report is a journal entry, so replay and crash recovery re-read it
// instead of counting it twice. When the step settles, the engine stores its
// total in the step row's metadata (`STEP_USAGE_METADATA_KEY`), so a run's
// usage is derived from stored rows on any backend: no column, no migration.
// ---------------------------------------------------------------------------

import { WorkflowBudgetExceededError } from "./durable-pipeline-error.ts";
import type { StepState } from "./workflow-state.ts";

/** What a step reports for one model call or other metered action. Omitted fields count as 0. */
export interface UsageReport {
  readonly inputTokens?: number;
  readonly outputTokens?: number;
  readonly costUsd?: number;
}

/** Accumulated usage. `totalTokens` is `inputTokens + outputTokens`. */
export interface UsageTotals {
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly totalTokens: number;
  readonly costUsd: number;
}

/** Caps for one scope. A field left unset is unlimited. */
export interface StepBudget {
  readonly maxTokens?: number;
  readonly maxCostUsd?: number;
}

/**
 * Budget of a workflow: `maxTokens` / `maxCostUsd` cap the whole run (all
 * steps together); `perStep` caps every step; `steps[name]` overrides
 * `perStep` for one step (per field).
 */
export interface WorkflowBudget extends StepBudget {
  readonly perStep?: StepBudget;
  readonly steps?: Readonly<Record<string, StepBudget>>;
}

/** `StepState.metadata` key holding a step's `UsageTotals`. */
export const STEP_USAGE_METADATA_KEY = "usage";

/** Journal activity name of a `ctx.reportUsage` entry. */
export const USAGE_ACTIVITY_NAME = "__usage";

export const ZERO_USAGE: UsageTotals = {
  inputTokens: 0,
  outputTokens: 0,
  totalTokens: 0,
  costUsd: 0,
};

/** Validated report: each given field must be a finite number >= 0. */
export function normalizeUsageReport(report: UsageReport): Required<UsageReport> {
  const field = (name: keyof UsageReport): number => {
    const value = report[name] ?? 0;
    if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
      throw new RangeError(`ctx.reportUsage: ${name} must be a finite number >= 0, got ${value}`);
    }
    return value;
  };
  return {
    inputTokens: field("inputTokens"),
    outputTokens: field("outputTokens"),
    costUsd: field("costUsd"),
  };
}

export function addUsage(a: UsageTotals, b: UsageReport): UsageTotals {
  const inputTokens = a.inputTokens + (b.inputTokens ?? 0);
  const outputTokens = a.outputTokens + (b.outputTokens ?? 0);
  return {
    inputTokens,
    outputTokens,
    totalTokens: inputTokens + outputTokens,
    costUsd: a.costUsd + (b.costUsd ?? 0),
  };
}

/** The usage stored on a step row, if it has any. */
export function stepUsage(step: Pick<StepState, "metadata">): UsageTotals | undefined {
  const raw = step.metadata?.[STEP_USAGE_METADATA_KEY];
  if (typeof raw !== "object" || raw === null) return undefined;
  const u = raw as Partial<UsageTotals>;
  if (typeof u.inputTokens !== "number" || typeof u.outputTokens !== "number") return undefined;
  return addUsage(ZERO_USAGE, {
    inputTokens: u.inputTokens,
    outputTokens: u.outputTokens,
    costUsd: typeof u.costUsd === "number" ? u.costUsd : 0,
  });
}

/** Sum of the usage stored on `steps`, or `undefined` when no step reported any. */
export function runUsage(steps: Readonly<Record<string, StepState>>): UsageTotals | undefined {
  let total: UsageTotals | undefined;
  for (const step of Object.values(steps)) {
    const u = stepUsage(step);
    if (u !== undefined) total = addUsage(total ?? ZERO_USAGE, u);
  }
  return total;
}

/** Effective caps of `stepName`: `steps[name]` over `perStep`, per field. */
export function stepLimits(params: { budget: WorkflowBudget; stepName: string }): StepBudget {
  const { budget, stepName } = params;
  const base = budget.perStep;
  const own = budget.steps?.[stepName];
  const maxTokens = own?.maxTokens ?? base?.maxTokens;
  const maxCostUsd = own?.maxCostUsd ?? base?.maxCostUsd;
  return {
    ...(maxTokens !== undefined && { maxTokens }),
    ...(maxCostUsd !== undefined && { maxCostUsd }),
  };
}

/**
 * The error for the first cap `used` crosses (tokens before cost), or
 * `undefined`. A cap is crossed when usage is strictly above it.
 */
export function exceededLimit(params: {
  workflowId: string;
  scope: "run" | "step";
  limits: StepBudget;
  used: UsageTotals;
  stepName?: string;
}): WorkflowBudgetExceededError | undefined {
  const { workflowId, scope, limits, used, stepName } = params;
  const checks = [
    ["maxTokens", limits.maxTokens, used.totalTokens, "tokens"],
    ["maxCostUsd", limits.maxCostUsd, used.costUsd, "USD"],
  ] as const;
  for (const [limit, max, spent, unit] of checks) {
    if (max === undefined || spent <= max) continue;
    const subject = scope === "step" ? `Step "${stepName}"` : `Workflow "${workflowId}"`;
    return new WorkflowBudgetExceededError({
      workflowId,
      scope,
      limit,
      max,
      used: spent,
      ...(stepName !== undefined && scope === "step" && { stepName }),
      message: `${subject} exceeded its ${limit} budget: used ${spent} ${unit}, max ${max}`,
    });
  }
  return undefined;
}

/** Step cap first, then the run cap, for a step that has spent `step` of a run that has spent `run`. */
export function checkBudget(params: {
  workflowId: string;
  budget: WorkflowBudget;
  stepName: string;
  step: UsageTotals;
  run: UsageTotals;
}): WorkflowBudgetExceededError | undefined {
  const { workflowId, budget, stepName } = params;
  return (
    exceededLimit({
      workflowId,
      scope: "step",
      limits: stepLimits({ budget, stepName }),
      used: params.step,
      stepName,
    }) ?? exceededLimit({ workflowId, scope: "run", limits: budget, used: params.run })
  );
}

/** Running total of one step body run. Fresh per run of the body, so a replay re-sums its journal. */
export interface StepUsageMeter {
  readonly total: UsageTotals;
  /** Add `report`, then throw `WorkflowBudgetExceededError` if a cap is crossed. */
  add(report: UsageReport): UsageTotals;
}

export function createStepUsageMeter(params: {
  workflowId: string;
  stepName: string;
  budget?: WorkflowBudget;
  /** Usage stored by the run's other steps when this one started. */
  priorRun?: UsageTotals;
}): StepUsageMeter {
  let total = ZERO_USAGE;
  return {
    get total() {
      return total;
    },
    add(report) {
      total = addUsage(total, report);
      if (params.budget !== undefined) {
        const error = checkBudget({
          workflowId: params.workflowId,
          budget: params.budget,
          stepName: params.stepName,
          step: total,
          run: addUsage(params.priorRun ?? ZERO_USAGE, total),
        });
        if (error !== undefined) throw error;
      }
      return total;
    },
  };
}
