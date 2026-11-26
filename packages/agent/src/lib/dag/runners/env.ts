import type { ActivityYield, JournaledContext, WallClock } from "@promin/workflow";
import type { BudgetUsage, TurnBudget } from "../../budget/budget.ts";
import type { CreateDagWorkflowConfig, DurableDagInput } from "../durable-executor.ts";
import type { DagNode } from "../types.ts";

/** What every runner needs from the DAG run it belongs to. */
export interface NodeRunEnv {
  readonly ctx: JournaledContext<DurableDagInput, unknown>;
  readonly input: DurableDagInput;
  readonly config: CreateDagWorkflowConfig;
  readonly clock: WallClock;
  /** Aborts when the run aborts. */
  readonly runSignal: AbortSignal;
}

export type AttemptOutcome =
  | {
      readonly ok: true;
      readonly output: unknown;
      readonly spend: BudgetUsage;
      readonly childRuns?: number;
      readonly attempts?: number;
    }
  | {
      readonly ok: false;
      readonly error: string;
      readonly retryable: boolean;
      readonly spend: BudgetUsage;
      readonly childRuns?: number;
      readonly attempts?: number;
    };

export interface AttemptParams<N extends DagNode = DagNode> {
  readonly node: N;
  readonly nodeInput: Record<string, unknown>;
  /** Names the child run and prefixes this attempt's journal entries. */
  readonly key: string;
  readonly budget?: TurnBudget;
  readonly signal: AbortSignal;
  /** Allow this attempt to create its child workflow. */
  readonly claimChildRun?: () => Generator<ActivityYield, boolean, any>;
  /** Descendant-run allowance handed to a sub-DAG child. */
  readonly childRunsAllowed?: number;
}

export type RunAttempt<N extends DagNode> = (
  env: NodeRunEnv,
  params: AttemptParams<N>,
) => Generator<ActivityYield, AttemptOutcome, any>;

/** The stable key for a plain node attempt. */
export function nodeKey(nodeId: string): string {
  return `node-${nodeId}`;
}

/** The child run id for one node attempt. */
export function childRunId(params: { readonly runId: string; readonly key: string }): string {
  return `${params.runId}.${params.key}`;
}
