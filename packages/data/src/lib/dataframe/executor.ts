// ---------------------------------------------------------------------------
// DataFrameExecutor — pluggable execution backend interface
// ---------------------------------------------------------------------------

import type { LogicalPlan } from "./logical-plan.ts";

export interface ExecutionCost {
  readonly ms: number;
  readonly memory: number;
}

export interface DataFrameExecutor {
  execute<T>(plan: LogicalPlan): Promise<T[]>;
  /** Sync execution — only available on sync backends (e.g. ArrayExecutor). */
  executeSync?<T>(plan: LogicalPlan): T[];
  supports(plan: LogicalPlan): boolean;
  estimateCost(plan: LogicalPlan): ExecutionCost;
}
