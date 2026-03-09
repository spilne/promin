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
  supports(plan: LogicalPlan): boolean;
  estimateCost(plan: LogicalPlan): ExecutionCost;
}
