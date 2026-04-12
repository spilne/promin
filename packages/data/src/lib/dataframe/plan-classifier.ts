// ---------------------------------------------------------------------------
// Plan classifier — determines if a plan can execute in streaming mode
// ---------------------------------------------------------------------------

import type { LogicalPlan } from "./logical-plan.ts";

export type PlanStreamability = "streamable" | "aggregating" | "materializing";

/**
 * Check if a plan can execute in streaming mode (per-chunk).
 *
 * Streamable plans only contain stateless per-row operations (filter, map,
 * select, etc.) that can be applied to each chunk independently.
 *
 * Materializing plans require all data at once (sort, groupBy, join, etc.)
 * and must fall back to full execution.
 */
export function classifyPlan(plan: LogicalPlan): PlanStreamability {
  switch (plan._tag) {
    // Per-chunk stateless operations — streamable
    case "Source":
      return "streamable";

    case "Filter":
    case "Map":
    case "Select":
    case "Drop":
    case "Rename":
    case "WithColumn":
      return classifyPlan(plan.input);

    // Position-dependent — need global row tracking, not per-chunk safe
    case "Limit":
    case "Offset":
    case "Slice":
    case "FillNull":
      return "materializing";

    // GroupBy with streamable input can use per-chunk aggregation + merge
    case "GroupBy":
      return classifyPlan(plan.input) === "streamable" ? "aggregating" : "materializing";

    // These need all data — materializing
    case "Sort":
    case "Distinct":
    case "Join":
    case "Window":
    case "Pivot":
    case "Unpivot":
    case "Explode":
    case "Rolling":
    case "Cumulative":
    case "Concat":
    case "Union":
    case "Reverse":
      return "materializing";

    default:
      return "materializing";
  }
}
