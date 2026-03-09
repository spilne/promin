// ---------------------------------------------------------------------------
// ArrayExecutor — execute logical plans using plain arrays
// ---------------------------------------------------------------------------

import type { DataFrameExecutor, ExecutionCost } from "./executor.ts";
import type { LogicalPlan, AggFn } from "./logical-plan.ts";

export class ArrayExecutor implements DataFrameExecutor {
  async execute<T>(plan: LogicalPlan): Promise<T[]> {
    return executePlan(plan) as T[];
  }

  supports(_plan: LogicalPlan): boolean {
    return true; // Array executor supports all operations
  }

  estimateCost(plan: LogicalPlan): ExecutionCost {
    const rows = estimateRows(plan);
    return { ms: rows * 0.001, memory: rows * 200 };
  }
}

function estimateRows(plan: LogicalPlan): number {
  switch (plan._tag) {
    case "Source":
      return plan.data.length;
    case "Filter":
      return estimateRows(plan.input) * 0.5;
    case "Limit":
      return Math.min(plan.n, estimateRows(plan.input));
    case "GroupBy":
      return estimateRows(plan.input) * 0.1;
    case "Join":
      return estimateRows(plan.left) * estimateRows(plan.right) * 0.5;
    default:
      return "input" in plan ? estimateRows((plan as any).input) : 1000;
  }
}

function executePlan(plan: LogicalPlan): unknown[] {
  switch (plan._tag) {
    case "Source":
      return plan.data;

    case "Filter":
      return executePlan(plan.input).filter(plan.fn);

    case "Map":
      return executePlan(plan.input).map(plan.fn);

    case "Select": {
      const cols = plan.columns;
      return executePlan(plan.input).map((row: any) => {
        const result: Record<string, unknown> = {};
        for (const col of cols) result[col] = row[col];
        return result;
      });
    }

    case "Drop": {
      const dropCols = new Set(plan.columns);
      return executePlan(plan.input).map((row: any) => {
        const result: Record<string, unknown> = {};
        for (const key of Object.keys(row)) {
          if (!dropCols.has(key)) result[key] = row[key];
        }
        return result;
      });
    }

    case "Rename": {
      const mapping = plan.mapping;
      return executePlan(plan.input).map((row: any) => {
        const result: Record<string, unknown> = {};
        for (const [key, value] of Object.entries(row)) {
          result[mapping[key] ?? key] = value;
        }
        return result;
      });
    }

    case "WithColumn":
      return executePlan(plan.input).map((row: any) => ({
        ...row,
        [plan.name]: plan.fn(row),
      }));

    case "Sort": {
      const rows = [...executePlan(plan.input)];
      const key = plan.by;
      const mult = plan.order === "desc" ? -1 : 1;
      return rows.sort((a: any, b: any) => {
        const va = a[key];
        const vb = b[key];
        if (va < vb) return -1 * mult;
        if (va > vb) return 1 * mult;
        return 0;
      });
    }

    case "Limit":
      return executePlan(plan.input).slice(0, plan.n);

    case "Offset":
      return executePlan(plan.input).slice(plan.n);

    case "Slice":
      return executePlan(plan.input).slice(plan.start, plan.end);

    case "Distinct": {
      const rows = executePlan(plan.input);
      if (plan.by) {
        const seen = new Set<unknown>();
        const result: unknown[] = [];
        const iter = plan.keep === "last" ? [...rows].reverse() : rows;
        for (const row of iter) {
          const key = (row as any)[plan.by];
          if (!seen.has(key)) {
            seen.add(key);
            result.push(row);
          }
        }
        return plan.keep === "last" ? result.reverse() : result;
      }
      const seen = new Set<string>();
      return rows.filter((row) => {
        const key = JSON.stringify(row);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    case "GroupBy": {
      const rows = executePlan(plan.input);
      const groups = new Map<string, unknown[]>();

      for (const row of rows) {
        const key = plan.columns.map((c) => (row as any)[c]).join("\0");
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key)!.push(row);
      }

      const result: unknown[] = [];
      for (const [, groupRows] of groups) {
        const first = groupRows[0] as any;
        const agged: Record<string, unknown> = {};

        // Include group key columns
        for (const col of plan.columns) {
          agged[col] = first[col];
        }

        // Compute aggregations
        for (const [col, fn] of Object.entries(plan.aggs)) {
          agged[col] = computeAgg(groupRows, col, fn);
        }

        result.push(agged);
      }
      return result;
    }

    case "Join": {
      const left = executePlan(plan.left);
      const right = executePlan(plan.right);
      return executeJoin(left, right, plan.on, plan.type);
    }
  }
}

function computeAgg(rows: unknown[], column: string, fn: AggFn): unknown {
  const values = rows.map((r: any) => r[column]).filter((v) => v != null);

  switch (fn) {
    case "count":
      return rows.length;
    case "sum":
      return values.reduce((acc: number, v) => acc + Number(v), 0);
    case "avg": {
      if (values.length === 0) return null;
      const sum = values.reduce((acc: number, v) => acc + Number(v), 0);
      return sum / values.length;
    }
    case "min":
      return values.length === 0 ? null : values.reduce((min, v) => (v < min! ? v : min));
    case "max":
      return values.length === 0 ? null : values.reduce((max, v) => (v > max! ? v : max));
    case "first":
      return values[0] ?? null;
    case "last":
      return values[values.length - 1] ?? null;
    case "collect":
      return values;
  }
}

function executeJoin(
  left: unknown[],
  right: unknown[],
  on: string,
  type: "inner" | "left" | "right" | "full" | "semi" | "anti",
): unknown[] {
  // Build right index
  const rightIndex = new Map<unknown, unknown[]>();
  for (const row of right) {
    const key = (row as any)[on];
    if (!rightIndex.has(key)) rightIndex.set(key, []);
    rightIndex.get(key)!.push(row);
  }

  switch (type) {
    case "inner": {
      const result: unknown[] = [];
      for (const l of left) {
        const matches = rightIndex.get((l as any)[on]);
        if (matches) {
          for (const r of matches) {
            result.push({ ...(l as any), ...(r as any) });
          }
        }
      }
      return result;
    }

    case "left": {
      const result: unknown[] = [];
      for (const l of left) {
        const matches = rightIndex.get((l as any)[on]);
        if (matches) {
          for (const r of matches) {
            result.push({ ...(l as any), ...(r as any) });
          }
        } else {
          result.push(l);
        }
      }
      return result;
    }

    case "right": {
      // Build left index and do right join
      const leftIndex = new Map<unknown, unknown[]>();
      for (const row of left) {
        const key = (row as any)[on];
        if (!leftIndex.has(key)) leftIndex.set(key, []);
        leftIndex.get(key)!.push(row);
      }
      const result: unknown[] = [];
      for (const r of right) {
        const matches = leftIndex.get((r as any)[on]);
        if (matches) {
          for (const l of matches) {
            result.push({ ...(l as any), ...(r as any) });
          }
        } else {
          result.push(r);
        }
      }
      return result;
    }

    case "full": {
      const result: unknown[] = [];
      const matchedRight = new Set<number>();
      for (const l of left) {
        const matches = rightIndex.get((l as any)[on]);
        if (matches) {
          for (const r of matches) {
            matchedRight.add(right.indexOf(r));
            result.push({ ...(l as any), ...(r as any) });
          }
        } else {
          result.push(l);
        }
      }
      for (let i = 0; i < right.length; i++) {
        if (!matchedRight.has(i)) result.push(right[i]!);
      }
      return result;
    }

    case "semi": {
      return left.filter((l) => rightIndex.has((l as any)[on]));
    }

    case "anti": {
      return left.filter((l) => !rightIndex.has((l as any)[on]));
    }
  }
}
