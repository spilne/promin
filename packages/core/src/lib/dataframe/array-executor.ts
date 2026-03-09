// ---------------------------------------------------------------------------
// ArrayExecutor — execute logical plans using plain arrays
// ---------------------------------------------------------------------------

import type { DataFrameExecutor, ExecutionCost } from "./executor.ts";
import type { LogicalPlan, AggFn, WindowFn, RollingFn, CumulativeFn } from "./logical-plan.ts";

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

    case "Window":
      return executeWindow(
        executePlan(plan.input),
        plan.name,
        plan.fn,
        plan.orderBy,
        plan.partitionBy,
        plan.args,
      );

    case "Pivot":
      return executePivot(executePlan(plan.input), plan.index, plan.columns, plan.values, plan.agg);

    case "Unpivot":
      return executeUnpivot(executePlan(plan.input), plan.id, plan.columns);

    case "Explode":
      return executeExplode(executePlan(plan.input), plan.column);

    case "Rolling":
      return executeRolling(
        executePlan(plan.input),
        plan.column,
        plan.window,
        plan.fn,
        plan.outputName,
      );

    case "Cumulative":
      return executeCumulative(executePlan(plan.input), plan.column, plan.fn, plan.outputName);
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

// ---------------------------------------------------------------------------
// Window functions
// ---------------------------------------------------------------------------

function executeWindow(
  rows: unknown[],
  name: string,
  fn: WindowFn,
  orderBy: string,
  partitionBy?: string,
  args?: { offset?: number; n?: number; default?: unknown },
): unknown[] {
  // Partition rows
  const partitions = partitionBy ? groupRowsBy(rows, partitionBy) : [rows];

  const result: unknown[] = [];
  for (const partition of partitions) {
    // Sort within partition
    const sorted = [...partition].sort((a: any, b: any) => {
      if (a[orderBy] < b[orderBy]) return -1;
      if (a[orderBy] > b[orderBy]) return 1;
      return 0;
    });

    for (let i = 0; i < sorted.length; i++) {
      const row = sorted[i] as any;
      let value: unknown;

      switch (fn) {
        case "row_number":
          value = i + 1;
          break;
        case "rank": {
          let rank = 1;
          for (let j = 0; j < i; j++) {
            if ((sorted[j] as any)[orderBy] !== row[orderBy]) rank = j + 1;
          }
          if (i > 0 && (sorted[i - 1] as any)[orderBy] !== row[orderBy]) rank = i + 1;
          value = rank;
          break;
        }
        case "dense_rank": {
          const uniqueVals = [...new Set(sorted.map((r: any) => r[orderBy]))];
          value = uniqueVals.indexOf(row[orderBy]) + 1;
          break;
        }
        case "lag": {
          const offset = args?.offset ?? 1;
          value = i - offset >= 0 ? (sorted[i - offset] as any)[orderBy] : (args?.default ?? null);
          break;
        }
        case "lead": {
          const offset = args?.offset ?? 1;
          value =
            i + offset < sorted.length
              ? (sorted[i + offset] as any)[orderBy]
              : (args?.default ?? null);
          break;
        }
        case "running_total":
        case "sum": {
          let sum = 0;
          for (let j = 0; j <= i; j++) sum += Number((sorted[j] as any)[orderBy] ?? 0);
          value = sum;
          break;
        }
        case "avg": {
          let sum = 0;
          for (let j = 0; j <= i; j++) sum += Number((sorted[j] as any)[orderBy] ?? 0);
          value = sum / (i + 1);
          break;
        }
        case "min": {
          let min = Number((sorted[0] as any)[orderBy]);
          for (let j = 1; j <= i; j++) min = Math.min(min, Number((sorted[j] as any)[orderBy]));
          value = min;
          break;
        }
        case "max": {
          let max = Number((sorted[0] as any)[orderBy]);
          for (let j = 1; j <= i; j++) max = Math.max(max, Number((sorted[j] as any)[orderBy]));
          value = max;
          break;
        }
        case "first":
          value = (sorted[0] as any)[orderBy];
          break;
        case "last":
          value = (sorted[i] as any)[orderBy];
          break;
        case "ntile": {
          const n = args?.n ?? 4;
          value = Math.floor((i * n) / sorted.length) + 1;
          break;
        }
      }

      result.push({ ...row, [name]: value });
    }
  }
  return result;
}

function groupRowsBy(rows: unknown[], column: string): unknown[][] {
  const groups = new Map<unknown, unknown[]>();
  for (const row of rows) {
    const key = (row as any)[column];
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key)!.push(row);
  }
  return [...groups.values()];
}

// ---------------------------------------------------------------------------
// Pivot / Unpivot
// ---------------------------------------------------------------------------

function executePivot(
  rows: unknown[],
  index: string,
  columns: string,
  values: string,
  agg: AggFn,
): unknown[] {
  // Group by index
  const groups = new Map<unknown, Map<unknown, unknown[]>>();
  for (const row of rows) {
    const idx = (row as any)[index];
    const col = (row as any)[columns];
    const val = (row as any)[values];
    if (!groups.has(idx)) groups.set(idx, new Map());
    const colMap = groups.get(idx)!;
    if (!colMap.has(col)) colMap.set(col, []);
    colMap.get(col)!.push(val);
  }

  const result: unknown[] = [];
  for (const [idx, colMap] of groups) {
    const row: Record<string, unknown> = { [index]: idx };
    for (const [col, vals] of colMap) {
      row[String(col)] = computeAgg(
        vals.map((v) => ({ [values]: v })),
        values,
        agg,
      );
    }
    result.push(row);
  }
  return result;
}

function executeUnpivot(rows: unknown[], id: string, columns: string[]): unknown[] {
  const result: unknown[] = [];
  for (const row of rows) {
    for (const col of columns) {
      result.push({
        [id]: (row as any)[id],
        variable: col,
        value: (row as any)[col],
      });
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// Explode
// ---------------------------------------------------------------------------

function executeExplode(rows: unknown[], column: string): unknown[] {
  const result: unknown[] = [];
  for (const row of rows) {
    const arr = (row as any)[column];
    if (Array.isArray(arr)) {
      for (const item of arr) {
        result.push({ ...(row as any), [column]: item });
      }
    } else {
      result.push(row);
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// Rolling windows
// ---------------------------------------------------------------------------

function executeRolling(
  rows: unknown[],
  column: string,
  window: number,
  fn: RollingFn,
  outputName: string,
): unknown[] {
  return rows.map((row, i) => {
    const start = Math.max(0, i - window + 1);
    const windowSlice = rows.slice(start, i + 1).map((r: any) => Number(r[column]));
    let value: number;

    switch (fn) {
      case "mean":
        value = windowSlice.reduce((a, b) => a + b, 0) / windowSlice.length;
        break;
      case "sum":
        value = windowSlice.reduce((a, b) => a + b, 0);
        break;
      case "min":
        value = Math.min(...windowSlice);
        break;
      case "max":
        value = Math.max(...windowSlice);
        break;
      case "std": {
        const mean = windowSlice.reduce((a, b) => a + b, 0) / windowSlice.length;
        const variance = windowSlice.reduce((a, v) => a + (v - mean) ** 2, 0) / windowSlice.length;
        value = Math.sqrt(variance);
        break;
      }
    }

    return { ...(row as any), [outputName]: value };
  });
}

// ---------------------------------------------------------------------------
// Cumulative operations
// ---------------------------------------------------------------------------

function executeCumulative(
  rows: unknown[],
  column: string,
  fn: CumulativeFn,
  outputName: string,
): unknown[] {
  let acc: number | null = null;

  return rows.map((row, i) => {
    const val = Number((row as any)[column]);
    let result: number;

    switch (fn) {
      case "sum":
        acc = (acc ?? 0) + val;
        result = acc;
        break;
      case "prod":
        acc = (acc ?? 1) * val;
        result = acc;
        break;
      case "min":
        acc = acc == null ? val : Math.min(acc, val);
        result = acc;
        break;
      case "max":
        acc = acc == null ? val : Math.max(acc, val);
        result = acc;
        break;
      case "pctChange":
        if (i === 0 || acc == null) {
          result = 0;
          acc = val;
        } else {
          result = acc === 0 ? 0 : (val - acc) / acc;
          acc = val;
        }
        break;
    }

    return { ...(row as any), [outputName]: result };
  });
}
