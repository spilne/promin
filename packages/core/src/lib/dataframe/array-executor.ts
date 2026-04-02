// ---------------------------------------------------------------------------
// ArrayExecutor — execute logical plans using plain arrays
// ---------------------------------------------------------------------------

import type { DataFrameExecutor, ExecutionCost } from "./executor.ts";
import type { LogicalPlan, AggFn, WindowFn, RollingFn, CumulativeFn } from "./logical-plan.ts";

export class ArrayExecutor implements DataFrameExecutor {
  async execute<T>(plan: LogicalPlan): Promise<T[]> {
    // Resolve any async sources (file-backed Frameables) before sync execution
    const resolved = await resolveAsyncSources(plan);
    return executePlan(resolved) as T[];
  }

  executeSync<T>(plan: LogicalPlan): T[] {
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

/** Walk the plan tree and resolve any Source nodes that have a load() but no data. */
async function resolveAsyncSources(plan: LogicalPlan): Promise<LogicalPlan> {
  if (plan._tag === "Source" && plan.load && plan.data.length === 0) {
    const data = await plan.load();
    return { ...plan, data, load: undefined };
  }
  if ("input" in plan && (plan as any).input) {
    return { ...plan, input: await resolveAsyncSources((plan as any).input) } as LogicalPlan;
  }
  if ("left" in plan && "right" in plan) {
    return {
      ...plan,
      left: await resolveAsyncSources((plan as any).left),
      right: await resolveAsyncSources((plan as any).right),
    } as LogicalPlan;
  }
  if (plan._tag === "Concat") {
    return { ...plan, inputs: await Promise.all(plan.inputs.map(resolveAsyncSources)) };
  }
  return plan;
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
      const rows = executePlan(plan.input);
      const key = plan.by;
      const desc = plan.order === "desc";
      const n = rows.length;
      if (n === 0) return rows;

      // Extract keys once — avoids repeated property access in comparator
      const keys = new Array(n);
      const indices = new Array(n);
      for (let i = 0; i < n; i++) {
        keys[i] = (rows[i] as any)[key];
        indices[i] = i;
      }

      // Numeric keys: subtraction comparator (faster than branching)
      const isNumeric = typeof keys[0] === "number";
      if (isNumeric) {
        if (desc) {
          indices.sort((a: number, b: number) => keys[b] - keys[a]);
        } else {
          indices.sort((a: number, b: number) => keys[a] - keys[b]);
        }
      } else if (desc) {
        indices.sort((a: number, b: number) =>
          keys[a] > keys[b] ? -1 : keys[a] < keys[b] ? 1 : 0,
        );
      } else {
        indices.sort((a: number, b: number) =>
          keys[a] < keys[b] ? -1 : keys[a] > keys[b] ? 1 : 0,
        );
      }

      const result = new Array(n);
      for (let i = 0; i < n; i++) result[i] = rows[indices[i]];
      return result;
    }

    case "Limit": {
      // Optimization: Sort + Limit → top-N selection (O(N) instead of O(N log N))
      const inner = plan.input;
      if (inner._tag === "Sort") {
        const rows = executePlan(inner.input);
        return topN(rows, plan.n, inner.by, inner.order);
      }
      return executePlan(plan.input).slice(0, plan.n);
    }

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
      const aggEntries = Object.entries(plan.aggs);
      const singleGroupCol = plan.columns.length === 1;

      // Incremental aggregation — single pass, no storing of row arrays.
      // Each group accumulates: count, sum, min, max, first, last, collect per agg column.
      const groups = new Map<
        string,
        {
          keyValues: any;
          accs: {
            count: number;
            sum: number;
            min: any;
            max: any;
            first: any;
            last: any;
            collect: any[];
          }[];
        }
      >();

      for (let i = 0; i < rows.length; i++) {
        const row = rows[i] as any;
        // Fast key for single-column groupBy (no array/join)
        const key = singleGroupCol
          ? String(row[plan.columns[0]!])
          : plan.columns.map((c) => row[c]).join("\0");

        let group = groups.get(key);
        if (!group) {
          const keyValues: any = {};
          for (const c of plan.columns) keyValues[c] = row[c];
          group = {
            keyValues,
            accs: aggEntries.map(() => ({
              count: 0,
              sum: 0,
              min: undefined as any,
              max: undefined as any,
              first: undefined as any,
              last: undefined as any,
              collect: [],
            })),
          };
          groups.set(key, group);
        }

        for (let j = 0; j < aggEntries.length; j++) {
          const [col] = aggEntries[j]!;
          const v = row[col];
          const acc = group.accs[j]!;
          acc.count++;
          if (v != null) {
            const num = Number(v);
            if (!Number.isNaN(num)) acc.sum += num;
            if (acc.min === undefined || v < acc.min) acc.min = v;
            if (acc.max === undefined || v > acc.max) acc.max = v;
          }
          if (acc.first === undefined) acc.first = v;
          acc.last = v;
          acc.collect.push(v);
        }
      }

      const result: unknown[] = [];
      for (const group of groups.values()) {
        const agged: Record<string, unknown> = { ...group.keyValues };
        for (let j = 0; j < aggEntries.length; j++) {
          const [col, fn] = aggEntries[j]!;
          const acc = group.accs[j]!;
          switch (fn) {
            case "count":
              agged[col] = acc.count;
              break;
            case "sum":
              agged[col] = acc.sum;
              break;
            case "avg":
              agged[col] = acc.count > 0 ? acc.sum / acc.count : null;
              break;
            case "min":
              agged[col] = acc.min;
              break;
            case "max":
              agged[col] = acc.max;
              break;
            case "first":
              agged[col] = acc.first;
              break;
            case "last":
              agged[col] = acc.last;
              break;
            case "collect":
              agged[col] = acc.collect;
              break;
          }
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

    case "Concat":
      return plan.inputs.flatMap((input) => executePlan(input));

    case "Union": {
      const left = executePlan(plan.left);
      const right = executePlan(plan.right);
      const combined = [...left, ...right];
      // Deduplicate by JSON serialization
      const seen = new Set<string>();
      return combined.filter((row) => {
        const key = JSON.stringify(row);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    case "Reverse":
      return executePlan(plan.input).reverse();
  }
}

/**
 * Top-N selection — O(N) instead of O(N log N) full sort.
 * Uses a bounded sorted array (insertion sort into top-N buffer).
 * At 1M rows with N=10: scans once, maintains a 10-element sorted buffer.
 */
function topN(rows: unknown[], n: number, by: string, order: "asc" | "desc"): unknown[] {
  if (rows.length <= n) {
    const result = [...rows];
    const mult = order === "desc" ? -1 : 1;
    return result.sort((a: any, b: any) => {
      if (a[by] < b[by]) return -1 * mult;
      if (a[by] > b[by]) return 1 * mult;
      return 0;
    });
  }

  const isDesc = order === "desc";
  const top: unknown[] = [];

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i] as any;
    const val = row[by];

    if (top.length < n) {
      // Buffer not full — insert in sorted position
      let pos = top.length;
      for (let j = top.length - 1; j >= 0; j--) {
        const cmp = isDesc ? val > (top[j] as any)[by] : val < (top[j] as any)[by];
        if (cmp) pos = j;
        else break;
      }
      top.splice(pos, 0, row);
    } else {
      // Buffer full — check if this row beats the worst
      const worst = (top[n - 1] as any)[by];
      const beats = isDesc ? val > worst : val < worst;
      if (beats) {
        // Find insertion point
        let pos = n - 1;
        for (let j = n - 2; j >= 0; j--) {
          const cmp = isDesc ? val > (top[j] as any)[by] : val < (top[j] as any)[by];
          if (cmp) pos = j;
          else break;
        }
        top.splice(pos, 0, row);
        top.length = n; // drop the worst
      }
    }
  }

  return top;
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
