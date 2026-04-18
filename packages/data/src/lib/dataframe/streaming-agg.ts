// ---------------------------------------------------------------------------
// Streaming aggregation — accumulate per-group state across chunks
// ---------------------------------------------------------------------------

import type { AggFn } from "./logical-plan.ts";

/** Accumulator state for streaming aggregation per group per agg column. */
export interface GroupAccumulator {
  count: number;
  sum: number;
  min: number;
  max: number;
  first: unknown;
  last: unknown;
  values: unknown[];
  distinct: Set<unknown>;
  sumSq: number;
  freqMap: Map<unknown, number>;
  customAcc: unknown;
}

function resolveAgg(agg: AggFn): Exclude<AggFn, import("./logical-plan.ts").ExprAgg> {
  return typeof agg === "object" && agg._tag === "expr" ? agg.agg : agg;
}

export function createAccumulator(agg: AggFn): GroupAccumulator {
  const resolved = resolveAgg(agg);
  return {
    count: 0,
    sum: 0,
    min: Infinity,
    max: -Infinity,
    first: undefined,
    last: undefined,
    values: [],
    distinct: new Set(),
    sumSq: 0,
    freqMap: new Map(),
    customAcc:
      typeof resolved === "object" && resolved._tag === "custom"
        ? structuredClone(resolved.init)
        : undefined,
  };
}

/** Accumulate a single value into a group accumulator. */
export function accumulate(acc: GroupAccumulator, agg: AggFn, value: unknown): void {
  const resolved = resolveAgg(agg);
  acc.count++;

  if (value != null) {
    const num = Number(value);
    if (!Number.isNaN(num)) {
      acc.sum += num;
      acc.sumSq += num * num;
      if (num < acc.min) acc.min = num;
      if (num > acc.max) acc.max = num;
    }
    acc.distinct.add(value);
  }

  if (acc.first === undefined) acc.first = value;
  acc.last = value;

  if (typeof resolved === "object" && resolved._tag === "custom") {
    acc.customAcc = resolved.accumulate(acc.customAcc, value);
  }

  if (resolved === "collect") {
    acc.values.push(value);
  }

  if (resolved === "median" || resolved === "stddev" || resolved === "variance") {
    if (value != null) {
      const num = Number(value);
      if (!Number.isNaN(num)) acc.values.push(num);
    }
  }

  if (resolved === "mode") {
    if (value != null) {
      acc.freqMap.set(value, (acc.freqMap.get(value) ?? 0) + 1);
    }
  }
}

/** Finalize an accumulator to produce the result value. */
export function finalize(acc: GroupAccumulator, agg: AggFn): unknown {
  const resolved = resolveAgg(agg);
  if (typeof resolved === "object" && resolved._tag === "custom") {
    return resolved.finalize(acc.customAcc);
  }

  switch (resolved) {
    case "sum":
      return acc.sum;
    case "count":
      return acc.count;
    case "avg":
      return acc.count > 0 ? acc.sum / acc.count : null;
    case "min":
      return acc.min === Infinity ? undefined : acc.min;
    case "max":
      return acc.max === -Infinity ? undefined : acc.max;
    case "first":
      return acc.first;
    case "last":
      return acc.last;
    case "collect":
      return acc.values;
    case "median": {
      const nums = (acc.values as number[]).slice().sort((a, b) => a - b);
      if (nums.length === 0) return null;
      const mid = Math.floor(nums.length / 2);
      return nums.length % 2 !== 0 ? nums[mid] : (nums[mid - 1]! + nums[mid]!) / 2;
    }
    case "mode": {
      let maxCount = 0;
      let modeVal: unknown = null;
      for (const [val, count] of acc.freqMap) {
        if (count > maxCount) {
          maxCount = count;
          modeVal = val;
        }
      }
      return modeVal;
    }
    case "stddev": {
      const nums = acc.values as number[];
      if (nums.length < 2) return null;
      const mean = nums.reduce((a: number, b: number) => a + b, 0) / nums.length;
      const variance =
        nums.reduce((a: number, v: number) => a + (v - mean) ** 2, 0) / (nums.length - 1);
      return Math.sqrt(variance);
    }
    case "variance": {
      const nums = acc.values as number[];
      if (nums.length < 2) return null;
      const mean = nums.reduce((a: number, b: number) => a + b, 0) / nums.length;
      return nums.reduce((a: number, v: number) => a + (v - mean) ** 2, 0) / (nums.length - 1);
    }
    case "countDistinct":
      return acc.distinct.size;
    default:
      return null;
  }
}
