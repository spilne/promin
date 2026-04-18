// ---------------------------------------------------------------------------
// IncrementalAggregation — maintain running aggregates across batches
//
// ## What is this?
// A stateful aggregate view that you update incrementally as new data arrives.
// Unlike a DataFrame groupBy (which re-computes from scratch), this maintains
// running accumulators that survive across ingest() calls.
//
// ## When to use
// - Dashboard metrics updated by Kafka events
// - Running totals across API request batches
// - Real-time analytics where re-computing from full history is too slow
// - Any append-only data source where you want live aggregates
//
// ## When NOT to use
// - One-shot analytics on a static dataset → use DataFrame.groupBy().agg()
// - Need exact percentiles over full history → use DataFrame (collects all values)
//
// ## State persistence
// By default, accumulators live in memory (lost on restart).
// Pass a StateBackend (Redis, Postgres) to survive restarts and share across workers.
//
// ## Example
// ```ts
// const sales = IncrementalAggregation.create({
//   groupBy: ["region"],
//   agg: { revenue: "sum", orderCount: "count" },
// });
//
// // Batch 1: historical load
// await sales.ingest(historicalData);
//
// // Batch 2+: Kafka events
// kafkaStream.groupWithin(10_000, 60_000).forEach(batch => sales.ingest(batch));
//
// // Query anytime
// const current = await sales.snapshot(); // DataFrame<{ region, revenue, orderCount }>
// ```
// ---------------------------------------------------------------------------

import { DataFrame } from "./dataframe.ts";
import type { AggFn } from "./logical-plan.ts";
import { createAccumulator, accumulate, finalize, type GroupAccumulator } from "./streaming-agg.ts";

/** Serializable accumulator state for persistence. */
interface SerializableAccumulator {
  count: number;
  sum: number;
  min: number;
  max: number;
  first: unknown;
  last: unknown;
  values: unknown[];
  distinct: unknown[];
  sumSq: number;
  freqMap: [unknown, number][];
  customAcc: unknown;
}

function toSerializable(acc: GroupAccumulator): SerializableAccumulator {
  return {
    count: acc.count,
    sum: acc.sum,
    min: acc.min,
    max: acc.max,
    first: acc.first,
    last: acc.last,
    values: acc.values,
    distinct: [...acc.distinct],
    sumSq: acc.sumSq,
    freqMap: [...acc.freqMap.entries()],
    customAcc: acc.customAcc,
  };
}

function fromSerializable(s: SerializableAccumulator): GroupAccumulator {
  return {
    count: s.count,
    sum: s.sum,
    min: s.min,
    max: s.max,
    first: s.first,
    last: s.last,
    values: s.values,
    distinct: new Set(s.distinct),
    sumSq: s.sumSq,
    freqMap: new Map(s.freqMap),
    customAcc: s.customAcc,
  };
}

/** Optional state backend for persisting accumulators across restarts. */
interface AggStateBackend {
  get(key: string): Promise<unknown | undefined>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
  keys(): Promise<string[]>;
}

export interface IncrementalAggregationConfig {
  /** Columns to group by. */
  groupBy: string[];
  /** Aggregation functions per column. */
  agg: Record<string, AggFn>;
  /**
   * Optional state backend for durable accumulators.
   * Without this, state lives in memory (lost on restart).
   * Use RedisStateBackend or PostgresStateBackend for production.
   */
  state?: AggStateBackend;
}

export class IncrementalAggregation {
  private readonly groupCols: string[];
  private readonly aggs: Record<string, AggFn>;
  private readonly state?: AggStateBackend;
  // In-memory accumulators: groupKey → { keyValues, accs per agg column }
  private groups = new Map<
    string,
    { keyValues: Record<string, unknown>; accs: Record<string, GroupAccumulator> }
  >();
  private loaded = false;

  private constructor(config: IncrementalAggregationConfig) {
    this.groupCols = config.groupBy;
    this.aggs = config.agg;
    this.state = config.state;
  }

  static create(config: IncrementalAggregationConfig): IncrementalAggregation {
    return new IncrementalAggregation(config);
  }

  /** Load state from backend (called lazily on first ingest/snapshot). */
  private async loadState(): Promise<void> {
    if (this.loaded || !this.state) {
      this.loaded = true;
      return;
    }
    const keys = await this.state.keys();
    for (const key of keys) {
      const stored = (await this.state.get(key)) as
        | { keyValues: Record<string, unknown>; accs: Record<string, SerializableAccumulator> }
        | undefined;
      if (stored) {
        const accs: Record<string, GroupAccumulator> = {};
        for (const [aggKey, serialized] of Object.entries(stored.accs)) {
          accs[aggKey] = fromSerializable(serialized);
        }
        this.groups.set(key, { keyValues: stored.keyValues, accs });
      }
    }
    this.loaded = true;
  }

  /** Save a group's state to backend. */
  private async saveGroup(key: string): Promise<void> {
    if (!this.state) return;
    const group = this.groups.get(key);
    if (!group) return;
    const serialized: Record<string, SerializableAccumulator> = {};
    for (const [aggKey, acc] of Object.entries(group.accs)) {
      serialized[aggKey] = toSerializable(acc);
    }
    await this.state.put(key, { keyValues: group.keyValues, accs: serialized });
  }

  /**
   * Ingest a batch of rows — updates running accumulators.
   * Can be called repeatedly with new data.
   */
  async ingest(rows: Record<string, unknown>[]): Promise<void> {
    await this.loadState();

    const dirtyKeys = new Set<string>();

    for (const row of rows) {
      const key = this.groupCols.map((c) => String(row[c])).join("\0");

      if (!this.groups.has(key)) {
        const keyValues: Record<string, unknown> = {};
        for (const c of this.groupCols) keyValues[c] = row[c];
        const accs: Record<string, GroupAccumulator> = {};
        for (const [aggKey, aggFn] of Object.entries(this.aggs)) {
          accs[aggKey] = createAccumulator(aggFn);
        }
        this.groups.set(key, { keyValues, accs });
      }

      const group = this.groups.get(key)!;
      for (const [aggKey, aggFn] of Object.entries(this.aggs)) {
        if (typeof aggFn === "object" && aggFn._tag === "expr") {
          if (aggFn.filter && !aggFn.filter.fn(row)) continue;
          accumulate(group.accs[aggKey]!, aggFn, aggFn.expr.fn(row));
        } else {
          accumulate(group.accs[aggKey]!, aggFn, row[aggKey]);
        }
      }
      dirtyKeys.add(key);
    }

    // Persist dirty groups
    for (const key of dirtyKeys) {
      await this.saveGroup(key);
    }
  }

  /**
   * Get current aggregates as a DataFrame.
   * Does not clear state — subsequent ingest() calls continue accumulating.
   */
  async snapshot(): Promise<DataFrame<Record<string, unknown>>> {
    await this.loadState();

    const result: Record<string, unknown>[] = [];
    for (const { keyValues, accs } of this.groups.values()) {
      const row: Record<string, unknown> = { ...keyValues };
      for (const [aggKey, aggFn] of Object.entries(this.aggs)) {
        row[aggKey] = finalize(accs[aggKey]!, aggFn);
      }
      result.push(row);
    }
    return DataFrame.fromArray(result);
  }

  /** Clear all accumulators. Next ingest() starts fresh. */
  async reset(): Promise<void> {
    if (this.state) {
      for (const key of this.groups.keys()) {
        await this.state.delete(key);
      }
    }
    this.groups.clear();
    this.loaded = false;
  }

  /** Number of groups currently tracked. */
  get groupCount(): number {
    return this.groups.size;
  }
}
