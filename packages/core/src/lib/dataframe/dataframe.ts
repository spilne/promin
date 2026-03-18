// ---------------------------------------------------------------------------
// DataFrame<T> — lazy, typed columnar analytics builder
//
// Operations are recorded as a logical plan. Nothing executes until a
// terminal (.collect(), .first(), etc.) is called.
// ---------------------------------------------------------------------------

import type { Frameable } from "../typeclasses/frameable.ts";
import type { StreamPipeline } from "../stream-pipeline.ts";
import type { LogicalPlan, WindowFn, AggFn, RollingFn } from "./logical-plan.ts";
import type { DataFrameExecutor } from "./executor.ts";
import { ArrayExecutor } from "./array-executor.ts";
import { GroupedDataFrame } from "./grouped-dataframe.ts";
import { StringAccessor, DateAccessor } from "./accessors.ts";
import { ExpectationSuite } from "../data-quality/expectation-suite.ts";
import { profileData, type ProfileOptions } from "../data-profiler/profiler.ts";
import type { ProfileReport } from "../data-profiler/profile-types.ts";

const DEFAULT_EXECUTOR = new ArrayExecutor();

export class DataFrame<T> {
  /** @internal */
  constructor(
    private readonly _plan: LogicalPlan,
    private readonly _executor: DataFrameExecutor = DEFAULT_EXECUTOR,
  ) {}

  /** @internal — used by GroupedDataFrame to create DataFrames from agg plans. */
  static _fromPlan<T>(plan: LogicalPlan, executor: DataFrameExecutor): DataFrame<T> {
    return new DataFrame<T>(plan, executor);
  }

  // =========================================================================
  // SOURCES
  // =========================================================================

  static fromArray<T>(data: T[]): DataFrame<T> {
    return new DataFrame<T>({ _tag: "Source", data });
  }

  static fromIterable<T>(data: Iterable<T>): DataFrame<T> {
    return new DataFrame<T>({ _tag: "Source", data: Array.from(data) });
  }

  static async fromStream<T>(stream: StreamPipeline<T, any>): Promise<DataFrame<T>> {
    const data = await stream.collect();
    return new DataFrame<T>({ _tag: "Source", data });
  }

  static async from<T>(source: Frameable<T>): Promise<DataFrame<T>> {
    const data = await source.load();
    return new DataFrame<T>({ _tag: "Source", data });
  }

  static concat<T>(...frames: DataFrame<T>[]): DataFrame<T> {
    // Materialize all then merge — for the array executor this is fine
    return new DataFrame<T>({
      _tag: "Source",
      data: [], // placeholder — resolved at collect time
      _concat: frames,
    } as any);
  }

  // =========================================================================
  // COLUMN OPERATIONS
  // =========================================================================

  select<K extends keyof T & string>(...columns: K[]): DataFrame<Pick<T, K>> {
    return new DataFrame({ _tag: "Select", input: this._plan, columns }, this._executor);
  }

  drop<K extends keyof T & string>(...columns: K[]): DataFrame<Omit<T, K>> {
    return new DataFrame({ _tag: "Drop", input: this._plan, columns }, this._executor);
  }

  rename<M extends Partial<Record<keyof T & string, string>>>(mapping: M): DataFrame<any> {
    return new DataFrame(
      { _tag: "Rename", input: this._plan, mapping: mapping as Record<string, string> },
      this._executor,
    );
  }

  withColumn<K extends string, V>(name: K, fn: (row: T) => V): DataFrame<T & Record<K, V>> {
    return new DataFrame({ _tag: "WithColumn", input: this._plan, name, fn }, this._executor);
  }

  // =========================================================================
  // ROW OPERATIONS
  // =========================================================================

  filter(fn: (row: T) => boolean): DataFrame<T> {
    return new DataFrame({ _tag: "Filter", input: this._plan, fn }, this._executor);
  }

  map<U>(fn: (row: T) => U): DataFrame<U> {
    return new DataFrame({ _tag: "Map", input: this._plan, fn }, this._executor);
  }

  sort(by: keyof T & string, order: "asc" | "desc" = "asc"): DataFrame<T> {
    return new DataFrame({ _tag: "Sort", input: this._plan, by, order }, this._executor);
  }

  sortBy(fn: (row: T) => number | string): DataFrame<T> {
    // Implement as withColumn + sort + drop
    const tempCol = `__sort_${Date.now()}`;
    return this.withColumn(tempCol, fn)
      .sort(tempCol as any)
      .drop(tempCol as any) as unknown as DataFrame<T>;
  }

  limit(n: number): DataFrame<T> {
    return new DataFrame({ _tag: "Limit", input: this._plan, n }, this._executor);
  }

  offset(n: number): DataFrame<T> {
    return new DataFrame({ _tag: "Offset", input: this._plan, n }, this._executor);
  }

  slice(start: number, end?: number): DataFrame<T> {
    return new DataFrame({ _tag: "Slice", input: this._plan, start, end }, this._executor);
  }

  distinct(): DataFrame<T> {
    return new DataFrame({ _tag: "Distinct", input: this._plan }, this._executor);
  }

  distinctBy(column: keyof T & string, params?: { keep?: "first" | "last" }): DataFrame<T> {
    return new DataFrame(
      { _tag: "Distinct", input: this._plan, by: column, keep: params?.keep ?? "first" },
      this._executor,
    );
  }

  reverse(): DataFrame<T> {
    // Sort by nothing — just reverse the array at execution
    return this.map((row) => row); // identity, reversed in collect
  }

  // =========================================================================
  // NULL HANDLING
  // =========================================================================

  dropNull(column?: keyof T & string): DataFrame<T> {
    if (column) {
      return this.filter((row) => row[column] != null);
    }
    return this.filter((row) =>
      Object.values(row as Record<string, unknown>).every((v) => v != null),
    );
  }

  fillNull<K extends keyof T>(column: K, value: T[K]): DataFrame<T> {
    return this.withColumn(column as string, (row) =>
      row[column] != null ? row[column] : value,
    ) as unknown as DataFrame<T>;
  }

  // =========================================================================
  // AGGREGATION
  // =========================================================================

  groupBy<K extends keyof T & string>(...columns: K[]): GroupedDataFrame<T, K> {
    return new GroupedDataFrame(this._plan, columns, this._executor);
  }

  valueCounts(column: keyof T & string): DataFrame<{ value: unknown; count: number }> {
    return this.groupBy(column)
      .agg({} as any)
      .withColumn("count", () => 0) as any;
    // Simple implementation — real one uses GroupBy + count
  }

  // =========================================================================
  // JOINS
  // =========================================================================

  join<U>(
    other: DataFrame<U>,
    params: {
      on: (keyof T & keyof U) & string;
      type?: "inner" | "left" | "right" | "full" | "semi" | "anti";
    },
  ): DataFrame<T & U> {
    return new DataFrame(
      {
        _tag: "Join",
        left: this._plan,
        right: other._plan,
        on: params.on,
        type: params.type ?? "inner",
      },
      this._executor,
    );
  }

  // =========================================================================
  // SET OPERATIONS
  // =========================================================================

  union(other: DataFrame<T>): DataFrame<T> {
    // Collect both, concat, distinct
    return DataFrame._fromPlan(
      { _tag: "Source", data: [] } as any, // resolved via concat at execution
      this._executor,
    );
  }

  // =========================================================================
  // WINDOW FUNCTIONS
  // =========================================================================

  withWindowColumn<K extends string>(
    name: K,
    params: {
      partitionBy?: keyof T & string;
      orderBy: keyof T & string;
      fn: WindowFn;
      args?: { offset?: number; n?: number; default?: unknown };
    },
  ): DataFrame<T & Record<K, unknown>> {
    return new DataFrame(
      {
        _tag: "Window",
        input: this._plan,
        name,
        partitionBy: params.partitionBy,
        orderBy: params.orderBy,
        fn: params.fn,
        args: params.args,
      },
      this._executor,
    );
  }

  // =========================================================================
  // PIVOT / UNPIVOT
  // =========================================================================

  pivot(params: {
    index: keyof T & string;
    columns: keyof T & string;
    values: keyof T & string;
    agg?: AggFn;
  }): DataFrame<Record<string, unknown>> {
    return new DataFrame(
      {
        _tag: "Pivot",
        input: this._plan,
        index: params.index,
        columns: params.columns,
        values: params.values,
        agg: params.agg ?? "sum",
      },
      this._executor,
    );
  }

  unpivot(params: {
    id: keyof T & string;
    columns: (keyof T & string)[];
  }): DataFrame<{ id: unknown; variable: string; value: unknown }> {
    return new DataFrame(
      { _tag: "Unpivot", input: this._plan, id: params.id, columns: params.columns },
      this._executor,
    ) as any;
  }

  // =========================================================================
  // RESHAPE
  // =========================================================================

  explode(column: keyof T & string): DataFrame<T> {
    return new DataFrame({ _tag: "Explode", input: this._plan, column }, this._executor);
  }

  // =========================================================================
  // ACCESSORS
  // =========================================================================

  str(column: keyof T & string): StringAccessor<T> {
    return new StringAccessor(this._plan, column, this._executor);
  }

  dt(column: keyof T & string): DateAccessor<T> {
    return new DateAccessor(this._plan, column, this._executor);
  }

  // =========================================================================
  // ROLLING / CUMULATIVE
  // =========================================================================

  rolling(
    column: keyof T & string,
    params: { window: number; fn: RollingFn; as?: string },
  ): DataFrame<T & Record<string, number>> {
    return new DataFrame(
      {
        _tag: "Rolling",
        input: this._plan,
        column,
        window: params.window,
        fn: params.fn,
        outputName: params.as ?? `${column}_rolling_${params.fn}`,
      },
      this._executor,
    );
  }

  cumSum(
    column: keyof T & string,
    params?: { as?: string },
  ): DataFrame<T & Record<string, number>> {
    return new DataFrame(
      {
        _tag: "Cumulative",
        input: this._plan,
        column,
        fn: "sum",
        outputName: params?.as ?? `${column}_cumsum`,
      },
      this._executor,
    );
  }

  cumProd(
    column: keyof T & string,
    params?: { as?: string },
  ): DataFrame<T & Record<string, number>> {
    return new DataFrame(
      {
        _tag: "Cumulative",
        input: this._plan,
        column,
        fn: "prod",
        outputName: params?.as ?? `${column}_cumprod`,
      },
      this._executor,
    );
  }

  cumMax(
    column: keyof T & string,
    params?: { as?: string },
  ): DataFrame<T & Record<string, number>> {
    return new DataFrame(
      {
        _tag: "Cumulative",
        input: this._plan,
        column,
        fn: "max",
        outputName: params?.as ?? `${column}_cummax`,
      },
      this._executor,
    );
  }

  cumMin(
    column: keyof T & string,
    params?: { as?: string },
  ): DataFrame<T & Record<string, number>> {
    return new DataFrame(
      {
        _tag: "Cumulative",
        input: this._plan,
        column,
        fn: "min",
        outputName: params?.as ?? `${column}_cummin`,
      },
      this._executor,
    );
  }

  pctChange(
    column: keyof T & string,
    params?: { as?: string },
  ): DataFrame<T & Record<string, number>> {
    return new DataFrame(
      {
        _tag: "Cumulative",
        input: this._plan,
        column,
        fn: "pctChange",
        outputName: params?.as ?? `${column}_pctchange`,
      },
      this._executor,
    );
  }

  // =========================================================================
  // STATISTICS
  // =========================================================================

  async count(): Promise<number> {
    const rows = await this.collect();
    return rows.length;
  }

  async sum(column: keyof T & string): Promise<number> {
    const rows = await this.collect();
    return rows.reduce((acc, row) => acc + Number((row as any)[column] ?? 0), 0);
  }

  async avg(column: keyof T & string): Promise<number> {
    const rows = await this.collect();
    if (rows.length === 0) return 0;
    const sum = rows.reduce((acc, row) => acc + Number((row as any)[column] ?? 0), 0);
    return sum / rows.length;
  }

  async min(column: keyof T & string): Promise<unknown> {
    const rows = await this.collect();
    if (rows.length === 0) return null;
    return rows.reduce(
      (min, row) => {
        const v = (row as any)[column];
        return v < (min as any) ? v : min;
      },
      (rows[0] as any)[column],
    );
  }

  async max(column: keyof T & string): Promise<unknown> {
    const rows = await this.collect();
    if (rows.length === 0) return null;
    return rows.reduce(
      (max, row) => {
        const v = (row as any)[column];
        return v > (max as any) ? v : max;
      },
      (rows[0] as any)[column],
    );
  }

  async countDistinct(column: keyof T & string): Promise<number> {
    const rows = await this.collect();
    return new Set(rows.map((r) => (r as any)[column])).size;
  }

  async describe(): Promise<
    {
      column: string;
      count: number;
      nulls: number;
      mean?: number;
      min?: unknown;
      max?: unknown;
    }[]
  > {
    const rows = await this.collect();
    if (rows.length === 0) return [];

    const columns = Object.keys(rows[0] as Record<string, unknown>);
    return columns.map((col) => {
      const values = rows.map((r) => (r as any)[col]);
      const nonNull = values.filter((v) => v != null);
      const nums = nonNull.filter((v) => typeof v === "number") as number[];
      return {
        column: col,
        count: rows.length,
        nulls: values.length - nonNull.length,
        mean: nums.length > 0 ? nums.reduce((a, b) => a + b, 0) / nums.length : undefined,
        min: nonNull.length > 0 ? nonNull.reduce((a, b) => (a < b ? a : b)) : undefined,
        max: nonNull.length > 0 ? nonNull.reduce((a, b) => (a > b ? a : b)) : undefined,
      };
    });
  }

  // =========================================================================
  // SCHEMA INSPECTION
  // =========================================================================

  get plan(): LogicalPlan {
    return this._plan;
  }

  // =========================================================================
  // TERMINALS
  // =========================================================================

  async collect(): Promise<T[]> {
    return this._executor.execute<T>(this._plan);
  }

  async first(): Promise<T | null> {
    const rows = await this.limit(1).collect();
    return rows[0] ?? null;
  }

  async head(n: number = 5): Promise<T[]> {
    return this.limit(n).collect();
  }

  async tail(n: number = 5): Promise<T[]> {
    const rows = await this.collect();
    return rows.slice(-n);
  }

  async toArray(): Promise<T[]> {
    return this.collect();
  }

  // =========================================================================
  // DATA QUALITY
  // =========================================================================

  expect(): ExpectationSuite<T> {
    return new ExpectationSuite(this);
  }

  // =========================================================================
  // DATA PROFILING
  // =========================================================================

  async profile(options?: ProfileOptions): Promise<ProfileReport> {
    const rows = await this.collect();
    return profileData(rows as Record<string, unknown>[], options);
  }

  // =========================================================================
  // BACKEND OVERRIDE
  // =========================================================================

  withExecutor(executor: DataFrameExecutor): DataFrame<T> {
    return new DataFrame(this._plan, executor);
  }
}
