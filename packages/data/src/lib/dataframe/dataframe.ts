// ---------------------------------------------------------------------------
// DataFrame<T> — lazy, typed columnar analytics builder
//
// Operations are recorded as a logical plan. Nothing executes until a
// terminal (.collect(), .first(), etc.) is called.
// ---------------------------------------------------------------------------

import { Effect, Stream, Chunk } from "effect";
import type { Frameable } from "@promin/core";
import { StreamPipeline } from "@promin/core";
import type { LogicalPlan, WindowFn, AggFn, RollingFn } from "./logical-plan.ts";
import { executeChunked } from "./chunked-executor.ts";
import type { FileSourceDescriptor } from "./file-source.ts";
import type { DataFrameExecutor } from "./executor.ts";
import type { DataFrameSink } from "./sink.ts";
import { Expr } from "./expr.ts";

function isExpr(value: unknown): value is Expr {
  return value instanceof Expr;
}

function parseInterval(s: string): number {
  const match = s.match(/^(\d+)\s*(s|m|h|d|w|M)$/);
  if (!match) throw new Error(`Invalid interval: ${s}`);
  const n = parseInt(match[1]!, 10);
  switch (match[2]) {
    case "s":
      return n * 1000;
    case "m":
      return n * 60_000;
    case "h":
      return n * 3_600_000;
    case "d":
      return n * 86_400_000;
    case "w":
      return n * 604_800_000;
    case "M":
      return n * 2_592_000_000; // ~30 days
    default:
      throw new Error(`Unknown unit: ${match[2]}`);
  }
}
import { ArrayExecutor } from "./array-executor.ts";
import { GroupedDataFrame } from "./grouped-dataframe.ts";
import { StringAccessor, DateAccessor } from "./accessors.ts";
import { ExpectationSuite } from "../data-quality/expectation-suite.ts";
import { profileData, type ProfileOptions } from "../data-profiler/profiler.ts";
import type { ProfileReport } from "../data-profiler/profile-types.ts";
import { dataDiff, schemaDiff } from "../data-diff/data-diff.ts";
import type { DataDiffResult, DiffOptions, SchemaDiffResult } from "../data-diff/diff-types.ts";

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
    const fileSrc = source as any;
    if (typeof fileSrc.hint === "string") {
      // File-backed source — defer loading, carry hint for executor-native reading
      return new DataFrame<T>({
        _tag: "Source",
        data: [],
        load: (() => source.load()) as () => Promise<unknown[]>,
        stream:
          typeof fileSrc.stream === "function"
            ? () => fileSrc.stream() as AsyncIterable<unknown>
            : undefined,
        hint: fileSrc.hint,
      });
    }
    const data = await source.load();
    return new DataFrame<T>({ _tag: "Source", data });
  }

  /**
   * Create a DataFrame from a file source. The file is not loaded until
   * `.collect()` is called — the executor decides how to read it.
   *
   * Executors that have a registered loader for the source's hint will use
   * native reading. Others fall back to `source.load()` (JS parsing).
   *
   * @example
   * ```ts
   * import { CsvFile, ParquetFile } from "@promin/core";
   *
   * // Executor with registered CSV loader reads natively
   * DataFrame.fromFile(CsvFile("sales.csv")).withExecutor(myExecutor)
   *
   * // Default ArrayExecutor parses in JS
   * DataFrame.fromFile(CsvFile("sales.csv")).collect()
   * ```
   */
  static fromFile<T>(source: FileSourceDescriptor): DataFrame<T> {
    return new DataFrame<T>({
      _tag: "Source",
      data: [],
      load: source.load as () => Promise<unknown[]>,
      stream: source.stream as (() => AsyncIterable<unknown>) | undefined,
      hint: source.hint,
    });
  }

  /**
   * Execute raw SQL against named DataFrames. Requires a DuckDB executor.
   *
   * Each DataFrame is registered as a named table that can be referenced
   * in the SQL query. The result is returned as a new DataFrame.
   *
   * @example
   * ```ts
   * const result = await DataFrame.sql(
   *   `SELECT u.name, SUM(o.amount) as total
   *    FROM users u
   *    JOIN orders o ON u.id = o.user_id
   *    WHERE u.active = true
   *    GROUP BY u.name`,
   *   { users: usersDF, orders: ordersDF },
   *   executor,
   * );
   * ```
   */
  static async sql<T = Record<string, unknown>>(
    query: string,
    tables: Record<string, DataFrame<any>>,
    executor: DataFrameExecutor,
  ): Promise<DataFrame<T>> {
    if (!("executeSql" in executor)) {
      throw new Error("SQL interface requires a DuckDB executor");
    }
    const rows: T[] = await (executor as any).executeSql(query, tables);
    return DataFrame.fromArray(rows);
  }

  static async diff<T>(
    before: DataFrame<T>,
    after: DataFrame<T>,
    options: DiffOptions,
  ): Promise<DataDiffResult> {
    const beforeRows = await before.collect();
    const afterRows = await after.collect();
    return dataDiff(
      beforeRows as Record<string, unknown>[],
      afterRows as Record<string, unknown>[],
      options,
    );
  }

  static schemaDiff(beforeColumns: string[], afterColumns: string[]): SchemaDiffResult {
    return schemaDiff(beforeColumns, afterColumns);
  }

  static concat<T>(...frames: DataFrame<T>[]): DataFrame<T> {
    if (frames.length === 0) return DataFrame.fromArray<T>([]);
    if (frames.length === 1) return frames[0]!;
    return new DataFrame<T>(
      { _tag: "Concat", inputs: frames.map((f) => f._plan) },
      frames[0]!._executor,
    );
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

  withColumn<K extends string, V>(
    name: K,
    fn: ((row: T) => V) | Expr,
  ): DataFrame<T & Record<K, V>> {
    const resolvedFn = isExpr(fn) ? fn.fn : fn;
    const expr = isExpr(fn) ? fn.ast : undefined;
    return new DataFrame(
      { _tag: "WithColumn", input: this._plan, name, fn: resolvedFn, expr },
      this._executor,
    );
  }

  withColumns<M extends Record<string, ((row: T) => unknown) | Expr>>(
    columns: M,
  ): DataFrame<T & { [K in keyof M]: unknown }> {
    let df: DataFrame<any> = this;
    for (const [name, fnOrExpr] of Object.entries(columns)) {
      df = df.withColumn(name, fnOrExpr as any);
    }
    return df;
  }

  // =========================================================================
  // ROW OPERATIONS
  // =========================================================================

  filter(fn: ((row: T) => boolean) | Expr): DataFrame<T> {
    const resolvedFn = isExpr(fn) ? fn.fn : fn;
    const expr = isExpr(fn) ? fn.ast : undefined;
    return new DataFrame(
      { _tag: "Filter", input: this._plan, fn: resolvedFn, expr },
      this._executor,
    );
  }

  map<U>(fn: (row: T) => U): DataFrame<U> {
    return new DataFrame({ _tag: "Map", input: this._plan, fn }, this._executor);
  }

  sort(by: keyof T & string, order?: "asc" | "desc"): DataFrame<T>;
  sort(by: { column: keyof T & string; order: "asc" | "desc" }[]): DataFrame<T>;
  sort(
    by: (keyof T & string) | { column: keyof T & string; order: "asc" | "desc" }[],
    order: "asc" | "desc" = "asc",
  ): DataFrame<T> {
    if (Array.isArray(by)) {
      return new DataFrame({ _tag: "Sort", input: this._plan, by, order: "asc" }, this._executor);
    }
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
    return new DataFrame<T>({ _tag: "Reverse", input: this._plan }, this._executor);
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

  fillNull<K extends keyof T>(
    column: K,
    valueOrOptions: T[K] | { method: "forward" | "backward" },
  ): DataFrame<T> {
    if (
      typeof valueOrOptions === "object" &&
      valueOrOptions !== null &&
      "method" in valueOrOptions
    ) {
      return new DataFrame(
        {
          _tag: "FillNull",
          input: this._plan,
          column: column as string,
          method: valueOrOptions.method,
        },
        this._executor,
      );
    }
    return new DataFrame(
      {
        _tag: "FillNull",
        input: this._plan,
        column: column as string,
        method: "value",
        value: valueOrOptions,
      },
      this._executor,
    );
  }

  // =========================================================================
  // AGGREGATION
  // =========================================================================

  groupBy<K extends keyof T & string>(...columns: K[]): GroupedDataFrame<T, K> {
    return new GroupedDataFrame(this._plan, columns, this._executor);
  }

  valueCounts(column: keyof T & string): DataFrame<{ value: unknown; count: number }> {
    // Add a _vc_count column, groupBy the target column, count _vc_count, then rename
    return DataFrame._fromPlan(
      {
        _tag: "Rename",
        mapping: { [column]: "value", _vc_count: "count" },
        input: {
          _tag: "GroupBy",
          columns: [column],
          aggs: { _vc_count: "count" as const },
          input: {
            _tag: "WithColumn",
            name: "_vc_count",
            fn: () => 1,
            input: this._plan,
          },
        },
      },
      this._executor,
    );
  }

  // =========================================================================
  // TIME-SERIES RESAMPLING
  // =========================================================================

  /**
   * Resample time-series data by truncating a time column to an interval,
   * grouping by the resulting buckets, and aggregating.
   *
   * @param timeColumn - The column containing timestamps (Date or numeric epoch ms)
   * @param interval - Interval string ("1h", "5m", "1d", "1w", "1M") or milliseconds
   * @param aggs - Aggregation functions per column (e.g. `{ value: "avg" }`)
   */
  resample(
    timeColumn: keyof T & string,
    interval: string | number,
    aggs: Record<string, AggFn>,
  ): DataFrame<Record<string, unknown>> {
    const intervalMs = typeof interval === "number" ? interval : parseInterval(interval);
    const bucketCol = `_${timeColumn}_bucket`;

    // Step 1: Add bucket column (truncate time to interval)
    let df: DataFrame<any> = this.withColumn(bucketCol, (row: any) => {
      const ts = row[timeColumn];
      const epoch = ts instanceof Date ? ts.getTime() : Number(ts);
      return new Date(Math.floor(epoch / intervalMs) * intervalMs);
    });

    // Step 2: GroupBy bucket + aggregate
    df = df.groupBy(bucketCol).agg(aggs);

    // Step 3: Rename bucket column back to original time column name
    df = df.rename({ [bucketCol]: timeColumn } as any);

    // Step 4: Sort by time
    df = df.sort(timeColumn as any, "asc");

    return df;
  }

  // =========================================================================
  // JOINS
  // =========================================================================

  join<U>(
    other: DataFrame<U>,
    params: {
      on: ((keyof T & keyof U) & string) | ((keyof T & keyof U) & string)[];
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
    return new DataFrame<T>(
      { _tag: "Union", left: this._plan, right: other._plan },
      this._executor,
    );
  }

  intersection(other: DataFrame<T>, on: keyof T & string): DataFrame<T> {
    return this.join(other, { on, type: "semi" }) as unknown as DataFrame<T>;
  }

  difference(other: DataFrame<T>, on: keyof T & string): DataFrame<T> {
    return this.join(other, { on, type: "anti" }) as unknown as DataFrame<T>;
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

  async median(column: keyof T & string): Promise<number | null> {
    const rows = await this.collect();
    const nums = rows.map((r) => Number((r as any)[column])).filter((v) => !Number.isNaN(v));
    if (nums.length === 0) return null;
    nums.sort((a, b) => a - b);
    const mid = Math.floor(nums.length / 2);
    return nums.length % 2 !== 0 ? nums[mid]! : (nums[mid - 1]! + nums[mid]!) / 2;
  }

  async std(column: keyof T & string): Promise<number | null> {
    const v = await this.variance(column);
    return v === null ? null : Math.sqrt(v);
  }

  async variance(column: keyof T & string): Promise<number | null> {
    const rows = await this.collect();
    const nums = rows.map((r) => Number((r as any)[column])).filter((v) => !Number.isNaN(v));
    if (nums.length < 2) return null;
    const mean = nums.reduce((a, b) => a + b, 0) / nums.length;
    return nums.reduce((acc, v) => acc + (v - mean) ** 2, 0) / (nums.length - 1);
  }

  async quantile(column: keyof T & string, q: number): Promise<number | null> {
    if (q < 0 || q > 1) throw new Error("Quantile must be between 0 and 1");
    const rows = await this.collect();
    const nums = rows.map((r) => Number((r as any)[column])).filter((v) => !Number.isNaN(v));
    if (nums.length === 0) return null;
    nums.sort((a, b) => a - b);
    const pos = q * (nums.length - 1);
    const lo = Math.floor(pos);
    const hi = Math.ceil(pos);
    if (lo === hi) return nums[lo]!;
    return nums[lo]! + (pos - lo) * (nums[hi]! - nums[lo]!);
  }

  async correlation(col1: keyof T & string, col2: keyof T & string): Promise<number | null> {
    const rows = await this.collect();
    const pairs = rows
      .map((r) => [Number((r as any)[col1]), Number((r as any)[col2])] as const)
      .filter(([a, b]) => !Number.isNaN(a) && !Number.isNaN(b));
    if (pairs.length < 2) return null;
    const n = pairs.length;
    const mean1 = pairs.reduce((acc, [a]) => acc + a, 0) / n;
    const mean2 = pairs.reduce((acc, [, b]) => acc + b, 0) / n;
    let cov = 0;
    let var1 = 0;
    let var2 = 0;
    for (const [a, b] of pairs) {
      const d1 = a - mean1;
      const d2 = b - mean2;
      cov += d1 * d2;
      var1 += d1 * d1;
      var2 += d2 * d2;
    }
    const denom = Math.sqrt(var1 * var2);
    return denom === 0 ? null : cov / denom;
  }

  async covariance(col1: keyof T & string, col2: keyof T & string): Promise<number | null> {
    const rows = await this.collect();
    const pairs = rows
      .map((r) => [Number((r as any)[col1]), Number((r as any)[col2])] as const)
      .filter(([a, b]) => !Number.isNaN(a) && !Number.isNaN(b));
    if (pairs.length < 2) return null;
    const n = pairs.length;
    const mean1 = pairs.reduce((acc, [a]) => acc + a, 0) / n;
    const mean2 = pairs.reduce((acc, [, b]) => acc + b, 0) / n;
    return pairs.reduce((acc, [a, b]) => acc + (a - mean1) * (b - mean2), 0) / (n - 1);
  }

  /** Sample skewness (Fisher-Pearson). Returns null if fewer than 3 numeric values. */
  async skew(column: keyof T & string): Promise<number | null> {
    const rows = await this.collect();
    const nums = rows.map((r) => Number((r as any)[column])).filter((v) => !Number.isNaN(v));
    if (nums.length < 3) return null;
    const n = nums.length;
    const mean = nums.reduce((a, b) => a + b, 0) / n;
    let m2 = 0;
    let m3 = 0;
    for (const v of nums) {
      const d = v - mean;
      m2 += d * d;
      m3 += d * d * d;
    }
    const variance = m2 / (n - 1);
    const stdev = Math.sqrt(variance);
    if (stdev === 0) return 0;
    const g1 = m3 / n / (stdev * stdev * stdev);
    return (Math.sqrt(n * (n - 1)) / (n - 2)) * g1;
  }

  /** Sample excess kurtosis. Returns null if fewer than 4 numeric values. */
  async kurtosis(column: keyof T & string): Promise<number | null> {
    const rows = await this.collect();
    const nums = rows.map((r) => Number((r as any)[column])).filter((v) => !Number.isNaN(v));
    if (nums.length < 4) return null;
    const n = nums.length;
    const mean = nums.reduce((a, b) => a + b, 0) / n;
    let m2 = 0;
    let m4 = 0;
    for (const v of nums) {
      const d = v - mean;
      const d2 = d * d;
      m2 += d2;
      m4 += d2 * d2;
    }
    if (m2 === 0) return 0;
    const numerator = (n * (n + 1) * m4) / (m2 * m2);
    return ((n - 1) / ((n - 2) * (n - 3))) * (numerator - 3 * (n - 1));
  }

  /** Shannon entropy of the column's discrete value distribution, in bits. */
  async entropy(column: keyof T & string): Promise<number | null> {
    const rows = await this.collect();
    const values = rows.map((r) => (r as any)[column]).filter((v) => v != null);
    if (values.length === 0) return null;
    const counts = new Map<unknown, number>();
    for (const v of values) counts.set(v, (counts.get(v) ?? 0) + 1);
    const n = values.length;
    let h = 0;
    for (const c of counts.values()) {
      const p = c / n;
      h -= p * Math.log2(p);
    }
    return h;
  }

  /** Approximate distinct count via HyperLogLog (14-bit precision, ~0.8% error). */
  async approxNUnique(column: keyof T & string): Promise<number> {
    const rows = await this.collect();
    const p = 14;
    const m = 1 << p; // 16384 buckets
    const registers = new Uint8Array(m);
    const alpha = 0.7213 / (1 + 1.079 / m);

    for (const r of rows) {
      const v = (r as any)[column];
      if (v == null) continue;
      const h = hash64(String(v));
      const index = Number(h & BigInt(m - 1));
      const w = h >> BigInt(p);
      // Leading zeros + 1 in the remaining 64-p bits
      const lz = w === 0n ? 64 - p + 1 : leadingZeros64Minus(w, 64 - p) + 1;
      if (lz > registers[index]!) registers[index] = lz;
    }

    let sum = 0;
    let zeros = 0;
    for (let i = 0; i < m; i++) {
      sum += 1 / (1 << registers[i]!);
      if (registers[i] === 0) zeros++;
    }
    let estimate = (alpha * m * m) / sum;
    if (estimate <= 2.5 * m && zeros > 0) {
      estimate = m * Math.log(m / zeros); // small-range correction
    }
    return Math.round(estimate);
  }

  /**
   * Exponentially weighted moving average.
   *
   * `ewm[0] = x[0]; ewm[i] = alpha * x[i] + (1 - alpha) * ewm[i-1]`
   *
   * Replaces the column with the smoothed series. Null values keep the previous
   * smoothed value (do not contribute to the update).
   */
  ewmMean(column: keyof T & string, params: { alpha: number }): DataFrame<T> {
    const alpha = params.alpha;
    if (alpha <= 0 || alpha > 1) throw new Error("ewmMean: alpha must be in (0, 1]");
    const col = column as string;
    return DataFrame._fromPlan<T>(
      {
        _tag: "Source",
        data: [],
        load: async () => {
          const rows = await this.collect();
          let prev: number | null = null;
          return rows.map((r) => {
            const raw = (r as any)[col];
            const v = raw == null ? NaN : Number(raw);
            if (Number.isNaN(v)) return { ...(r as any), [col]: prev };
            prev = prev === null ? v : alpha * v + (1 - alpha) * prev;
            return { ...(r as any), [col]: prev };
          });
        },
      },
      this._executor,
    );
  }

  /** Fill null/NaN gaps in a numeric column via linear interpolation. */
  interpolate(column: keyof T & string): DataFrame<T> {
    const col = column as string;
    return DataFrame._fromPlan<T>(
      {
        _tag: "Source",
        data: [],
        load: async () => {
          const rows = await this.collect();
          const result = rows.map((r) => ({ ...(r as any) }));
          const isMissing = (v: unknown): boolean => v == null || Number.isNaN(Number(v));

          let i = 0;
          while (i < result.length) {
            if (!isMissing(result[i]![col])) {
              i++;
              continue;
            }
            // Find the run of missing values
            const start = i;
            while (i < result.length && isMissing(result[i]![col])) i++;
            const end = i; // exclusive
            const before = start > 0 ? Number(result[start - 1]![col]) : null;
            const after = end < result.length ? Number(result[end]![col]) : null;

            if (before !== null && after !== null) {
              const gap = end - start + 1;
              const step = (after - before) / gap;
              for (let k = 0; k < end - start; k++) {
                result[start + k]![col] = before + step * (k + 1);
              }
            } else if (before !== null) {
              for (let k = start; k < end; k++) result[k]![col] = before;
            } else if (after !== null) {
              for (let k = start; k < end; k++) result[k]![col] = after;
            }
            // both null — leave as-is
          }
          return result;
        },
      },
      this._executor,
    );
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

  /**
   * Execute the plan in streaming mode — returns a StreamPipeline that yields rows.
   *
   * For streamable plans (filter, map, select, withColumn, etc.), processes the
   * source data in fixed-size chunks with constant memory.
   *
   * For materializing plans (sort, groupBy, join, etc.), falls back to full
   * execution and emits all rows at once.
   *
   * @param options.chunkSize - Number of source rows per chunk (default 10,000)
   */
  stream(options?: { chunkSize?: number }): StreamPipeline<T, never> {
    const chunkSize = options?.chunkSize ?? 10_000;
    const plan = this._plan;

    // Resolve chunks eagerly, then stream via native Chunk path.
    // Stream.fromChunks is 56x faster than fromAsyncIterable for batch data.
    const s = Stream.unwrap(
      Effect.promise(async () => {
        const allChunks: Chunk.Chunk<T>[] = [];
        for await (const chunk of executeChunked<T>({ plan, chunkSize })) {
          allChunks.push(Chunk.fromIterable(chunk));
        }
        return Stream.fromChunks(...allChunks);
      }),
    );
    return StreamPipeline.from(s);
  }

  async collect(): Promise<T[]> {
    return this._executor.execute<T>(this._plan);
  }

  /**
   * Synchronous collect — avoids async/Promise overhead.
   * Only works with sync backends (ArrayExecutor). Throws if the executor
   * doesn't support sync execution.
   */
  collectSync(): T[] {
    if (!this._executor.executeSync) {
      throw new Error("collectSync() requires a sync executor (e.g. ArrayExecutor)");
    }
    return this._executor.executeSync<T>(this._plan);
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

  /** Write all rows to a sink. Collects the plan then streams rows to the sink. */
  async to(sink: DataFrameSink<T>): Promise<void> {
    const rows = await this.collect();
    for (const row of rows) await sink.write(row);
    await sink.end();
  }

  // =========================================================================
  // SQL INTERFACE
  // =========================================================================

  /**
   * Run raw SQL against this DataFrame (referenced as "self" in the query).
   * Requires a DuckDB executor.
   *
   * @example
   * ```ts
   * const df = DataFrame.fromArray(data).withExecutor(duckdbExecutor);
   * const result = await df.sql<{ total: number }>("SELECT SUM(x) as total FROM self");
   * ```
   */
  async sql<U = Record<string, unknown>>(query: string): Promise<DataFrame<U>> {
    if (!("executeSql" in this._executor)) {
      throw new Error("SQL interface requires a DuckDB executor");
    }
    const rows: U[] = await (this._executor as any).executeSql(query, { self: this });
    return DataFrame.fromArray(rows);
  }

  // =========================================================================
  // DATA DIFF (instance method)
  // =========================================================================

  async diff(other: DataFrame<T>, options: DiffOptions): Promise<DataDiffResult> {
    return DataFrame.diff(this, other, options);
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

// ---------------------------------------------------------------------------
// HyperLogLog helpers — used by DataFrame.approxNUnique
// ---------------------------------------------------------------------------

/**
 * 64-bit hash of a string: FNV-1a mixing + Murmur3 finalizer for avalanche.
 *
 * FNV-1a alone has weak avalanche that biases HLL bucket assignment; applying
 * the Murmur3 fmix64 finalizer fixes the distribution without a full Murmur
 * rewrite.
 */
function hash64(s: string): bigint {
  const FNV_OFFSET = 0xcbf29ce484222325n;
  const FNV_PRIME = 0x100000001b3n;
  const MASK = 0xffffffffffffffffn;
  let h = FNV_OFFSET;
  for (let i = 0; i < s.length; i++) {
    h ^= BigInt(s.charCodeAt(i));
    h = (h * FNV_PRIME) & MASK;
  }
  // Murmur3 fmix64
  h ^= h >> 33n;
  h = (h * 0xff51afd7ed558ccdn) & MASK;
  h ^= h >> 33n;
  h = (h * 0xc4ceb9fe1a85ec53n) & MASK;
  h ^= h >> 33n;
  return h;
}

/** Number of leading zeros in the low `bits` of `w` (a BigInt). */
function leadingZeros64Minus(w: bigint, bits: number): number {
  let lz = 0;
  for (let i = bits - 1; i >= 0; i--) {
    if ((w >> BigInt(i)) & 1n) break;
    lz++;
  }
  return lz;
}
