// ---------------------------------------------------------------------------
// DuckDBExecutor — compiles DataFrame logical plans to SQL, executes via DuckDB
//
// ## When to use DuckDB vs ArrayExecutor
//
// DuckDB has a fixed overhead per query: data must be loaded from JS into
// DuckDB's columnar format (~65ms for 100K rows via JSON serialization).
// Once loaded, DuckDB's vectorized engine is significantly faster for
// analytical operations.
//
// Benchmarks (1M rows, Apple M2 Max):
//
//   Operation          DuckDB (warm)    Array         Winner
//   ─────────────────  ──────────────   ──────────    ──────
//   groupBy+sum+avg    4.0ms            11.5ms        DuckDB (3x)
//   sort+limit 10      1.4ms            215ms         DuckDB (154x)
//   distinct            3.5ms            6.7ms         DuckDB (2x)
//   filter (return 50%) 190ms            9.1ms         Array (21x)
//
// Rule of thumb:
//   - DuckDB wins on operations that REDUCE data (groupBy, sort+limit, distinct)
//   - Array wins on operations that RETURN most rows (filter, map)
//   - DuckDB wins more as data size grows (columnar scales better)
//   - "Load once, query many" pattern: DuckDB is 12x faster at 1M rows
//
// ## Table caching
//
// Source data is cached in DuckDB tables by identity (WeakRef to source array).
// First query pays the load cost; subsequent queries on the same source skip it.
// This makes the "load once, query many" pattern automatic:
//
//   const executor = new DuckDBExecutor();
//   const df = DataFrame.fromArray(bigData).withExecutor(executor);
//
//   await df.groupBy("region").agg({ revenue: "sum" }).collect();  // loads data
//   await df.sort("revenue", "desc").limit(10).collect();           // cached, fast
//   await df.distinct().collect();                                   // cached, fast
//
// ---------------------------------------------------------------------------

import { Database } from "duckdb-async";
import { writeFileSync, unlinkSync, mkdtempSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import type { DataFrameExecutor, ExecutionCost, LogicalPlan, AggFn, WindowFn } from "@promin/core";

/**
 * DuckDB-backed DataFrame executor. Compiles logical plans to SQL and
 * executes via an in-memory DuckDB instance.
 *
 * Source data is cached — the first query on a dataset loads it into DuckDB,
 * subsequent queries reuse the cached table. Best for analytical workloads
 * (groupBy, sort, join, window) on 10K+ rows.
 *
 * @example
 * ```ts
 * const executor = new DuckDBExecutor();
 * const df = DataFrame.fromArray(data).withExecutor(executor);
 *
 * // First query loads data into DuckDB (~65ms for 100K rows)
 * await df.groupBy("region").agg({ revenue: "sum" }).collect();
 *
 * // Subsequent queries are fast (data already in DuckDB)
 * await df.sort("revenue", "desc").limit(10).collect(); // ~1.4ms
 * ```
 */
export class DuckDBExecutor implements DataFrameExecutor {
  private db: Database | null = null;
  private tmpDir: string;

  /**
   * Cache: maps source array identity → DuckDB table name.
   * Uses WeakRef so cached tables don't prevent GC of source arrays.
   */
  private sourceCache = new Map<number, { ref: WeakRef<unknown[]>; tableName: string }>();
  private cacheCounter = 0;

  constructor() {
    this.tmpDir = mkdtempSync(join(tmpdir(), "duckdb-df-"));
  }

  private async getDb(): Promise<Database> {
    if (!this.db) {
      this.db = await Database.create(":memory:");
    }
    return this.db;
  }

  async execute<T>(plan: LogicalPlan): Promise<T[]> {
    const db = await this.getDb();
    const ctx = new CompilationContext(db, this.tmpDir, this.sourceCache, this.cacheCounter);

    try {
      const sql = await ctx.compile(plan);
      this.cacheCounter = ctx.getCacheCounter();
      const rows = await db.all(sql);
      return convertBigInts(rows) as T[];
    } finally {
      await ctx.cleanupTempTables();
    }
  }

  /**
   * Load a CSV file directly into DuckDB — no JS serialization overhead.
   * DuckDB reads and parses the file natively (much faster than JS CSV parsing).
   *
   * @example
   * ```ts
   * const executor = new DuckDBExecutor();
   * const df = await executor.fromCsv<SalesRow>("data/sales.csv");
   * await df.groupBy("region").agg({ revenue: "sum" }).collect();
   * ```
   */
  async fromCsv<T>(
    path: string,
    params?: { delimiter?: string; header?: boolean },
  ): Promise<import("@promin/core").DataFrame<T>> {
    const db = await this.getDb();
    const tableName = `_file${this.cacheCounter++}`;
    const opts: string[] = [];
    if (params?.delimiter) opts.push(`delim='${params.delimiter}'`);
    if (params?.header === false) opts.push("header=false");
    const optsStr = opts.length > 0 ? `, ${opts.join(", ")}` : "";
    await db.run(`CREATE TABLE "${tableName}" AS SELECT * FROM read_csv_auto('${path}'${optsStr})`);

    // Create a DataFrame with a sentinel source that the compiler recognizes
    const { DataFrame } = await import("@promin/core");
    return DataFrame._fromPlan<T>(
      { _tag: "Source", data: [], _duckdbTable: tableName } as any,
      this,
    );
  }

  /**
   * Load a Parquet file directly into DuckDB — zero-copy columnar read.
   * This is the fastest path: no serialization, no JS parsing.
   *
   * @example
   * ```ts
   * const executor = new DuckDBExecutor();
   * const df = await executor.fromParquet<LogRow>("logs/2024-01.parquet");
   * await df.filter(r => r.level === "error").collect();
   * ```
   */
  async fromParquet<T>(path: string): Promise<import("@promin/core").DataFrame<T>> {
    const db = await this.getDb();
    const tableName = `_file${this.cacheCounter++}`;
    await db.run(`CREATE TABLE "${tableName}" AS SELECT * FROM read_parquet('${path}')`);

    const { DataFrame } = await import("@promin/core");
    return DataFrame._fromPlan<T>(
      { _tag: "Source", data: [], _duckdbTable: tableName } as any,
      this,
    );
  }

  /**
   * Load a JSON file directly into DuckDB.
   *
   * @example
   * ```ts
   * const df = await executor.fromJson<Event>("events.json");
   * ```
   */
  async fromJson<T>(path: string): Promise<import("@promin/core").DataFrame<T>> {
    const db = await this.getDb();
    const tableName = `_file${this.cacheCounter++}`;
    await db.run(`CREATE TABLE "${tableName}" AS SELECT * FROM read_json_auto('${path}')`);

    const { DataFrame } = await import("@promin/core");
    return DataFrame._fromPlan<T>(
      { _tag: "Source", data: [], _duckdbTable: tableName } as any,
      this,
    );
  }

  /**
   * Execute raw SQL against the DuckDB instance.
   * Useful for complex queries, CTEs, or operations not expressible via DataFrame API.
   *
   * @example
   * ```ts
   * const executor = new DuckDBExecutor();
   * await executor.fromCsv("sales.csv");
   * const df = await executor.sql<Result>("SELECT region, SUM(revenue) FROM _file0 GROUP BY region");
   * ```
   */
  async sql<T>(query: string): Promise<import("@promin/core").DataFrame<T>> {
    const db = await this.getDb();
    const tableName = `_sql${this.cacheCounter++}`;
    await db.run(`CREATE TABLE "${tableName}" AS ${query}`);

    const { DataFrame } = await import("@promin/core");
    return DataFrame._fromPlan<T>(
      { _tag: "Source", data: [], _duckdbTable: tableName } as any,
      this,
    );
  }

  supports(_plan: LogicalPlan): boolean {
    return true;
  }

  estimateCost(plan: LogicalPlan): ExecutionCost {
    const rows = estimateRows(plan);
    return { ms: 5 + rows * 0.0001, memory: rows * 100 };
  }
}

// ---------------------------------------------------------------------------
// BigInt → Number conversion (DuckDB returns BigInt for integers)
// ---------------------------------------------------------------------------

function convertBigInts(rows: any[]): any[] {
  return rows.map((row) => {
    const converted: any = {};
    for (const [key, value] of Object.entries(row)) {
      converted[key] = typeof value === "bigint" ? Number(value) : value;
    }
    return converted;
  });
}

// ---------------------------------------------------------------------------
// SQL compilation context
// ---------------------------------------------------------------------------

type SourceCache = Map<number, { ref: WeakRef<unknown[]>; tableName: string }>;

class CompilationContext {
  /** Temp tables created during this execution (cleaned up after). */
  private tempTables: string[] = [];
  private files: string[] = [];
  private tempCounter = 0;
  private cacheCounter: number;

  constructor(
    private db: Database,
    private tmpDir: string,
    private sourceCache: SourceCache,
    cacheCounter: number,
  ) {
    this.cacheCounter = cacheCounter;
  }

  getCacheCounter(): number {
    return this.cacheCounter;
  }

  private nextTempTable(): string {
    const name = `_tmp${this.tempCounter++}`;
    this.tempTables.push(name);
    return name;
  }

  async cleanupTempTables(): Promise<void> {
    for (const file of this.files) {
      try {
        unlinkSync(file);
      } catch {
        // ignore
      }
    }
    // Only clean up temp tables, NOT cached source tables
    for (const table of this.tempTables) {
      try {
        await this.db.run(`DROP TABLE IF EXISTS "${table}"`);
      } catch {
        // ignore
      }
    }
  }

  /**
   * Register source data in DuckDB with caching.
   * If the same array (by identity) was already loaded, reuse the cached table.
   */
  async registerSource(data: unknown[]): Promise<string> {
    // Check cache — look for a live WeakRef pointing to the same array
    for (const [id, entry] of this.sourceCache) {
      const cached = entry.ref.deref();
      if (cached === data) {
        return entry.tableName;
      }
      // Clean up dead refs
      if (!cached) {
        try {
          await this.db.run(`DROP TABLE IF EXISTS "${entry.tableName}"`);
        } catch {
          // ignore
        }
        this.sourceCache.delete(id);
      }
    }

    // Not cached — load into DuckDB
    if (data.length === 0) {
      const name = `_src${this.cacheCounter++}`;
      await this.db.run(`CREATE TABLE "${name}" AS SELECT 1 WHERE FALSE`);
      this.sourceCache.set(this.cacheCounter, { ref: new WeakRef(data), tableName: name });
      return name;
    }

    const name = `_src${this.cacheCounter++}`;
    const filePath = join(this.tmpDir, `${name}.json`);
    writeFileSync(filePath, JSON.stringify(data));
    this.files.push(filePath);

    await this.db.run(`CREATE TABLE "${name}" AS SELECT * FROM read_json_auto('${filePath}')`);
    this.sourceCache.set(this.cacheCounter, { ref: new WeakRef(data), tableName: name });
    return name;
  }

  /** Register intermediate data as a temp table (not cached). */
  async registerTemp(data: unknown[]): Promise<string> {
    if (data.length === 0) {
      const name = this.nextTempTable();
      await this.db.run(`CREATE TEMP TABLE "${name}" AS SELECT 1 WHERE FALSE`);
      return name;
    }

    const name = this.nextTempTable();
    const filePath = join(this.tmpDir, `${name}.json`);
    writeFileSync(filePath, JSON.stringify(data));
    this.files.push(filePath);

    await this.db.run(`CREATE TEMP TABLE "${name}" AS SELECT * FROM read_json_auto('${filePath}')`);
    return name;
  }

  async compile(plan: LogicalPlan): Promise<string> {
    switch (plan._tag) {
      case "Source": {
        // Check if pre-loaded from executor.fromCsv/fromParquet/fromJson
        const preloaded = (plan as any)._duckdbTable as string | undefined;
        if (preloaded) {
          return `SELECT * FROM "${preloaded}"`;
        }

        // Check if file-backed via DataFrame.fromFile() or DataFrame.from(CsvFile(...))
        const frameable = plan.frameable;
        if (frameable) {
          const tableName = `_file${this.counter++}`;
          const db = this.db;
          if (frameable.format === "csv") {
            const opts: string[] = [];
            if (frameable.options?.delimiter) opts.push(`delim='${frameable.options.delimiter}'`);
            if (frameable.options?.header === false) opts.push("header=false");
            const optsStr = opts.length > 0 ? `, ${opts.join(", ")}` : "";
            await db.run(
              `CREATE TABLE "${tableName}" AS SELECT * FROM read_csv_auto('${frameable.path}'${optsStr})`,
            );
          } else if (frameable.format === "parquet") {
            await db.run(
              `CREATE TABLE "${tableName}" AS SELECT * FROM read_parquet('${frameable.path}')`,
            );
          } else if (frameable.format === "json") {
            await db.run(
              `CREATE TABLE "${tableName}" AS SELECT * FROM read_json_auto('${frameable.path}')`,
            );
          }
          this.tempTables.push(tableName);
          return `SELECT * FROM "${tableName}"`;
        }

        // Otherwise, load JS array data with caching
        const table = await this.registerSource(plan.data);
        return `SELECT * FROM "${table}"`;
      }

      case "Filter": {
        const input = await this.compile(plan.input);
        return this.applyJsFilter(input, plan.fn);
      }

      case "Map": {
        const input = await this.compile(plan.input);
        return this.applyJsMap(input, plan.fn);
      }

      case "Select": {
        const input = await this.compile(plan.input);
        const cols = plan.columns.map((c) => `"${c}"`).join(", ");
        return `SELECT ${cols} FROM (${input})`;
      }

      case "Drop": {
        const input = await this.compile(plan.input);
        return `SELECT * EXCLUDE (${plan.columns.map((c) => `"${c}"`).join(", ")}) FROM (${input})`;
      }

      case "Rename": {
        const input = await this.compile(plan.input);
        const renames = Object.entries(plan.mapping)
          .map(([from, to]) => `"${from}" AS "${to}"`)
          .join(", ");
        return `SELECT * REPLACE (${renames}) FROM (${input})`;
      }

      case "WithColumn": {
        const input = await this.compile(plan.input);
        return this.applyJsWithColumn(input, plan.name, plan.fn);
      }

      case "Sort": {
        const input = await this.compile(plan.input);
        return `SELECT * FROM (${input}) ORDER BY "${plan.by}" ${plan.order === "desc" ? "DESC" : "ASC"}`;
      }

      case "Limit": {
        const input = await this.compile(plan.input);
        return `SELECT * FROM (${input}) LIMIT ${plan.n}`;
      }

      case "Offset": {
        const input = await this.compile(plan.input);
        return `SELECT * FROM (${input}) OFFSET ${plan.n}`;
      }

      case "Slice": {
        const input = await this.compile(plan.input);
        const limit = plan.end !== undefined ? plan.end - plan.start : "ALL";
        return `SELECT * FROM (${input}) LIMIT ${limit} OFFSET ${plan.start}`;
      }

      case "Distinct": {
        const input = await this.compile(plan.input);
        if (plan.by) {
          return `SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "${plan.by}" ORDER BY ROWID) as _rn FROM (${input})) WHERE _rn = 1`;
        }
        return `SELECT DISTINCT * FROM (${input})`;
      }

      case "GroupBy": {
        const input = await this.compile(plan.input);
        const groupCols = plan.columns.map((c) => `"${c}"`).join(", ");
        const aggExprs = Object.entries(plan.aggs)
          .map(([col, fn]) => `${aggFnToSql(fn)}("${col}") AS "${col}_${fn}"`)
          .join(", ");
        return `SELECT ${groupCols}, ${aggExprs} FROM (${input}) GROUP BY ${groupCols}`;
      }

      case "Join": {
        const left = await this.compile(plan.left);
        const right = await this.compile(plan.right);

        if (plan.type === "semi") {
          return `SELECT __left.* FROM (${left}) AS __left WHERE __left."${plan.on}" IN (SELECT "${plan.on}" FROM (${right}))`;
        }
        if (plan.type === "anti") {
          return `SELECT __left.* FROM (${left}) AS __left WHERE __left."${plan.on}" NOT IN (SELECT "${plan.on}" FROM (${right}))`;
        }

        const joinType = joinTypeToSql(plan.type);
        return `SELECT * FROM (${left}) AS __left ${joinType} (${right}) AS __right USING ("${plan.on}")`;
      }

      case "Window": {
        const input = await this.compile(plan.input);
        const partitionClause = plan.partitionBy ? `PARTITION BY "${plan.partitionBy}"` : "";
        const orderClause = `ORDER BY "${plan.orderBy}"`;
        const windowFn = windowFnToSql(plan.fn, plan.args);
        return `SELECT *, ${windowFn} OVER (${partitionClause} ${orderClause}) AS "${plan.name}" FROM (${input})`;
      }

      case "Concat": {
        const queries = await Promise.all(plan.inputs.map((input) => this.compile(input)));
        return queries.join(" UNION ALL ");
      }

      case "Union": {
        const left = await this.compile(plan.left);
        const right = await this.compile(plan.right);
        return `${left} UNION ${right}`;
      }

      case "Reverse": {
        const input = await this.compile(plan.input);
        return `SELECT * EXCLUDE (_rn) FROM (SELECT *, ROW_NUMBER() OVER () AS _rn FROM (${input})) ORDER BY _rn DESC`;
      }

      case "Pivot":
      case "Unpivot":
      case "Explode":
      case "Rolling":
      case "Cumulative": {
        // Fallback to ArrayExecutor for operations that don't compile to SQL
        const { ArrayExecutor } = await import("@promin/core");
        const rows = await new ArrayExecutor().execute(plan);
        const table = await this.registerTemp(rows);
        return `SELECT * FROM "${table}"`;
      }
    }
  }

  // -------------------------------------------------------------------------
  // JS function fallbacks
  // -------------------------------------------------------------------------

  private async applyJsFilter(inputSql: string, fn: (row: any) => boolean): Promise<string> {
    const rows = await this.db.all(inputSql);
    const filtered = convertBigInts(rows).filter(fn);
    const table = await this.registerTemp(filtered);
    return `SELECT * FROM "${table}"`;
  }

  private async applyJsMap(inputSql: string, fn: (row: any) => any): Promise<string> {
    const rows = await this.db.all(inputSql);
    const mapped = convertBigInts(rows).map(fn);
    const table = await this.registerTemp(mapped);
    return `SELECT * FROM "${table}"`;
  }

  private async applyJsWithColumn(
    inputSql: string,
    name: string,
    fn: (row: any) => any,
  ): Promise<string> {
    const rows = await this.db.all(inputSql);
    const result = convertBigInts(rows).map((row) => ({ ...row, [name]: fn(row) }));
    const table = await this.registerTemp(result);
    return `SELECT * FROM "${table}"`;
  }
}

// ---------------------------------------------------------------------------
// SQL helpers
// ---------------------------------------------------------------------------

function aggFnToSql(fn: AggFn): string {
  switch (fn) {
    case "sum":
      return "SUM";
    case "count":
      return "COUNT";
    case "avg":
      return "AVG";
    case "min":
      return "MIN";
    case "max":
      return "MAX";
    case "first":
      return "FIRST";
    case "last":
      return "LAST";
    case "collect":
      return "LIST";
  }
}

function joinTypeToSql(type: string): string {
  switch (type) {
    case "inner":
      return "INNER JOIN";
    case "left":
      return "LEFT JOIN";
    case "right":
      return "RIGHT JOIN";
    case "full":
      return "FULL OUTER JOIN";
    default:
      return "INNER JOIN";
  }
}

function windowFnToSql(
  fn: WindowFn,
  args?: { offset?: number; n?: number; default?: unknown },
): string {
  switch (fn) {
    case "row_number":
      return "ROW_NUMBER()";
    case "rank":
      return "RANK()";
    case "dense_rank":
      return "DENSE_RANK()";
    case "lag":
      return `LAG("${fn}", ${args?.offset ?? 1}, ${args?.default !== undefined ? `'${args.default}'` : "NULL"})`;
    case "lead":
      return `LEAD("${fn}", ${args?.offset ?? 1}, ${args?.default !== undefined ? `'${args.default}'` : "NULL"})`;
    case "sum":
      return "SUM(*)";
    case "avg":
      return "AVG(*)";
    case "min":
      return "MIN(*)";
    case "max":
      return "MAX(*)";
    case "running_total":
      return "SUM(*) OVER (ROWS UNBOUNDED PRECEDING)";
    case "first":
      return "FIRST_VALUE(*)";
    case "last":
      return "LAST_VALUE(*)";
    case "ntile":
      return `NTILE(${args?.n ?? 4})`;
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
      return Math.min(100, estimateRows(plan.input));
    case "Join":
      return estimateRows(plan.left) * estimateRows(plan.right) * 0.1;
    default:
      return "input" in plan ? estimateRows((plan as any).input) : 1000;
  }
}
