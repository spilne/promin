// ---------------------------------------------------------------------------
// DuckDBExecutor — compiles DataFrame logical plans to SQL and executes via DuckDB
//
// Best for: groupBy, sort, window functions, complex joins at 10K-10M rows.
// ArrayExecutor is faster for simple filter/map on small data (<10K rows)
// because DuckDB has fixed overhead for data registration + result transfer.
// ---------------------------------------------------------------------------

import { Database } from "duckdb-async";
import { writeFileSync, unlinkSync, mkdtempSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import type { DataFrameExecutor, ExecutionCost, LogicalPlan, AggFn, WindowFn } from "@promin/core";

export class DuckDBExecutor implements DataFrameExecutor {
  private db: Database | null = null;
  private tableCounter = 0;
  private tmpDir: string;

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
    const ctx = new CompilationContext(db, this.tmpDir);

    try {
      const sql = await ctx.compile(plan);
      const rows = await db.all(sql);
      return convertBigInts(rows) as T[];
    } finally {
      ctx.cleanup();
    }
  }

  supports(_plan: LogicalPlan): boolean {
    return true;
  }

  estimateCost(plan: LogicalPlan): ExecutionCost {
    const rows = estimateRows(plan);
    // DuckDB has fixed overhead (~5ms) but scales better than Array for large data
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
// SQL compilation context — tracks registered tables and temp files
// ---------------------------------------------------------------------------

class CompilationContext {
  private tables: string[] = [];
  private files: string[] = [];
  private counter = 0;

  constructor(
    private db: Database,
    private tmpDir: string,
  ) {}

  private nextTable(): string {
    const name = `_t${this.counter++}`;
    this.tables.push(name);
    return name;
  }

  async cleanup(): Promise<void> {
    for (const file of this.files) {
      try {
        unlinkSync(file);
      } catch {
        // ignore
      }
    }
    for (const table of this.tables) {
      try {
        await this.db.run(`DROP TABLE IF EXISTS "${table}"`);
      } catch {
        // ignore
      }
    }
  }

  async registerData(data: unknown[]): Promise<string> {
    if (data.length === 0) {
      const name = this.nextTable();
      await this.db.run(`CREATE TEMP TABLE "${name}" AS SELECT 1 WHERE FALSE`);
      return name;
    }

    const name = this.nextTable();
    const filePath = join(this.tmpDir, `${name}.json`);
    writeFileSync(filePath, JSON.stringify(data));
    this.files.push(filePath);

    await this.db.run(`CREATE TEMP TABLE "${name}" AS SELECT * FROM read_json_auto('${filePath}')`);
    return name;
  }

  async compile(plan: LogicalPlan): Promise<string> {
    switch (plan._tag) {
      case "Source": {
        const table = await this.registerData(plan.data);
        return `SELECT * FROM "${table}"`;
      }

      case "Filter": {
        const input = await this.compile(plan.input);
        // Filter uses JS function — we can't compile it to SQL.
        // Fallback: materialize input, filter in JS, re-register.
        // TODO: Support predicate pushdown for common patterns.
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
        // We need all columns — use * REPLACE pattern or select explicitly
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
        const joinType = joinTypeToSql(plan.type);

        if (plan.type === "semi") {
          return `SELECT __left.* FROM (${left}) AS __left WHERE __left."${plan.on}" IN (SELECT "${plan.on}" FROM (${right}))`;
        }
        if (plan.type === "anti") {
          return `SELECT __left.* FROM (${left}) AS __left WHERE __left."${plan.on}" NOT IN (SELECT "${plan.on}" FROM (${right}))`;
        }

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
        // DuckDB doesn't have a native reverse — use row_number and reverse order
        return `SELECT * EXCLUDE (_rn) FROM (SELECT *, ROW_NUMBER() OVER () AS _rn FROM (${input})) ORDER BY _rn DESC`;
      }

      case "Pivot":
      case "Unpivot":
      case "Explode":
      case "Rolling":
      case "Cumulative": {
        // Fallback to ArrayExecutor for operations that don't compile to SQL easily
        const { ArrayExecutor } = await import("@promin/core");
        return new ArrayExecutor().execute(plan).then(async (rows) => {
          const table = await this.registerData(rows);
          return `SELECT * FROM "${table}"`;
        });
      }
    }
  }

  // -------------------------------------------------------------------------
  // JS function fallbacks — for operations that can't be compiled to SQL
  // -------------------------------------------------------------------------

  private async applyJsFilter(inputSql: string, fn: (row: any) => boolean): Promise<string> {
    const rows = await this.db.all(inputSql);
    const filtered = convertBigInts(rows).filter(fn);
    const table = await this.registerData(filtered);
    return `SELECT * FROM "${table}"`;
  }

  private async applyJsMap(inputSql: string, fn: (row: any) => any): Promise<string> {
    const rows = await this.db.all(inputSql);
    const mapped = convertBigInts(rows).map(fn);
    const table = await this.registerData(mapped);
    return `SELECT * FROM "${table}"`;
  }

  private async applyJsWithColumn(
    inputSql: string,
    name: string,
    fn: (row: any) => any,
  ): Promise<string> {
    const rows = await this.db.all(inputSql);
    const result = convertBigInts(rows).map((row) => ({ ...row, [name]: fn(row) }));
    const table = await this.registerData(result);
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
