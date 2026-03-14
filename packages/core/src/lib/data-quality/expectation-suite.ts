// ---------------------------------------------------------------------------
// ExpectationSuite — declarative chain of data quality checks on a DataFrame
// ---------------------------------------------------------------------------

import type { DataFrame } from "../dataframe/dataframe.ts";
import type { Expectation, ExpectationResult, ValidationResult } from "./expectation.ts";

export class ExpectationSuite<T> {
  private readonly expectations: Expectation<T>[] = [];

  constructor(private readonly df: DataFrame<T>) {}

  // =========================================================================
  // Built-in expectations
  // =========================================================================

  expectNotNull(
    column: keyof T & string,
    params?: { severity?: "error" | "warning" },
  ): ExpectationSuite<T> {
    this.expectations.push({
      name: `expect_not_null(${column})`,
      severity: params?.severity ?? "error",
      check: async (df) => {
        const rows = await df.collect();
        const nullCount = rows.filter((r) => (r as any)[column] == null).length;
        return {
          expectation: `expect_not_null(${column})`,
          passed: nullCount === 0,
          severity: params?.severity ?? "error",
          details: {
            observed: nullCount,
            expected: 0,
            failingRows: nullCount,
            sampleFailures: rows.filter((r) => (r as any)[column] == null).slice(0, 5),
          },
        };
      },
    });
    return this;
  }

  expectUnique(
    column: keyof T & string,
    params?: { severity?: "error" | "warning" },
  ): ExpectationSuite<T> {
    this.expectations.push({
      name: `expect_unique(${column})`,
      severity: params?.severity ?? "error",
      check: async (df) => {
        const rows = await df.collect();
        const values = rows.map((r) => (r as any)[column]);
        const uniqueCount = new Set(values).size;
        const duplicates = values.length - uniqueCount;
        return {
          expectation: `expect_unique(${column})`,
          passed: duplicates === 0,
          severity: params?.severity ?? "error",
          details: {
            observed: `${duplicates} duplicates`,
            expected: "all unique",
            failingRows: duplicates,
          },
        };
      },
    });
    return this;
  }

  expectBetween(
    column: keyof T & string,
    params: { min: number; max: number; severity?: "error" | "warning" },
  ): ExpectationSuite<T> {
    this.expectations.push({
      name: `expect_between(${column}, ${params.min}, ${params.max})`,
      severity: params.severity ?? "error",
      check: async (df) => {
        const rows = await df.collect();
        const failing = rows.filter((r) => {
          const v = Number((r as any)[column]);
          return v < params.min || v > params.max;
        });
        return {
          expectation: `expect_between(${column}, ${params.min}, ${params.max})`,
          passed: failing.length === 0,
          severity: params.severity ?? "error",
          details: {
            observed: `${failing.length} out of range`,
            expected: `all between ${params.min} and ${params.max}`,
            failingRows: failing.length,
            sampleFailures: failing.slice(0, 5).map((r) => (r as any)[column]),
          },
        };
      },
    });
    return this;
  }

  expectMatch(
    column: keyof T & string,
    params: { pattern: RegExp; severity?: "error" | "warning" },
  ): ExpectationSuite<T> {
    this.expectations.push({
      name: `expect_match(${column}, ${params.pattern})`,
      severity: params.severity ?? "error",
      check: async (df) => {
        const rows = await df.collect();
        const failing = rows.filter(
          (r) => (r as any)[column] != null && !params.pattern.test(String((r as any)[column])),
        );
        return {
          expectation: `expect_match(${column}, ${params.pattern})`,
          passed: failing.length === 0,
          severity: params.severity ?? "error",
          details: {
            observed: `${failing.length} non-matching`,
            expected: `all match ${params.pattern}`,
            failingRows: failing.length,
            sampleFailures: failing.slice(0, 5).map((r) => (r as any)[column]),
          },
        };
      },
    });
    return this;
  }

  expectIn(
    column: keyof T & string,
    params: { values: unknown[]; severity?: "error" | "warning" },
  ): ExpectationSuite<T> {
    const allowedSet = new Set(params.values);
    this.expectations.push({
      name: `expect_in(${column}, [${params.values.join(", ")}])`,
      severity: params.severity ?? "error",
      check: async (df) => {
        const rows = await df.collect();
        const failing = rows.filter((r) => !allowedSet.has((r as any)[column]));
        return {
          expectation: `expect_in(${column})`,
          passed: failing.length === 0,
          severity: params.severity ?? "error",
          details: {
            observed: `${failing.length} invalid values`,
            expected: `all in [${params.values.join(", ")}]`,
            failingRows: failing.length,
            sampleFailures: failing.slice(0, 5).map((r) => (r as any)[column]),
          },
        };
      },
    });
    return this;
  }

  expectRowCount(params: {
    min?: number;
    max?: number;
    severity?: "error" | "warning";
  }): ExpectationSuite<T> {
    this.expectations.push({
      name: `expect_row_count(${params.min ?? 0}-${params.max ?? "∞"})`,
      severity: params.severity ?? "error",
      check: async (df) => {
        const count = await df.count();
        const passed =
          (params.min === undefined || count >= params.min) &&
          (params.max === undefined || count <= params.max);
        return {
          expectation: `expect_row_count`,
          passed,
          severity: params.severity ?? "error",
          details: {
            observed: count,
            expected: `${params.min ?? 0} - ${params.max ?? "∞"}`,
          },
        };
      },
    });
    return this;
  }

  expectFreshness(
    column: keyof T & string,
    params: { maxAgeMs: number; severity?: "error" | "warning" },
  ): ExpectationSuite<T> {
    this.expectations.push({
      name: `expect_freshness(${column}, ${params.maxAgeMs}ms)`,
      severity: params.severity ?? "error",
      check: async (df) => {
        const rows = await df.collect();
        if (rows.length === 0) {
          return {
            expectation: `expect_freshness(${column})`,
            passed: false,
            severity: params.severity ?? "error",
            details: { observed: "no rows", expected: `data within ${params.maxAgeMs}ms` },
          };
        }
        const latest = rows.reduce((max, r) => {
          const d = new Date((r as any)[column]).getTime();
          return d > max ? d : max;
        }, 0);
        const age = Date.now() - latest;
        return {
          expectation: `expect_freshness(${column})`,
          passed: age <= params.maxAgeMs,
          severity: params.severity ?? "error",
          details: {
            observed: `${Math.round(age / 1000)}s old`,
            expected: `within ${Math.round(params.maxAgeMs / 1000)}s`,
          },
        };
      },
    });
    return this;
  }

  expectReferentialIntegrity(
    column: keyof T & string,
    params: {
      referenceTable: DataFrame<Record<string, unknown>>;
      referenceColumn: string;
      severity?: "error" | "warning";
    },
  ): ExpectationSuite<T> {
    this.expectations.push({
      name: `expect_referential_integrity(${column} → ${params.referenceColumn})`,
      severity: params.severity ?? "error",
      check: async (df) => {
        const rows = await df.collect();
        const refRows = await params.referenceTable.collect();
        const refValues = new Set(refRows.map((r) => r[params.referenceColumn]));
        const orphans = rows.filter((r) => !refValues.has((r as any)[column]));
        return {
          expectation: `expect_referential_integrity(${column})`,
          passed: orphans.length === 0,
          severity: params.severity ?? "error",
          details: {
            observed: `${orphans.length} orphan rows`,
            expected: "all values exist in reference",
            failingRows: orphans.length,
            sampleFailures: orphans.slice(0, 5).map((r) => (r as any)[column]),
          },
        };
      },
    });
    return this;
  }

  expect(
    name: string,
    check: (df: DataFrame<T>) => Promise<boolean>,
    params?: { severity?: "error" | "warning"; description?: string },
  ): ExpectationSuite<T> {
    this.expectations.push({
      name,
      severity: params?.severity ?? "error",
      check: async (df) => {
        const passed = await check(df);
        return {
          expectation: name,
          passed,
          severity: params?.severity ?? "error",
          details: {
            observed: passed ? "passed" : "failed",
            expected: "pass",
          },
        };
      },
    });
    return this;
  }

  // =========================================================================
  // Validate — run all expectations
  // =========================================================================

  async validate(): Promise<ValidationResult> {
    const start = Date.now();
    const results: ExpectationResult[] = [];

    for (const exp of this.expectations) {
      results.push(await exp.check(this.df));
    }

    const passed = results.filter((r) => r.passed).length;
    const failed = results.filter((r) => !r.passed && r.severity === "error").length;
    const warnings = results.filter((r) => !r.passed && r.severity === "warning").length;

    return {
      passed: failed === 0,
      summary: { total: results.length, passed, failed, warnings },
      results,
      timestamp: new Date(),
      durationMs: Date.now() - start,
    };
  }
}
