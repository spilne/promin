// ---------------------------------------------------------------------------
// Expectation — a single data quality check
// ---------------------------------------------------------------------------

import type { DataFrame } from "../dataframe/dataframe.ts";

export interface ExpectationResult {
  readonly expectation: string;
  readonly passed: boolean;
  readonly severity: "error" | "warning";
  readonly details: {
    observed: unknown;
    expected: unknown;
    failingRows?: number;
    sampleFailures?: unknown[];
  };
}

export interface Expectation<T> {
  readonly name: string;
  readonly severity: "error" | "warning";
  check(df: DataFrame<T>): Promise<ExpectationResult>;
}

export interface ValidationResult {
  readonly passed: boolean;
  readonly summary: {
    total: number;
    passed: number;
    failed: number;
    warnings: number;
  };
  readonly results: ExpectationResult[];
  readonly timestamp: Date;
  readonly durationMs: number;
}
