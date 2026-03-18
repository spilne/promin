// ---------------------------------------------------------------------------
// Profile types — result of DataFrame.profile()
// ---------------------------------------------------------------------------

export interface ProfileReport {
  readonly rowCount: number;
  readonly columnCount: number;
  readonly duplicateRows: number;
  readonly columns: Record<string, ColumnProfile>;
  readonly correlations: CorrelationPair[];
  readonly warnings: ProfileWarning[];
  readonly durationMs: number;
}

export type ColumnProfile = NumericProfile | StringProfile | BooleanProfile | DateProfile;

export interface NumericProfile {
  readonly type: "numeric";
  readonly nullCount: number;
  readonly nullPct: number;
  readonly uniqueCount: number;
  readonly mean: number;
  readonly median: number;
  readonly std: number;
  readonly min: number;
  readonly max: number;
  readonly percentiles: { p5: number; p25: number; p50: number; p75: number; p95: number };
  readonly zeros: number;
  readonly negatives: number;
}

export interface StringProfile {
  readonly type: "string";
  readonly nullCount: number;
  readonly nullPct: number;
  readonly uniqueCount: number;
  readonly avgLength: number;
  readonly minLength: number;
  readonly maxLength: number;
  readonly emptyStrings: number;
  readonly topValues: { value: string; count: number }[];
}

export interface BooleanProfile {
  readonly type: "boolean";
  readonly nullCount: number;
  readonly nullPct: number;
  readonly trueCount: number;
  readonly falseCount: number;
  readonly truePct: number;
}

export interface DateProfile {
  readonly type: "date";
  readonly nullCount: number;
  readonly nullPct: number;
  readonly min: string;
  readonly max: string;
  readonly uniqueCount: number;
}

export interface CorrelationPair {
  readonly pair: [string, string];
  readonly value: number;
  readonly strength: "weak" | "moderate" | "strong";
}

export type WarningType =
  | "high_nulls"
  | "high_cardinality"
  | "constant"
  | "duplicates"
  | "high_correlation"
  | "outliers";

export interface ProfileWarning {
  readonly severity: "info" | "warning" | "error";
  readonly column?: string;
  readonly message: string;
  readonly type: WarningType;
}
