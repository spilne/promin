// ---------------------------------------------------------------------------
// Profiler — comprehensive dataset analysis in one call
// ---------------------------------------------------------------------------

import type {
  ProfileReport,
  ColumnProfile,
  NumericProfile,
  StringProfile,
  BooleanProfile,
  DateProfile,
  CorrelationPair,
  ProfileWarning,
} from "./profile-types.ts";

export interface ProfileOptions {
  /** Compute correlations between numeric columns. Default: true. */
  correlations?: boolean;
  /** Max top values to report for string columns. Default: 10. */
  topValuesLimit?: number;
}

export async function profileData(
  rows: Record<string, unknown>[],
  options?: ProfileOptions,
): Promise<ProfileReport> {
  const start = Date.now();
  if (rows.length === 0) {
    return {
      rowCount: 0,
      columnCount: 0,
      duplicateRows: 0,
      columns: {},
      correlations: [],
      warnings: [],
      durationMs: Date.now() - start,
    };
  }

  const columns = Object.keys(rows[0]!);
  const rowCount = rows.length;

  // Duplicate row detection
  const rowStrings = new Set<string>();
  let duplicateRows = 0;
  for (const row of rows) {
    const key = JSON.stringify(row);
    if (rowStrings.has(key)) duplicateRows++;
    else rowStrings.add(key);
  }

  // Profile each column
  const columnProfiles: Record<string, ColumnProfile> = {};
  for (const col of columns) {
    const values = rows.map((r) => r[col]);
    columnProfiles[col] = profileColumn(col, values, rowCount, options);
  }

  // Correlations
  const correlations: CorrelationPair[] = [];
  if (options?.correlations !== false) {
    const numericCols = columns.filter((c) => columnProfiles[c]?.type === "numeric");
    for (let i = 0; i < numericCols.length; i++) {
      for (let j = i + 1; j < numericCols.length; j++) {
        const a = rows.map((r) => Number(r[numericCols[i]!]));
        const b = rows.map((r) => Number(r[numericCols[j]!]));
        const corr = pearsonCorrelation(a, b);
        if (!isNaN(corr)) {
          const abs = Math.abs(corr);
          correlations.push({
            pair: [numericCols[i]!, numericCols[j]!],
            value: Math.round(corr * 1000) / 1000,
            strength: abs >= 0.7 ? "strong" : abs >= 0.4 ? "moderate" : "weak",
          });
        }
      }
    }
  }

  // Warnings
  const warnings = generateWarnings(columnProfiles, rowCount, duplicateRows, correlations);

  return {
    rowCount,
    columnCount: columns.length,
    duplicateRows,
    columns: columnProfiles,
    correlations: correlations.filter((c) => c.strength !== "weak"),
    warnings,
    durationMs: Date.now() - start,
  };
}

// ---------------------------------------------------------------------------
// Column profiling
// ---------------------------------------------------------------------------

function profileColumn(
  name: string,
  values: unknown[],
  rowCount: number,
  options?: ProfileOptions,
): ColumnProfile {
  const nonNull = values.filter((v) => v != null);
  const nullCount = values.length - nonNull.length;
  const nullPct = rowCount > 0 ? Math.round((nullCount / rowCount) * 10000) / 100 : 0;

  if (nonNull.length === 0) {
    return {
      type: "string",
      nullCount,
      nullPct,
      uniqueCount: 0,
      avgLength: 0,
      minLength: 0,
      maxLength: 0,
      emptyStrings: 0,
      topValues: [],
    };
  }

  const sample = nonNull[0];

  if (typeof sample === "boolean") {
    return profileBoolean(nonNull as boolean[], nullCount, nullPct, rowCount);
  }

  if (
    typeof sample === "number" ||
    (typeof sample === "string" && !isNaN(Number(sample)) && sample !== "")
  ) {
    const nums = nonNull.map(Number).filter((n) => !isNaN(n));
    if (nums.length > nonNull.length * 0.8) {
      return profileNumeric(nums, nullCount, nullPct);
    }
  }

  if (
    sample instanceof Date ||
    (typeof sample === "string" &&
      !isNaN(Date.parse(sample as string)) &&
      (sample as string).length > 8)
  ) {
    const dates = nonNull.filter((v) => !isNaN(Date.parse(String(v))));
    if (dates.length > nonNull.length * 0.8) {
      return profileDate(dates.map(String), nullCount, nullPct);
    }
  }

  return profileString(nonNull.map(String), nullCount, nullPct, options?.topValuesLimit ?? 10);
}

function profileNumeric(values: number[], nullCount: number, nullPct: number): NumericProfile {
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  const sum = sorted.reduce((a, b) => a + b, 0);
  const mean = sum / n;
  const variance = sorted.reduce((a, v) => a + (v - mean) ** 2, 0) / n;

  return {
    type: "numeric",
    nullCount,
    nullPct,
    uniqueCount: new Set(values).size,
    mean: round(mean),
    median: round(sorted[Math.floor(n / 2)]!),
    std: round(Math.sqrt(variance)),
    min: sorted[0]!,
    max: sorted[n - 1]!,
    percentiles: {
      p5: round(sorted[Math.floor(n * 0.05)]!),
      p25: round(sorted[Math.floor(n * 0.25)]!),
      p50: round(sorted[Math.floor(n * 0.5)]!),
      p75: round(sorted[Math.floor(n * 0.75)]!),
      p95: round(sorted[Math.floor(n * 0.95)]!),
    },
    zeros: values.filter((v) => v === 0).length,
    negatives: values.filter((v) => v < 0).length,
  };
}

function profileString(
  values: string[],
  nullCount: number,
  nullPct: number,
  topLimit: number,
): StringProfile {
  const lengths = values.map((s) => s.length);
  const counts = new Map<string, number>();
  for (const v of values) counts.set(v, (counts.get(v) ?? 0) + 1);

  const topValues = [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, topLimit)
    .map(([value, count]) => ({ value, count }));

  return {
    type: "string",
    nullCount,
    nullPct,
    uniqueCount: counts.size,
    avgLength: round(lengths.reduce((a, b) => a + b, 0) / lengths.length),
    minLength: Math.min(...lengths),
    maxLength: Math.max(...lengths),
    emptyStrings: values.filter((s) => s === "").length,
    topValues,
  };
}

function profileBoolean(
  values: boolean[],
  nullCount: number,
  nullPct: number,
  rowCount: number,
): BooleanProfile {
  const trueCount = values.filter(Boolean).length;
  return {
    type: "boolean",
    nullCount,
    nullPct,
    trueCount,
    falseCount: values.length - trueCount,
    truePct: round((trueCount / values.length) * 100),
  };
}

function profileDate(values: string[], nullCount: number, nullPct: number): DateProfile {
  const timestamps = values.map((v) => new Date(v).getTime()).filter((t) => !isNaN(t));
  const sorted = [...timestamps].sort((a, b) => a - b);

  return {
    type: "date",
    nullCount,
    nullPct,
    min: sorted.length > 0 ? new Date(sorted[0]!).toISOString() : "",
    max: sorted.length > 0 ? new Date(sorted[sorted.length - 1]!).toISOString() : "",
    uniqueCount: new Set(values).size,
  };
}

// ---------------------------------------------------------------------------
// Correlations
// ---------------------------------------------------------------------------

function pearsonCorrelation(x: number[], y: number[]): number {
  const n = x.length;
  if (n < 3) return NaN;
  const xMean = x.reduce((a, b) => a + b, 0) / n;
  const yMean = y.reduce((a, b) => a + b, 0) / n;
  let num = 0;
  let denomX = 0;
  let denomY = 0;
  for (let i = 0; i < n; i++) {
    const dx = x[i]! - xMean;
    const dy = y[i]! - yMean;
    num += dx * dy;
    denomX += dx * dx;
    denomY += dy * dy;
  }
  const denom = Math.sqrt(denomX * denomY);
  return denom === 0 ? 0 : num / denom;
}

// ---------------------------------------------------------------------------
// Warnings
// ---------------------------------------------------------------------------

function generateWarnings(
  columns: Record<string, ColumnProfile>,
  rowCount: number,
  duplicateRows: number,
  correlations: CorrelationPair[],
): ProfileWarning[] {
  const warnings: ProfileWarning[] = [];

  if (duplicateRows > 0) {
    warnings.push({
      severity: duplicateRows > rowCount * 0.1 ? "warning" : "info",
      message: `${duplicateRows} duplicate rows (${round((duplicateRows / rowCount) * 100)}%)`,
      type: "duplicates",
    });
  }

  for (const [col, profile] of Object.entries(columns)) {
    if (profile.nullPct > 50) {
      warnings.push({
        severity: "warning",
        column: col,
        message: `${profile.nullPct}% null values`,
        type: "high_nulls",
      });
    }

    if (profile.type === "string" && profile.uniqueCount === rowCount && rowCount > 10) {
      warnings.push({
        severity: "info",
        column: col,
        message: `all values unique — possible ID column (${profile.uniqueCount} unique)`,
        type: "high_cardinality",
      });
    }

    const uniqueCount = profile.type !== "boolean" ? profile.uniqueCount : undefined;
    if (uniqueCount === 1 && rowCount > 1) {
      warnings.push({
        severity: "warning",
        column: col,
        message: "constant value — column carries no information",
        type: "constant",
      });
    }
  }

  for (const corr of correlations) {
    if (corr.strength === "strong") {
      warnings.push({
        severity: "info",
        message: `strong correlation (${corr.value}) between ${corr.pair[0]} and ${corr.pair[1]}`,
        type: "high_correlation",
      });
    }
  }

  return warnings;
}

function round(n: number): number {
  return Math.round(n * 100) / 100;
}
