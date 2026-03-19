// ---------------------------------------------------------------------------
// dataDiff — compare two datasets row by row
// ---------------------------------------------------------------------------

import type { DataDiffResult, DiffOptions, Modification, SchemaDiffResult } from "./diff-types.ts";

export function dataDiff(
  beforeRows: Record<string, unknown>[],
  afterRows: Record<string, unknown>[],
  options: DiffOptions,
): DataDiffResult {
  const { key, tolerance = 0, sampleModifications = 100 } = options;

  // Index by key
  const beforeMap = new Map<unknown, Record<string, unknown>>();
  for (const row of beforeRows) beforeMap.set(row[key], row);

  const afterMap = new Map<unknown, Record<string, unknown>>();
  for (const row of afterRows) afterMap.set(row[key], row);

  // Determine columns to compare
  const allColumns =
    options.columns ??
    [
      ...new Set([
        ...(beforeRows[0] ? Object.keys(beforeRows[0]) : []),
        ...(afterRows[0] ? Object.keys(afterRows[0]) : []),
      ]),
    ].filter((c) => c !== key);

  // Added rows (in after, not in before)
  const addedRows: Record<string, unknown>[] = [];
  for (const [k, row] of afterMap) {
    if (!beforeMap.has(k)) addedRows.push(row);
  }

  // Removed rows (in before, not in after)
  const removedRows: Record<string, unknown>[] = [];
  for (const [k, row] of beforeMap) {
    if (!afterMap.has(k)) removedRows.push(row);
  }

  // Modified rows
  const modifications: Modification[] = [];
  let modifiedCount = 0;
  let unchangedCount = 0;

  for (const [k, beforeRow] of beforeMap) {
    const afterRow = afterMap.get(k);
    if (!afterRow) continue;

    let rowModified = false;
    for (const col of allColumns) {
      const bVal = beforeRow[col];
      const aVal = afterRow[col];

      if (!valuesEqual(bVal, aVal, tolerance)) {
        rowModified = true;
        if (modifications.length < sampleModifications) {
          modifications.push({ key: k, column: col, before: bVal, after: aVal });
        }
      }
    }

    if (rowModified) modifiedCount++;
    else unchangedCount++;
  }

  return {
    summary: {
      added: addedRows.length,
      removed: removedRows.length,
      modified: modifiedCount,
      unchanged: unchangedCount,
      total: beforeRows.length + addedRows.length,
    },
    addedRows,
    removedRows,
    modifications,
  };
}

function valuesEqual(a: unknown, b: unknown, tolerance: number): boolean {
  if (a === b) return true;
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  if (typeof a === "number" && typeof b === "number" && tolerance > 0) {
    return Math.abs(a - b) <= tolerance;
  }
  return JSON.stringify(a) === JSON.stringify(b);
}

export function schemaDiff(beforeColumns: string[], afterColumns: string[]): SchemaDiffResult {
  const beforeSet = new Set(beforeColumns);
  const afterSet = new Set(afterColumns);

  const addedColumns = afterColumns.filter((c) => !beforeSet.has(c));
  const removedColumns = beforeColumns.filter((c) => !afterSet.has(c));

  return {
    addedColumns,
    removedColumns,
    compatible: removedColumns.length === 0, // non-breaking if only additions
  };
}
