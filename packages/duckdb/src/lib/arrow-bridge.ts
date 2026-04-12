// ---------------------------------------------------------------------------
// Arrow IPC bridge — zero-copy data transfer between JavaScript and DuckDB
//
// Instead of serializing rows to JSON, writing to a temp file, and loading
// via read_json_auto, this module converts JS objects to/from Arrow IPC
// buffers that DuckDB can ingest directly via register_buffer / arrowIPCAll.
//
// The DuckDB arrow extension is required and automatically installed from
// the community repository on first use.
//
// Benchmarks (100K rows, Apple M-series):
//   Input:  Arrow ~67ms vs JSON ~76ms  (1.1x faster)
//   Output: Arrow ~14ms vs JSON ~62ms  (4.4x faster)
// ---------------------------------------------------------------------------

import {
  tableToIPC,
  tableFromIPC,
  Table,
  vectorFromArray,
  Utf8,
  Bool,
  type DataType,
} from "apache-arrow";
import type { Database } from "duckdb-async";

/**
 * Load the DuckDB arrow extension (required for register_buffer / arrowIPCAll).
 * Installs from the community repository on first call, then loads per-database.
 *
 * IMPORTANT: Due to a Bun NAPI bug, loading the arrow extension into multiple
 * DuckDB instances causes a segfault on process exit. We track the number of
 * active arrow-loaded databases and only allow loading when safe.
 */
let arrowInstalled: Promise<void> | null = null;
let arrowLoadedCount = 0;
const MAX_ARROW_INSTANCES = 1;

export async function ensureArrowExtension(db: Database): Promise<void> {
  // Guard: only allow one arrow-loaded DB at a time to avoid Bun NAPI segfault
  if (arrowLoadedCount >= MAX_ARROW_INSTANCES) {
    throw new Error("Arrow extension already loaded in another DuckDB instance");
  }

  // INSTALL is global (downloads to ~/.duckdb/extensions), only needed once
  if (!arrowInstalled) {
    arrowInstalled = db.all("INSTALL arrow FROM community").then(() => {});
  }
  await arrowInstalled;
  // LOAD is per-database connection
  await db.all("LOAD arrow");
  arrowLoadedCount++;
}

/**
 * Signal that an arrow-loaded database was closed, freeing a slot.
 */
export function releaseArrowSlot(): void {
  if (arrowLoadedCount > 0) arrowLoadedCount--;
}

/**
 * Reset the install state — used when testing or after errors.
 */
export function resetArrowState(): void {
  arrowInstalled = null;
  arrowLoadedCount = 0;
}

/**
 * Convert an array of JS objects to Arrow IPC stream buffers for DuckDB register_buffer.
 *
 * Returns a Uint8Array[] (ArrowIterable) suitable for `db.register_buffer()`.
 * Empty input returns an empty array.
 *
 * NOTE: We avoid `tableFromJSON` because it dictionary-encodes string columns,
 * which DuckDB's arrow extension does not support. Instead we build the table
 * column-by-column using `vectorFromArray` with explicit non-dictionary types.
 */
export function rowsToArrow(rows: Record<string, unknown>[]): Uint8Array[] {
  if (rows.length === 0) return [];

  // Extract column names from first row
  const keys = Object.keys(rows[0]!);
  const columns: Record<string, any> = {};

  for (const key of keys) {
    const values = rows.map((r) => r[key]);
    const arrowType = inferArrowType(values);
    columns[key] = arrowType ? vectorFromArray(values, arrowType) : vectorFromArray(values);
  }

  const table = new Table(columns);
  const ipc = tableToIPC(table, "stream");
  return [ipc];
}

/**
 * Infer Arrow type for a column, returning an explicit type only for columns
 * that need it (strings → Utf8, booleans → Bool) to avoid dictionary encoding.
 * Returns null for numeric types which vectorFromArray handles correctly.
 */
function inferArrowType(values: unknown[]): DataType | null {
  for (const v of values) {
    if (v == null) continue;
    if (typeof v === "string") return new Utf8();
    if (typeof v === "boolean") return new Bool();
    return null; // numeric — let vectorFromArray infer
  }
  return null;
}

/**
 * Convert Arrow IPC buffers from DuckDB arrowIPCAll to JS objects.
 *
 * Handles BigInt → Number conversion (DuckDB integers come as BigInt via Arrow).
 */
export function arrowToRows<T>(buffers: Uint8Array[]): T[] {
  if (buffers.length === 0) return [];
  const table = tableFromIPC(buffers);
  return table.toArray().map((row: any) => {
    const obj: Record<string, unknown> = {};
    for (const field of table.schema.fields) {
      let val = row[field.name];
      if (typeof val === "bigint") val = Number(val);
      obj[field.name] = val;
    }
    return obj as unknown as T;
  });
}
