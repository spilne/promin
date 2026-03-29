// ---------------------------------------------------------------------------
// File source adapters for DataFrame
//
// Each adapter provides:
//   - load(): async function to parse file into JS objects (works with any executor)
//   - hint: optional "format:path" string for executor-native loading
//
// The executor decides which to use:
//   - DuckDB: uses duckdbSql if present, otherwise calls load()
//   - Array: always calls load()
//
// Adding new formats: just create a function that returns { load, duckdbSql? }.
// No executor changes needed.
// ---------------------------------------------------------------------------

import type { Frameable, FrameSchema } from "../typeclasses/frameable.ts";
import type { Codec } from "../typeclasses/codec.ts";

/** Source descriptor that can provide data + optional executor hint. */
export interface FileSourceDescriptor {
  /** Async loader — parses file into JS objects. Works with any executor. */
  readonly load: () => Promise<unknown[]>;
  /**
   * Source hint — `"format:path"` string for executor-native loading.
   * Executors register handlers for formats they support.
   * If no handler matches, the executor falls back to `load()`.
   */
  readonly hint?: string;
}

const defaultCodec: Codec<any> = {
  encode: (v: any) => JSON.stringify(v),
  decode: (s: string) => JSON.parse(s),
};

const unknownSchema: FrameSchema = { columns: [] };

/**
 * CSV file source.
 * - Any executor: parses in JS
 * - DuckDB: `read_csv_auto()` natively
 */
export function CsvFile<T = Record<string, unknown>>(
  path: string,
  options?: { delimiter?: string; header?: boolean },
): Frameable<T> & FileSourceDescriptor {
  const delim = options?.delimiter ?? ",";

  return {
    schema: unknownSchema,
    codec: defaultCodec,
    hint: `csv:${path}`,
    async load(): Promise<T[]> {
      const fs = await import("fs");
      const content = fs.readFileSync(path, "utf-8");
      const lines = content.trim().split("\n");
      if (lines.length < 2) return [];
      const header = options?.header === false ? null : lines[0]!;
      const cols = header ? header.split(delim).map((c) => c.trim()) : [];
      const startIdx = header ? 1 : 0;
      return lines.slice(startIdx).map((line) => {
        const values = line.split(delim);
        const row: Record<string, unknown> = {};
        for (let i = 0; i < cols.length; i++) {
          const v = values[i]?.trim() ?? "";
          const num = Number(v);
          row[cols[i]!] = v === "" ? null : Number.isNaN(num) ? v : num;
        }
        return row as T;
      });
    },
  };
}

/**
 * Parquet file source.
 * - Any executor: parses via hyparquet (pure JS, zero native deps)
 * - DuckDB: `read_parquet()` natively
 */
export function ParquetFile<T = Record<string, unknown>>(
  path: string,
): Frameable<T> & FileSourceDescriptor {
  return {
    schema: unknownSchema,
    codec: defaultCodec,
    hint: `parquet:${path}`,
    async load(): Promise<T[]> {
      const { readFileSync } = await import("fs");
      const { parquetRead } = await import("hyparquet");
      const buffer = readFileSync(path);
      const arrayBuffer = buffer.buffer.slice(
        buffer.byteOffset,
        buffer.byteOffset + buffer.byteLength,
      );
      return new Promise<T[]>((resolve) => {
        parquetRead({
          file: arrayBuffer,
          rowFormat: "object",
          onComplete: (rows: Record<string, any>[]) => resolve(rows as T[]),
        });
      });
    },
  };
}

/**
 * JSON file source.
 * - Any executor: `JSON.parse()`
 * - DuckDB: `read_json_auto()` natively
 */
export function JsonFile<T = Record<string, unknown>>(
  path: string,
): Frameable<T> & FileSourceDescriptor {
  return {
    schema: unknownSchema,
    codec: defaultCodec,
    hint: `json:${path}`,
    async load(): Promise<T[]> {
      const fs = await import("fs");
      const content = fs.readFileSync(path, "utf-8");
      return JSON.parse(content);
    },
  };
}

/**
 * TSV file source (tab-separated values).
 * - Any executor: parses in JS
 * - DuckDB: `read_csv_auto()` with tab delimiter
 */
export function TsvFile<T = Record<string, unknown>>(
  path: string,
): Frameable<T> & FileSourceDescriptor {
  return CsvFile<T>(path, { delimiter: "\t" });
}

/** Type guard for file-backed sources with a load function. */
export function isFileSource(value: unknown): value is FileSourceDescriptor {
  return (
    value !== null &&
    typeof value === "object" &&
    "load" in value &&
    typeof (value as any).load === "function"
  );
}
