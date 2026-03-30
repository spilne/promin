// ---------------------------------------------------------------------------
// File source adapters for DataFrame
//
// These implement Frameable<T> so they work with DataFrame.from().
// They also carry file metadata so DuckDB can use native readers.
//
// Two loading paths:
//   - DuckDBExecutor sees frameable.path → uses read_csv_auto/read_parquet (fast)
//   - ArrayExecutor calls frameable.load() → parses in JS (universal)
//
// Usage:
//   // Lazy — file read deferred, executor chooses strategy
//   DataFrame.fromFile(CsvFile("sales.csv")).withExecutor(duckdb)
//
//   // Eager — loads via Frameable.load(), always JS parsing
//   await DataFrame.from(CsvFile("sales.csv"))
// ---------------------------------------------------------------------------

import type { Frameable, FrameSchema } from "../typeclasses/frameable.ts";
import type { Codec } from "../typeclasses/codec.ts";

/** Metadata carried on SourcePlan for executor-native file reading. */
export interface FileSourceDescriptor {
  readonly path: string;
  readonly format: "csv" | "parquet" | "json";
  readonly options?: Record<string, unknown>;
}

const defaultCodec: Codec<any> = {
  encode: (v: any) => JSON.stringify(v),
  decode: (s: string) => JSON.parse(s),
};

const unknownSchema: FrameSchema = { columns: [] };

/**
 * CSV file source. Implements Frameable for eager loading via DataFrame.from().
 * Also works with DataFrame.fromFile() for lazy/executor-native loading.
 */
export function CsvFile<T = Record<string, unknown>>(
  path: string,
  options?: { delimiter?: string; header?: boolean },
): Frameable<T> & FileSourceDescriptor {
  return {
    path,
    format: "csv",
    options: options as Record<string, unknown>,
    schema: unknownSchema,
    codec: defaultCodec,
    async load(): Promise<T[]> {
      const fs = await import("fs");
      const content = fs.readFileSync(path, "utf-8");
      const lines = content.trim().split("\n");
      if (lines.length < 2) return [];
      const header = options?.header === false ? null : lines[0]!;
      const delim = options?.delimiter ?? ",";
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
 * Parquet file source. Requires DuckDB executor for reading.
 * ArrayExecutor will throw — Parquet parsing needs a native engine.
 */
export function ParquetFile<T = Record<string, unknown>>(
  path: string,
): Frameable<T> & FileSourceDescriptor {
  return {
    path,
    format: "parquet",
    schema: unknownSchema,
    codec: defaultCodec,
    async load(): Promise<T[]> {
      throw new Error(
        `ParquetFile("${path}") requires DuckDBExecutor. Use .withExecutor(new DuckDBExecutor()) or use DataFrame.from() with a DuckDB-backed executor.`,
      );
    },
  };
}

/**
 * JSON file source. Implements Frameable for eager loading.
 * DuckDB reads natively via read_json_auto.
 */
export function JsonFile<T = Record<string, unknown>>(
  path: string,
): Frameable<T> & FileSourceDescriptor {
  return {
    path,
    format: "json",
    schema: unknownSchema,
    codec: defaultCodec,
    async load(): Promise<T[]> {
      const fs = await import("fs");
      const content = fs.readFileSync(path, "utf-8");
      return JSON.parse(content);
    },
  };
}

/** Type guard for file-backed Frameable sources. */
export function isFileSource(value: unknown): value is FileSourceDescriptor {
  return (
    value !== null &&
    typeof value === "object" &&
    "path" in value &&
    "format" in value &&
    typeof (value as any).path === "string"
  );
}
