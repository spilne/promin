// ---------------------------------------------------------------------------
// DataFrameSink<T> — pluggable row-oriented sinks for DataFrame
//
// Unlike Sinkable<T> (which requires a Codec), DataFrameSink works with
// raw objects — no encoding layer needed.
// ---------------------------------------------------------------------------

import { StreamingCsvSink } from "./csv-stream.ts";

/** Sink that receives rows from a DataFrame. */
export interface DataFrameSink<T> {
  /** Called for each row. Sinks buffer internally as needed. */
  write(row: T): Promise<void>;
  /** Called when all rows have been written. Flush buffers, close files. */
  end(): Promise<void>;
}

/**
 * Write CSV to a file. Backed by a streaming writer that flushes in 64 KB
 * chunks so memory stays bounded regardless of row count.
 */
export function CsvSink<T>(
  path: string,
  options?: {
    delimiter?: string;
    header?: boolean;
  },
): DataFrameSink<T> {
  return StreamingCsvSink<T>(path, options);
}

/** Write JSONL (newline-delimited JSON) to a file. One JSON object per line. */
export function JsonlSink<T>(path: string): DataFrameSink<T> {
  const lines: string[] = [];

  return {
    async write(row: T) {
      lines.push(JSON.stringify(row));
    },
    async end() {
      const content = lines.join("\n") + "\n";
      const fs = await import("node:fs/promises");
      await fs.writeFile(path, content);
    },
  };
}
