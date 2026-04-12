// ---------------------------------------------------------------------------
// DataFrameSink<T> — pluggable row-oriented sinks for DataFrame
//
// Unlike Sinkable<T> (which requires a Codec), DataFrameSink works with
// raw objects — no encoding layer needed.
// ---------------------------------------------------------------------------

/** Sink that receives rows from a DataFrame. */
export interface DataFrameSink<T> {
  /** Called for each row. Sinks buffer internally as needed. */
  write(row: T): Promise<void>;
  /** Called when all rows have been written. Flush buffers, close files. */
  end(): Promise<void>;
}

/** Write CSV to a file. Buffers rows in memory, flushes on `end()`. */
export function CsvSink<T>(
  path: string,
  options?: {
    delimiter?: string;
    header?: boolean;
  },
): DataFrameSink<T> {
  const delimiter = options?.delimiter ?? ",";
  const includeHeader = options?.header ?? true;
  const lines: string[] = [];
  let headerWritten = false;

  return {
    async write(row: T) {
      const obj = row as Record<string, unknown>;
      if (includeHeader && !headerWritten) {
        lines.push(Object.keys(obj).join(delimiter));
        headerWritten = true;
      }
      lines.push(
        Object.values(obj)
          .map((v) => {
            if (v === null || v === undefined) return "";
            const s = String(v);
            return s.includes(delimiter) || s.includes('"') || s.includes("\n")
              ? `"${s.replace(/"/g, '""')}"`
              : s;
          })
          .join(delimiter),
      );
    },
    async end() {
      const content = lines.join("\n") + "\n";
      const fs = await import("node:fs/promises");
      await fs.writeFile(path, content);
    },
  };
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
