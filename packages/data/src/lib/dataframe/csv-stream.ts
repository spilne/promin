// ---------------------------------------------------------------------------
// Streaming CSV reader + writer — true disk-to-aggregate pipeline.
//
// CsvFile / CsvSink in file-source.ts / sink.ts buffer the entire file in
// memory. These helpers read and write incrementally so workloads in the
// millions of rows don't hit RSS ceilings on load/write.
//
// Not hooked into DataFrame yet — consumed directly by chunked-executor
// integration work (see promin-5q70).
// ---------------------------------------------------------------------------

import { createReadStream, createWriteStream } from "node:fs";
import { createInterface } from "node:readline";

export interface StreamingCsvOptions {
  delimiter?: string;
  /** True (default) if the first line is a header row. */
  header?: boolean;
}

/**
 * Async-iterable row reader — yields parsed objects line-by-line without
 * materializing the whole file in memory.
 *
 * Uses readline over a read stream, so the file is consumed in 64KB chunks
 * internally by Node's fs layer.
 */
export async function* streamingCsvRows<T = Record<string, unknown>>(
  path: string,
  options: StreamingCsvOptions = {},
): AsyncGenerator<T> {
  const delim = options.delimiter ?? ",";
  const hasHeader = options.header !== false;

  const stream = createReadStream(path, { encoding: "utf-8" });
  const rl = createInterface({ input: stream, crlfDelay: Infinity });

  let cols: string[] | null = null;
  let first = true;

  for await (const line of rl) {
    if (line.length === 0) continue;
    if (first && hasHeader) {
      cols = line.split(delim).map((c) => c.trim());
      first = false;
      continue;
    }
    first = false;
    const values = line.split(delim);
    const row: Record<string, unknown> = {};
    if (cols) {
      for (let i = 0; i < cols.length; i++) {
        const v = values[i]?.trim() ?? "";
        const num = Number(v);
        row[cols[i]!] = v === "" ? null : Number.isNaN(num) ? v : num;
      }
    } else {
      // Headerless — name columns c0, c1, ...
      for (let i = 0; i < values.length; i++) {
        const v = values[i]?.trim() ?? "";
        const num = Number(v);
        row[`c${i}`] = v === "" ? null : Number.isNaN(num) ? v : num;
      }
    }
    yield row as T;
  }
}

/**
 * Streaming CSV writer — accumulates lines into a memory buffer and flushes
 * to disk in 64KB chunks. Awaiting the stream's `drain` event per-line adds
 * an event-loop tick per write, which dominates at the 1M-row scale. This
 * write-through-buffer approach gives both bounded memory and reasonable
 * throughput.
 *
 * High-water mark defaults to 64KB; callers can override via
 * `bufferBytes` if they want to tune for smaller RSS or larger batches.
 */
export function StreamingCsvSink<T>(
  path: string,
  options: StreamingCsvOptions & { bufferBytes?: number } = {},
): { write: (row: T) => Promise<void>; end: () => Promise<void> } {
  const delim = options.delimiter ?? ",";
  const includeHeader = options.header !== false;
  const bufferBytes = options.bufferBytes ?? 64 * 1024;
  const stream = createWriteStream(path, { encoding: "utf-8" });
  let headerWritten = false;
  let buffer = "";

  function encodeCell(v: unknown): string {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return s.includes(delim) || s.includes('"') || s.includes("\n")
      ? `"${s.replace(/"/g, '""')}"`
      : s;
  }

  function flushIfFull(): Promise<void> {
    if (buffer.length < bufferBytes) return Promise.resolve();
    const toWrite = buffer;
    buffer = "";
    if (stream.write(toWrite)) return Promise.resolve();
    return new Promise((resolve) => stream.once("drain", resolve));
  }

  return {
    async write(row: T) {
      const obj = row as Record<string, unknown>;
      if (includeHeader && !headerWritten) {
        buffer += Object.keys(obj).join(delim) + "\n";
        headerWritten = true;
      }
      buffer += Object.values(obj).map(encodeCell).join(delim) + "\n";
      await flushIfFull();
    },
    end(): Promise<void> {
      return new Promise((resolve, reject) => {
        if (buffer.length > 0) stream.write(buffer);
        buffer = "";
        stream.end((err?: Error | null) => (err ? reject(err) : resolve()));
      });
    },
  };
}
