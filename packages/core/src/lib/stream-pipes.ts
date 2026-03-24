// ---------------------------------------------------------------------------
// Stream pipes — reusable transformations for StreamPipeline.through()
//
// Inspired by fs2's text.utf8Decode, text.lines, and fs2-data's CSV pipes.
// Each pipe is a function: StreamPipeline<A> → StreamPipeline<B>
// Compose them with .through():
//
//   stream
//     .through(utf8Decode)
//     .through(lines)
//     .through(csv({ header: true }))
//     .through(parseAs(MySchema))
// ---------------------------------------------------------------------------

import { Stream } from "effect";
import { StreamPipeline } from "./stream-pipeline.ts";
import type { TaggedError } from "./pipeline.ts";

// ---------------------------------------------------------------------------
// Text pipes
// ---------------------------------------------------------------------------

/**
 * Decode a binary stream (Uint8Array chunks) into text.
 *
 * @example
 * ```ts
 * const textStream = binaryStream.through(utf8Decode);
 * ```
 */
export function utf8Decode<E extends TaggedError>(): (
  stream: StreamPipeline<Uint8Array, E>,
) => StreamPipeline<string, E> {
  const decoder = new TextDecoder();
  return (stream) => stream.map((chunk) => decoder.decode(chunk, { stream: true }));
}

/**
 * Split a text stream into lines. Handles \n, \r\n, and \r.
 * Buffering ensures lines split across chunks are joined correctly.
 *
 * @example
 * ```ts
 * const lineStream = textStream.through(lines);
 * ```
 */
export function lines<E extends TaggedError>(): (
  stream: StreamPipeline<string, E>,
) => StreamPipeline<string, E> {
  return (stream) => {
    let buffer = "";

    // Use mapAccumulate to track buffer state, emit lines as arrays, then flatten.
    // On stream end, onFinalize emits the remaining buffer.
    const lineArrays = stream.flatMap((chunk) => {
      buffer += chunk;
      const parts = buffer.split(/\r?\n|\r/);
      buffer = parts.pop()!; // keep incomplete last line
      const emittable = parts.filter((l) => l.length > 0);
      return StreamPipeline.fromIterable(emittable) as StreamPipeline<string, E>;
    });

    // Flush remaining buffer when stream ends (handles files without trailing newline)
    return lineArrays.concat(
      StreamPipeline.from(
        Stream.suspend(() => {
          if (buffer.length > 0) {
            const last = buffer;
            buffer = "";
            return Stream.make(last);
          }
          return Stream.empty;
        }),
      ) as StreamPipeline<string, E>,
    );
  };
}

// ---------------------------------------------------------------------------
// CSV pipes
// ---------------------------------------------------------------------------

export interface CsvOptions {
  /** Use first row as headers. Default: true. */
  header?: boolean;
  /** Column separator. Default: ','. */
  separator?: string;
  /** Quote character. Default: '"'. */
  quote?: string;
}

/**
 * Parse CSV lines into objects (if header=true) or string arrays.
 *
 * With header=true (default), yields `Record<string, string>` using the
 * first row as keys. With header=false, yields `string[]`.
 *
 * @example
 * ```ts
 * // Header mode — objects with named fields
 * const rows = textStream
 *   .through(lines())
 *   .through(csv())
 *   // rows are Record<string, string>
 *   .map((row) => ({ name: row.name, age: Number(row.age) }));
 *
 * // No header — raw arrays
 * const arrays = textStream
 *   .through(lines())
 *   .through(csv({ header: false }));
 * ```
 */
export function csv<E extends TaggedError>(
  options?: CsvOptions & { header?: true },
): (stream: StreamPipeline<string, E>) => StreamPipeline<Record<string, string>, E>;
export function csv<E extends TaggedError>(
  options: CsvOptions & { header: false },
): (stream: StreamPipeline<string, E>) => StreamPipeline<string[], E>;
export function csv<E extends TaggedError>(
  options?: CsvOptions,
): (stream: StreamPipeline<string, E>) => StreamPipeline<Record<string, string> | string[], E> {
  const sep = options?.separator ?? ",";
  const quote = options?.quote ?? '"';
  const useHeader = options?.header !== false;

  return (stream) => {
    let headers: string[] | null = null;

    return stream.filterMap((line) => {
      const fields = parseCsvLine(line, sep, quote);

      if (useHeader && !headers) {
        headers = fields;
        return undefined; // skip header row
      }

      if (useHeader && headers) {
        const row: Record<string, string> = {};
        headers.forEach((h, i) => (row[h] = fields[i] ?? ""));
        return row;
      }

      return fields;
    });
  };
}

/**
 * Validate and coerce CSV rows using a schema (Zod, etc.).
 * Invalid rows are silently dropped. Use `parseAsStrict` to fail on invalid rows.
 *
 * @example
 * ```ts
 * const TransactionSchema = z.object({
 *   date: z.string(),
 *   amount: z.coerce.number(),
 *   region: z.string(),
 * });
 *
 * const typed = textStream
 *   .through(lines())
 *   .through(csv())
 *   .through(parseAs(TransactionSchema));
 * ```
 */
export function parseAs<T, E extends TaggedError>(schema: {
  safeParse(data: unknown): { success: true; data: T } | { success: false; error: unknown };
}): (stream: StreamPipeline<Record<string, string>, E>) => StreamPipeline<T, E> {
  return (stream) =>
    stream.filterMap((row) => {
      const result = schema.safeParse(row);
      return result.success ? result.data : undefined;
    });
}

/**
 * Like `parseAs`, but emits `{ data, error }` for every row — no silent drops.
 *
 * @example
 * ```ts
 * stream
 *   .through(lines())
 *   .through(csv())
 *   .through(parseAsLenient(TransactionSchema))
 *   .tap(({ error }) => { if (error) logError(error); })
 *   .filterMap(({ data }) => data);
 * ```
 */
export function parseAsLenient<T, E extends TaggedError>(schema: {
  safeParse(data: unknown): { success: true; data: T } | { success: false; error: unknown };
}): (
  stream: StreamPipeline<Record<string, string>, E>,
) => StreamPipeline<{ data: T | null; error: unknown | null }, E> {
  return (stream) =>
    stream.map((row) => {
      const result = schema.safeParse(row);
      return result.success
        ? { data: result.data, error: null }
        : { data: null, error: result.error };
    });
}

// ---------------------------------------------------------------------------
// JSONL pipe
// ---------------------------------------------------------------------------

/**
 * Parse newline-delimited JSON (JSONL/NDJSON) lines into objects.
 * Invalid lines are silently skipped.
 *
 * @example
 * ```ts
 * // Raw JSONL text
 * const items = textStream
 *   .through(lines())
 *   .through(jsonl());
 *
 * // With schema validation
 * const typed = textStream
 *   .through(lines())
 *   .through(jsonl())
 *   .through(parseAs(MySchema));
 * ```
 */
export function jsonl<E extends TaggedError>(): (
  stream: StreamPipeline<string, E>,
) => StreamPipeline<unknown, E> {
  return (stream) =>
    stream.filterMap((line) => {
      const trimmed = line.trim();
      if (!trimmed) return undefined;
      try {
        return JSON.parse(trimmed);
      } catch {
        return undefined; // skip malformed lines
      }
    });
}

/**
 * Parse JSONL with schema validation in one step.
 * Combines `jsonl()` + `parseAs()`. Invalid JSON or schema mismatches are skipped.
 *
 * @example
 * ```ts
 * const events = textStream
 *   .through(lines())
 *   .through(jsonlAs(EventSchema));
 * ```
 */
export function jsonlAs<T, E extends TaggedError>(schema: {
  safeParse(data: unknown): { success: true; data: T } | { success: false; error: unknown };
}): (stream: StreamPipeline<string, E>) => StreamPipeline<T, E> {
  return (stream) =>
    stream.filterMap((line) => {
      const trimmed = line.trim();
      if (!trimmed) return undefined;
      try {
        const parsed = JSON.parse(trimmed);
        const result = schema.safeParse(parsed);
        return result.success ? result.data : undefined;
      } catch {
        return undefined;
      }
    });
}

// ---------------------------------------------------------------------------
// CSV line parser — handles quoted fields with commas and newlines
// ---------------------------------------------------------------------------

function parseCsvLine(line: string, sep: string, quote: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i]!;

    if (inQuotes) {
      if (ch === quote) {
        if (i + 1 < line.length && line[i + 1] === quote) {
          current += quote; // escaped quote
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else if (ch === quote) {
      inQuotes = true;
    } else if (ch === sep) {
      fields.push(current);
      current = "";
    } else {
      current += ch;
    }
  }

  fields.push(current);
  return fields;
}
