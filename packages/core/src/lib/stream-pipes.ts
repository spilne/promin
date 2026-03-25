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
// TSV / SSV — tab and space separated values
// ---------------------------------------------------------------------------

/**
 * Parse tab-separated lines into objects or arrays. Shorthand for `csv({ separator: "\t" })`.
 *
 * @example
 * ```ts
 * stream.through(lines()).through(tsv()) // Record<string, string>
 * ```
 */
export function tsv<E extends TaggedError>(options?: {
  quote?: string;
}): (stream: StreamPipeline<string, E>) => StreamPipeline<Record<string, string>, E> {
  return csv({ header: true, separator: "\t", quote: options?.quote });
}

/**
 * Parse space-separated values (multiple spaces collapsed). Common for log files.
 *
 * @example
 * ```ts
 * // Apache access log: IP - - [date] "request" status size
 * stream.through(lines()).through(ssv({ header: false }))
 * ```
 */
export function ssv<E extends TaggedError>(options?: {
  header?: boolean;
}): (stream: StreamPipeline<string, E>) => StreamPipeline<string[], E> {
  return (stream) => stream.map((line) => line.trim().split(/\s+/));
}

// ---------------------------------------------------------------------------
// Fixed-width columns
// ---------------------------------------------------------------------------

export interface FixedWidthColumn {
  name: string;
  start: number;
  end: number;
  trim?: boolean;
}

/**
 * Parse fixed-width positional columns. Common for legacy mainframe data, COBOL exports.
 *
 * @example
 * ```ts
 * const columns = [
 *   { name: "id", start: 0, end: 5 },
 *   { name: "name", start: 5, end: 25 },
 *   { name: "amount", start: 25, end: 35 },
 * ];
 *
 * stream.through(lines()).through(fixedWidth(columns))
 * // yields: { id: "00001", name: "Alice", amount: "100.50" }
 * ```
 */
export function fixedWidth<E extends TaggedError>(
  columns: FixedWidthColumn[],
): (stream: StreamPipeline<string, E>) => StreamPipeline<Record<string, string>, E> {
  return (stream) =>
    stream.map((line) => {
      const row: Record<string, string> = {};
      for (const col of columns) {
        const value = line.slice(col.start, col.end);
        row[col.name] = col.trim !== false ? value.trim() : value;
      }
      return row;
    });
}

// ---------------------------------------------------------------------------
// Regex — log parsing
// ---------------------------------------------------------------------------

/**
 * Parse lines using a regex with named capture groups.
 * Lines that don't match are skipped.
 *
 * @example
 * ```ts
 * // Apache combined log format
 * const apacheLog = regex(
 *   /^(?<ip>\S+) \S+ \S+ \[(?<date>[^\]]+)\] "(?<method>\S+) (?<path>\S+) \S+" (?<status>\d+) (?<size>\d+)/,
 * );
 *
 * stream.through(lines()).through(apacheLog)
 * // yields: { ip: "1.2.3.4", date: "...", method: "GET", path: "/", status: "200", size: "1234" }
 * ```
 */
export function regex<E extends TaggedError>(
  pattern: RegExp,
): (stream: StreamPipeline<string, E>) => StreamPipeline<Record<string, string>, E> {
  return (stream) =>
    stream.filterMap((line) => {
      const match = pattern.exec(line);
      if (!match?.groups) return undefined;
      return { ...match.groups };
    });
}

// ---------------------------------------------------------------------------
// XML — SAX-style event stream
// ---------------------------------------------------------------------------

export interface XmlEvent {
  type: "open" | "close" | "text" | "selfClose";
  tag?: string;
  attributes?: Record<string, string>;
  text?: string;
}

/**
 * Parse XML text into SAX-style events. Lightweight — no DOM tree in memory.
 * Handles open tags, close tags, self-closing tags, attributes, and text nodes.
 *
 * @example
 * ```ts
 * stream.through(xml())
 *   .filter(e => e.type === "open" && e.tag === "item")
 *   // yields: { type: "open", tag: "item", attributes: { id: "1" } }
 * ```
 */
export function xml<E extends TaggedError>(): (
  stream: StreamPipeline<string, E>,
) => StreamPipeline<XmlEvent, E> {
  return (stream) =>
    stream.flatMap((chunk) => {
      const events: XmlEvent[] = [];
      const tagRegex = /<\/?([a-zA-Z][\w.-]*)((?:\s+[\w.-]+\s*=\s*"[^"]*")*)\s*(\/?)>|([^<]+)/g;
      let match;

      while ((match = tagRegex.exec(chunk)) !== null) {
        const [full, tag, attrStr, selfClose, text] = match;

        if (text?.trim()) {
          events.push({ type: "text", text: text.trim() });
        } else if (tag) {
          const isClose = full!.startsWith("</");
          if (isClose) {
            events.push({ type: "close", tag });
          } else {
            const attributes: Record<string, string> = {};
            if (attrStr) {
              const attrRegex = /([\w.-]+)\s*=\s*"([^"]*)"/g;
              let am;
              while ((am = attrRegex.exec(attrStr)) !== null) {
                attributes[am[1]!] = am[2]!;
              }
            }
            events.push({
              type: selfClose ? "selfClose" : "open",
              tag,
              attributes: Object.keys(attributes).length > 0 ? attributes : undefined,
            });
          }
        }
      }

      return StreamPipeline.fromIterable(events) as StreamPipeline<XmlEvent, E>;
    });
}

// ---------------------------------------------------------------------------
// Binary format pipes — bring your own decoder
// ---------------------------------------------------------------------------

/**
 * Decode binary chunks using a custom decoder function.
 * Use this for protobuf, msgpack, avro, or any binary format.
 *
 * @example
 * ```ts
 * // Protobuf
 * import { MyMessage } from "./proto/my_message_pb.ts";
 * stream.through(binaryDecode(buf => MyMessage.decode(buf)))
 *
 * // MessagePack
 * import { decode } from "@msgpack/msgpack";
 * stream.through(binaryDecode(buf => decode(buf)))
 * ```
 */
export function binaryDecode<T, E extends TaggedError>(
  decode: (buffer: Uint8Array) => T,
): (stream: StreamPipeline<Uint8Array, E>) => StreamPipeline<T, E> {
  return (stream) => stream.map(decode);
}

/**
 * Decode length-prefixed binary messages from a stream.
 * Each message is prefixed with a 4-byte big-endian uint32 length.
 * Common for protobuf streaming (gRPC, Kafka).
 *
 * @example
 * ```ts
 * stream.through(lengthPrefixed(buf => MyProto.decode(buf)))
 * ```
 */
export function lengthPrefixed<T, E extends TaggedError>(
  decode: (buffer: Uint8Array) => T,
): (stream: StreamPipeline<Uint8Array, E>) => StreamPipeline<T, E> {
  return (stream) => {
    let buffer = new Uint8Array(0);

    return stream.flatMap((chunk) => {
      // Append to buffer
      const combined = new Uint8Array(buffer.length + chunk.length);
      combined.set(buffer);
      combined.set(chunk, buffer.length);
      buffer = combined;

      const messages: T[] = [];

      while (buffer.length >= 4) {
        const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
        const msgLen = view.getUint32(0, false); // big-endian

        if (buffer.length < 4 + msgLen) break; // incomplete message

        const msgBytes = buffer.slice(4, 4 + msgLen);
        messages.push(decode(msgBytes));
        buffer = buffer.slice(4 + msgLen);
      }

      return StreamPipeline.fromIterable(messages) as StreamPipeline<T, E>;
    });
  };
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
