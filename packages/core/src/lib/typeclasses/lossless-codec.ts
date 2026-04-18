// ---------------------------------------------------------------------------
// LosslessJsonCodec — superjson-style serializer.
//
// The identity `JsonCodec` silently breaks when a step returns values that
// JSON can't represent: `Date` becomes a string on the replay side, `BigInt`
// throws, `Map`/`Set` collapse to `{}`/`[]`, and so on. The pipeline runs on
// in-memory storage look fine (no round trip), but the same workflow blows up
// in prod under Postgres or Redis because they do JSON.stringify/parse.
//
// This codec wraps the problem types as plain JSON objects tagged with `__t`
// so that a round trip through JSON.stringify → JSON.parse → decode recovers
// the original value. Output is always JSON-native, so downstream storage
// layers can keep doing JSON.stringify on it.
//
// Supported: Date, BigInt, Map, Set, Error, RegExp, URL, undefined, NaN,
// ±Infinity, -0. Anything else JSON-native passes through untouched.
// ---------------------------------------------------------------------------

import type { Codec } from "./codec.ts";

type Tag =
  | "undefined"
  | "nan"
  | "inf"
  | "-inf"
  | "-0"
  | "bigint"
  | "date"
  | "map"
  | "set"
  | "error"
  | "regexp"
  | "url"
  | "raw";

interface Tagged {
  __t: Tag;
  v?: unknown;
}

function isTagged(value: unknown): value is Tagged {
  return typeof value === "object" && value !== null && "__t" in value;
}

function encodeValue(value: unknown): unknown {
  if (value === undefined) return { __t: "undefined" };
  if (value === null) return null;

  if (typeof value === "number") {
    if (Number.isNaN(value)) return { __t: "nan" };
    if (value === Infinity) return { __t: "inf" };
    if (value === -Infinity) return { __t: "-inf" };
    if (value === 0 && 1 / value === -Infinity) return { __t: "-0" };
    return value;
  }

  if (typeof value === "bigint") return { __t: "bigint", v: value.toString() };
  if (typeof value === "function") {
    throw new TypeError("LosslessJsonCodec: cannot serialize a function");
  }
  if (typeof value === "symbol") {
    throw new TypeError("LosslessJsonCodec: cannot serialize a symbol");
  }
  if (typeof value !== "object") return value; // string, boolean

  if (value instanceof Date) return { __t: "date", v: value.toISOString() };
  if (value instanceof RegExp)
    return { __t: "regexp", v: { source: value.source, flags: value.flags } };
  if (value instanceof URL) return { __t: "url", v: value.href };
  if (value instanceof Map) {
    const entries: [unknown, unknown][] = [];
    for (const [k, v] of value) entries.push([encodeValue(k), encodeValue(v)]);
    return { __t: "map", v: entries };
  }
  if (value instanceof Set) {
    return { __t: "set", v: [...value].map(encodeValue) };
  }
  if (value instanceof Error) {
    return {
      __t: "error",
      v: { name: value.name, message: value.message, stack: value.stack ?? null },
    };
  }
  if (Array.isArray(value)) return value.map(encodeValue);

  // Plain object. If it happens to carry a `__t` key, escape the whole object
  // so the decoder treats it as a literal and doesn't try to hydrate it.
  if (isTagged(value)) {
    const cloned: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) cloned[k] = encodeValue(v);
    return { __t: "raw", v: cloned };
  }

  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value)) out[k] = encodeValue(v);
  return out;
}

function decodeValue(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(decodeValue);

  if (isTagged(value)) {
    switch (value.__t) {
      case "undefined":
        return undefined;
      case "nan":
        return NaN;
      case "inf":
        return Infinity;
      case "-inf":
        return -Infinity;
      case "-0":
        return -0;
      case "bigint":
        return BigInt(value.v as string);
      case "date":
        return new Date(value.v as string);
      case "regexp": {
        const { source, flags } = value.v as { source: string; flags: string };
        return new RegExp(source, flags);
      }
      case "url":
        return new URL(value.v as string);
      case "map": {
        const entries = value.v as [unknown, unknown][];
        return new Map(entries.map(([k, v]) => [decodeValue(k), decodeValue(v)]));
      }
      case "set":
        return new Set((value.v as unknown[]).map(decodeValue));
      case "error": {
        const { name, message, stack } = value.v as {
          name: string;
          message: string;
          stack: string | null;
        };
        const err = new Error(message);
        err.name = name;
        if (stack !== null) err.stack = stack;
        return err;
      }
      case "raw":
        // Literal object that happens to have a __t field. Decode its inner
        // values but don't try to hydrate the outer object.
        return decodePlainObject(value.v as Record<string, unknown>);
    }
  }

  return decodePlainObject(value as Record<string, unknown>);
}

function decodePlainObject(obj: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) out[k] = decodeValue(v);
  return out;
}

/**
 * Default codec for step and activity results. Round-trips Date / BigInt /
 * Map / Set / Error / RegExp / URL / undefined / NaN / ±Infinity / -0 without
 * loss. JSON-native values pass through unchanged.
 */
export const LosslessJsonCodec: Codec<unknown> = {
  encode: encodeValue,
  decode: decodeValue,
};
