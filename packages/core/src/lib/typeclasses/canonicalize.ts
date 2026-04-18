// ---------------------------------------------------------------------------
// canonicalize + payloadHash — deterministic fingerprint of an arbitrary value
// ---------------------------------------------------------------------------
//
// `canonicalJSON` produces a byte-for-byte stable JSON encoding of a value:
// object keys sorted, Set/Map entries ordered, Date/BigInt tagged. Two values
// that are structurally equal encode to identical strings regardless of the
// key insertion order or platform.
//
// `payloadHash` runs canonicalJSON through SHA-256 and returns a 64-char hex
// digest — the fingerprint stored alongside a journaled activity entry so the
// engine can detect payload drift between a fresh run and a replay. A change
// as small as one property rename or one numeric value edit flips the hash.
//
// Scope: mirrors LosslessJsonCodec's coverage (Date, BigInt, Map, Set,
// RegExp, URL, Error, NaN, ±Infinity, -0, undefined) so everything an
// activity input could reasonably carry hashes cleanly. Functions, class
// instances with private state, and cyclic structures aren't supported —
// they throw.
// ---------------------------------------------------------------------------

import { createHash } from "node:crypto";

/**
 * Canonical JSON encoding of `value`. Deterministic across runs and
 * processes: object keys are sorted, Set members are stringified and
 * sorted, Map entries are sorted by key. Typed wrappers (`{ "$date": ... }`
 * etc.) tag non-JSON primitives so equal inputs encode identically even
 * when a round-trip through JSON would erase the type.
 *
 * Throws on cycles and on values that can't be represented (functions,
 * symbols). Use this only on data you'd be willing to store in a journal
 * row.
 */
export function canonicalJSON(value: unknown): string {
  const seen = new WeakSet<object>();
  return stringify(value, seen);
}

/**
 * Hex SHA-256 digest of `canonicalJSON(value)`. 64 chars. Used by the
 * workflow engine's `payloadHash` option to detect silent activity-input
 * drift across a restart.
 */
export function payloadHash(value: unknown): string {
  return createHash("sha256").update(canonicalJSON(value)).digest("hex");
}

function stringify(value: unknown, seen: WeakSet<object>): string {
  if (value === null) return "null";
  if (value === undefined) return '{"$undefined":true}';

  switch (typeof value) {
    case "string":
      return JSON.stringify(value);
    case "boolean":
      return value ? "true" : "false";
    case "number":
      return encodeNumber(value);
    case "bigint":
      return `{"$bigint":"${value.toString()}"}`;
    case "function":
    case "symbol":
      throw new TypeError(`canonicalJSON: cannot serialize ${typeof value}`);
  }

  if (value instanceof Date) {
    const t = value.getTime();
    // Invalid dates encode distinctly so they don't collide with epoch 0.
    return Number.isNaN(t) ? '{"$date":"invalid"}' : `{"$date":${t}}`;
  }
  if (value instanceof RegExp) {
    return `{"$regexp":${JSON.stringify(value.source)},"flags":${JSON.stringify(value.flags)}}`;
  }
  if (value instanceof URL) {
    return `{"$url":${JSON.stringify(value.toString())}}`;
  }
  if (value instanceof Error) {
    return `{"$error":{"name":${JSON.stringify(value.name)},"message":${JSON.stringify(value.message)}}}`;
  }
  if (value instanceof Map) {
    assertUnseen(value, seen);
    const entries = [...value.entries()]
      .map(([k, v]) => [stringify(k, seen), stringify(v, seen)] as const)
      // Sort by the canonicalized key so insertion order doesn't leak.
      .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
    const body = entries.map(([k, v]) => `[${k},${v}]`).join(",");
    return `{"$map":[${body}]}`;
  }
  if (value instanceof Set) {
    assertUnseen(value, seen);
    const members = [...value.values()].map((v) => stringify(v, seen)).sort();
    return `{"$set":[${members.join(",")}]}`;
  }
  if (Array.isArray(value)) {
    assertUnseen(value, seen);
    return `[${value.map((v) => stringify(v, seen)).join(",")}]`;
  }

  // Plain object — sort keys so `{ a, b }` and `{ b, a }` hash the same.
  const obj = value as Record<string, unknown>;
  assertUnseen(obj, seen);
  const keys = Object.keys(obj).sort();
  const body = keys.map((k) => `${JSON.stringify(k)}:${stringify(obj[k], seen)}`).join(",");
  return `{${body}}`;
}

function encodeNumber(n: number): string {
  if (Number.isNaN(n)) return '{"$nan":true}';
  if (n === Infinity) return '{"$inf":1}';
  if (n === -Infinity) return '{"$inf":-1}';
  // Distinguish -0 from 0 so they don't collide.
  if (n === 0 && 1 / n === -Infinity) return '{"$negzero":true}';
  // JSON.stringify produces the shortest round-trippable form for finite
  // numbers — stable enough that `1` and `1.0` both encode to `"1"`.
  return JSON.stringify(n);
}

function assertUnseen(v: object, seen: WeakSet<object>): void {
  if (seen.has(v)) {
    throw new TypeError("canonicalJSON: cannot serialize cyclic structure");
  }
  seen.add(v);
}
