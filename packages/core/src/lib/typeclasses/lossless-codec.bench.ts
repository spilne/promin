// ---------------------------------------------------------------------------
// LosslessJsonCodec micro-benchmarks.
//
// Measures the per-operation cost of the new default codec relative to the
// identity JsonCodec and raw JSON.stringify/parse. Each step and activity
// boundary pays two of these operations (encode + decode), so even a small
// per-call overhead shows up in workflows that chain many steps.
//
// Run:  bun packages/core/src/lib/typeclasses/lossless-codec.bench.ts
// ---------------------------------------------------------------------------

import { group, bench, run } from "mitata";
import { JsonCodec, LosslessJsonCodec } from "./index.ts";

// ---------------------------------------------------------------------------
// Payload fixtures
// ---------------------------------------------------------------------------

const tiny = { a: 1, b: "hello" };

const smallJsonNative = {
  id: "u-12345",
  email: "alice@example.com",
  age: 30,
  active: true,
  tags: ["admin", "beta"],
  score: 98.6,
  region: "us-east",
  created_by: "system",
  nested: { foo: "bar", count: 10 },
};

function makeFlatMedium() {
  const obj: Record<string, unknown> = {};
  for (let i = 0; i < 100; i++) obj[`key_${i}`] = i % 2 === 0 ? `string-${i}` : i;
  return obj;
}
const flatMedium = makeFlatMedium();

function makeDeepNested(depth: number): unknown {
  let out: any = { leaf: "value", value: 42 };
  for (let i = 0; i < depth; i++) out = { nested: out, level: i };
  return out;
}
const deepNested = makeDeepNested(20);

const withDate = {
  id: "evt-1",
  createdAt: new Date("2026-04-16T00:00:00Z"),
  updatedAt: new Date("2026-04-16T12:00:00Z"),
  payload: { amount: 100, region: "us-east" },
};

const withMixedSpecial = {
  id: "mixed",
  createdAt: new Date("2026-04-16T00:00:00Z"),
  balance: 12345678901234567890n,
  tags: new Set(["a", "b", "c"]),
  meta: new Map<string, unknown>([
    ["last_login", new Date("2026-04-15T12:00:00Z")],
    ["is_admin", true],
  ]),
};

function makeLargeSet(n: number) {
  const s = new Set<number>();
  for (let i = 0; i < n; i++) s.add(i);
  return s;
}
const largeSet = makeLargeSet(1_000);

function makeLargeMap(n: number) {
  const m = new Map<string, number>();
  for (let i = 0; i < n; i++) m.set(`key_${i}`, i);
  return m;
}
const largeMap = makeLargeMap(1_000);

const longArrayOfDates = Array.from(
  { length: 1_000 },
  (_, i) => new Date(2_000_000_000_000 + i * 1_000),
);

// Pre-encode forms so decode benches measure decoding only.
const lossless_tiny = LosslessJsonCodec.encode(tiny);
const lossless_small = LosslessJsonCodec.encode(smallJsonNative);
const lossless_flatMedium = LosslessJsonCodec.encode(flatMedium);
const lossless_deepNested = LosslessJsonCodec.encode(deepNested);
const lossless_withDate = LosslessJsonCodec.encode(withDate);
const lossless_withMixedSpecial = LosslessJsonCodec.encode(withMixedSpecial);
const lossless_largeSet = LosslessJsonCodec.encode(largeSet);
const lossless_largeMap = LosslessJsonCodec.encode(largeMap);
const lossless_longArrayOfDates = LosslessJsonCodec.encode(longArrayOfDates);

// Size stats so the output helps reason about payload weight.
function bytesOf(encoded: unknown): number {
  return new TextEncoder().encode(JSON.stringify(encoded)).byteLength;
}

console.log("\n=== payload sizes (bytes, JSON-encoded Lossless form) ===");
console.log(`  tiny                    ${bytesOf(lossless_tiny)}`);
console.log(`  smallJsonNative         ${bytesOf(lossless_small)}`);
console.log(`  flatMedium (100 keys)   ${bytesOf(lossless_flatMedium)}`);
console.log(`  deepNested (depth 20)   ${bytesOf(lossless_deepNested)}`);
console.log(`  withDate                ${bytesOf(lossless_withDate)}`);
console.log(`  withMixedSpecial        ${bytesOf(lossless_withMixedSpecial)}`);
console.log(`  largeSet (1K entries)   ${bytesOf(lossless_largeSet)}`);
console.log(`  largeMap (1K entries)   ${bytesOf(lossless_largeMap)}`);
console.log(`  longArrayOfDates (1K)   ${bytesOf(lossless_longArrayOfDates)}\n`);

// ---------------------------------------------------------------------------
// Encode — one end of the boundary
// ---------------------------------------------------------------------------

group("encode — tiny { a, b }", () => {
  bench("JsonCodec (identity)", () => JsonCodec.encode(tiny));
  bench("LosslessJsonCodec", () => LosslessJsonCodec.encode(tiny));
  bench("JSON.stringify", () => JSON.stringify(tiny));
});

group("encode — small JSON-native (9 keys + nested)", () => {
  bench("JsonCodec (identity)", () => JsonCodec.encode(smallJsonNative));
  bench("LosslessJsonCodec", () => LosslessJsonCodec.encode(smallJsonNative));
  bench("JSON.stringify", () => JSON.stringify(smallJsonNative));
});

group("encode — flat 100-key object", () => {
  bench("JsonCodec (identity)", () => JsonCodec.encode(flatMedium));
  bench("LosslessJsonCodec", () => LosslessJsonCodec.encode(flatMedium));
  bench("JSON.stringify", () => JSON.stringify(flatMedium));
});

group("encode — deep nested (depth 20)", () => {
  bench("JsonCodec (identity)", () => JsonCodec.encode(deepNested));
  bench("LosslessJsonCodec", () => LosslessJsonCodec.encode(deepNested));
  bench("JSON.stringify", () => JSON.stringify(deepNested));
});

group("encode — with Dates", () => {
  bench("JsonCodec (identity)", () => JsonCodec.encode(withDate));
  bench("LosslessJsonCodec", () => LosslessJsonCodec.encode(withDate));
  bench("JSON.stringify", () => JSON.stringify(withDate));
});

group("encode — mixed BigInt / Map / Set / Date", () => {
  bench("LosslessJsonCodec", () => LosslessJsonCodec.encode(withMixedSpecial));
  // JsonCodec identity is not meaningful here — it wouldn't JSON-safe the
  // payload. JSON.stringify would throw on BigInt; skip those.
});

group("encode — large Set (1K entries)", () => {
  bench("LosslessJsonCodec", () => LosslessJsonCodec.encode(largeSet));
});

group("encode — large Map (1K entries)", () => {
  bench("LosslessJsonCodec", () => LosslessJsonCodec.encode(largeMap));
});

group("encode — array of 1K Dates", () => {
  bench("LosslessJsonCodec", () => LosslessJsonCodec.encode(longArrayOfDates));
});

// ---------------------------------------------------------------------------
// Decode — the other end
// ---------------------------------------------------------------------------

group("decode — small JSON-native", () => {
  bench("JsonCodec (identity)", () => JsonCodec.decode(lossless_small));
  bench("LosslessJsonCodec", () => LosslessJsonCodec.decode(lossless_small));
});

group("decode — flat 100-key object", () => {
  bench("JsonCodec (identity)", () => JsonCodec.decode(lossless_flatMedium));
  bench("LosslessJsonCodec", () => LosslessJsonCodec.decode(lossless_flatMedium));
});

group("decode — with Dates", () => {
  bench("LosslessJsonCodec", () => LosslessJsonCodec.decode(lossless_withDate));
});

group("decode — mixed BigInt / Map / Set / Date", () => {
  bench("LosslessJsonCodec", () => LosslessJsonCodec.decode(lossless_withMixedSpecial));
});

group("decode — large Set (1K entries)", () => {
  bench("LosslessJsonCodec", () => LosslessJsonCodec.decode(lossless_largeSet));
});

group("decode — array of 1K Dates", () => {
  bench("LosslessJsonCodec", () => LosslessJsonCodec.decode(lossless_longArrayOfDates));
});

// ---------------------------------------------------------------------------
// Full round-trip — what prod actually pays per step or activity:
//   codec.encode → JSON.stringify (storage write) → JSON.parse (storage read)
//   → codec.decode
// ---------------------------------------------------------------------------

function jsonRoundTrip<T>(codec: typeof LosslessJsonCodec, value: T): T {
  const encoded = codec.encode(value);
  return codec.decode(JSON.parse(JSON.stringify(encoded))) as T;
}

group("full storage round-trip — small JSON-native", () => {
  bench("JsonCodec (identity)", () => jsonRoundTrip(JsonCodec, smallJsonNative));
  bench("LosslessJsonCodec", () => jsonRoundTrip(LosslessJsonCodec, smallJsonNative));
});

group("full storage round-trip — flat 100-key object", () => {
  bench("JsonCodec (identity)", () => jsonRoundTrip(JsonCodec, flatMedium));
  bench("LosslessJsonCodec", () => jsonRoundTrip(LosslessJsonCodec, flatMedium));
});

group("full storage round-trip — with Dates", () => {
  bench("JsonCodec (identity, Date becomes string)", () => jsonRoundTrip(JsonCodec, withDate));
  bench("LosslessJsonCodec (Date stays Date)", () => jsonRoundTrip(LosslessJsonCodec, withDate));
});

group("full storage round-trip — mixed special", () => {
  bench("LosslessJsonCodec", () => jsonRoundTrip(LosslessJsonCodec, withMixedSpecial));
});

await run();
