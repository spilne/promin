import { describe, it, expect } from "bun:test";
import { canonicalJSON, payloadHash } from "../canonicalize.ts";

describe("canonicalJSON — stability", () => {
  it("sorts object keys", () => {
    const a = canonicalJSON({ b: 1, a: 2, c: 3 });
    const b = canonicalJSON({ a: 2, c: 3, b: 1 });
    expect(a).toBe(b);
    expect(a).toBe('{"a":2,"b":1,"c":3}');
  });

  it("sorts nested object keys recursively", () => {
    const a = canonicalJSON({ outer: { z: 1, a: 2 } });
    const b = canonicalJSON({ outer: { a: 2, z: 1 } });
    expect(a).toBe(b);
  });

  it("preserves array order (semantic)", () => {
    expect(canonicalJSON([3, 1, 2])).toBe("[3,1,2]");
    expect(canonicalJSON([1, 2, 3])).toBe("[1,2,3]");
    expect(canonicalJSON([3, 1, 2])).not.toBe(canonicalJSON([1, 2, 3]));
  });

  it("sorts Set members so insertion order doesn't leak", () => {
    const a = canonicalJSON(new Set([3, 1, 2]));
    const b = canonicalJSON(new Set([1, 2, 3]));
    expect(a).toBe(b);
  });

  it("sorts Map entries by canonicalized key", () => {
    const a = new Map<string, number>();
    a.set("b", 1);
    a.set("a", 2);
    const b = new Map<string, number>();
    b.set("a", 2);
    b.set("b", 1);
    expect(canonicalJSON(a)).toBe(canonicalJSON(b));
  });
});

describe("canonicalJSON — type tags", () => {
  it("tags Date so it round-trips distinctly from a number", () => {
    const d = new Date("2026-01-01T00:00:00Z");
    expect(canonicalJSON(d)).toBe(`{"$date":${d.getTime()}}`);
    expect(canonicalJSON(d)).not.toBe(canonicalJSON(d.getTime()));
  });

  it("tags BigInt so it doesn't collide with its numeric twin", () => {
    expect(canonicalJSON(5n)).toBe('{"$bigint":"5"}');
    expect(canonicalJSON(5n)).not.toBe(canonicalJSON(5));
  });

  it("tags undefined so keys with explicit `undefined` don't vanish", () => {
    expect(canonicalJSON({ a: undefined })).toBe('{"a":{"$undefined":true}}');
  });

  it("tags NaN / ±Infinity / -0 distinctly", () => {
    expect(canonicalJSON(NaN)).toBe('{"$nan":true}');
    expect(canonicalJSON(Infinity)).toBe('{"$inf":1}');
    expect(canonicalJSON(-Infinity)).toBe('{"$inf":-1}');
    expect(canonicalJSON(-0)).toBe('{"$negzero":true}');
    // -0 and 0 must hash differently.
    expect(canonicalJSON(-0)).not.toBe(canonicalJSON(0));
  });

  it("tags RegExp / URL / Error", () => {
    expect(canonicalJSON(/foo/gi)).toBe('{"$regexp":"foo","flags":"gi"}');
    expect(canonicalJSON(new URL("https://x.test/"))).toBe('{"$url":"https://x.test/"}');
    expect(canonicalJSON(new Error("boom"))).toBe('{"$error":{"name":"Error","message":"boom"}}');
  });
});

describe("canonicalJSON — errors", () => {
  it("throws on cycles", () => {
    const a: { self?: unknown } = {};
    a.self = a;
    expect(() => canonicalJSON(a)).toThrow(/cyclic/);
  });

  it("throws on functions", () => {
    expect(() => canonicalJSON(() => 1)).toThrow(/function/);
  });

  it("throws on symbols", () => {
    expect(() => canonicalJSON(Symbol("x"))).toThrow(/symbol/);
  });
});

describe("payloadHash", () => {
  it("is 64 hex chars (SHA-256)", () => {
    const h = payloadHash({ id: 1 });
    expect(h).toMatch(/^[0-9a-f]{64}$/);
  });

  it("agrees for structurally-equal inputs with different key order", () => {
    expect(payloadHash({ a: 1, b: 2 })).toBe(payloadHash({ b: 2, a: 1 }));
  });

  it("disagrees on any semantic change", () => {
    const base = { id: "ord-1", items: [{ sku: "x", qty: 1 }] };
    const h1 = payloadHash(base);
    const h2 = payloadHash({ ...base, id: "ord-2" });
    const h3 = payloadHash({ ...base, items: [{ sku: "x", qty: 2 }] });
    expect(h1).not.toBe(h2);
    expect(h1).not.toBe(h3);
    expect(h2).not.toBe(h3);
  });

  it("treats string '1' and number 1 as distinct", () => {
    expect(payloadHash("1")).not.toBe(payloadHash(1));
  });

  it("treats Date and number (same epoch) as distinct", () => {
    const d = new Date(1_700_000_000_000);
    expect(payloadHash(d)).not.toBe(payloadHash(d.getTime()));
  });
});
