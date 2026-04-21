import { describe, it, expect } from "bun:test";
import { LosslessJsonCodec } from "../lossless-codec.ts";

/**
 * Simulates a trip through Postgres/Redis: encode → JSON.stringify →
 * JSON.parse → decode. The codec's encoded form must be JSON-native so this
 * round-trip is lossless.
 */
function storageRoundTrip<T>(value: T): T {
  const encoded = LosslessJsonCodec.encode(value);
  const jsonTrip = JSON.parse(JSON.stringify(encoded));
  return LosslessJsonCodec.decode(jsonTrip) as T;
}

describe("LosslessJsonCodec — primitives round-trip through storage", () => {
  it("passes JSON-native primitives unchanged", () => {
    expect(storageRoundTrip("hello")).toBe("hello");
    expect(storageRoundTrip(42)).toBe(42);
    expect(storageRoundTrip(3.14)).toBe(3.14);
    expect(storageRoundTrip(true)).toBe(true);
    expect(storageRoundTrip(false)).toBe(false);
    expect(storageRoundTrip(null)).toBeNull();
  });

  it("preserves undefined", () => {
    expect(storageRoundTrip(undefined)).toBeUndefined();
  });

  it("preserves NaN / Infinity / -Infinity / -0", () => {
    const nan = storageRoundTrip(Number.NaN);
    expect(Number.isNaN(nan)).toBe(true);
    expect(storageRoundTrip(Number.POSITIVE_INFINITY)).toBe(Infinity);
    expect(storageRoundTrip(Number.NEGATIVE_INFINITY)).toBe(-Infinity);
    const negZero = storageRoundTrip(-0);
    expect(negZero).toBe(-0);
    // Distinguish -0 from +0 via 1/x
    expect(1 / negZero).toBe(-Infinity);
  });

  it("preserves BigInt", () => {
    const big = 12345678901234567890n;
    expect(storageRoundTrip(big)).toBe(big);
    expect(storageRoundTrip(0n)).toBe(0n);
    expect(storageRoundTrip(-1n)).toBe(-1n);
  });
});

describe("LosslessJsonCodec — core object types", () => {
  it("preserves Date values exactly (millisecond granularity)", () => {
    const d = new Date("2026-04-16T12:34:56.789Z");
    const decoded = storageRoundTrip(d);
    expect(decoded).toBeInstanceOf(Date);
    expect(decoded.getTime()).toBe(d.getTime());
  });

  it("preserves RegExp with flags", () => {
    const re = /foo.*bar/gim;
    const decoded = storageRoundTrip(re);
    expect(decoded).toBeInstanceOf(RegExp);
    expect(decoded.source).toBe(re.source);
    expect(decoded.flags).toBe(re.flags);
    expect(decoded.test("foo-bar")).toBe(true);
  });

  it("preserves URL values", () => {
    const url = new URL("https://example.com/path?q=1");
    const decoded = storageRoundTrip(url);
    expect(decoded).toBeInstanceOf(URL);
    expect(decoded.href).toBe(url.href);
  });

  it("preserves Error name / message / stack", () => {
    const err = new TypeError("boom");
    const decoded = storageRoundTrip(err);
    expect(decoded).toBeInstanceOf(Error);
    expect(decoded.name).toBe("TypeError");
    expect(decoded.message).toBe("boom");
    // Stack is best-effort; just check it's a string or undefined.
    expect(typeof decoded.stack === "string" || decoded.stack === undefined).toBe(true);
  });
});

describe("LosslessJsonCodec — collection types", () => {
  it("preserves Map with primitive keys and values", () => {
    const m = new Map<string, number>([
      ["a", 1],
      ["b", 2],
    ]);
    const decoded = storageRoundTrip(m);
    expect(decoded).toBeInstanceOf(Map);
    expect(decoded.size).toBe(2);
    expect(decoded.get("a")).toBe(1);
    expect(decoded.get("b")).toBe(2);
  });

  it("preserves Map with Date keys and BigInt values", () => {
    const k1 = new Date("2026-01-01T00:00:00Z");
    const k2 = new Date("2026-06-01T00:00:00Z");
    const m = new Map<Date, bigint>([
      [k1, 1n],
      [k2, 2n],
    ]);
    const decoded = storageRoundTrip(m);
    expect(decoded).toBeInstanceOf(Map);
    expect(decoded.size).toBe(2);
    // Can't compare Date keys by reference; find by time value
    const entries = [...decoded.entries()];
    expect(entries[0]![0]).toBeInstanceOf(Date);
    expect(typeof entries[0]![1]).toBe("bigint");
  });

  it("preserves Set", () => {
    const s = new Set([1, 2, 3, "hello"]);
    const decoded = storageRoundTrip(s);
    expect(decoded).toBeInstanceOf(Set);
    expect(decoded.size).toBe(4);
    expect(decoded.has(1)).toBe(true);
    expect(decoded.has("hello")).toBe(true);
  });

  it("preserves arrays with mixed special values", () => {
    const arr = [new Date("2026-01-01Z"), 42n, null, undefined, NaN];
    const decoded = storageRoundTrip(arr);
    expect(decoded).toHaveLength(5);
    expect(decoded[0]).toBeInstanceOf(Date);
    expect(typeof decoded[1]).toBe("bigint");
    expect(decoded[2]).toBeNull();
    expect(decoded[3]).toBeUndefined();
    expect(Number.isNaN(decoded[4])).toBe(true);
  });
});

describe("LosslessJsonCodec — nested structures", () => {
  it("preserves deeply nested special values", () => {
    const input = {
      user: {
        id: "u1",
        createdAt: new Date("2026-04-01T00:00:00Z"),
        balance: 12345678901234567890n,
        tags: new Set(["alpha", "beta"]),
        metadata: new Map<string, unknown>([
          ["last_login", new Date("2026-04-15T12:00:00Z")],
          ["is_admin", true],
        ]),
      },
    };
    const decoded = storageRoundTrip(input);
    expect(decoded.user.createdAt).toBeInstanceOf(Date);
    expect(decoded.user.createdAt.getTime()).toBe(input.user.createdAt.getTime());
    expect(decoded.user.balance).toBe(input.user.balance);
    expect(decoded.user.tags).toBeInstanceOf(Set);
    expect(decoded.user.tags.has("alpha")).toBe(true);
    expect(decoded.user.metadata).toBeInstanceOf(Map);
    expect((decoded.user.metadata.get("last_login") as Date).getTime()).toBe(
      input.user.metadata.get("last_login")!.getTime(),
    );
  });

  it("preserves a literal object that carries a __t field", () => {
    const input = { __t: "undefined", payload: 5 };
    const decoded = storageRoundTrip(input);
    // Must NOT be decoded as undefined; must survive as the literal object.
    expect(decoded).toEqual(input);
  });

  it("literal __t object with nested special values still preserves inner types", () => {
    const input = { __t: "custom", inner: new Date("2026-01-01Z") };
    const decoded = storageRoundTrip(input) as any;
    expect(decoded.__t).toBe("custom");
    expect(decoded.inner).toBeInstanceOf(Date);
  });
});

describe("LosslessJsonCodec — rejects non-serializable values", () => {
  it("throws on a function", () => {
    expect(() => LosslessJsonCodec.encode(() => 1)).toThrow(TypeError);
  });

  it("throws on a symbol", () => {
    expect(() => LosslessJsonCodec.encode(Symbol("x"))).toThrow(TypeError);
  });

  it("throws on a function nested in an object", () => {
    expect(() => LosslessJsonCodec.encode({ fn: () => 1 })).toThrow(TypeError);
  });
});

describe("LosslessJsonCodec — backward compat with JSON-native inputs", () => {
  it("decode(encode(x)) equals the original for JSON-native shapes", () => {
    const cases = [
      {},
      { a: 1, b: "two" },
      [],
      [1, 2, 3],
      { nested: { deeper: { leaf: "ok" } } },
      [{ id: 1 }, { id: 2 }],
    ];
    for (const c of cases) {
      expect(storageRoundTrip(c)).toEqual(c);
    }
  });
});
