import { describe, it, expect } from "bun:test";
import {
  JsonCodec,
  codecFromSchema,
  codecTuple,
  codecRecord,
  codecArray,
  JsonEq,
  eqFromCodec,
  JsonShow,
  arrayMonoid,
  sumMonoid,
  stringMonoid,
  numberOrd,
  stringOrd,
  ordBy,
} from "../index.ts";

// ---------------------------------------------------------------------------
// Minimal Zod-like schema for testing (avoids adding zod dep to core)
// ---------------------------------------------------------------------------

function fakeSchema<T>(parse: (data: unknown) => T) {
  return { parse };
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

describe("Codec", () => {
  describe("JsonCodec", () => {
    it("encode returns value as-is", () => {
      expect(JsonCodec.encode({ a: 1 })).toEqual({ a: 1 });
    });

    it("decode returns value as-is", () => {
      expect(JsonCodec.decode("hello")).toBe("hello");
    });
  });

  describe("codecFromSchema", () => {
    it("encode returns value as-is", () => {
      const schema = fakeSchema((d) => d as { name: string });
      const codec = codecFromSchema(schema);
      expect(codec.encode({ name: "test" })).toEqual({ name: "test" });
    });

    it("decode uses schema.parse", () => {
      const schema = fakeSchema((d) => {
        const obj = d as { name: string };
        if (typeof obj.name !== "string") throw new Error("invalid");
        return obj;
      });
      const codec = codecFromSchema(schema);
      expect(codec.decode({ name: "hello" })).toEqual({ name: "hello" });
    });

    it("decode throws on invalid data", () => {
      const schema = fakeSchema((d) => {
        const obj = d as { name: string };
        if (typeof obj.name !== "string") throw new Error("invalid");
        return obj;
      });
      const codec = codecFromSchema(schema);
      expect(() => codec.decode({ name: 42 })).toThrow("invalid");
    });
  });

  describe("codecTuple", () => {
    it("round-trips a tuple", () => {
      const codec = codecTuple(JsonCodec, JsonCodec);
      const value: [string, number] = ["hello", 42];
      const encoded = codec.encode(value);
      expect(codec.decode(encoded)).toEqual(value);
    });

    it("applies inner codecs", () => {
      const doubler = {
        encode: (n: number) => n * 2,
        decode: (raw: unknown) => (raw as number) / 2,
      };
      const codec = codecTuple(JsonCodec, doubler);
      const encoded = codec.encode(["a", 5]);
      expect(encoded).toEqual(["a", 10]);
      expect(codec.decode(encoded)).toEqual(["a", 5]);
    });
  });

  describe("codecRecord", () => {
    it("round-trips a record", () => {
      const codec = codecRecord(JsonCodec);
      const value = { a: 1, b: 2 };
      const encoded = codec.encode(value);
      expect(codec.decode(encoded)).toEqual(value);
    });
  });

  describe("codecArray", () => {
    it("round-trips an array", () => {
      const codec = codecArray(JsonCodec);
      const value = [1, 2, 3];
      const encoded = codec.encode(value);
      expect(codec.decode(encoded)).toEqual(value);
    });

    it("applies inner codec to each element", () => {
      const doubler = {
        encode: (n: number) => n * 2,
        decode: (raw: unknown) => (raw as number) / 2,
      };
      const codec = codecArray(doubler);
      expect(codec.encode([1, 2, 3])).toEqual([2, 4, 6]);
      expect(codec.decode([2, 4, 6])).toEqual([1, 2, 3]);
    });
  });
});

// ---------------------------------------------------------------------------
// Eq
// ---------------------------------------------------------------------------

describe("Eq", () => {
  describe("JsonEq", () => {
    it("returns true for equal objects", () => {
      expect(JsonEq.equals({ a: 1 }, { a: 1 })).toBe(true);
    });

    it("returns false for different objects", () => {
      expect(JsonEq.equals({ a: 1 }, { a: 2 })).toBe(false);
    });

    it("returns true for equal primitives", () => {
      expect(JsonEq.equals(42, 42)).toBe(true);
    });

    it("returns true for equal arrays", () => {
      expect(JsonEq.equals([1, 2], [1, 2])).toBe(true);
    });

    it("returns false for different arrays", () => {
      expect(JsonEq.equals([1, 2], [2, 1])).toBe(false);
    });
  });

  describe("eqFromCodec", () => {
    it("uses codec.encode for comparison", () => {
      const codec = {
        encode: (v: { id: number; name: string }) => ({ id: v.id }),
        decode: (raw: unknown) => raw as { id: number; name: string },
      };
      const eq = eqFromCodec(codec);
      expect(eq.equals({ id: 1, name: "a" }, { id: 1, name: "b" })).toBe(true);
      expect(eq.equals({ id: 1, name: "a" }, { id: 2, name: "a" })).toBe(false);
    });
  });
});

// ---------------------------------------------------------------------------
// Show
// ---------------------------------------------------------------------------

describe("Show", () => {
  describe("JsonShow", () => {
    it("shows a simple value", () => {
      expect(JsonShow.show(42)).toBe("42");
    });

    it("shows an object", () => {
      expect(JsonShow.show({ name: "test" })).toBe('{"name":"test"}');
    });

    it("truncates long values to 200 chars", () => {
      const longObj = { data: "x".repeat(300) };
      const result = JsonShow.show(longObj);
      expect(result.length).toBe(200);
    });

    it("handles undefined", () => {
      expect(JsonShow.show(undefined)).toBe("undefined");
    });
  });
});

// ---------------------------------------------------------------------------
// Monoid
// ---------------------------------------------------------------------------

describe("Monoid", () => {
  describe("arrayMonoid", () => {
    it("has empty as []", () => {
      expect(arrayMonoid<number>().empty).toEqual([]);
    });

    it("concat concatenates arrays", () => {
      expect(arrayMonoid<number>().concat([1, 2], [3, 4])).toEqual([1, 2, 3, 4]);
    });

    it("empty is identity", () => {
      const m = arrayMonoid<number>();
      expect(m.concat(m.empty, [1, 2])).toEqual([1, 2]);
      expect(m.concat([1, 2], m.empty)).toEqual([1, 2]);
    });
  });

  describe("sumMonoid", () => {
    it("has empty as 0", () => {
      expect(sumMonoid.empty).toBe(0);
    });

    it("concat adds numbers", () => {
      expect(sumMonoid.concat(3, 4)).toBe(7);
    });

    it("empty is identity", () => {
      expect(sumMonoid.concat(sumMonoid.empty, 5)).toBe(5);
    });
  });

  describe("stringMonoid", () => {
    it("has empty as empty string", () => {
      expect(stringMonoid.empty).toBe("");
    });

    it("concat joins strings", () => {
      expect(stringMonoid.concat("hello", " world")).toBe("hello world");
    });
  });
});

// ---------------------------------------------------------------------------
// Ord
// ---------------------------------------------------------------------------

describe("Ord", () => {
  describe("numberOrd", () => {
    it("equals returns true for same numbers", () => {
      expect(numberOrd.equals(5, 5)).toBe(true);
    });

    it("equals returns false for different numbers", () => {
      expect(numberOrd.equals(5, 6)).toBe(false);
    });

    it("compare returns -1 for less", () => {
      expect(numberOrd.compare(1, 2)).toBe(-1);
    });

    it("compare returns 0 for equal", () => {
      expect(numberOrd.compare(3, 3)).toBe(0);
    });

    it("compare returns 1 for greater", () => {
      expect(numberOrd.compare(5, 3)).toBe(1);
    });
  });

  describe("stringOrd", () => {
    it("compare works lexicographically", () => {
      expect(stringOrd.compare("abc", "def")).toBe(-1);
      expect(stringOrd.compare("def", "abc")).toBe(1);
      expect(stringOrd.compare("abc", "abc")).toBe(0);
    });
  });

  describe("ordBy", () => {
    it("derives ord from a key extractor", () => {
      const byAge = ordBy((p: { name: string; age: number }) => p.age);
      expect(byAge.compare({ name: "a", age: 20 }, { name: "b", age: 30 })).toBe(-1);
      expect(byAge.compare({ name: "a", age: 30 }, { name: "b", age: 20 })).toBe(1);
      expect(byAge.equals({ name: "a", age: 20 }, { name: "b", age: 20 })).toBe(true);
    });
  });
});
