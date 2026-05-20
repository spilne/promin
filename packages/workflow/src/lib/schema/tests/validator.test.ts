import { describe, expect, it } from "bun:test";
import { s } from "../builder.ts";
import { validate } from "../validator.ts";

describe("validator — primitives", () => {
  it("accepts a matching string", () => {
    expect(validate("hi", s.string().jsonSchema)).toEqual({ ok: true, errors: [] });
  });

  it("rejects a non-string", () => {
    const result = validate(42, s.string().jsonSchema);
    expect(result.ok).toBe(false);
    expect(result.errors).toEqual([{ path: "", message: "expected string" }]);
  });

  it("respects enum", () => {
    const schema = s.enum(["a", "b"] as const).jsonSchema;
    expect(validate("a", schema).ok).toBe(true);
    expect(validate("c", schema).ok).toBe(false);
  });

  it("enforces number bounds", () => {
    const schema = s.number({ minimum: 0, maximum: 100 }).jsonSchema;
    expect(validate(50, schema).ok).toBe(true);
    expect(validate(-1, schema).errors[0]?.message).toBe("must be >= 0");
    expect(validate(101, schema).errors[0]?.message).toBe("must be <= 100");
  });

  it("integer rejects non-integers", () => {
    const schema = s.integer().jsonSchema;
    expect(validate(3, schema).ok).toBe(true);
    expect(validate(3.5, schema).ok).toBe(false);
  });
});

describe("validator — objects", () => {
  const userSchema = s.object({
    name: s.string(),
    age: s.integer().optional(),
  }).jsonSchema;

  it("accepts a minimal valid object", () => {
    expect(validate({ name: "Ada" }, userSchema).ok).toBe(true);
  });

  it("accepts the full shape", () => {
    expect(validate({ name: "Ada", age: 30 }, userSchema).ok).toBe(true);
  });

  it("rejects a missing required field with a useful path", () => {
    const result = validate({ age: 30 }, userSchema);
    expect(result.ok).toBe(false);
    expect(result.errors).toContainEqual({ path: "name", message: "missing required field" });
  });

  it("rejects extras when additionalProperties is false", () => {
    const result = validate({ name: "Ada", x: 1 }, userSchema);
    expect(result.errors).toContainEqual({ path: "x", message: "unexpected property" });
  });

  it("reports nested field paths", () => {
    const schema = s.object({
      address: s.object({
        zip: s.string(),
      }),
    }).jsonSchema;
    const result = validate({ address: { zip: 12345 } }, schema);
    expect(result.errors).toEqual([{ path: "address.zip", message: "expected string" }]);
  });
});

describe("validator — arrays + unions", () => {
  it("validates array items", () => {
    const schema = s.array(s.integer()).jsonSchema;
    expect(validate([1, 2, 3], schema).ok).toBe(true);
    const result = validate([1, "two", 3], schema);
    expect(result.errors).toEqual([{ path: "[1]", message: "expected integer" }]);
  });

  it("union accepts any matching branch", () => {
    const schema = s.union<string | number>([s.string(), s.number()]).jsonSchema;
    expect(validate("hi", schema).ok).toBe(true);
    expect(validate(42, schema).ok).toBe(true);
  });

  it("union rejects when no branch matches", () => {
    const schema = s.union<string | number>([s.string(), s.number()]).jsonSchema;
    const result = validate(true, schema);
    expect(result.ok).toBe(false);
    expect(result.errors[0]?.message).toContain("did not match any branch");
  });
});

describe("validator — unknown / empty schema", () => {
  it("accepts any value against s.unknown()", () => {
    const schema = s.unknown().jsonSchema;
    expect(validate("hi", schema).ok).toBe(true);
    expect(validate(42, schema).ok).toBe(true);
    expect(validate({ deeply: { nested: true } }, schema).ok).toBe(true);
    expect(validate(null, schema).ok).toBe(true);
  });
});
