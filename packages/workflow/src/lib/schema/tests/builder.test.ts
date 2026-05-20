import { describe, expect, it } from "bun:test";
import { type Infer, s } from "../builder.ts";

describe("s.* builder — JSON Schema output", () => {
  it("produces a plain string schema", () => {
    expect(s.string().jsonSchema).toEqual({ type: "string" });
  });

  it("carries description through", () => {
    expect(s.string({ description: "the user's name" }).jsonSchema).toEqual({
      type: "string",
      description: "the user's name",
    });
  });

  it(".describe() sets/overwrites description preserving other fields", () => {
    const annotated = s.number({ minimum: 0 }).describe("non-negative");
    expect(annotated.jsonSchema).toEqual({
      type: "number",
      minimum: 0,
      description: "non-negative",
    });
  });

  it("enum produces type=string with enum values", () => {
    const colour = s.enum(["red", "green", "blue"] as const);
    expect(colour.jsonSchema).toEqual({
      type: "string",
      enum: ["red", "green", "blue"],
    });
  });

  it("object infers required + optional split", () => {
    const userSchema = s.object({
      name: s.string(),
      age: s.integer().optional(),
    });
    expect(userSchema.jsonSchema).toEqual({
      type: "object",
      properties: {
        name: { type: "string" },
        age: { type: "integer" },
      },
      required: ["name"],
      additionalProperties: false,
    });
  });

  it("object with no required fields omits the required array", () => {
    const allOptional = s.object({
      a: s.string().optional(),
      b: s.number().optional(),
    });
    expect((allOptional.jsonSchema as { required?: string[] }).required).toBeUndefined();
  });

  it("nested object schemas compose", () => {
    const schema = s.object({
      address: s.object({
        street: s.string(),
        zip: s.string().optional(),
      }),
    });
    const root = schema.jsonSchema as Extract<typeof schema.jsonSchema, { type: "object" }>;
    expect(root.properties.address).toMatchObject({
      type: "object",
      properties: { street: { type: "string" }, zip: { type: "string" } },
      required: ["street"],
    });
  });

  it("array carries item schema", () => {
    const items = s.array(s.integer());
    expect(items.jsonSchema).toEqual({
      type: "array",
      items: { type: "integer" },
    });
  });

  it("union produces anyOf", () => {
    const stringOrNum = s.union<string | number>([s.string(), s.number()]);
    expect(stringOrNum.jsonSchema).toEqual({
      anyOf: [{ type: "string" }, { type: "number" }],
    });
  });

  it("s.unknown produces an empty schema", () => {
    expect(s.unknown().jsonSchema).toEqual({});
  });

  it("s.unknown carries description", () => {
    expect(s.unknown({ description: "any json" }).jsonSchema).toEqual({
      description: "any json",
    });
  });
});

describe("s.* builder — optional / describe ordering", () => {
  // Footgun guard: authors should be able to call .optional() and .describe()
  // in any order without one stripping the other's effect.
  it(".optional().describe() keeps the field optional", () => {
    const schema = s.object({
      name: s.string().optional().describe("display name"),
    });
    expect((schema.jsonSchema as { required?: string[] }).required).toBeUndefined();
  });

  it(".describe().optional() keeps the description", () => {
    const schema = s.object({
      name: s.string().describe("display name").optional(),
    });
    const root = schema.jsonSchema as Extract<typeof schema.jsonSchema, { type: "object" }>;
    expect(root.properties.name).toEqual({ type: "string", description: "display name" });
    expect(root.required).toBeUndefined();
  });
});

describe("Infer<> type util", () => {
  // These are compile-time assertions — if Infer<> regresses, this file
  // fails to typecheck. The `expectType` helper is a runtime no-op.
  function expectType<_T>(_v: unknown): void {}

  it("infers primitive types", () => {
    const schema = s.string();
    type T = Infer<typeof schema>;
    expectType<T>("hello" as T);
    expect(schema.jsonSchema).toBeDefined();
  });

  it("infers object with optional", () => {
    const schema = s.object({
      id: s.string(),
      tags: s.array(s.string()).optional(),
    });
    type T = Infer<typeof schema>;
    const sample: T = { id: "abc" };
    const withTags: T = { id: "abc", tags: ["a", "b"] };
    expect(sample.id).toBe("abc");
    expect(withTags.tags).toEqual(["a", "b"]);
  });

  it("infers enum as string literal union", () => {
    const schema = s.enum(["pending", "approved", "rejected"] as const);
    type T = Infer<typeof schema>;
    const v: T = "approved";
    expect(v).toBe("approved");
  });
});
