// ---------------------------------------------------------------------------
// zodToJsonSchema — pins the schema shapes we ship to LLM tool APIs.
//
// The converter feeds tool definitions to providers (Anthropic, OpenAI)
// whose validators reject schemas missing required fields — most often
// `type: "object"` at the top level for tool input_schemas. Bugs here
// surface as runtime 400s the moment a model tries to call the tool,
// which is the worst possible feedback loop. These tests catch
// regressions at build time instead.
//
// Anthropic-tool contract pinned here: any schema we emit for a tool's
// parameters MUST declare `type: "object"` at the top level. Plain
// objects, optional-wrapped objects, discriminated unions, and effects-
// wrapped objects all need to satisfy this — and a regression on any
// one of them broke claude-bot in production once already.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { zodToJsonSchema } from "../zod-to-json-schema.ts";

describe("zodToJsonSchema — primitives", () => {
  it("strings include description when set", () => {
    expect(zodToJsonSchema(z.string())).toEqual({ type: "string" });
    expect(zodToJsonSchema(z.string().describe("a name"))).toEqual({
      type: "string",
      description: "a name",
    });
  });

  it("numbers / booleans / null map cleanly", () => {
    expect(zodToJsonSchema(z.number())).toEqual({ type: "number" });
    expect(zodToJsonSchema(z.boolean())).toEqual({ type: "boolean" });
    expect(zodToJsonSchema(z.null())).toEqual({ type: "null" });
  });

  it("arrays carry element schemas", () => {
    expect(zodToJsonSchema(z.array(z.string()))).toEqual({
      type: "array",
      items: { type: "string" },
    });
  });

  it("enums emit string + enum option list", () => {
    expect(zodToJsonSchema(z.enum(["a", "b", "c"]))).toEqual({
      type: "string",
      enum: ["a", "b", "c"],
    });
  });

  it("literals emit a const", () => {
    expect(zodToJsonSchema(z.literal("create"))).toEqual({ const: "create" });
    expect(zodToJsonSchema(z.literal(42))).toEqual({ const: 42 });
  });
});

describe("zodToJsonSchema — wrappers unwrap to inner schema", () => {
  it("optional unwraps", () => {
    expect(zodToJsonSchema(z.string().optional())).toEqual({ type: "string" });
  });

  it("default unwraps", () => {
    expect(zodToJsonSchema(z.number().default(0))).toEqual({ type: "number" });
  });

  it("nullable emits oneOf [inner, null]", () => {
    expect(zodToJsonSchema(z.string().nullable())).toEqual({
      oneOf: [{ type: "string" }, { type: "null" }],
    });
  });

  it("effects (refine / transform) unwrap to inner schema", () => {
    const refined = z.string().refine((s) => s.length > 0, "non-empty");
    expect(zodToJsonSchema(refined)).toEqual({ type: "string" });
  });
});

describe("zodToJsonSchema — objects", () => {
  it("emits type=object with properties + required list", () => {
    const schema = z.object({
      name: z.string(),
      age: z.number(),
    });
    expect(zodToJsonSchema(schema)).toEqual({
      type: "object",
      properties: { name: { type: "string" }, age: { type: "number" } },
      required: ["name", "age"],
    });
  });

  it("optional + default fields are excluded from required", () => {
    const schema = z.object({
      id: z.string(),
      label: z.string().optional(),
      count: z.number().default(0),
    });
    const result = zodToJsonSchema(schema);
    expect(result["required"]).toEqual(["id"]);
    expect(result["properties"]).toMatchObject({
      id: { type: "string" },
      label: { type: "string" },
      count: { type: "number" },
    });
  });

  it("empty objects omit required key entirely", () => {
    expect(zodToJsonSchema(z.object({}))).toEqual({
      type: "object",
      properties: {},
    });
  });

  it("propagates object-level description", () => {
    const schema = z.object({ task: z.string() }).describe("a unit of work");
    expect(zodToJsonSchema(schema)).toMatchObject({
      type: "object",
      description: "a unit of work",
    });
  });
});

describe("zodToJsonSchema — unions", () => {
  it("ZodUnion emits oneOf", () => {
    const schema = z.union([z.string(), z.number()]);
    expect(zodToJsonSchema(schema)).toEqual({
      oneOf: [{ type: "string" }, { type: "number" }],
    });
  });

  it("ZodDiscriminatedUnion flattens to a single object — Anthropic forbids top-level oneOf", () => {
    // Regression: Zod 4 separates ZodDiscriminatedUnion from ZodUnion.
    // Anthropic's tool input_schema validator rejects BOTH a missing
    // top-level `type` AND any top-level `oneOf` / `allOf` / `anyOf`
    // ("input_schema does not support oneOf, allOf, or anyOf at the
    // top level"). The converter flattens the union into one object:
    // discriminator field becomes an enum over the branch literals,
    // every other field becomes optional, and a generated description
    // tells the model which fields go with which discriminator value.
    const schema = z.discriminatedUnion("kind", [
      z.object({ kind: z.literal("a"), x: z.string() }),
      z.object({ kind: z.literal("b"), y: z.number() }),
    ]);
    const result = zodToJsonSchema(schema);
    expect(result["type"]).toBe("object");
    expect(result).not.toHaveProperty("oneOf");
    expect(result).not.toHaveProperty("anyOf");
    expect(result).not.toHaveProperty("allOf");

    const properties = result["properties"] as Record<string, Record<string, unknown>>;
    // Discriminator becomes an enum over the literals.
    expect(properties.kind).toEqual({ enum: ["a", "b"] });
    // Branch fields land as optional top-level properties.
    expect(properties.x).toEqual({ type: "string" });
    expect(properties.y).toEqual({ type: "number" });
    // Only the discriminator is required at the JSON-Schema level —
    // per-branch requirements live in the description for the model.
    expect(result["required"]).toEqual(["kind"]);
    expect(typeof result["description"]).toBe("string");
    expect(result["description"]).toContain("kind");
  });
});

// ---------------------------------------------------------------------------
// Provider-shape contract tests — the actual bug class.
//
// We don't ship raw schemas to the LLM; we ship them as `tool.input_schema`
// (Anthropic) or `function.parameters` (OpenAI). Both validators expect
// the top level to declare `type: "object"`. Anything we plug into a
// tool's `parameters: ...` field at the AgentTool API MUST satisfy this,
// even when the underlying Zod shape is exotic (discriminated union,
// effects wrapper, default-wrapped object).
//
// One assertion per shape we've actually shipped a tool with — pinning
// "future-you can drop this shape into a tool without re-tripping the
// Anthropic 400".
// ---------------------------------------------------------------------------
describe("zodToJsonSchema — Anthropic tool input_schema contract", () => {
  it.each<[string, z.ZodType]>([
    ["plain object", z.object({ x: z.string() })],
    [
      "discriminated union (scheduler-tool shape)",
      z.discriminatedUnion("command", [
        z.object({ command: z.literal("create"), task: z.string() }),
        z.object({ command: z.literal("list") }),
        z.object({ command: z.literal("cancel"), id: z.string() }),
      ]),
    ],
    [
      "object with optional + default fields",
      z.object({
        a: z.string(),
        b: z.string().optional(),
        c: z.number().default(0),
      }),
    ],
    [
      "object wrapped in refine (.refine())",
      z.object({ task: z.string() }).refine((v) => v.task.length > 0),
    ],
  ])("%s emits top-level type=object with no oneOf/anyOf/allOf", (_label, schema) => {
    const result = zodToJsonSchema(schema);
    // Anthropic rejects missing `type: "object"` AND any top-level
    // `oneOf` / `anyOf` / `allOf`. Both bites have caused 400s in
    // production; pinning both here so the next exotic Zod shape
    // either complies or fails the build.
    expect(result["type"]).toBe("object");
    expect(result).not.toHaveProperty("oneOf");
    expect(result).not.toHaveProperty("anyOf");
    expect(result).not.toHaveProperty("allOf");
  });
});
