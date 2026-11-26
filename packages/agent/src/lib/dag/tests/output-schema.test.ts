import { describe, expect, it } from "bun:test";

import {
  compileOutputSchema,
  extractJson,
  OutputSchemaError,
  schemaInstruction,
} from "../output-schema.ts";

const schema = {
  type: "object",
  properties: {
    questions: { type: "array", items: { type: "string" } },
  },
  required: ["questions"],
};

describe("output schema", () => {
  it("validates a matching value", () => {
    expect(compileOutputSchema(schema).safeParse({ questions: ["a"] }).success).toBe(true);
    expect(compileOutputSchema(schema).safeParse({ questions: "a" }).success).toBe(false);
  });

  it.each([
    ["a misspelt type", { type: "objct" }],
    ["a nested misspelt type", { type: "object", properties: { a: { type: "strng" } } }],
    ["a non-object top level", { type: "array", items: { type: "string" } }],
    ["not an object at all", "string"],
    ["over 16 KB", { type: "object", description: "x".repeat(17_000) }],
  ])("rejects %s", (_name, bad) => {
    expect(() => compileOutputSchema(bad)).toThrow(OutputSchemaError);
  });

  it("extracts JSON from a fenced reply", () => {
    expect(extractJson('```json\n{"a":1}\n```')).toEqual({ a: 1 });
    expect(extractJson('{"a":1}')).toEqual({ a: 1 });
    expect(() => extractJson("not json")).toThrow(OutputSchemaError);
  });

  it("builds the exact fallback instruction", () => {
    expect(schemaInstruction(schema)).toBe(
      "Reply with one JSON object and nothing else. It must match this JSON Schema:\n" +
        JSON.stringify(schema, null, 2),
    );
  });
});
