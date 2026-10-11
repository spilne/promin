import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { tool, createScopedTool } from "../tool.ts";
import { buildToolDefs } from "../tool-registry.ts";
import {
  agentToolJsonSchema,
  normalizeToolJsonSchema,
  zodToToolJsonSchema,
} from "../tool-json-schema.ts";

type Schema = Record<string, unknown>;

function props(schema: Schema): Record<string, Schema> {
  return schema["properties"] as Record<string, Schema>;
}

/**
 * Top-level shape every provider accepts: Anthropic requires `type: "object"`
 * and rejects top-level oneOf/anyOf/allOf; OpenAI requires an object
 * parameters schema; Ollama decodes only type/properties/required/$defs.
 */
function expectProviderSafe(schema: Schema): void {
  expect(schema["type"]).toBe("object");
  expect(schema["properties"]).toBeDefined();
  for (const key of ["oneOf", "anyOf", "allOf", "$schema", "$ref"]) {
    expect(schema).not.toHaveProperty(key);
  }
}

describe("zodToToolJsonSchema: field metadata", () => {
  const schema = zodToToolJsonSchema(
    z
      .object({
        nickname: z.string().optional().describe("what to call the user"),
        tags: z.array(z.string()).describe("labels to attach"),
        mode: z.enum(["fast", "slow"]).describe("execution mode"),
        count: z.number().int().min(1).max(10),
        email: z.string().email(),
        page: z.number().default(1),
      })
      .describe("search arguments"),
  );

  it("keeps descriptions on optional, array and enum fields", () => {
    expect(props(schema)["nickname"]).toMatchObject({
      type: "string",
      description: "what to call the user",
    });
    expect(props(schema)["tags"]).toMatchObject({
      type: "array",
      items: { type: "string" },
      description: "labels to attach",
    });
    expect(props(schema)["mode"]).toMatchObject({
      enum: ["fast", "slow"],
      description: "execution mode",
    });
    expect(schema["description"]).toBe("search arguments");
  });

  it("keeps integer, bounds, formats and defaults", () => {
    expect(props(schema)["count"]).toMatchObject({ type: "integer", minimum: 1, maximum: 10 });
    expect(props(schema)["email"]).toMatchObject({ type: "string", format: "email" });
    expect(props(schema)["page"]).toMatchObject({ type: "number", default: 1 });
  });

  it("marks only fields the model must send as required (input side)", () => {
    expect(schema["required"]).toEqual(["tags", "mode", "count", "email"]);
  });

  it("strips $schema and stays provider-safe", () => {
    expectProviderSafe(schema);
  });
});

describe("zodToToolJsonSchema: types the old converter dropped", () => {
  const schema = zodToToolJsonSchema(
    z.object({
      headers: z.record(z.string(), z.number()),
      point: z.tuple([z.number(), z.number()]),
      at: z.coerce.date(),
      strictAt: z.date(),
      note: z.string().nullable(),
    }),
  );

  it("represents records with their value schema", () => {
    expect(props(schema)["headers"]).toMatchObject({
      type: "object",
      additionalProperties: { type: "number" },
    });
  });

  it("represents tuples with per-position items", () => {
    expect(props(schema)["point"]).toMatchObject({
      type: "array",
      prefixItems: [{ type: "number" }, { type: "number" }],
      minItems: 2,
      maxItems: 2,
    });
  });

  it("represents dates as ISO date-time strings", () => {
    expect(props(schema)["at"]).toEqual({ type: "string", format: "date-time" });
    expect(props(schema)["strictAt"]).toEqual({ type: "string", format: "date-time" });
  });

  it("represents nullables as a type list", () => {
    expect(props(schema)["note"]).toEqual({ type: ["string", "null"] });
  });

  it("describes a transform's input type", () => {
    const s = zodToToolJsonSchema(z.object({ n: z.string().transform(Number) }));
    expect(props(s)["n"]).toEqual({ type: "string" });
  });
});

describe("zodToToolJsonSchema: top-level unions", () => {
  it("flattens a discriminated union into one object with a discriminator enum", () => {
    const schema = zodToToolJsonSchema(
      z
        .discriminatedUnion("command", [
          z.object({ command: z.literal("create"), task: z.string().describe("what to do") }),
          z.object({ command: z.literal("list") }),
          z.object({ command: z.literal("cancel"), id: z.string(), force: z.boolean().optional() }),
        ])
        .describe("Scheduler."),
    );
    expectProviderSafe(schema);
    expect(schema["required"]).toEqual(["command"]);
    expect(props(schema)["command"]).toEqual({
      type: "string",
      enum: ["create", "list", "cancel"],
      description: expect.stringContaining("REQUIRED"),
    });
    // Branch fields become optional top-level properties, metadata intact.
    expect(props(schema)["task"]).toEqual({ type: "string", description: "what to do" });
    expect(props(schema)["force"]).toEqual({ type: "boolean" });
    expect(Object.keys(props(schema))).toEqual(["command", "task", "id", "force"]);
    // Per-branch requirements surface in the description for the model.
    const description = schema["description"] as string;
    expect(description.startsWith("Scheduler. ")).toBe(true);
    expect(description).toContain('"create": requires task');
    expect(description).toContain('"list": no extra fields');
    expect(description).toContain('"cancel": requires id');
  });

  it("types a numeric discriminator as a number", () => {
    const schema = zodToToolJsonSchema(
      z.discriminatedUnion("v", [
        z.object({ v: z.literal(1), a: z.string() }),
        z.object({ v: z.literal(2), b: z.string() }),
      ]),
    );
    expect(props(schema)["v"]).toMatchObject({ type: "number", enum: [1, 2] });
  });

  it("flattens a plain union of objects, keeping fields every branch requires", () => {
    const schema = zodToToolJsonSchema(
      z.union([
        z.object({ id: z.string(), url: z.string() }),
        z.object({ id: z.string(), path: z.string() }),
      ]),
    );
    expectProviderSafe(schema);
    expect(Object.keys(props(schema))).toEqual(["id", "url", "path"]);
    expect(schema["required"]).toEqual(["id"]);
    expect(schema["description"]).toContain("shape 1: requires id, url");
  });

  it("merges a top-level intersection into one object", () => {
    const schema = zodToToolJsonSchema(
      z.object({ a: z.string() }).and(z.object({ b: z.number().optional() })),
    );
    expectProviderSafe(schema);
    expect(Object.keys(props(schema))).toEqual(["a", "b"]);
    expect(schema["required"]).toEqual(["a"]);
  });

  it("rejects a union with a non-object branch", () => {
    expect(() => zodToToolJsonSchema(z.union([z.object({ a: z.string() }), z.string()]))).toThrow(
      /must describe objects/,
    );
  });

  it("rejects a non-object top level", () => {
    expect(() => zodToToolJsonSchema(z.string())).toThrow(/must describe an object/);
  });

  it.each<[string, z.ZodType]>([
    ["refined object", z.object({ task: z.string() }).refine((v) => v.task.length > 0)],
    ["defaulted object", z.object({ a: z.string() }).default({ a: "x" })],
    ["record", z.record(z.string(), z.unknown())],
    ["any", z.any()],
    ["empty object", z.object({})],
  ])("%s is provider-safe", (_label, schema) => {
    expectProviderSafe(zodToToolJsonSchema(schema));
  });
});

describe("normalizeToolJsonSchema", () => {
  it("leaves nested combinators alone and only fixes the top level", () => {
    const raw = {
      $schema: "https://json-schema.org/draft/2020-12/schema",
      properties: { v: { anyOf: [{ type: "string" }, { type: "number" }] } },
    };
    const out = normalizeToolJsonSchema(raw);
    expect(out).toEqual({
      type: "object",
      properties: { v: { anyOf: [{ type: "string" }, { type: "number" }] } },
    });
    // Input is not mutated.
    expect(raw).toHaveProperty("$schema");
  });

  it("accepts a nullable object root as an object", () => {
    expect(normalizeToolJsonSchema({ type: ["object", "null"], properties: {} })["type"]).toBe(
      "object",
    );
  });

  it("flattens a raw oneOf with a const discriminator", () => {
    const out = normalizeToolJsonSchema({
      oneOf: [
        { type: "object", properties: { op: { const: "get" }, key: { type: "string" } } },
        { type: "object", properties: { op: { enum: ["put"] }, value: { type: "string" } } },
      ],
    });
    expectProviderSafe(out);
    expect(props(out)["op"]).toMatchObject({ enum: ["get", "put"] });
    expect(out["required"]).toEqual(["op"]);
  });
});

describe("AgentTool.jsonSchema override", () => {
  const mcpInputSchema = {
    $schema: "http://json-schema.org/draft-07/schema#",
    type: "object",
    properties: {
      path: { type: "string", description: "File path", pattern: "^/" },
      mode: { anyOf: [{ const: "r" }, { const: "w" }] },
    },
    required: ["path"],
    additionalProperties: false,
  };

  it("is advertised verbatim apart from top-level guards", () => {
    const t = tool({
      name: "read_file",
      description: "Read a file",
      parameters: z.record(z.string(), z.unknown()),
      jsonSchema: mcpInputSchema,
      execute: async () => "ok",
    });
    const { $schema: _s, ...expected } = mcpInputSchema;
    expect(agentToolJsonSchema(t)).toEqual(expected);
    expect(buildToolDefs({ read_file: t })[0]!.parameters).toEqual(expected);
  });

  it("gets the same top-level guards as a zod schema", () => {
    const t = tool({
      name: "op",
      description: "op",
      parameters: z.record(z.string(), z.unknown()),
      jsonSchema: {
        anyOf: [
          { type: "object", properties: { a: { type: "string" } }, required: ["a"] },
          { type: "object", properties: { b: { type: "string" } }, required: ["b"] },
        ],
      },
      execute: async () => "ok",
    });
    const schema = agentToolJsonSchema(t);
    expectProviderSafe(schema);
    expect(Object.keys(props(schema))).toEqual(["a", "b"]);
  });

  it("is carried through createScopedTool", () => {
    const t = createScopedTool({
      name: "scoped",
      description: "scoped",
      parameters: z.record(z.string(), z.unknown()),
      jsonSchema: mcpInputSchema,
      execute: async () => "ok",
    });
    expect(props(agentToolJsonSchema(t))["path"]).toMatchObject({ pattern: "^/" });
  });

  it("falls back to the zod parameters when unset", () => {
    const t = tool({
      name: "add",
      description: "add",
      parameters: z.object({ a: z.number().int().describe("first") }),
      execute: async () => 0,
    });
    expect(props(agentToolJsonSchema(t))["a"]).toEqual({ type: "integer", description: "first" });
  });
});
