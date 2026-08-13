import { z } from "zod";

export function zodToJsonSchema(schema: z.ZodType): Record<string, unknown> {
  if (schema instanceof z.ZodString) {
    return { type: "string", ...(schema.description ? { description: schema.description } : {}) };
  }
  if (schema instanceof z.ZodNumber) {
    return { type: "number", ...(schema.description ? { description: schema.description } : {}) };
  }
  if (schema instanceof z.ZodBoolean) {
    return { type: "boolean" };
  }
  if (schema instanceof z.ZodNull) {
    return { type: "null" };
  }
  if (schema instanceof z.ZodArray) {
    return { type: "array", items: zodToJsonSchema(schema.element as unknown as z.ZodType) };
  }
  if (schema instanceof z.ZodEnum) {
    return { type: "string", enum: schema.options };
  }
  if (schema instanceof z.ZodLiteral) {
    return { const: schema.value };
  }
  if (schema instanceof z.ZodOptional) {
    return zodToJsonSchema(schema.unwrap() as unknown as z.ZodType);
  }
  if (schema instanceof z.ZodNullable) {
    return { oneOf: [zodToJsonSchema(schema.unwrap() as unknown as z.ZodType), { type: "null" }] };
  }
  if (schema instanceof z.ZodDefault) {
    return zodToJsonSchema(schema.removeDefault() as unknown as z.ZodType);
  }
  if (schema instanceof z.ZodPipe) {
    return zodToJsonSchema(schema._def.in as z.ZodType);
  }
  // Zod 4 separates `ZodDiscriminatedUnion` from `ZodUnion`. Anthropic's
  // tool input_schema validator requires `type: "object"` at the top
  // level AND explicitly forbids top-level `oneOf` / `allOf` / `anyOf`
  // ("input_schema does not support oneOf, allOf, or anyOf at the top
  // level"). So we flatten: one object with the union of all branch
  // properties, the discriminator field as a required enum over the
  // branch literals, and every non-discriminator field as optional —
  // which fields are needed depends on the discriminator value, and
  // we encode that contract in the field descriptions for the model.
  // Runtime Zod validation still rejects malformed combinations.
  if (schema instanceof z.ZodDiscriminatedUnion) {
    return discriminatedUnionToFlatObject(
      schema as unknown as {
        discriminator?: string;
        options: z.ZodType[];
        _def?: { discriminator?: string };
      },
    );
  }
  if (schema instanceof z.ZodUnion) {
    return { oneOf: (schema.options as z.ZodType[]).map(zodToJsonSchema) };
  }
  if (schema instanceof z.ZodObject) {
    const shape = schema.shape as Record<string, z.ZodType>;
    const properties: Record<string, unknown> = {};
    const required: string[] = [];
    for (const [key, value] of Object.entries(shape)) {
      properties[key] = zodToJsonSchema(value);
      if (!(value instanceof z.ZodOptional) && !(value instanceof z.ZodDefault)) {
        required.push(key);
      }
    }
    const result: Record<string, unknown> = { type: "object", properties };
    if (required.length > 0) result["required"] = required;
    if (schema.description) result["description"] = schema.description;
    return result;
  }
  return {};
}

/**
 * Flatten a `z.discriminatedUnion(key, [...])` into a single JSON
 * Schema object that Anthropic's tool input_schema validator accepts.
 *
 * Strategy:
 *   - Top-level `type: "object"` (required by Anthropic).
 *   - Discriminator field becomes `{ enum: [...all branch literals] }`,
 *     marked required.
 *   - All other fields from every branch become optional properties
 *     (the discriminator value decides which subset is "really"
 *     required, but JSON Schema can't express that without `oneOf`).
 *   - Per-branch contract surfaces in the auto-generated description
 *     so the model knows which fields go with which value.
 */
function discriminatedUnionToFlatObject(schema: {
  discriminator?: string;
  options: z.ZodType[];
  _def?: { discriminator?: string };
}): Record<string, unknown> {
  const discriminator = schema.discriminator ?? schema._def?.discriminator;
  if (!discriminator) return {};
  const branches = schema.options.map((opt) => zodToJsonSchema(opt));
  const properties: Record<string, unknown> = {};
  const literals: unknown[] = [];
  const branchSummaries: string[] = [];

  for (const branch of branches) {
    const branchProps = (branch["properties"] as Record<string, unknown>) ?? {};
    const branchRequired = (branch["required"] as string[]) ?? [];
    const discProp = branchProps[discriminator] as { const?: unknown } | undefined;
    if (discProp && "const" in discProp) literals.push(discProp.const);

    // Collect non-discriminator fields, narrowing types via simple
    // last-write-wins. Branches usually carry disjoint extra fields;
    // when they overlap we keep the most permissive shape we've seen.
    for (const [key, value] of Object.entries(branchProps)) {
      if (key === discriminator) continue;
      if (!(key in properties)) properties[key] = value;
    }

    // Build a "when {disc}={literal}, requires: x, y" line for the
    // model's description. Skips the discriminator itself.
    const literal = discProp && "const" in discProp ? JSON.stringify(discProp.const) : "(unknown)";
    const required = branchRequired.filter((k) => k !== discriminator);
    const summary =
      required.length > 0
        ? `${literal}: requires ${required.join(", ")}`
        : `${literal}: no extra fields`;
    branchSummaries.push(summary);
  }

  // Infer the discriminator's primitive type from the literal values so
  // the JSON schema is self-describing. Small models (qwen2.5:3b,
  // llama3.2:3b) drop required-discriminator fields when the property
  // has no `type`. Falls back to "string" if the literals are mixed —
  // unusual but cheap to handle.
  const inferredType = literals.every((v) => typeof v === "string")
    ? "string"
    : literals.every((v) => typeof v === "number")
      ? "number"
      : literals.every((v) => typeof v === "boolean")
        ? "boolean"
        : "string";

  const branchHint = branchSummaries.join("; ");
  const result: Record<string, unknown> = {
    type: "object",
    properties: {
      [discriminator]: {
        type: inferredType,
        enum: literals,
        description: `REQUIRED. Pick one. ${branchHint}.`,
      },
      ...properties,
    },
    required: [discriminator],
  };

  if (branchSummaries.length > 0) {
    result["description"] = `Discriminated by \`${discriminator}\`. ${branchHint}.`;
  }
  return result;
}
