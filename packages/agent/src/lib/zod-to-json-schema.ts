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
    return { type: "array", items: zodToJsonSchema(schema.element) };
  }
  if (schema instanceof z.ZodEnum) {
    return { type: "string", enum: schema.options };
  }
  if (schema instanceof z.ZodLiteral) {
    return { const: schema.value };
  }
  if (schema instanceof z.ZodOptional) {
    return zodToJsonSchema(schema.unwrap());
  }
  if (schema instanceof z.ZodNullable) {
    return { oneOf: [zodToJsonSchema(schema.unwrap()), { type: "null" }] };
  }
  if (schema instanceof z.ZodDefault) {
    return zodToJsonSchema(schema.removeDefault());
  }
  if (schema instanceof z.ZodEffects) {
    return zodToJsonSchema(schema.innerType());
  }
  if (schema instanceof z.ZodUnion) {
    return { oneOf: (schema.options as z.ZodType[]).map(zodToJsonSchema) };
  }
  // Zod 4 separates `ZodDiscriminatedUnion` from `ZodUnion` even though
  // they share `.options`. Anthropic requires top-level tool input_schema
  // to declare `type: "object"`, so we keep the `oneOf` branches for
  // the model's benefit and add `type: "object"` so the validator
  // accepts the schema (legal JSON Schema — `type` and `oneOf` compose).
  if (schema instanceof z.ZodDiscriminatedUnion) {
    const options = (schema as unknown as { options: z.ZodType[] }).options;
    return { type: "object", oneOf: options.map(zodToJsonSchema) };
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
