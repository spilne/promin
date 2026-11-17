import { z } from "zod";
import type { JSONSchema } from "zod/v4/core";

export const MAX_OUTPUT_SCHEMA_BYTES = 16_384;

const JSON_SCHEMA_TYPES = new Set([
  "object",
  "array",
  "string",
  "number",
  "integer",
  "boolean",
  "null",
]);

export class OutputSchemaError extends Error {
  readonly _tag = "OutputSchemaError";

  constructor(message: string) {
    super(message);
    this.name = "OutputSchemaError";
  }
}

/** Compile the supported object-rooted JSON Schema into a Zod validator. */
export function compileOutputSchema(schema: unknown): z.ZodType {
  if (!isPlainObject(schema)) {
    throw new OutputSchemaError("schema must be a plain object");
  }
  if (schema.type !== "object") {
    throw new OutputSchemaError('top-level type must be "object"');
  }

  let serialised: string;
  try {
    serialised = JSON.stringify(schema);
  } catch (error) {
    throw new OutputSchemaError(`schema must be JSON-serialisable: ${errorMessage(error)}`);
  }
  if (Buffer.byteLength(serialised, "utf8") > MAX_OUTPUT_SCHEMA_BYTES) {
    throw new OutputSchemaError(`schema exceeds ${MAX_OUTPUT_SCHEMA_BYTES} bytes`);
  }

  assertTypeKeywords(schema, "$", new Set<object>());
  try {
    return z.fromJSONSchema(schema as JSONSchema.JSONSchema);
  } catch (error) {
    throw new OutputSchemaError(`schema could not be compiled: ${errorMessage(error)}`);
  }
}

/** Parse a JSON reply, allowing one surrounding `json` Markdown fence. */
export function extractJson(text: string): unknown {
  const match = /^\s*```json\s*\n([\s\S]*?)\n```\s*$/.exec(text);
  const json = match?.[1] ?? text;
  try {
    return JSON.parse(json);
  } catch (error) {
    throw new OutputSchemaError(`response is not valid JSON: ${errorMessage(error)}`);
  }
}

/** Instruction for invoke-style agents that cannot receive a schema directly. */
export function schemaInstruction(schema: Record<string, unknown>): string {
  return (
    "Reply with one JSON object and nothing else. It must match this JSON Schema:\n" +
    JSON.stringify(schema, null, 2)
  );
}

function assertTypeKeywords(value: unknown, path: string, seen: Set<object>): void {
  if (value === null || typeof value !== "object") return;
  if (seen.has(value)) {
    throw new OutputSchemaError(`schema contains a cycle at ${path}`);
  }
  seen.add(value);

  if (Array.isArray(value)) {
    for (const [index, item] of value.entries()) {
      assertTypeKeywords(item, `${path}[${index}]`, seen);
    }
    seen.delete(value);
    return;
  }

  for (const [key, child] of Object.entries(value)) {
    if (key === "type") assertTypeKeyword(child, `${path}.type`);
    assertTypeKeywords(child, `${path}.${key}`, seen);
  }
  seen.delete(value);
}

function assertTypeKeyword(value: unknown, path: string): void {
  const types = Array.isArray(value) ? value : [value];
  if (
    types.length === 0 ||
    types.some((type) => typeof type !== "string" || !JSON_SCHEMA_TYPES.has(type))
  ) {
    throw new OutputSchemaError(`${path} contains an unknown JSON Schema type`);
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
