import { z } from "zod";
import type { AgentTool } from "./tool.ts";

/**
 * JSON Schema for an LLM tool's input (`input_schema` on Anthropic,
 * `function.parameters` on OpenAI / Ollama).
 *
 * The body comes from zod's own `z.toJSONSchema` (input side, draft
 * 2020-12), so descriptions, `integer`, min/max, formats, defaults,
 * records, tuples and nullables survive. Only the top level is
 * post-processed, because that is where the providers disagree with
 * plain JSON Schema:
 *
 * - Anthropic requires `type: "object"` and rejects `oneOf` / `anyOf` /
 *   `allOf` at the top level.
 * - OpenAI requires the parameters schema to be `type: "object"`.
 * - Ollama decodes the top level into a fixed `{ type, properties,
 *   required, $defs }` shape, so anything else there is silently lost.
 *
 * A top-level union of objects is flattened into one object (see
 * {@link normalizeToolJsonSchema}). Runtime validation is still done
 * by the Zod schema, so the flattened shape only has to guide the model.
 */
export function zodToToolJsonSchema(schema: z.ZodType): Record<string, unknown> {
  const raw = z.toJSONSchema(schema, {
    io: "input",
    target: "draft-2020-12",
    unrepresentable: ({ zodSchema }) =>
      // A date arrives from the model as an ISO string (use `z.coerce.date()`
      // to accept it); every other unrepresentable type becomes `{}`.
      zodSchema._zod.def.type === "date" ? { type: "string", format: "date-time" } : "any",
    override: ({ jsonSchema }) => {
      // `.int()` implies the safe-integer range; spelling it out on every
      // integer field is noise in the model's context.
      if (jsonSchema.minimum === Number.MIN_SAFE_INTEGER) delete jsonSchema.minimum;
      if (jsonSchema.maximum === Number.MAX_SAFE_INTEGER) delete jsonSchema.maximum;
    },
  }) as Record<string, unknown>;
  return normalizeToolJsonSchema(raw);
}

/**
 * The JSON Schema advertised for `tool`: its raw `jsonSchema` override when
 * set (with the same top-level guards), otherwise one derived from
 * `tool.parameters`.
 */
export function agentToolJsonSchema(tool: AgentTool<any, any>): Record<string, unknown> {
  return tool.jsonSchema !== undefined
    ? normalizeToolJsonSchema(tool.jsonSchema)
    : zodToToolJsonSchema(tool.parameters);
}

/**
 * Apply the top-level guards every provider needs to an arbitrary JSON
 * Schema, without touching nested schemas. Returns a new object.
 *
 * - drops `$schema`;
 * - flattens a top-level `oneOf` / `anyOf` of object branches into one
 *   object: properties are merged, and a property that is a `const` in
 *   every branch becomes a required enum (the discriminator), with a
 *   description telling the model which fields go with each value;
 * - merges a top-level `allOf` of object branches;
 * - guarantees `type: "object"` and a `properties` map.
 *
 * Throws when the schema cannot describe an object (a top-level string,
 * a union with a non-object branch): tool arguments are always objects.
 */
export function normalizeToolJsonSchema(
  schema: Readonly<Record<string, unknown>>,
): Record<string, unknown> {
  const { $schema: _ignored, ...rest } = structuredClone(schema) as Record<string, unknown>;
  let out = rest;
  const union = (out["oneOf"] ?? out["anyOf"]) as unknown;
  if (Array.isArray(union)) out = flattenUnion({ root: out, branches: union });
  if (Array.isArray(out["allOf"])) out = mergeAllOf({ root: out, branches: out["allOf"] });

  out["type"] = objectType(out["type"]);
  if (!isRecord(out["properties"])) out["properties"] = {};
  return out;
}

function objectType(type: unknown): "object" {
  if (type === undefined || isObjectType(type)) return "object";
  throw new TypeError(
    `Tool input schema must describe an object, got type ${JSON.stringify(type)}`,
  );
}

function isObjectType(type: unknown): boolean {
  return type === "object" || (Array.isArray(type) && type.includes("object"));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

interface ObjectBranch {
  properties: Record<string, unknown>;
  required: string[];
}

function objectBranch(branch: unknown): ObjectBranch {
  const describesObject =
    isRecord(branch) &&
    (isObjectType(branch["type"]) ||
      (branch["type"] === undefined && isRecord(branch["properties"])));
  if (!describesObject) {
    throw new TypeError(
      `Tool input schema union branches must describe objects, got ${JSON.stringify(branch)}`,
    );
  }
  return {
    properties: isRecord(branch["properties"]) ? branch["properties"] : {},
    required: Array.isArray(branch["required"]) ? (branch["required"] as string[]) : [],
  };
}

/** The single value a property schema pins, if it pins exactly one. */
function constValue(prop: unknown): { value: unknown } | undefined {
  if (!isRecord(prop)) return undefined;
  if ("const" in prop) return { value: prop["const"] };
  const e = prop["enum"];
  if (Array.isArray(e) && e.length === 1) return { value: e[0] };
  return undefined;
}

function findDiscriminator(branches: ObjectBranch[]): string | undefined {
  const [first] = branches;
  if (!first) return undefined;
  return Object.keys(first.properties).find((key) => {
    const values = branches.map((b) => constValue(b.properties[key]));
    if (values.some((v) => v === undefined)) return false;
    const distinct = new Set(values.map((v) => JSON.stringify(v!.value)));
    return distinct.size === branches.length;
  });
}

function flattenUnion(params: {
  root: Record<string, unknown>;
  branches: unknown[];
}): Record<string, unknown> {
  const { oneOf: _o, anyOf: _a, ...root } = params.root;
  if (params.branches.length === 0) throw new TypeError("Tool input schema union has no branches");
  const branches = params.branches.map(objectBranch);
  const discriminator = findDiscriminator(branches);

  // First branch to declare a property wins; branches usually carry
  // disjoint extra fields.
  const properties: Record<string, unknown> = {};
  for (const branch of branches) {
    for (const [key, value] of Object.entries(branch.properties)) {
      if (key !== discriminator && !(key in properties)) properties[key] = value;
    }
  }

  const summaries = branches.map((branch, i) => {
    const label =
      discriminator !== undefined
        ? JSON.stringify(constValue(branch.properties[discriminator])!.value)
        : `shape ${i + 1}`;
    const required = branch.required.filter((k) => k !== discriminator);
    return required.length > 0
      ? `${label}: requires ${required.join(", ")}`
      : `${label}: no extra fields`;
  });
  const hint = summaries.join("; ");
  const ownDescription = typeof root["description"] === "string" ? `${root["description"]} ` : "";

  if (discriminator === undefined) {
    // No discriminator: only the fields every branch requires stay required.
    const required = branches
      .map((b) => b.required)
      .reduce((acc, r) => acc.filter((k) => r.includes(k)));
    return {
      ...root,
      type: "object",
      description: `${ownDescription}Provide exactly one of these shapes. ${hint}.`,
      properties,
      ...(required.length > 0 && { required }),
    };
  }

  const literals = branches.map((b) => constValue(b.properties[discriminator])!.value);
  // Small tool-tuned models drop a required enum that has no `type`.
  const literalType = literals.every((v) => typeof v === "number")
    ? "number"
    : literals.every((v) => typeof v === "boolean")
      ? "boolean"
      : "string";
  return {
    ...root,
    type: "object",
    description: `${ownDescription}Discriminated by \`${discriminator}\`. ${hint}.`,
    properties: {
      [discriminator]: {
        type: literalType,
        enum: literals,
        description: `REQUIRED. Pick one. ${hint}.`,
      },
      ...properties,
    },
    required: [discriminator],
  };
}

function mergeAllOf(params: {
  root: Record<string, unknown>;
  branches: unknown[];
}): Record<string, unknown> {
  const { allOf: _all, ...root } = params.root;
  const properties: Record<string, unknown> = isRecord(root["properties"])
    ? { ...root["properties"] }
    : {};
  const required = new Set<string>(Array.isArray(root["required"]) ? root["required"] : []);
  for (const branch of params.branches.map(objectBranch)) {
    for (const [key, value] of Object.entries(branch.properties)) {
      if (!(key in properties)) properties[key] = value;
    }
    for (const key of branch.required) required.add(key);
  }
  return {
    ...root,
    type: "object",
    properties,
    ...(required.size > 0 && { required: [...required] }),
  };
}
