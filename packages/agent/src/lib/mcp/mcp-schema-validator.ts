// ---------------------------------------------------------------------------
// JSON Schema validation for schemas an MCP server wrote.
//
// A server's schema is untrusted input to our validator:
//
//   - `pattern`, `patternProperties` and `format` compile to JavaScript
//     regular expressions, which backtrack: `^(a+)+$` against "aaaa…!"
//     blocks the event loop for seconds to hours. There is no linear-time
//     regex engine among our dependencies (re2 needs a native build), so
//     validation runs on a copy of the schema that keeps only an allowlist
//     of keywords — structure, types and bounds, none regex-backed. The
//     model still sees the full schema and the server validates its own
//     input.
//   - Anything not on the allowlist (`default`, `examples`, `x-*`, unknown
//     keywords) is dropped too, and every `$ref` must be a JSON pointer that
//     walks only through subschema keywords to a subschema of the copy: a
//     pointer into data (`#/default`, `#/properties/w/const`) or into a
//     dropped key would let Ajv compile an unsanitized subtree. A schema
//     with any other `$ref` is not validated (the server validates).
//   - `$id` would register a schema in the Ajv instance, so one server could
//     shadow or `$ref` another's; it is dropped with the rest.
//   - Every pool entry (server config × tenant × credential) gets its own
//     Ajv instance, so nothing compiled for one server is visible to another.
//
// Output schemas (`structuredContent`) are not validated at all: agents read
// a tool's content blocks, never its structured output, so the SDK client is
// given a validator that accepts everything.
// ---------------------------------------------------------------------------

import { AjvJsonSchemaValidator } from "@modelcontextprotocol/sdk/validation/ajv";
import type {
  JsonSchemaType,
  JsonSchemaValidator,
  jsonSchemaValidator,
} from "@modelcontextprotocol/sdk/validation/types.js";

export type CompiledInputValidator = (input: unknown) => {
  readonly valid: boolean;
  readonly errorMessage?: string;
};

/** Keywords whose value is a map of name → subschema. */
const SCHEMA_MAPS = new Set(["properties", "$defs", "definitions", "dependentSchemas"]);
/** Keywords whose value is one subschema. */
const SINGLE_SCHEMAS = new Set([
  "additionalItems",
  "additionalProperties",
  "unevaluatedItems",
  "unevaluatedProperties",
  "propertyNames",
  "contains",
  "not",
  "if",
  "then",
  "else",
]);
/** Keywords whose value is an array of subschemas. */
const SCHEMA_ARRAYS = new Set(["allOf", "anyOf", "oneOf", "prefixItems"]);
/** Keywords kept as plain values: none is regex-backed or a subschema. */
const VALUE_KEYWORDS = new Set([
  "type",
  "enum",
  "const",
  "required",
  "minimum",
  "maximum",
  "exclusiveMinimum",
  "exclusiveMaximum",
  "multipleOf",
  "minLength",
  "maxLength",
  "minItems",
  "maxItems",
  "uniqueItems",
  "minContains",
  "maxContains",
  "minProperties",
  "maxProperties",
  "dependentRequired",
]);
const MAX_DEPTH = 32;
const MAX_NODES = 5_000;

/**
 * The part of `schema` that is safe to validate with (see above), or
 * `null` when it can't be validated safely (a `$ref` that leaves the
 * schema's subschemas, too deep, too big).
 */
export function sanitizeValidationSchema(
  schema: Readonly<Record<string, unknown>>,
): Record<string, unknown> | null {
  const state = { nodes: 0, refs: [] as string[] };
  try {
    const safe = sanitizeSchema({ node: schema, depth: 0, state });
    if (typeof safe !== "object" || safe === null) throw new UnsafeSchema();
    for (const ref of state.refs) {
      if (!pointsToSubschema({ root: safe, ref })) throw new UnsafeSchema();
    }
    return safe as Record<string, unknown>;
  } catch (err) {
    if (err instanceof UnsafeSchema) return null;
    throw err;
  }
}

class UnsafeSchema extends Error {}

interface SanitizeState {
  nodes: number;
  readonly refs: string[];
}

function sanitizeSchema(params: { node: unknown; depth: number; state: SanitizeState }): unknown {
  const { node, depth, state } = params;
  if (typeof node === "boolean") return node;
  if (typeof node !== "object" || node === null || Array.isArray(node)) throw new UnsafeSchema();
  state.nodes += 1;
  if (depth > MAX_DEPTH || state.nodes > MAX_NODES) throw new UnsafeSchema();
  const sub = (value: unknown) => sanitizeSchema({ node: value, depth: depth + 1, state });
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(node as Record<string, unknown>)) {
    if (key === "$ref") {
      if (typeof value !== "string") throw new UnsafeSchema();
      state.refs.push(value);
      out[key] = value;
    } else if (SCHEMA_MAPS.has(key)) {
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new UnsafeSchema();
      }
      const map: Record<string, unknown> = {};
      for (const [name, s] of Object.entries(value as Record<string, unknown>)) map[name] = sub(s);
      out[key] = map;
    } else if (SINGLE_SCHEMAS.has(key)) {
      out[key] = sub(value);
    } else if (SCHEMA_ARRAYS.has(key)) {
      if (!Array.isArray(value)) throw new UnsafeSchema();
      out[key] = value.map(sub);
    } else if (key === "items") {
      out[key] = Array.isArray(value) ? value.map(sub) : sub(value);
    } else if (VALUE_KEYWORDS.has(key)) {
      out[key] = value;
    }
    // Everything else — pattern, patternProperties, format, $id, anchors,
    // default, examples, x-*, content*, unknown keywords — is dropped.
  }
  return out;
}

/**
 * Whether `ref` is a JSON pointer (`#` or `#/…`) that reaches a subschema
 * of `root` through subschema keywords only.
 */
function pointsToSubschema(params: { root: unknown; ref: string }): boolean {
  const { ref } = params;
  if (ref === "#") return true;
  if (!ref.startsWith("#/")) return false;
  let tokens: string[];
  try {
    tokens = ref
      .slice(2)
      .split("/")
      .map((t) => decodeURIComponent(t).replace(/~1/g, "/").replace(/~0/g, "~"));
  } catch {
    return false;
  }
  let node: unknown = params.root;
  for (let i = 0; i < tokens.length; i++) {
    if (typeof node !== "object" || node === null || Array.isArray(node)) return false;
    const keyword = tokens[i]!;
    const value = (node as Record<string, unknown>)[keyword];
    if (value === undefined || !Object.hasOwn(node, keyword)) return false;
    if (SINGLE_SCHEMAS.has(keyword) || (keyword === "items" && !Array.isArray(value))) {
      node = value;
      continue;
    }
    const next = tokens[i + 1];
    if (next === undefined) return false;
    i += 1;
    if (SCHEMA_MAPS.has(keyword)) {
      if (typeof value !== "object" || value === null || !Object.hasOwn(value, next)) return false;
      node = (value as Record<string, unknown>)[next];
    } else if (SCHEMA_ARRAYS.has(keyword) || keyword === "items") {
      if (!Array.isArray(value) || !/^(0|[1-9]\d*)$/.test(next)) return false;
      node = value[Number(next)];
      if (node === undefined) return false;
    } else {
      return false;
    }
  }
  return typeof node === "boolean" || (typeof node === "object" && node !== null);
}

const CACHE_LIMIT = 256;

/**
 * Compiled input validators for one MCP server under one tenant: its own
 * Ajv instance, schemas cached by text (least recently used dropped past
 * the cap), so re-resolving an agent doesn't recompile.
 */
export class McpInputValidators {
  private readonly ajv = new AjvJsonSchemaValidator();
  private readonly cache = new Map<string, CompiledInputValidator | null>();

  /** The validator for `schema`, or `null` when it can't be validated safely. */
  get(schema: Readonly<Record<string, unknown>>): CompiledInputValidator | null {
    const key = JSON.stringify(schema);
    if (this.cache.has(key)) {
      const hit = this.cache.get(key) ?? null;
      this.cache.delete(key);
      this.cache.set(key, hit);
      return hit;
    }
    const compiled = this.compile(schema);
    this.cache.set(key, compiled);
    if (this.cache.size > CACHE_LIMIT) {
      const oldest = this.cache.keys().next().value;
      if (oldest !== undefined) this.cache.delete(oldest);
    }
    return compiled;
  }

  private compile(schema: Readonly<Record<string, unknown>>): CompiledInputValidator | null {
    const safe = sanitizeValidationSchema(schema);
    if (!safe) return null;
    try {
      return this.ajv.getValidator(safe as JsonSchemaType);
    } catch {
      return null;
    }
  }
}

/** A `jsonSchemaValidator` for the SDK client that accepts every output. */
export const ACCEPT_ANY_OUTPUT: jsonSchemaValidator = {
  getValidator<T>(): JsonSchemaValidator<T> {
    return (input: unknown) => ({ valid: true, data: input as T, errorMessage: undefined });
  },
};
