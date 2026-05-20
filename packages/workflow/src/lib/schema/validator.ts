// ---------------------------------------------------------------------------
// JSON Schema subset validator — hand-rolled, ~100 LOC, zero deps.
//
// Pairs with `./builder.ts` — covers exactly the shapes the builder produces
// (string / number / integer / boolean / null / enum / array / object with
// required[] + additionalProperties / anyOf / const). Anything richer
// ($ref, $defs, conditional, allOf, format-asserts, pattern, etc.) is
// outside scope — if authors ever write those, swap to ajv. The author-
// facing API doesn't change.
//
// Returns `{ ok, errors }` with field paths in `errors[].path` (dot/bracket
// notation) so the server can surface "schema_mismatch: items[2].qty must
// be >= 0" without the caller having to walk the schema themselves.
// ---------------------------------------------------------------------------

import type { JsonSchema } from "./builder.ts";

export interface ValidationError {
  /** Dot/bracket-notation path into the value. `""` for root failures. */
  readonly path: string;
  readonly message: string;
}

export interface ValidationResult {
  readonly ok: boolean;
  readonly errors: ReadonlyArray<ValidationError>;
}

export function validate(value: unknown, schema: JsonSchema): ValidationResult {
  const errors: ValidationError[] = [];
  walk(value, schema, "", errors);
  return { ok: errors.length === 0, errors };
}

function walk(value: unknown, schema: JsonSchema, path: string, errors: ValidationError[]): void {
  // Empty schema (no type, no anyOf, no const) — matches everything. JSON
  // Schema convention; `s.unknown()` produces this shape.
  if (!("type" in schema) && !("anyOf" in schema) && !("const" in schema)) {
    return;
  }
  if ("anyOf" in schema) {
    // First-match wins; if none match, surface a single root-level error
    // (per-branch errors would be noisy and rarely actionable).
    for (const branch of schema.anyOf) {
      const branchErrors: ValidationError[] = [];
      walk(value, branch, path, branchErrors);
      if (branchErrors.length === 0) return;
    }
    errors.push({ path, message: "did not match any branch of anyOf" });
    return;
  }
  if ("const" in schema) {
    if (value !== schema.const) {
      errors.push({ path, message: `expected literal ${JSON.stringify(schema.const)}` });
    }
    return;
  }
  switch (schema.type) {
    case "null":
      if (value !== null) errors.push({ path, message: "expected null" });
      return;
    case "boolean":
      if (typeof value !== "boolean") errors.push({ path, message: "expected boolean" });
      return;
    case "string": {
      if (typeof value !== "string") {
        errors.push({ path, message: "expected string" });
        return;
      }
      if (schema.enum && !schema.enum.includes(value)) {
        errors.push({ path, message: `expected one of [${schema.enum.join(", ")}]` });
      }
      return;
    }
    case "number": {
      if (typeof value !== "number" || Number.isNaN(value)) {
        errors.push({ path, message: "expected number" });
        return;
      }
      if (schema.minimum !== undefined && value < schema.minimum) {
        errors.push({ path, message: `must be >= ${schema.minimum}` });
      }
      if (schema.maximum !== undefined && value > schema.maximum) {
        errors.push({ path, message: `must be <= ${schema.maximum}` });
      }
      return;
    }
    case "integer": {
      if (typeof value !== "number" || !Number.isInteger(value)) {
        errors.push({ path, message: "expected integer" });
        return;
      }
      if (schema.minimum !== undefined && value < schema.minimum) {
        errors.push({ path, message: `must be >= ${schema.minimum}` });
      }
      if (schema.maximum !== undefined && value > schema.maximum) {
        errors.push({ path, message: `must be <= ${schema.maximum}` });
      }
      return;
    }
    case "array": {
      if (!Array.isArray(value)) {
        errors.push({ path, message: "expected array" });
        return;
      }
      for (let i = 0; i < value.length; i++) {
        walk(value[i], schema.items, `${path}[${i}]`, errors);
      }
      return;
    }
    case "object": {
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
        errors.push({ path, message: "expected object" });
        return;
      }
      const obj = value as Record<string, unknown>;
      for (const requiredKey of schema.required ?? []) {
        if (!(requiredKey in obj)) {
          errors.push({ path: child(path, requiredKey), message: "missing required field" });
        }
      }
      for (const [key, childSchema] of Object.entries(schema.properties)) {
        if (key in obj) walk(obj[key], childSchema, child(path, key), errors);
      }
      if (schema.additionalProperties === false) {
        for (const k of Object.keys(obj)) {
          if (!(k in schema.properties)) {
            errors.push({ path: child(path, k), message: "unexpected property" });
          }
        }
      }
      return;
    }
  }
}

function child(parent: string, key: string): string {
  return parent === "" ? key : `${parent}.${key}`;
}
