import { canonicalJson } from "../versioned-registry/store-helpers.ts";
import { evaluateCondition } from "./conditions.ts";
import { pickPath } from "./dag-helpers.ts";
import type { TransformNode, TransformOp } from "./types.ts";

export class TransformError extends Error {
  readonly _tag = "TransformError";

  constructor(params: { op: TransformOp["op"]; index: number; reason: string }) {
    super(`transform_failed: ${params.op} at ops[${params.index}]: ${params.reason}`);
    this.name = "TransformError";
  }
}

function arrayValue(params: { op: TransformOp["op"]; value: unknown }): ReadonlyArray<unknown> {
  if (!Array.isArray(params.value)) {
    throw new Error("expected an array");
  }
  return params.value;
}

function dedupe(params: { values: ReadonlyArray<unknown>; by?: string }): ReadonlyArray<unknown> {
  const seen = new Set<string>();
  const result: unknown[] = [];
  for (const value of params.values) {
    const key = params.by === undefined ? value : pickPath(value, params.by);
    const encoded = canonicalJson(key);
    if (seen.has(encoded)) continue;
    seen.add(encoded);
    result.push(value);
  }
  return result;
}

type SortKey = string | number | null;

function sortKey(params: { value: unknown; path: string }): SortKey {
  const key = pickPath(params.value, params.path);
  if (key === null || typeof key === "string" || typeof key === "number") return key;
  throw new Error(`sort key at "${params.path}" must be a string, number, or null`);
}

function sorted(params: {
  values: ReadonlyArray<unknown>;
  by: string;
  order?: "asc" | "desc";
}): ReadonlyArray<unknown> {
  const decorated = params.values.map((value, index) => ({
    index,
    key: sortKey({ value, path: params.by }),
    value,
  }));
  const keyTypes = new Set(
    decorated.flatMap(({ key }) => (key === null ? [] : ([typeof key] as const))),
  );
  if (keyTypes.size > 1) {
    throw new Error(`sort keys at "${params.by}" must not mix strings and numbers`);
  }
  const direction = params.order === "desc" ? -1 : 1;
  decorated.sort((left, right) => {
    if (left.key === null && right.key === null) return left.index - right.index;
    if (left.key === null) return 1;
    if (right.key === null) return -1;
    const comparison = left.key < right.key ? -1 : left.key > right.key ? 1 : 0;
    return comparison === 0 ? left.index - right.index : comparison * direction;
  });
  return decorated.map(({ value }) => value);
}

function applyOperation(params: {
  op: TransformOp;
  value: unknown;
  inputs: Record<string, unknown>;
}): unknown {
  const { op, value } = params;
  switch (op.op) {
    case "collect":
      return op.fields.map((field) => params.inputs[field]);
    case "pick":
      return pickPath(value, op.path);
    case "flatten":
      return arrayValue({ op: op.op, value }).flat();
    case "compact":
      return arrayValue({ op: op.op, value }).filter((item) => item !== null && item !== undefined);
    case "count":
      return arrayValue({ op: op.op, value }).length;
    case "dedupe":
      return dedupe({ values: arrayValue({ op: op.op, value }), ...(op.by ? { by: op.by } : {}) });
    case "filter":
      return arrayValue({ op: op.op, value }).filter((item) =>
        evaluateCondition({ condition: op.where, value: item }),
      );
    case "sort":
      return sorted({
        values: arrayValue({ op: op.op, value }),
        by: op.by,
        ...(op.order ? { order: op.order } : {}),
      });
    case "take":
      if (!Number.isInteger(op.count) || op.count < 0) {
        throw new Error("count must be a non-negative integer");
      }
      return arrayValue({ op: op.op, value }).slice(0, op.count);
  }
}

export function applyTransform(params: {
  node: TransformNode;
  inputs: Record<string, unknown>;
}): unknown {
  let value: unknown =
    params.node.from === undefined ? params.inputs : params.inputs[params.node.from];
  for (const [index, op] of params.node.ops.entries()) {
    try {
      value = applyOperation({ op, value, inputs: params.inputs });
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      throw new TransformError({ op: op.op, index, reason });
    }
  }
  return value;
}
