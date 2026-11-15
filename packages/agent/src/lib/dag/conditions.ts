import { pickPath } from "./dag-helpers.ts";
import type { Condition, ConditionScalar } from "./types.ts";

export const MAX_CONDITION_DEPTH = 8;

function scalarAtPath(params: { value: unknown; path: string }): unknown {
  return pickPath(params.value, params.path) ?? null;
}

function equalsScalar(params: { observed: unknown; expected: ConditionScalar }): boolean {
  return params.observed === params.expected;
}

export function evaluateCondition(params: { condition: Condition; value: unknown }): boolean {
  const { condition, value } = params;
  switch (condition.kind) {
    case "equals":
      return equalsScalar({
        observed: scalarAtPath({ value, path: condition.path }),
        expected: condition.value,
      });
    case "in": {
      const observed = scalarAtPath({ value, path: condition.path });
      return condition.values.some((expected) => equalsScalar({ observed, expected }));
    }
    case "exists":
      return pickPath(value, condition.path) != null;
    case "compare": {
      const observed = pickPath(value, condition.path);
      if (typeof observed !== "number") return false;
      switch (condition.op) {
        case "gt":
          return observed > condition.value;
        case "gte":
          return observed >= condition.value;
        case "lt":
          return observed < condition.value;
        case "lte":
          return observed <= condition.value;
      }
    }
    case "and":
      return condition.all.every((part) => evaluateCondition({ condition: part, value }));
    case "or":
      return condition.any.some((part) => evaluateCondition({ condition: part, value }));
    case "not":
      return !evaluateCondition({ condition: condition.of, value });
  }
}

export function conditionDepth(condition: Condition): number {
  switch (condition.kind) {
    case "and":
      return 1 + Math.max(0, ...condition.all.map(conditionDepth));
    case "or":
      return 1 + Math.max(0, ...condition.any.map(conditionDepth));
    case "not":
      return 1 + conditionDepth(condition.of);
    default:
      return 1;
  }
}
