import { describe, expect, it } from "bun:test";

import { conditionDepth, evaluateCondition } from "../conditions.ts";
import type { Condition } from "../types.ts";

describe("DAG conditions", () => {
  const value = { verdict: "pass", score: 0.8, n: null, list: ["a"] };
  const evaluate = (condition: Condition) => evaluateCondition({ condition, value });

  it("equals reads a path; a missing path reads as null", () => {
    expect(evaluate({ kind: "equals", path: "verdict", value: "pass" })).toBe(true);
    expect(evaluate({ kind: "equals", path: "missing", value: null })).toBe(true);
    expect(evaluate({ kind: "equals", path: "n", value: null })).toBe(true);
  });

  it("exists is false for null and for a missing path", () => {
    expect(evaluate({ kind: "exists", path: "n" })).toBe(false);
    expect(evaluate({ kind: "exists", path: "missing" })).toBe(false);
    expect(evaluate({ kind: "exists", path: "list.0" })).toBe(true);
  });

  it("in matches any listed scalar", () => {
    expect(evaluate({ kind: "in", path: "verdict", values: ["fail", "pass"] })).toBe(true);
  });

  it("compare is false on a non-number", () => {
    expect(evaluate({ kind: "compare", path: "score", op: "gte", value: 0.8 })).toBe(true);
    expect(evaluate({ kind: "compare", path: "score", op: "gt", value: 0.8 })).toBe(false);
    expect(evaluate({ kind: "compare", path: "verdict", op: "gt", value: 0 })).toBe(false);
  });

  it("and / or / not combine", () => {
    const pass: Condition = { kind: "equals", path: "verdict", value: "pass" };
    expect(evaluate({ kind: "and", all: [pass, { kind: "not", of: pass }] })).toBe(false);
    expect(evaluate({ kind: "or", any: [{ kind: "not", of: pass }, pass] })).toBe(true);
  });

  it("the empty path is the value itself", () => {
    expect(
      evaluateCondition({
        condition: { kind: "equals", path: "", value: "yes" },
        value: "yes",
      }),
    ).toBe(true);
  });

  it("conditionDepth counts nesting", () => {
    let condition: Condition = { kind: "exists", path: "x" };
    for (let i = 0; i < 8; i++) condition = { kind: "not", of: condition };
    expect(conditionDepth(condition)).toBe(9);
  });
});
