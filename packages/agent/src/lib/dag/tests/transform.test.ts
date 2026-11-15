import { describe, expect, it } from "bun:test";

import { applyTransform, TransformError } from "../transform.ts";
import type { TransformNode, TransformOp } from "../types.ts";

function run(
  ops: ReadonlyArray<TransformOp>,
  inputs: Record<string, unknown>,
  from?: string,
): unknown {
  const node: TransformNode = {
    id: "transform",
    kind: "transform",
    inputs: {},
    ...(from === undefined ? {} : { from }),
    ops,
  };
  return applyTransform({ node, inputs });
}

describe("applyTransform", () => {
  it("collect keeps the listed order", () => {
    expect(run([{ op: "collect", fields: ["c", "a"] }], { a: 1, b: 2, c: 3 })).toEqual([3, 1]);
  });

  it("pick selects a field", () => {
    expect(run([{ op: "pick", path: "items" }], { r: { items: [1] } }, "r")).toEqual([1]);
  });

  it("flatten is one level", () => {
    expect(run([{ op: "flatten" }], { x: [[1, [2]], [3]] }, "x")).toEqual([1, [2], 3]);
  });

  it("compact drops null and undefined only", () => {
    expect(run([{ op: "compact" }], { x: [0, null, "", undefined, false] }, "x")).toEqual([
      0,
      "",
      false,
    ]);
  });

  it("dedupe by path keeps the first", () => {
    expect(
      run(
        [{ op: "dedupe", by: "u" }],
        {
          x: [
            { u: "a", n: 1 },
            { u: "a", n: 2 },
          ],
        },
        "x",
      ),
    ).toEqual([{ u: "a", n: 1 }]);
  });

  it("dedupe without by ignores key order", () => {
    expect(
      run(
        [{ op: "dedupe" }],
        {
          x: [
            { a: 1, b: 2 },
            { b: 2, a: 1 },
          ],
        },
        "x",
      ),
    ).toEqual([{ a: 1, b: 2 }]);
  });

  it("filter keeps matches", () => {
    expect(
      run(
        [{ op: "filter", where: { kind: "equals", path: "v", value: "pass" } }],
        { x: [{ v: "pass" }, { v: "fail" }] },
        "x",
      ),
    ).toEqual([{ v: "pass" }]);
  });

  it("sort is stable and by code unit", () => {
    expect(
      run([{ op: "sort", by: "k" }], { x: [{ k: "b" }, { k: "B" }, { k: "a" }] }, "x"),
    ).toEqual([{ k: "B" }, { k: "a" }, { k: "b" }]);
  });

  it("sort desc on numbers", () => {
    expect(
      run([{ op: "sort", by: "k", order: "desc" }], { x: [{ k: 1 }, { k: 3 }, { k: 2 }] }, "x"),
    ).toEqual([{ k: 3 }, { k: 2 }, { k: 1 }]);
  });

  it("take and count chain", () => {
    expect(run([{ op: "take", count: 2 }, { op: "count" }], { x: [1, 2, 3] }, "x")).toBe(2);
  });

  it("a wrong shape names the operation", () => {
    expect(() => run([{ op: "flatten" }], { x: "str" }, "x")).toThrow(
      /^transform_failed: flatten at ops\[0\]/,
    );
  });

  it("sort refuses mixed or non-scalar keys", () => {
    expect(() => run([{ op: "sort", by: "k" }], { x: [{ k: 1 }, { k: "a" }] }, "x")).toThrow(
      TransformError,
    );
  });
});
