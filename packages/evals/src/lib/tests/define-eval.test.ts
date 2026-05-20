import { describe, expect, it } from "bun:test";
import { inlineDataset } from "../datasets/inline.ts";
import { defineEval } from "../define-eval.ts";
import { exactMatch } from "../scorers/exact-match.ts";
import { fnTarget } from "../targets/fn-target.ts";

describe("defineEval", () => {
  it("coerces a bare case array and a bare function", async () => {
    const evaluation = defineEval("smoke", {
      data: [{ id: "1", input: "a", expected: "A" }],
      target: (input) => String(input).toUpperCase(),
      scorers: [exactMatch],
    });
    expect(evaluation.name).toBe("smoke");
    const summary = await evaluation.run();
    expect(summary.passRate).toBe(1);
    expect(summary.datasetId).toBe("smoke");
  });

  it("accepts ready-made dataset and target objects", async () => {
    const summary = await defineEval("obj", {
      data: inlineDataset([{ id: "1", input: "a", expected: "WRONG" }], "ds"),
      target: fnTarget((input) => String(input), { id: "t" }),
      scorers: [exactMatch],
    }).run();
    expect(summary.datasetId).toBe("ds");
    expect(summary.targetId).toBe("t");
    expect(summary.passRate).toBe(0);
  });
});
