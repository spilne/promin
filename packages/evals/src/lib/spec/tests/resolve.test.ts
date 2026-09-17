import { describe, expect, it } from "bun:test";
import { fnTarget } from "../../targets/fn-target.ts";
import { resolveEvalSpec, runEvalSpec } from "../resolve.ts";
import type { EvalSpec } from "../types.ts";

function spec(over: Partial<EvalSpec> = {}): EvalSpec {
  return {
    id: "s",
    version: "v1",
    dataset: { kind: "inline", id: "ds", cases: [{ id: "1", input: "a", expected: "A" }] },
    targets: [{ kind: "recipe", recipeId: "bot" }],
    scorers: [{ kind: "exactMatch" }],
    createdAt: 0,
    updatedAt: 0,
    ...over,
  };
}

describe("resolveEvalSpec", () => {
  it("materializes the dataset, targets and scorers", async () => {
    const resolved = await resolveEvalSpec(spec(), {
      resolveTarget: () => fnTarget((input) => String(input).toUpperCase(), { id: "bot" }),
    });
    expect(resolved.dataset.id).toBe("ds");
    expect(resolved.targets.length).toBe(1);
    expect(resolved.scorers[0]?.id).toBe("exactMatch");
  });

  it("applies threshold / required overrides to a deterministic scorer", async () => {
    const resolved = await resolveEvalSpec(
      spec({ scorers: [{ kind: "regexMatch", pattern: "x", threshold: 0.5, required: false }] }),
      { resolveTarget: () => fnTarget(() => "x") },
    );
    expect(resolved.scorers[0]?.threshold).toBe(0.5);
    expect(resolved.scorers[0]?.required).toBe(false);
  });

  it("throws for a stored dataset when no datasetStore is supplied", async () => {
    await expect(
      resolveEvalSpec(spec({ dataset: { kind: "stored", datasetId: "missing" } }), {
        resolveTarget: () => fnTarget(() => "x"),
      }),
    ).rejects.toThrow("stored");
  });

  it("throws for an llmJudge scorer when its judge ref is unresolved", async () => {
    await expect(
      resolveEvalSpec(spec({ scorers: [{ kind: "llmJudge", judgeRef: "main", rubric: "r" }] }), {
        resolveTarget: () => fnTarget(() => "x"),
      }),
    ).rejects.toThrow("judge");
  });
});

describe("runEvalSpec", () => {
  it("resolves and runs a spec as a matrix", async () => {
    const result = await runEvalSpec(
      spec({
        targets: [
          { kind: "recipe", recipeId: "good" },
          { kind: "recipe", recipeId: "bad" },
        ],
      }),
      {
        resolveTarget: (target) =>
          target.recipeId === "good"
            ? fnTarget((input) => String(input).toUpperCase(), { id: "good" })
            : fnTarget(() => "nope", { id: "bad" }),
      },
    );
    expect(result.summaries.length).toBe(2);
    expect(result.diffs.length).toBe(1);
    expect(result.summaries[0]?.passRate).toBe(1);
    expect(result.summaries[1]?.passRate).toBe(0);
  });
});
