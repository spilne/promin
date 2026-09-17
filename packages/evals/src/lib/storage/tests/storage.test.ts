import { describe, expect, it } from "bun:test";
import { inlineDataset } from "../../datasets/inline.ts";
import { runEval } from "../../runner.ts";
import { exactMatch } from "../../scorers/exact-match.ts";
import { fnTarget } from "../../targets/fn-target.ts";
import type { EvalCase, EvalDataset } from "../../types.ts";
import { compareRuns } from "../compare-runs.ts";
import { InMemoryEvalDatasetStore } from "../in-memory-dataset-store.ts";
import { InMemoryEvalRunStore } from "../in-memory-run-store.ts";
import { storedDataset } from "../stored-dataset.ts";

const cases: EvalCase[] = [
  { id: "1", input: "a", expected: "A" },
  { id: "2", input: "b", expected: "B" },
];

async function drain(dataset: EvalDataset): Promise<void> {
  for await (const evalCase of dataset.cases()) void evalCase;
}

describe("compareRuns", () => {
  it("diffs two stored runs", async () => {
    const store = new InMemoryEvalRunStore();
    const dataset = inlineDataset(cases, "ds");
    const baseline = await runEval({
      dataset,
      target: fnTarget(() => "x", { id: "t" }),
      scorers: [exactMatch],
    });
    const candidate = await runEval({
      dataset,
      target: fnTarget((input) => String(input).toUpperCase(), { id: "t", version: "v2" }),
      scorers: [exactMatch],
    });
    const baselineId = await store.save(baseline);
    const candidateId = await store.save(candidate);

    const diff = await compareRuns(store, baselineId, candidateId);
    expect(diff.passRateDelta).toBe(1);
    expect(diff.regressions).toEqual([]);
  });

  it("throws when a run id is missing", async () => {
    const store = new InMemoryEvalRunStore();
    await expect(compareRuns(store, "missing-a", "missing-b")).rejects.toThrow("baseline");
  });
});

describe("storedDataset", () => {
  it("materializes a stored dataset for runEval", async () => {
    const datasets = new InMemoryEvalDatasetStore();
    await datasets.save("support", cases);
    const summary = await runEval({
      dataset: storedDataset(datasets, "support"),
      target: fnTarget((input) => String(input).toUpperCase()),
      scorers: [exactMatch],
    });
    expect(summary.datasetId).toBe("support");
    expect(summary.passRate).toBe(1);
  });

  it("throws for an unknown dataset", async () => {
    const datasets = new InMemoryEvalDatasetStore();
    await expect(drain(storedDataset(datasets, "ghost"))).rejects.toThrow("ghost");
  });
});
