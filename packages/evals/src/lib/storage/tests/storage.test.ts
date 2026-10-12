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
    const dataset = inlineDataset({ cases, id: "ds" });
    await runEval({
      dataset,
      target: fnTarget({ fn: () => "x", id: "t" }),
      scorers: [exactMatch],
      persist: { store, runId: "base" },
    });
    await runEval({
      dataset,
      target: fnTarget({ fn: (input) => String(input).toUpperCase(), id: "t", version: "v2" }),
      scorers: [exactMatch],
      persist: { store, runId: "cand" },
    });

    const diff = await compareRuns({ store, baselineRunId: "base", candidateRunId: "cand" });
    expect(diff.passRateDelta).toBe(1);
    expect(diff.regressions).toEqual([]);
    expect(diff.perCase.map((c) => c.status)).toEqual(["improved", "improved"]);
    expect(diff.candidate.targetVersion).toBe("v2");
  });

  it("throws when a run id is missing", async () => {
    const store = new InMemoryEvalRunStore();
    await expect(
      compareRuns({ store, baselineRunId: "missing-a", candidateRunId: "missing-b" }),
    ).rejects.toThrow("baseline");
  });
});

describe("storedDataset", () => {
  it("materializes a stored dataset for runEval", async () => {
    const datasets = new InMemoryEvalDatasetStore();
    await datasets.save({ datasetId: "support", cases });
    const summary = await runEval({
      dataset: storedDataset({ store: datasets, datasetId: "support" }),
      target: fnTarget({ fn: (input) => String(input).toUpperCase() }),
      scorers: [exactMatch],
    });
    expect(summary.datasetId).toBe("support");
    expect(summary.passRate).toBe(1);
  });

  it("throws for an unknown dataset", async () => {
    const datasets = new InMemoryEvalDatasetStore();
    await expect(drain(storedDataset({ store: datasets, datasetId: "ghost" }))).rejects.toThrow(
      "ghost",
    );
  });
});
