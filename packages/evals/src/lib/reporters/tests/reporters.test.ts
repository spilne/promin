import { describe, expect, it } from "bun:test";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { inlineDataset } from "../../datasets/inline.ts";
import { runEval } from "../../runner.ts";
import { exactMatch } from "../../scorers/exact-match.ts";
import { fnTarget } from "../../targets/fn-target.ts";
import { consoleReporter } from "../console.ts";
import { jsonReporter } from "../json.ts";

const dataset = inlineDataset({ cases: [{ id: "c1", input: "a", expected: "A" }], id: "ds" });
const upper = fnTarget({ fn: (input) => String(input).toUpperCase() });

describe("consoleReporter", () => {
  it("emits a line per case and a summary block", async () => {
    const lines: string[] = [];
    await runEval({
      dataset,
      target: upper,
      scorers: [exactMatch],
      reporters: [consoleReporter({ log: (line) => lines.push(line) })],
    });
    expect(lines.some((line) => line.includes("PASS") && line.includes("c1"))).toBe(true);
    expect(lines.some((line) => line.includes("exactMatch"))).toBe(true);
  });
});

describe("jsonReporter", () => {
  it("writes the run summary as JSON", async () => {
    const dir = await mkdtemp(path.join(tmpdir(), "evals-report-"));
    const file = path.join(dir, "report.json");
    await runEval({
      dataset,
      target: upper,
      scorers: [exactMatch],
      reporters: [jsonReporter(file)],
    });
    const written = JSON.parse(await Bun.file(file).text()) as {
      datasetId: string;
      passRate: number;
    };
    expect(written.datasetId).toBe("ds");
    expect(written.passRate).toBe(1);
  });
});
