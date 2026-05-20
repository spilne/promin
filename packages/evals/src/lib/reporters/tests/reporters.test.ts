import { describe, expect, it } from "bun:test";
import { inlineDataset } from "../../datasets/inline.ts";
import { runEval } from "../../runner.ts";
import { exactMatch } from "../../scorers/exact-match.ts";
import { fnTarget } from "../../targets/fn-target.ts";
import { consoleReporter } from "../console.ts";
import { jsonReporter } from "../json.ts";

describe("consoleReporter", () => {
  it("emits a line per case and a summary block", async () => {
    const lines: string[] = [];
    await runEval({
      dataset: inlineDataset([{ id: "c1", input: "a", expected: "A" }], "ds"),
      target: fnTarget((input) => String(input).toUpperCase()),
      scorers: [exactMatch],
      reporters: [consoleReporter({ log: (line) => lines.push(line) })],
    });
    expect(lines.some((line) => line.includes("PASS") && line.includes("c1"))).toBe(true);
    expect(lines.some((line) => line.includes("exactMatch"))).toBe(true);
  });
});

describe("jsonReporter", () => {
  it("writes the run summary as JSON", async () => {
    const path = `/tmp/evals-report-${Date.now()}.json`;
    await runEval({
      dataset: inlineDataset([{ id: "c1", input: "a", expected: "A" }], "ds"),
      target: fnTarget((input) => String(input).toUpperCase()),
      scorers: [exactMatch],
      reporters: [jsonReporter(path)],
    });
    const written = JSON.parse(await Bun.file(path).text()) as {
      datasetId: string;
      passRate: number;
    };
    expect(written.datasetId).toBe("ds");
    expect(written.passRate).toBe(1);
  });
});
