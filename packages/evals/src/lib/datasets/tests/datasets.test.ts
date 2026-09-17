import { describe, expect, it } from "bun:test";
import type { EvalCase, EvalDataset } from "../../types.ts";
import { inlineDataset } from "../inline.ts";
import { jsonlDataset } from "../jsonl.ts";

async function collect(dataset: EvalDataset): Promise<EvalCase[]> {
  const cases: EvalCase[] = [];
  for await (const evalCase of dataset.cases()) cases.push(evalCase);
  return cases;
}

describe("inlineDataset", () => {
  it("yields the supplied cases and carries an id", async () => {
    const dataset = inlineDataset(
      [
        { id: "a", input: 1 },
        { id: "b", input: 2 },
      ],
      "demo",
    );
    expect(dataset.id).toBe("demo");
    const cases = await collect(dataset);
    expect(cases.map((c) => c.id)).toEqual(["a", "b"]);
  });
});

describe("jsonlDataset", () => {
  it("parses one case per line, skips blanks, synthesises missing ids", async () => {
    const path = `/tmp/evals-jsonl-${Date.now()}.jsonl`;
    await Bun.write(path, `{"input":"x","expected":"X"}\n\n{"id":"k","input":"y"}\n`);
    const cases = await collect(jsonlDataset(path, "file-ds"));
    expect(cases.length).toBe(2);
    expect(cases[0]?.id).toBe("file-ds-0");
    expect(cases[0]?.expected).toBe("X");
    expect(cases[1]?.id).toBe("k");
  });
});
