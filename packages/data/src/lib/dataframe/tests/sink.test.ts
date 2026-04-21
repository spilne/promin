import { describe, it, expect, afterEach } from "bun:test";
import { readFileSync, unlinkSync, existsSync } from "node:fs";
import { DataFrame } from "../dataframe.ts";
import { CsvSink, JsonlSink } from "../sink.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const tempFiles: string[] = [];

function tempPath(ext: string): string {
  const p = `/tmp/promin-sink-test-${Date.now()}-${Math.random().toString(36).slice(2)}.${ext}`;
  tempFiles.push(p);
  return p;
}

afterEach(() => {
  for (const p of tempFiles) {
    if (existsSync(p)) unlinkSync(p);
  }
  tempFiles.length = 0;
});

// ---------------------------------------------------------------------------
// CsvSink
// ---------------------------------------------------------------------------

describe("CsvSink", () => {
  it("writes rows with header", async () => {
    const df = DataFrame.fromArray([
      { name: "Alice", age: 30 },
      { name: "Bob", age: 25 },
    ]);
    const path = tempPath("csv");
    await df.to(CsvSink(path));
    const content = readFileSync(path, "utf-8");
    expect(content).toBe("name,age\nAlice,30\nBob,25\n");
  });

  it("writes without header when disabled", async () => {
    const df = DataFrame.fromArray([{ x: 1, y: 2 }]);
    const path = tempPath("csv");
    await df.to(CsvSink(path, { header: false }));
    const content = readFileSync(path, "utf-8");
    expect(content).toBe("1,2\n");
  });

  it("uses custom delimiter", async () => {
    const df = DataFrame.fromArray([{ a: "foo", b: "bar" }]);
    const path = tempPath("tsv");
    await df.to(CsvSink(path, { delimiter: "\t" }));
    const content = readFileSync(path, "utf-8");
    expect(content).toBe("a\tb\nfoo\tbar\n");
  });

  it("escapes delimiters and quotes", async () => {
    const df = DataFrame.fromArray([{ name: 'O"Brien', city: "New York, NY" }]);
    const path = tempPath("csv");
    await df.to(CsvSink(path));
    const content = readFileSync(path, "utf-8");
    expect(content).toContain('"O""Brien"');
    expect(content).toContain('"New York, NY"');
  });

  it("handles null and undefined values", async () => {
    const df = DataFrame.fromArray([{ a: null, b: undefined, c: 1 }]);
    const path = tempPath("csv");
    await df.to(CsvSink(path));
    const content = readFileSync(path, "utf-8");
    expect(content).toBe("a,b,c\n,,1\n");
  });
});

// ---------------------------------------------------------------------------
// JsonlSink
// ---------------------------------------------------------------------------

describe("JsonlSink", () => {
  it("writes one JSON object per line", async () => {
    const df = DataFrame.fromArray([
      { x: 1, y: "a" },
      { x: 2, y: "b" },
    ]);
    const path = tempPath("jsonl");
    await df.to(JsonlSink(path));
    const lines = readFileSync(path, "utf-8").trim().split("\n");
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0]!)).toEqual({ x: 1, y: "a" });
    expect(JSON.parse(lines[1]!)).toEqual({ x: 2, y: "b" });
  });

  it("handles empty dataframe", async () => {
    const df = DataFrame.fromArray([]);
    const path = tempPath("jsonl");
    await df.to(JsonlSink(path));
    const content = readFileSync(path, "utf-8");
    expect(content).toBe("\n");
  });
});
