import { describe, it, expect } from "bun:test";
import { DataFrame } from "./dataframe.ts";
import { classifyPlan } from "./plan-classifier.ts";
import type { LogicalPlan } from "./logical-plan.ts";

// ---------------------------------------------------------------------------
// Plan classifier
// ---------------------------------------------------------------------------

describe("classifyPlan", () => {
  it("classifies Source as streamable", () => {
    const plan: LogicalPlan = { _tag: "Source", data: [] };
    expect(classifyPlan(plan)).toBe("streamable");
  });

  it("classifies Filter -> Source as streamable", () => {
    const plan: LogicalPlan = {
      _tag: "Filter",
      input: { _tag: "Source", data: [] },
      fn: () => true,
    };
    expect(classifyPlan(plan)).toBe("streamable");
  });

  it("classifies Select -> Filter -> Source as streamable", () => {
    const plan: LogicalPlan = {
      _tag: "Select",
      columns: ["a"],
      input: {
        _tag: "Filter",
        input: { _tag: "Source", data: [] },
        fn: () => true,
      },
    };
    expect(classifyPlan(plan)).toBe("streamable");
  });

  it("classifies Sort as materializing", () => {
    const plan: LogicalPlan = {
      _tag: "Sort",
      input: { _tag: "Source", data: [] },
      by: "x",
      order: "asc",
    };
    expect(classifyPlan(plan)).toBe("materializing");
  });

  it("classifies GroupBy as materializing", () => {
    const plan: LogicalPlan = {
      _tag: "GroupBy",
      input: { _tag: "Source", data: [] },
      columns: ["x"],
      aggs: { x: "count" },
    };
    expect(classifyPlan(plan)).toBe("materializing");
  });

  it("classifies Filter -> Sort -> Source as materializing", () => {
    const plan: LogicalPlan = {
      _tag: "Filter",
      fn: () => true,
      input: {
        _tag: "Sort",
        input: { _tag: "Source", data: [] },
        by: "x",
        order: "asc",
      },
    };
    expect(classifyPlan(plan)).toBe("materializing");
  });
});

// ---------------------------------------------------------------------------
// DataFrame.stream()
// ---------------------------------------------------------------------------

describe("DataFrame.stream()", () => {
  it("yields rows from a streamable plan", async () => {
    const df = DataFrame.fromArray([
      { id: 1, name: "a" },
      { id: 2, name: "b" },
      { id: 3, name: "c" },
      { id: 4, name: "d" },
      { id: 5, name: "e" },
    ]);

    const rows = await df
      .filter((r: any) => r.id > 2)
      .select("id", "name")
      .stream({ chunkSize: 2 })
      .collect();

    expect(rows).toEqual([
      { id: 3, name: "c" },
      { id: 4, name: "d" },
      { id: 5, name: "e" },
    ]);
  });

  it("processes in batches with chunkSize", async () => {
    const data = Array.from({ length: 100 }, (_, i) => ({ v: i }));
    const df = DataFrame.fromArray(data);

    const result = await df
      .filter((r: any) => r.v >= 50)
      .stream({ chunkSize: 20 })
      .collect();

    expect(result).toHaveLength(50);
    expect(result[0]!.v).toBe(50);
  });

  it("falls back for materializing plans", async () => {
    const df = DataFrame.fromArray([{ v: 3 }, { v: 1 }, { v: 2 }]);

    const result = await df.sort("v", "asc").stream().collect();

    expect(result).toEqual([{ v: 1 }, { v: 2 }, { v: 3 }]);
  });

  it("works with map and withColumn", async () => {
    const df = DataFrame.fromArray([{ x: 1 }, { x: 2 }, { x: 3 }]);

    const result = await df
      .withColumn("doubled", (r: any) => r.x * 2)
      .stream({ chunkSize: 1 })
      .collect();

    expect(result).toEqual([
      { x: 1, doubled: 2 },
      { x: 2, doubled: 4 },
      { x: 3, doubled: 6 },
    ]);
  });

  it("works with rename", async () => {
    const df = DataFrame.fromArray([{ a: 1 }, { a: 2 }]);

    const result = await df.rename({ a: "b" }).stream({ chunkSize: 1 }).collect();

    expect(result).toEqual([{ b: 1 }, { b: 2 }]);
  });

  it("works with drop", async () => {
    const df = DataFrame.fromArray([
      { a: 1, b: 2, c: 3 },
      { a: 4, b: 5, c: 6 },
    ]);

    const result = await df.drop("b").stream({ chunkSize: 1 }).collect();

    expect(result).toEqual([
      { a: 1, c: 3 },
      { a: 4, c: 6 },
    ]);
  });

  it("yields empty for empty source", async () => {
    const df = DataFrame.fromArray<{ v: number }>([]);

    const result = await df
      .filter((r: any) => r.v > 0)
      .stream({ chunkSize: 10 })
      .collect();

    expect(result).toEqual([]);
  });

  it("uses default chunkSize when not specified", async () => {
    const data = Array.from({ length: 5 }, (_, i) => ({ v: i }));
    const df = DataFrame.fromArray(data);

    const result = await df.stream().collect();

    expect(result).toEqual(data);
  });

  it("falls back for limit (position-dependent)", async () => {
    const data = Array.from({ length: 10 }, (_, i) => ({ v: i }));
    const df = DataFrame.fromArray(data);

    const result = await df.limit(3).stream({ chunkSize: 2 }).collect();

    expect(result).toEqual([{ v: 0 }, { v: 1 }, { v: 2 }]);
  });
});
