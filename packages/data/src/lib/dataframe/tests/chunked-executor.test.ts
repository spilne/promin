import { describe, it, expect } from "bun:test";
import { DataFrame } from "../dataframe.ts";
import { classifyPlan } from "../plan-classifier.ts";
import type { LogicalPlan } from "../logical-plan.ts";
import { percentile, reduce, exprAgg } from "../logical-plan.ts";
import { col } from "../expr.ts";

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

  it("classifies GroupBy with streamable input as aggregating", () => {
    const plan: LogicalPlan = {
      _tag: "GroupBy",
      input: { _tag: "Source", data: [] },
      columns: ["x"],
      aggs: { x: "count" },
    };
    expect(classifyPlan(plan)).toBe("aggregating");
  });

  it("classifies GroupBy with materializing input as materializing", () => {
    const plan: LogicalPlan = {
      _tag: "GroupBy",
      input: { _tag: "Sort", input: { _tag: "Source", data: [] }, by: "x", order: "asc" },
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

  // Data integrity across chunk boundaries
  it("no rows lost or duplicated across chunks", async () => {
    const data = Array.from({ length: 97 }, (_, i) => ({ id: i }));
    const df = DataFrame.fromArray(data);
    const result = await df.stream({ chunkSize: 10 }).collect();
    expect(result).toHaveLength(97);
    expect(result.map((r: any) => r.id)).toEqual(data.map((d) => d.id));
  });

  // Chained streamable ops
  it("chained filter -> withColumn -> select streams correctly", async () => {
    const data = Array.from({ length: 50 }, (_, i) => ({
      id: i,
      name: `item-${i}`,
      score: i * 10,
    }));
    const df = DataFrame.fromArray(data);
    const result = await df
      .filter((r: any) => r.id >= 20)
      .withColumn("doubled", (r: any) => r.score * 2)
      .select("id", "doubled")
      .stream({ chunkSize: 7 })
      .collect();
    expect(result).toHaveLength(30);
    expect(result[0]).toEqual({ id: 20, doubled: 400 });
  });

  // Stream -> StreamPipeline composition
  it("stream integrates with StreamPipeline operators", async () => {
    const data = Array.from({ length: 20 }, (_, i) => ({ v: i }));
    const df = DataFrame.fromArray(data);
    const result = await df
      .filter((r: any) => r.v >= 10)
      .stream({ chunkSize: 5 })
      .map((r) => ({ ...r, squared: (r as any).v ** 2 }))
      .take(3)
      .collect();
    expect(result).toHaveLength(3);
    expect((result[0] as any).squared).toBe(100);
  });
});

// ---------------------------------------------------------------------------
// Custom aggregation edge cases (non-streaming, via collect)
// ---------------------------------------------------------------------------

describe("custom aggregation edge cases", () => {
  it("percentile handles single value", async () => {
    const df = DataFrame.fromArray([{ g: "a", v: 42 }]);
    const result = await df
      .groupBy("g")
      .agg({ v: percentile(0.5) })
      .collect();
    expect(result[0]!.v).toBe(42);
  });

  it("mode with all unique values returns first", async () => {
    const df = DataFrame.fromArray([
      { g: "a", v: 1 },
      { g: "a", v: 2 },
      { g: "a", v: 3 },
    ]);
    const result = await df.groupBy("g").agg({ v: "mode" }).collect();
    expect([1, 2, 3]).toContain(result[0]!.v);
  });

  it("custom reducer with finalize", async () => {
    const sumSquares = reduce(
      0,
      (acc: number, val) => acc + Number(val) ** 2,
      (acc: number) => Math.sqrt(acc),
    );
    const df = DataFrame.fromArray([
      { g: "a", v: 3 },
      { g: "a", v: 4 },
    ]);
    const result = await df.groupBy("g").agg({ v: sumSquares }).collect();
    expect(result[0]!.v).toBe(5); // sqrt(9 + 16) = 5
  });

  it("multiple different agg columns in same groupBy", async () => {
    const df = DataFrame.fromArray([
      { g: "a", revenue: 10, orders: 1 },
      { g: "a", revenue: 20, orders: 2 },
      { g: "a", revenue: 30, orders: 3 },
      { g: "b", revenue: 5, orders: 1 },
      { g: "b", revenue: 15, orders: 2 },
    ]);
    const result = await df.groupBy("g").agg({ revenue: "sum", orders: "count" }).collect();
    const sortByG = (a: any, b: any) => a.g.localeCompare(b.g);
    result.sort(sortByG);
    expect(result[0]).toEqual({ g: "a", revenue: 60, orders: 3 });
    expect(result[1]).toEqual({ g: "b", revenue: 20, orders: 2 });
  });

  it("percentile 0.9 on larger dataset", async () => {
    const data = Array.from({ length: 100 }, (_, i) => ({ g: "a", v: i + 1 }));
    const df = DataFrame.fromArray(data);
    const result = await df
      .groupBy("g")
      .agg({ v: percentile(0.9) })
      .collect();
    // percentile(0.9) => idx = ceil(0.9 * 100) - 1 = 89 => value 90
    expect(result[0]!.v).toBe(90);
  });
});

// ---------------------------------------------------------------------------
// Streaming groupBy aggregation
// ---------------------------------------------------------------------------

describe("streaming groupBy aggregation", () => {
  it("streaming sum matches non-streaming", async () => {
    const data = Array.from({ length: 100 }, (_, i) => ({
      group: i % 3 === 0 ? "a" : i % 3 === 1 ? "b" : "c",
      value: i,
    }));
    const df = DataFrame.fromArray(data);

    const regular = await df.groupBy("group").agg({ value: "sum" }).collect();
    const streamed = await df
      .groupBy("group")
      .agg({ value: "sum" })
      .stream({ chunkSize: 10 })
      .collect();

    const sortByGroup = (a: any, b: any) => a.group.localeCompare(b.group);
    expect(streamed.sort(sortByGroup)).toEqual(regular.sort(sortByGroup));
  });

  it("streaming avg matches non-streaming", async () => {
    const data = Array.from({ length: 50 }, (_, i) => ({
      g: i % 2 === 0 ? "even" : "odd",
      v: i,
    }));
    const df = DataFrame.fromArray(data);

    const regular = await df.groupBy("g").agg({ v: "avg" }).collect();
    const streamed = await df.groupBy("g").agg({ v: "avg" }).stream({ chunkSize: 7 }).collect();

    const sortByG = (a: any, b: any) => a.g.localeCompare(b.g);
    regular.sort(sortByG);
    streamed.sort(sortByG);
    expect((streamed[0] as any).v).toBeCloseTo((regular[0] as any).v, 5);
    expect((streamed[1] as any).v).toBeCloseTo((regular[1] as any).v, 5);
  });

  it("streaming min matches non-streaming", async () => {
    const data = Array.from({ length: 80 }, (_, i) => ({
      g: i < 40 ? "a" : "b",
      v: i * 7 + 3, // deterministic values
    }));
    const df = DataFrame.fromArray(data);

    const regular = await df.groupBy("g").agg({ v: "min" }).collect();
    const streamed = await df.groupBy("g").agg({ v: "min" }).stream({ chunkSize: 15 }).collect();

    const sortByG = (a: any, b: any) => a.g.localeCompare(b.g);
    expect(streamed.sort(sortByG)).toEqual(regular.sort(sortByG));
  });

  it("streaming count matches non-streaming", async () => {
    const data = Array.from({ length: 60 }, (_, i) => ({
      g: i % 4 === 0 ? "a" : i % 4 === 1 ? "b" : i % 4 === 2 ? "c" : "d",
      v: i,
    }));
    const df = DataFrame.fromArray(data);

    const regular = await df.groupBy("g").agg({ v: "count" }).collect();
    const streamed = await df.groupBy("g").agg({ v: "count" }).stream({ chunkSize: 9 }).collect();

    const sortByG = (a: any, b: any) => a.g.localeCompare(b.g);
    expect(streamed.sort(sortByG)).toEqual(regular.sort(sortByG));
  });

  it("streaming countDistinct works across chunks", async () => {
    const data = [
      { g: "a", v: 1 },
      { g: "a", v: 2 },
      { g: "a", v: 1 },
      { g: "a", v: 3 },
      { g: "a", v: 2 },
      { g: "b", v: 10 },
    ];
    const df = DataFrame.fromArray(data);
    const result = await df
      .groupBy("g")
      .agg({ v: "countDistinct" })
      .stream({ chunkSize: 2 })
      .collect();
    const a = result.find((r: any) => r.g === "a");
    expect(a!.v).toBe(3);
  });

  it("streaming mode works across chunks", async () => {
    const data = [
      { g: "a", v: 1 },
      { g: "a", v: 2 },
      { g: "a", v: 2 },
      { g: "a", v: 3 },
      { g: "a", v: 2 },
      { g: "a", v: 1 },
    ];
    const df = DataFrame.fromArray(data);
    const result = await df.groupBy("g").agg({ v: "mode" }).stream({ chunkSize: 2 }).collect();
    expect(result[0]!.v).toBe(2); // mode is 2 (appears 3 times)
  });

  it("streaming custom reducer matches non-streaming", async () => {
    const sumSquares = reduce(
      0,
      (acc: number, val) => acc + Number(val) ** 2,
      (acc: number) => Math.sqrt(acc),
    );
    const data = [
      { g: "a", v: 3 },
      { g: "a", v: 4 },
      { g: "b", v: 5 },
      { g: "b", v: 12 },
    ];
    const df = DataFrame.fromArray(data);

    const regular = await df.groupBy("g").agg({ v: sumSquares }).collect();
    const streamed = await df
      .groupBy("g")
      .agg({ v: sumSquares })
      .stream({ chunkSize: 2 })
      .collect();

    const sortByG = (a: any, b: any) => a.g.localeCompare(b.g);
    expect(streamed.sort(sortByG)).toEqual(regular.sort(sortByG));
  });

  it("streaming groupBy with filter on input", async () => {
    const data = Array.from({ length: 100 }, (_, i) => ({
      g: i % 2 === 0 ? "even" : "odd",
      v: i,
    }));
    const df = DataFrame.fromArray(data);

    const regular = await df
      .filter((r: any) => r.v >= 50)
      .groupBy("g")
      .agg({ v: "sum" })
      .collect();
    const streamed = await df
      .filter((r: any) => r.v >= 50)
      .groupBy("g")
      .agg({ v: "sum" })
      .stream({ chunkSize: 10 })
      .collect();

    const sortByG = (a: any, b: any) => a.g.localeCompare(b.g);
    expect(streamed.sort(sortByG)).toEqual(regular.sort(sortByG));
  });
});

// ---------------------------------------------------------------------------
// Streaming exprAgg — expression-level aggregations across chunks
// ---------------------------------------------------------------------------

describe("streaming exprAgg", () => {
  const sortByG = (a: any, b: any) => String(a.g).localeCompare(String(b.g));

  it("sum of computed expression matches non-streaming", async () => {
    const data = Array.from({ length: 100 }, (_, i) => ({
      g: i % 3 === 0 ? "a" : i % 3 === 1 ? "b" : "c",
      revenue: i * 10,
      tax: i,
    }));
    const df = DataFrame.fromArray(data);

    const aggDef = {
      total: exprAgg({ expr: col("revenue").add(col("tax")), agg: "sum" as const }),
    };
    const regular = await df.groupBy("g").agg(aggDef).collect();
    const streamed = await df.groupBy("g").agg(aggDef).stream({ chunkSize: 7 }).collect();

    expect(streamed.sort(sortByG)).toEqual(regular.sort(sortByG));
    const total = (regular as any[]).reduce((s, r) => s + Number(r.total), 0);
    const expected = data.reduce((s, r) => s + r.revenue + r.tax, 0);
    expect(total).toBe(expected);
  });

  it("avg of expression matches non-streaming across chunk boundaries", async () => {
    const data = Array.from({ length: 50 }, (_, i) => ({
      g: i < 25 ? "x" : "y",
      a: i,
      b: 2,
    }));
    const df = DataFrame.fromArray(data);
    const aggDef = { m: exprAgg({ expr: col("a").mul(col("b")), agg: "avg" as const }) };

    const regular = await df.groupBy("g").agg(aggDef).collect();
    const streamed = await df.groupBy("g").agg(aggDef).stream({ chunkSize: 11 }).collect();

    regular.sort(sortByG);
    streamed.sort(sortByG);
    expect((streamed[0] as any).m).toBeCloseTo((regular[0] as any).m, 5);
    expect((streamed[1] as any).m).toBeCloseTo((regular[1] as any).m, 5);
  });

  it("filter excludes rows consistently across chunks", async () => {
    const data = Array.from({ length: 80 }, (_, i) => ({
      g: i % 2 === 0 ? "even" : "odd",
      v: i,
      premium: i % 3 === 0,
    }));
    const df = DataFrame.fromArray(data);
    const aggDef = {
      v: exprAgg({
        expr: col("v"),
        agg: "sum" as const,
        filter: col("premium").eq(true),
      }),
    };

    const regular = await df.groupBy("g").agg(aggDef).collect();
    const streamed = await df.groupBy("g").agg(aggDef).stream({ chunkSize: 9 }).collect();

    expect(streamed.sort(sortByG)).toEqual(regular.sort(sortByG));
  });

  it("count with filter across chunks", async () => {
    const data = Array.from({ length: 40 }, (_, i) => ({
      g: i % 2 === 0 ? "a" : "b",
      active: i % 4 !== 0,
    }));
    const df = DataFrame.fromArray(data);
    const aggDef = {
      active: exprAgg({
        expr: col("active"),
        agg: "count" as const,
        filter: col("active").eq(true),
      }),
    };

    const regular = await df.groupBy("g").agg(aggDef).collect();
    const streamed = await df.groupBy("g").agg(aggDef).stream({ chunkSize: 6 }).collect();

    expect(streamed.sort(sortByG)).toEqual(regular.sort(sortByG));
  });

  it("min/max of expression across chunks", async () => {
    const data = Array.from({ length: 60 }, (_, i) => ({
      g: i % 2 === 0 ? "a" : "b",
      x: (i * 37) % 100, // spread values across chunks
    }));
    const df = DataFrame.fromArray(data);
    const aggDef = {
      lo: exprAgg({ expr: col("x").mul(2), agg: "min" as const }),
      hi: exprAgg({ expr: col("x").mul(2), agg: "max" as const }),
    };

    const regular = await df.groupBy("g").agg(aggDef).collect();
    const streamed = await df.groupBy("g").agg(aggDef).stream({ chunkSize: 8 }).collect();

    expect(streamed.sort(sortByG)).toEqual(regular.sort(sortByG));
  });

  it("countDistinct over expression across chunks", async () => {
    // 4 distinct values of floor(v/10) in group a: 0..3
    const data = [
      ...Array.from({ length: 10 }, (_, i) => ({ g: "a", v: i })),
      ...Array.from({ length: 10 }, (_, i) => ({ g: "a", v: 10 + i })),
      ...Array.from({ length: 10 }, (_, i) => ({ g: "a", v: 20 + i })),
      ...Array.from({ length: 10 }, (_, i) => ({ g: "a", v: 30 + i })),
    ];
    const df = DataFrame.fromArray(data);
    const aggDef = {
      v: exprAgg({
        expr: col("v").div(10).cast("number"),
        agg: "countDistinct" as const,
      }),
    };

    const regular = await df.groupBy("g").agg(aggDef).collect();
    const streamed = await df.groupBy("g").agg(aggDef).stream({ chunkSize: 3 }).collect();

    expect((regular[0] as any).v).toBeGreaterThanOrEqual(4);
    expect(streamed).toEqual(regular);
  });

  it("exprAgg with percentile custom agg across chunks", async () => {
    const data = Array.from({ length: 100 }, (_, i) => ({ g: "a", score: i + 1 }));
    const df = DataFrame.fromArray(data);
    const aggDef = {
      score: exprAgg({
        expr: col("score").mul(2),
        agg: percentile(0.9),
      }),
    };

    const regular = await df.groupBy("g").agg(aggDef).collect();
    const streamed = await df.groupBy("g").agg(aggDef).stream({ chunkSize: 13 }).collect();

    expect((streamed[0] as any).score).toBe((regular[0] as any).score);
    // percentile(0.9) of [2, 4, ..., 200]: idx = ceil(0.9*100)-1 = 89 => value 180
    expect((regular[0] as any).score).toBe(180);
  });

  it("exprAgg with custom reducer across chunks", async () => {
    const sumSquares = reduce(
      0,
      (acc: number, val) => acc + Number(val) ** 2,
      (acc: number) => Math.sqrt(acc),
    );
    const data = [
      { g: "a", x: 1, y: 2 }, // (1+2)^2 = 9
      { g: "a", x: 2, y: 2 }, // (2+2)^2 = 16 => sqrt(9+16)=5
      { g: "b", x: 3, y: 0 }, // 9
      { g: "b", x: 0, y: 4 }, // 16 => sqrt=5
    ];
    const df = DataFrame.fromArray(data);
    const aggDef = {
      v: exprAgg({ expr: col("x").add(col("y")), agg: sumSquares }),
    };

    const regular = await df.groupBy("g").agg(aggDef).collect();
    const streamed = await df.groupBy("g").agg(aggDef).stream({ chunkSize: 1 }).collect();

    expect(streamed.sort(sortByG)).toEqual(regular.sort(sortByG));
    const a = (regular as any[]).find((r) => r.g === "a");
    expect(a!.v).toBe(5);
  });

  it("mixes plain agg and exprAgg in same groupBy", async () => {
    const data = Array.from({ length: 30 }, (_, i) => ({
      g: i % 2 === 0 ? "a" : "b",
      revenue: 100,
      tax: 10,
      unit: 1,
    }));
    const df = DataFrame.fromArray(data);
    const aggDef = {
      unit: "count" as const,
      revenue: exprAgg({ expr: col("revenue").add(col("tax")), agg: "sum" as const }),
    };

    const regular = await df.groupBy("g").agg(aggDef).collect();
    const streamed = await df.groupBy("g").agg(aggDef).stream({ chunkSize: 4 }).collect();

    expect(streamed.sort(sortByG)).toEqual(regular.sort(sortByG));
    const a = (regular as any[]).find((r) => r.g === "a");
    expect(a!.unit).toBe(15);
    expect(a!.revenue).toBe(15 * 110);
  });

  it("filter that matches nothing yields empty-state finalize", async () => {
    const data = Array.from({ length: 20 }, (_, i) => ({ g: "a", v: i, active: false }));
    const df = DataFrame.fromArray(data);
    const aggDef = {
      v: exprAgg({
        expr: col("v"),
        agg: "sum" as const,
        filter: col("active").eq(true),
      }),
    };

    const regular = await df.groupBy("g").agg(aggDef).collect();
    const streamed = await df.groupBy("g").agg(aggDef).stream({ chunkSize: 5 }).collect();

    expect(streamed).toEqual(regular);
    expect((regular[0] as any).v).toBe(0); // nothing accumulated
  });

  it("expression over nested field access", async () => {
    const data = Array.from({ length: 30 }, (_, i) => ({
      g: i % 2 === 0 ? "a" : "b",
      meta: { price: i * 10 },
    }));
    const df = DataFrame.fromArray(data);
    const aggDef = {
      price: exprAgg({ expr: col("meta").field("price"), agg: "sum" as const }),
    };

    const regular = await df.groupBy("g").agg(aggDef).collect();
    const streamed = await df.groupBy("g").agg(aggDef).stream({ chunkSize: 4 }).collect();

    expect(streamed.sort(sortByG)).toEqual(regular.sort(sortByG));
  });
});
