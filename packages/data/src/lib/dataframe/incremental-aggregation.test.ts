import { describe, it, expect } from "bun:test";
import { IncrementalAggregation } from "./incremental-aggregation.ts";
import { percentile, reduce, exprAgg } from "./logical-plan.ts";
import { col } from "./expr.ts";

describe("IncrementalAggregation", () => {
  it("single batch sum and count", async () => {
    const agg = IncrementalAggregation.create({
      groupBy: ["region"],
      agg: { revenue: "sum", revenue2: "count" },
    });

    await agg.ingest([
      { region: "US", revenue: 100, revenue2: 1 },
      { region: "EU", revenue: 200, revenue2: 1 },
      { region: "US", revenue: 150, revenue2: 1 },
    ]);

    const snap = await agg.snapshot();
    const rows = (await snap.collect()).sort((a: any, b: any) => a.region.localeCompare(b.region));
    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual({ region: "EU", revenue: 200, revenue2: 1 });
    expect(rows[1]).toEqual({ region: "US", revenue: 250, revenue2: 2 });
  });

  it("accumulates across multiple batches", async () => {
    const agg = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { v: "sum" },
    });

    await agg.ingest([
      { g: "a", v: 10 },
      { g: "b", v: 20 },
    ]);
    await agg.ingest([
      { g: "a", v: 30 },
      { g: "b", v: 40 },
    ]);
    await agg.ingest([{ g: "a", v: 50 }]);

    const rows = (await (await agg.snapshot()).collect()).sort((a: any, b: any) =>
      a.g.localeCompare(b.g),
    );
    expect(rows[0]).toEqual({ g: "a", v: 90 });
    expect(rows[1]).toEqual({ g: "b", v: 60 });
  });

  it("snapshot does not clear state", async () => {
    const agg = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { v: "sum" },
    });

    await agg.ingest([{ g: "x", v: 100 }]);
    await agg.snapshot(); // should not reset

    await agg.ingest([{ g: "x", v: 50 }]);
    const rows = await (await agg.snapshot()).collect();
    expect(rows[0]!.v).toBe(150); // accumulated, not 50
  });

  it("reset clears all state", async () => {
    const agg = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { v: "sum" },
    });

    await agg.ingest([{ g: "a", v: 100 }]);
    expect(agg.groupCount).toBe(1);

    await agg.reset();
    expect(agg.groupCount).toBe(0);

    await agg.ingest([{ g: "a", v: 10 }]);
    const rows = await (await agg.snapshot()).collect();
    expect(rows[0]!.v).toBe(10); // fresh start
  });

  it("avg across batches is correct", async () => {
    const agg = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { v: "avg" },
    });

    await agg.ingest([
      { g: "a", v: 10 },
      { g: "a", v: 20 },
    ]);
    await agg.ingest([{ g: "a", v: 30 }]);

    const rows = await (await agg.snapshot()).collect();
    expect(rows[0]!.v).toBe(20); // (10+20+30)/3
  });

  it("min and max across batches", async () => {
    const agg = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { v: "min", v2: "max" },
    });

    await agg.ingest([{ g: "a", v: 50, v2: 50 }]);
    await agg.ingest([{ g: "a", v: 10, v2: 90 }]);
    await agg.ingest([{ g: "a", v: 30, v2: 70 }]);

    const rows = await (await agg.snapshot()).collect();
    expect(rows[0]!.v).toBe(10);
    expect(rows[0]!.v2).toBe(90);
  });

  it("countDistinct across batches", async () => {
    const agg = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { v: "countDistinct" },
    });

    await agg.ingest([
      { g: "a", v: 1 },
      { g: "a", v: 2 },
      { g: "a", v: 1 },
    ]);
    await agg.ingest([
      { g: "a", v: 3 },
      { g: "a", v: 2 },
    ]);

    const rows = await (await agg.snapshot()).collect();
    expect(rows[0]!.v).toBe(3); // {1, 2, 3}
  });

  it("mode across batches", async () => {
    const agg = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { v: "mode" },
    });

    await agg.ingest([
      { g: "a", v: "x" },
      { g: "a", v: "y" },
    ]);
    await agg.ingest([
      { g: "a", v: "x" },
      { g: "a", v: "x" },
    ]);

    const rows = await (await agg.snapshot()).collect();
    expect(rows[0]!.v).toBe("x"); // x appears 3 times
  });

  it("custom reducer across batches", async () => {
    const concat = reduce("", (acc: string, val) => acc + String(val));
    const agg = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { v: concat },
    });

    await agg.ingest([
      { g: "a", v: "A" },
      { g: "a", v: "B" },
    ]);
    await agg.ingest([{ g: "a", v: "C" }]);

    const rows = await (await agg.snapshot()).collect();
    expect(rows[0]!.v).toBe("ABC");
  });

  it("multi-column groupBy", async () => {
    const agg = IncrementalAggregation.create({
      groupBy: ["region", "product"],
      agg: { amount: "sum" },
    });

    await agg.ingest([
      { region: "US", product: "A", amount: 100 },
      { region: "US", product: "B", amount: 200 },
      { region: "EU", product: "A", amount: 50 },
    ]);
    await agg.ingest([{ region: "US", product: "A", amount: 150 }]);

    const rows = (await (await agg.snapshot()).collect()).sort((a: any, b: any) =>
      `${a.region}:${a.product}`.localeCompare(`${b.region}:${b.product}`),
    );
    expect(rows).toHaveLength(3);
    expect(rows.find((r: any) => r.region === "US" && r.product === "A")!.amount).toBe(250);
  });

  // -------------------------------------------------------------------------
  // exprAgg — expression-level aggregations
  // -------------------------------------------------------------------------

  it("exprAgg sum of expression across batches", async () => {
    const agg = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: {
        total: exprAgg({ expr: col("revenue").add(col("tax")), agg: "sum" }),
      },
    });

    await agg.ingest([
      { g: "a", revenue: 100, tax: 10 },
      { g: "b", revenue: 50, tax: 5 },
    ]);
    await agg.ingest([
      { g: "a", revenue: 200, tax: 20 },
      { g: "b", revenue: 70, tax: 7 },
    ]);

    const rows = (await (await agg.snapshot()).collect()).sort((a: any, b: any) =>
      a.g.localeCompare(b.g),
    );
    expect(rows[0]!.total).toBe(330); // (100+10) + (200+20)
    expect(rows[1]!.total).toBe(132); // (50+5) + (70+7)
  });

  it("exprAgg filter excludes rows across batches", async () => {
    const aggregator = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: {
        v: exprAgg({
          expr: col("v"),
          agg: "sum",
          filter: col("premium").eq(true),
        }),
      },
    });

    await aggregator.ingest([
      { g: "a", v: 100, premium: true },
      { g: "a", v: 50, premium: false },
    ]);
    await aggregator.ingest([
      { g: "a", v: 200, premium: true },
      { g: "a", v: 30, premium: false },
    ]);

    const rows = await (await aggregator.snapshot()).collect();
    expect(rows[0]!.v).toBe(300); // only premium:true rows (100 + 200)
  });

  it("exprAgg filter that matches no rows still emits group", async () => {
    const aggregator = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: {
        v: exprAgg({
          expr: col("v"),
          agg: "count",
          filter: col("active").eq(true),
        }),
      },
    });

    await aggregator.ingest([
      { g: "a", v: 1, active: false },
      { g: "a", v: 2, active: false },
    ]);

    const rows = await (await aggregator.snapshot()).collect();
    expect(rows[0]!.v).toBe(0);
  });

  it("exprAgg count with filter across batches", async () => {
    const aggregator = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: {
        n: exprAgg({
          expr: col("active"),
          agg: "count",
          filter: col("active").eq(true),
        }),
      },
    });

    await aggregator.ingest([
      { g: "a", active: true },
      { g: "a", active: false },
    ]);
    await aggregator.ingest([
      { g: "a", active: true },
      { g: "a", active: true },
    ]);

    const rows = await (await aggregator.snapshot()).collect();
    expect(rows[0]!.n).toBe(3);
  });

  it("exprAgg avg of expression across batches", async () => {
    const aggregator = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: {
        mean: exprAgg({ expr: col("a").mul(2), agg: "avg" }),
      },
    });

    await aggregator.ingest([
      { g: "a", a: 10 },
      { g: "a", a: 20 },
    ]);
    await aggregator.ingest([{ g: "a", a: 30 }]);

    const rows = await (await aggregator.snapshot()).collect();
    expect(rows[0]!.mean).toBe(40); // (20 + 40 + 60) / 3
  });

  it("exprAgg min and max across batches", async () => {
    const aggregator = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: {
        lo: exprAgg({ expr: col("v").neg(), agg: "min" }),
        hi: exprAgg({ expr: col("v").neg(), agg: "max" }),
      },
    });

    await aggregator.ingest([
      { g: "a", v: 1 },
      { g: "a", v: 5 },
    ]);
    await aggregator.ingest([
      { g: "a", v: 3 },
      { g: "a", v: 10 },
    ]);

    const rows = await (await aggregator.snapshot()).collect();
    expect(rows[0]!.lo).toBe(-10);
    expect(rows[0]!.hi).toBe(-1);
  });

  it("exprAgg countDistinct over expression across batches", async () => {
    const aggregator = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: {
        v: exprAgg({ expr: col("v").mod(3), agg: "countDistinct" }),
      },
    });

    await aggregator.ingest([
      { g: "a", v: 0 },
      { g: "a", v: 1 },
    ]);
    await aggregator.ingest([
      { g: "a", v: 3 }, // mod 3 = 0 (already seen)
      { g: "a", v: 2 },
    ]);

    const rows = await (await aggregator.snapshot()).collect();
    expect(rows[0]!.v).toBe(3); // {0, 1, 2}
  });

  it("exprAgg with percentile custom agg across batches", async () => {
    const aggregator = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: {
        p: exprAgg({ expr: col("score").mul(10), agg: percentile(0.5) }),
      },
    });

    await aggregator.ingest([
      { g: "a", score: 1 },
      { g: "a", score: 2 },
      { g: "a", score: 3 },
    ]);
    await aggregator.ingest([
      { g: "a", score: 4 },
      { g: "a", score: 5 },
    ]);

    const rows = await (await aggregator.snapshot()).collect();
    // values: [10, 20, 30, 40, 50], p50 => idx = ceil(0.5 * 5) - 1 = 2 => 30
    expect(rows[0]!.p).toBe(30);
  });

  it("exprAgg with custom reducer across batches", async () => {
    const sumSquares = reduce(
      0,
      (acc: number, val) => acc + Number(val) ** 2,
      (acc: number) => Math.sqrt(acc),
    );
    const aggregator = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: {
        v: exprAgg({ expr: col("x").add(col("y")), agg: sumSquares }),
      },
    });

    await aggregator.ingest([
      { g: "a", x: 1, y: 2 }, // (1+2)^2 = 9
    ]);
    await aggregator.ingest([
      { g: "a", x: 2, y: 2 }, // (2+2)^2 = 16 => sqrt(25) = 5
    ]);

    const rows = await (await aggregator.snapshot()).collect();
    expect(rows[0]!.v).toBe(5);
  });

  it("exprAgg mixes with plain aggs in same groupBy", async () => {
    const aggregator = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: {
        n: "count",
        total: exprAgg({ expr: col("a").add(col("b")), agg: "sum" }),
        premium: exprAgg({
          expr: col("a"),
          agg: "sum",
          filter: col("tier").eq("gold"),
        }),
      },
    });

    await aggregator.ingest([
      { g: "a", a: 10, b: 1, tier: "gold" },
      { g: "a", a: 20, b: 2, tier: "silver" },
    ]);
    await aggregator.ingest([{ g: "a", a: 30, b: 3, tier: "gold" }]);

    const rows = await (await aggregator.snapshot()).collect();
    expect(rows[0]!.n).toBe(3);
    expect(rows[0]!.total).toBe(66); // 11 + 22 + 33
    expect(rows[0]!.premium).toBe(40); // 10 + 30 (gold only)
  });

  it("exprAgg with state backend persistence", async () => {
    const store = new Map<string, unknown>();
    const backend = {
      get: async (key: string) => store.get(key),
      put: async (key: string, value: unknown) => {
        store.set(key, value);
      },
      delete: async (key: string) => {
        store.delete(key);
      },
      keys: async () => [...store.keys()],
    };

    const agg1 = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { total: exprAgg({ expr: col("a").add(col("b")), agg: "sum" }) },
      state: backend,
    });
    await agg1.ingest([
      { g: "a", a: 10, b: 5 },
      { g: "b", a: 20, b: 2 },
    ]);

    const agg2 = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { total: exprAgg({ expr: col("a").add(col("b")), agg: "sum" }) },
      state: backend,
    });
    await agg2.ingest([{ g: "a", a: 100, b: 50 }]);

    const rows = (await (await agg2.snapshot()).collect()).sort((a: any, b: any) =>
      a.g.localeCompare(b.g),
    );
    expect(rows[0]).toEqual({ g: "a", total: 165 }); // (10+5) + (100+50)
    expect(rows[1]).toEqual({ g: "b", total: 22 }); // unchanged
  });

  it("works with state backend for persistence", async () => {
    // Simple in-memory state backend
    const store = new Map<string, unknown>();
    const backend = {
      get: async (key: string) => store.get(key),
      put: async (key: string, value: unknown) => {
        store.set(key, value);
      },
      delete: async (key: string) => {
        store.delete(key);
      },
      keys: async () => [...store.keys()],
    };

    // Session 1: ingest and persist
    const agg1 = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { v: "sum" },
      state: backend,
    });
    await agg1.ingest([
      { g: "a", v: 100 },
      { g: "b", v: 200 },
    ]);

    // Session 2: new instance, restores from state
    const agg2 = IncrementalAggregation.create({
      groupBy: ["g"],
      agg: { v: "sum" },
      state: backend,
    });
    await agg2.ingest([{ g: "a", v: 50 }]);

    const rows = (await (await agg2.snapshot()).collect()).sort((a: any, b: any) =>
      a.g.localeCompare(b.g),
    );
    expect(rows[0]).toEqual({ g: "a", v: 150 }); // 100 + 50
    expect(rows[1]).toEqual({ g: "b", v: 200 }); // unchanged
  });
});
