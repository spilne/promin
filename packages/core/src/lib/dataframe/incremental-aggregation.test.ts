import { describe, it, expect } from "bun:test";
import { IncrementalAggregation } from "./incremental-aggregation.ts";
import { percentile, reduce } from "./logical-plan.ts";

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
