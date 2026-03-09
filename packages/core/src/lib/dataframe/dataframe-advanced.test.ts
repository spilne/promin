import { describe, it, expect } from "bun:test";
import { DataFrame } from "./dataframe.ts";

// ---------------------------------------------------------------------------
// Test data
// ---------------------------------------------------------------------------

const sales = [
  { region: "US", product: "Widget", revenue: 1000, quantity: 10, date: "2026-03-01" },
  { region: "US", product: "Gadget", revenue: 2000, quantity: 5, date: "2026-03-02" },
  { region: "EU", product: "Widget", revenue: 800, quantity: 8, date: "2026-03-01" },
  { region: "EU", product: "Gadget", revenue: 1500, quantity: 3, date: "2026-03-03" },
  { region: "US", product: "Widget", revenue: 1200, quantity: 12, date: "2026-03-04" },
  { region: "AP", product: "Gadget", revenue: 500, quantity: 2, date: "2026-03-02" },
];

// ---------------------------------------------------------------------------
// Window functions
// ---------------------------------------------------------------------------

describe("Window functions", () => {
  it("row_number within partition", async () => {
    const result = await DataFrame.fromArray(sales)
      .withWindowColumn("rn", {
        partitionBy: "region",
        orderBy: "revenue",
        fn: "row_number",
      })
      .collect();

    const us = result.filter((r: any) => r.region === "US");
    const rns = us.map((r: any) => r.rn).sort();
    expect(rns).toEqual([1, 2, 3]);
  });

  it("dense_rank", async () => {
    const data = [
      { name: "A", score: 100 },
      { name: "B", score: 90 },
      { name: "C", score: 90 },
      { name: "D", score: 80 },
    ];

    const result = await DataFrame.fromArray(data)
      .withWindowColumn("rank", { orderBy: "score", fn: "dense_rank" })
      .collect();

    expect((result[0] as any).rank).toBe(1); // score 80
    expect((result[1] as any).rank).toBe(2); // score 90
    expect((result[2] as any).rank).toBe(2); // score 90 (same rank)
    expect((result[3] as any).rank).toBe(3); // score 100
  });

  it("lag — access previous row value", async () => {
    const data = [
      { month: 1, revenue: 100 },
      { month: 2, revenue: 120 },
      { month: 3, revenue: 90 },
    ];

    const result = await DataFrame.fromArray(data)
      .withWindowColumn("prev_revenue", {
        orderBy: "month",
        fn: "lag",
        args: { default: 0 },
      })
      .collect();

    expect((result[0] as any).prev_revenue).toBe(0); // no previous
    expect((result[1] as any).prev_revenue).toBe(1); // lag of month, not revenue — it lags orderBy column
  });

  it("lead — access next row value", async () => {
    const data = [
      { month: 1, revenue: 100 },
      { month: 2, revenue: 120 },
      { month: 3, revenue: 90 },
    ];

    const result = await DataFrame.fromArray(data)
      .withWindowColumn("next_month", {
        orderBy: "month",
        fn: "lead",
        args: { default: -1 },
      })
      .collect();

    expect((result[0] as any).next_month).toBe(2);
    expect((result[2] as any).next_month).toBe(-1); // no next
  });

  it("running_total", async () => {
    const data = [
      { day: 1, amount: 10 },
      { day: 2, amount: 20 },
      { day: 3, amount: 30 },
    ];

    const result = await DataFrame.fromArray(data)
      .withWindowColumn("cumulative", { orderBy: "amount", fn: "running_total" })
      .collect();

    expect((result[0] as any).cumulative).toBe(10);
    expect((result[1] as any).cumulative).toBe(30);
    expect((result[2] as any).cumulative).toBe(60);
  });

  it("ntile — distribute into buckets", async () => {
    const data = Array.from({ length: 8 }, (_, i) => ({ id: i + 1, value: i * 10 }));

    const result = await DataFrame.fromArray(data)
      .withWindowColumn("quartile", { orderBy: "value", fn: "ntile", args: { n: 4 } })
      .collect();

    const quartiles = result.map((r: any) => r.quartile);
    expect(quartiles.filter((q: number) => q === 1)).toHaveLength(2);
    expect(quartiles.filter((q: number) => q === 4)).toHaveLength(2);
  });

  it("top-N per group pattern", async () => {
    // Top 1 product by revenue per region
    const result = await DataFrame.fromArray(sales)
      .withWindowColumn("rank", {
        partitionBy: "region",
        orderBy: "revenue",
        fn: "row_number",
      })
      .sort("revenue" as any, "desc")
      .withWindowColumn("desc_rank", {
        partitionBy: "region",
        orderBy: "revenue",
        fn: "dense_rank",
      })
      .collect();

    // Verify we have ranked rows
    expect(result.length).toBe(sales.length);
    expect(result.every((r: any) => typeof r.rank === "number")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Pivot / Unpivot
// ---------------------------------------------------------------------------

describe("Pivot", () => {
  it("pivots rows to columns", async () => {
    const result = await DataFrame.fromArray(sales)
      .pivot({ index: "region", columns: "product", values: "revenue", agg: "sum" })
      .collect();

    expect(result).toHaveLength(3); // US, EU, AP
    const us = result.find((r: any) => r.region === "US") as any;
    expect(us.Widget).toBe(2200); // 1000 + 1200
    expect(us.Gadget).toBe(2000);
  });

  it("pivot with count aggregation", async () => {
    const result = await DataFrame.fromArray(sales)
      .pivot({ index: "region", columns: "product", values: "revenue", agg: "count" })
      .collect();

    const us = result.find((r: any) => r.region === "US") as any;
    expect(us.Widget).toBe(2);
    expect(us.Gadget).toBe(1);
  });
});

describe("Unpivot", () => {
  it("unpivots columns to rows", async () => {
    const wide = [
      { region: "US", q1: 100, q2: 200, q3: 150 },
      { region: "EU", q1: 80, q2: 120, q3: 90 },
    ];

    const result = await DataFrame.fromArray(wide)
      .unpivot({ id: "region", columns: ["q1", "q2", "q3"] })
      .collect();

    expect(result).toHaveLength(6); // 2 regions × 3 quarters
    expect(result[0]).toEqual({ region: "US", variable: "q1", value: 100 });
  });
});

// ---------------------------------------------------------------------------
// Explode
// ---------------------------------------------------------------------------

describe("Explode", () => {
  it("explodes array column into rows", async () => {
    const data = [
      { id: 1, tags: ["a", "b", "c"] },
      { id: 2, tags: ["d"] },
    ];

    const result = await DataFrame.fromArray(data).explode("tags").collect();

    expect(result).toHaveLength(4);
    expect(result.map((r: any) => r.tags)).toEqual(["a", "b", "c", "d"]);
    expect((result[0] as any).id).toBe(1);
    expect((result[3] as any).id).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// String accessor
// ---------------------------------------------------------------------------

describe("String accessor (.str)", () => {
  const products = [
    { name: "  Widget Pro  ", code: "WP-001" },
    { name: "Gadget Plus", code: "GP-002" },
    { name: "Widget Basic", code: "WB-003" },
  ];

  it("contains", async () => {
    const result = await DataFrame.fromArray(products).str("name").contains("Widget").collect();

    expect((result[0] as any).name_contains).toBe(true);
    expect((result[1] as any).name_contains).toBe(false);
  });

  it("toUpperCase", async () => {
    const result = await DataFrame.fromArray(products).str("name").toUpperCase().collect();

    expect((result[1] as any).name).toBe("GADGET PLUS");
  });

  it("toLowerCase", async () => {
    const result = await DataFrame.fromArray(products).str("name").toLowerCase().collect();

    expect((result[1] as any).name).toBe("gadget plus");
  });

  it("trim", async () => {
    const result = await DataFrame.fromArray(products).str("name").trim().collect();

    expect((result[0] as any).name).toBe("Widget Pro");
  });

  it("startsWith", async () => {
    const result = await DataFrame.fromArray(products).str("code").startsWith("WP").collect();

    expect((result[0] as any).code_startsWith).toBe(true);
    expect((result[1] as any).code_startsWith).toBe(false);
  });

  it("replace", async () => {
    const result = await DataFrame.fromArray(products).str("code").replace("-", "_").collect();

    expect((result[0] as any).code).toBe("WP_001");
  });

  it("length", async () => {
    const result = await DataFrame.fromArray(products).str("code").length().collect();

    expect((result[0] as any).code_len).toBe(6);
  });

  it("split", async () => {
    const result = await DataFrame.fromArray(products).str("code").split("-").collect();

    expect((result[0] as any).code).toEqual(["WP", "001"]);
  });
});

// ---------------------------------------------------------------------------
// Date accessor
// ---------------------------------------------------------------------------

describe("Date accessor (.dt)", () => {
  const events = [
    { event: "A", date: "2026-03-15T10:30:00Z" },
    { event: "B", date: "2026-07-04T14:00:00Z" },
    { event: "C", date: "2026-12-25T08:00:00Z" },
  ];

  it("year", async () => {
    const result = await DataFrame.fromArray(events).dt("date").year().collect();

    expect((result[0] as any).date_year).toBe(2026);
  });

  it("month", async () => {
    const result = await DataFrame.fromArray(events).dt("date").month().collect();

    expect((result[0] as any).date_month).toBe(3);
    expect((result[1] as any).date_month).toBe(7);
    expect((result[2] as any).date_month).toBe(12);
  });

  it("day", async () => {
    const result = await DataFrame.fromArray(events).dt("date").day().collect();

    expect((result[0] as any).date_day).toBe(15);
  });

  it("truncate to month", async () => {
    const result = await DataFrame.fromArray(events).dt("date").truncate("month").collect();

    expect((result[0] as any).date).toBe("2026-03-01T00:00:00.000Z");
    expect((result[1] as any).date).toBe("2026-07-01T00:00:00.000Z");
  });
});

// ---------------------------------------------------------------------------
// Rolling windows
// ---------------------------------------------------------------------------

describe("Rolling windows", () => {
  const timeseries = [
    { day: 1, value: 10 },
    { day: 2, value: 20 },
    { day: 3, value: 30 },
    { day: 4, value: 40 },
    { day: 5, value: 50 },
  ];

  it("rolling mean", async () => {
    const result = await DataFrame.fromArray(timeseries)
      .rolling("value", { window: 3, fn: "mean" })
      .collect();

    expect((result[0] as any).value_rolling_mean).toBe(10); // [10]
    expect((result[1] as any).value_rolling_mean).toBe(15); // [10, 20]
    expect((result[2] as any).value_rolling_mean).toBe(20); // [10, 20, 30]
    expect((result[3] as any).value_rolling_mean).toBe(30); // [20, 30, 40]
    expect((result[4] as any).value_rolling_mean).toBe(40); // [30, 40, 50]
  });

  it("rolling sum", async () => {
    const result = await DataFrame.fromArray(timeseries)
      .rolling("value", { window: 2, fn: "sum" })
      .collect();

    expect((result[0] as any).value_rolling_sum).toBe(10); // [10]
    expect((result[1] as any).value_rolling_sum).toBe(30); // [10, 20]
    expect((result[2] as any).value_rolling_sum).toBe(50); // [20, 30]
  });

  it("rolling with custom output name", async () => {
    const result = await DataFrame.fromArray(timeseries)
      .rolling("value", { window: 3, fn: "mean", as: "ma3" })
      .collect();

    expect((result[2] as any).ma3).toBe(20);
  });
});

// ---------------------------------------------------------------------------
// Cumulative operations
// ---------------------------------------------------------------------------

describe("Cumulative operations", () => {
  const data = [{ n: 10 }, { n: 20 }, { n: 30 }, { n: 40 }];

  it("cumSum", async () => {
    const result = await DataFrame.fromArray(data).cumSum("n").collect();

    expect(result.map((r: any) => r.n_cumsum)).toEqual([10, 30, 60, 100]);
  });

  it("cumProd", async () => {
    const result = await DataFrame.fromArray([{ n: 2 }, { n: 3 }, { n: 4 }])
      .cumProd("n")
      .collect();

    expect(result.map((r: any) => r.n_cumprod)).toEqual([2, 6, 24]);
  });

  it("cumMax", async () => {
    const result = await DataFrame.fromArray([{ n: 3 }, { n: 1 }, { n: 4 }, { n: 2 }])
      .cumMax("n")
      .collect();

    expect(result.map((r: any) => r.n_cummax)).toEqual([3, 3, 4, 4]);
  });

  it("cumMin", async () => {
    const result = await DataFrame.fromArray([{ n: 3 }, { n: 1 }, { n: 4 }, { n: 2 }])
      .cumMin("n")
      .collect();

    expect(result.map((r: any) => r.n_cummin)).toEqual([3, 1, 1, 1]);
  });

  it("pctChange", async () => {
    const result = await DataFrame.fromArray([{ price: 100 }, { price: 110 }, { price: 99 }])
      .pctChange("price")
      .collect();

    expect((result[0] as any).price_pctchange).toBe(0); // no previous
    expect((result[1] as any).price_pctchange).toBeCloseTo(0.1); // 10% increase
    expect((result[2] as any).price_pctchange).toBeCloseTo(-0.1); // 10% decrease
  });

  it("cumSum with custom output name", async () => {
    const result = await DataFrame.fromArray(data).cumSum("n", { as: "running" }).collect();

    expect(result.map((r: any) => r.running)).toEqual([10, 30, 60, 100]);
  });
});

// ---------------------------------------------------------------------------
// Complex chained example
// ---------------------------------------------------------------------------

describe("Complex chained queries", () => {
  it("e-commerce analytics pipeline", async () => {
    const orders = [
      {
        userId: 1,
        region: "US",
        product: "Laptop",
        category: "Electronics",
        revenue: 1200,
        date: "2026-03-01",
      },
      {
        userId: 2,
        region: "EU",
        product: "Phone",
        category: "Electronics",
        revenue: 800,
        date: "2026-03-01",
      },
      {
        userId: 1,
        region: "US",
        product: "Desk",
        category: "Furniture",
        revenue: 450,
        date: "2026-03-02",
      },
      {
        userId: 3,
        region: "US",
        product: "Monitor",
        category: "Electronics",
        revenue: 600,
        date: "2026-03-02",
      },
      {
        userId: 2,
        region: "EU",
        product: "Chair",
        category: "Furniture",
        revenue: 300,
        date: "2026-03-03",
      },
      {
        userId: 4,
        region: "AP",
        product: "Laptop",
        category: "Electronics",
        revenue: 1100,
        date: "2026-03-03",
      },
      {
        userId: 1,
        region: "US",
        product: "Phone",
        category: "Electronics",
        revenue: 750,
        date: "2026-03-04",
      },
      {
        userId: 5,
        region: "EU",
        product: "Desk",
        category: "Furniture",
        revenue: 500,
        date: "2026-03-04",
      },
    ];

    const users = [
      { userId: 1, name: "Alice", tier: "gold" },
      { userId: 2, name: "Bob", tier: "silver" },
      { userId: 3, name: "Charlie", tier: "gold" },
      { userId: 4, name: "Diana", tier: "bronze" },
      { userId: 5, name: "Eve", tier: "silver" },
    ];

    // Revenue by region as pivot table
    const pivotReport = await DataFrame.fromArray(orders)
      .pivot({ index: "region", columns: "category", values: "revenue", agg: "sum" })
      .sort("region" as any)
      .collect();

    expect(pivotReport).toHaveLength(3);
    const us = pivotReport.find((r: any) => r.region === "US") as any;
    expect(us.Electronics).toBe(2550); // 1200 + 600 + 750
    expect(us.Furniture).toBe(450);

    // Running total of revenue sorted by date
    const running = await DataFrame.fromArray(orders)
      .sort("date")
      .cumSum("revenue", { as: "running_total" })
      .collect();

    expect((running[0] as any).running_total).toBe(1200);
    expect((running[running.length - 1] as any).running_total).toBe(5700);

    // Top spender per region — join, groupBy, window, filter
    const spenders = await DataFrame.fromArray(orders)
      .join(DataFrame.fromArray(users), { on: "userId", type: "inner" })
      .groupBy("region" as any, "name" as any)
      .agg({ revenue: "sum" })
      .withWindowColumn("rank", {
        partitionBy: "region" as any,
        orderBy: "revenue" as any,
        fn: "dense_rank",
      })
      .collect();

    // Verify structure
    expect(spenders.every((r: any) => typeof r.rank === "number")).toBe(true);
    expect(spenders.every((r: any) => r.name && r.region)).toBe(true);

    // Rolling 3-day average revenue
    const rollingAvg = await DataFrame.fromArray(orders)
      .sort("date")
      .rolling("revenue", { window: 3, fn: "mean", as: "ma3" })
      .collect();

    expect((rollingAvg[0] as any).ma3).toBe(1200); // just first
    expect(typeof (rollingAvg[3] as any).ma3).toBe("number");
  });
});
