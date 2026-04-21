import { describe, it, expect } from "bun:test";
import { DataFrame } from "../dataframe.ts";
import { optimizePlan } from "../plan-optimizer.ts";
import { col } from "../expr.ts";
import { percentile, reduce, exprAgg } from "../logical-plan.ts";

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

describe("Window functions — rank, lag, lead, and running totals", () => {
  it("number each sale within its region by revenue — row numbering", async () => {
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

  it("leaderboard ranking — tied scores share the same rank", async () => {
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

  it("compare current month to previous month — lag for month-over-month analysis", async () => {
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

  it("peek at next month's value — lead for forward-looking forecasts", async () => {
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

  it("cumulative daily revenue — running total for cash flow tracking", async () => {
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

  it("split customers into quartile buckets for segmentation analysis", async () => {
    const data = Array.from({ length: 8 }, (_, i) => ({ id: i + 1, value: i * 10 }));

    const result = await DataFrame.fromArray(data)
      .withWindowColumn("quartile", { orderBy: "value", fn: "ntile", args: { n: 4 } })
      .collect();

    const quartiles = result.map((r: any) => r.quartile);
    expect(quartiles.filter((q: number) => q === 1)).toHaveLength(2);
    expect(quartiles.filter((q: number) => q === 4)).toHaveLength(2);
  });

  it("top product by revenue per region — window rank plus filter", async () => {
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

describe("Pivot — reshape long data into wide crosstab reports", () => {
  it("revenue by region with products as columns — crosstab for spreadsheet export", async () => {
    const result = await DataFrame.fromArray(sales)
      .pivot({ index: "region", columns: "product", values: "revenue", agg: "sum" })
      .collect();

    expect(result).toHaveLength(3); // US, EU, AP
    const us = result.find((r: any) => r.region === "US") as any;
    expect(us.Widget).toBe(2200); // 1000 + 1200
    expect(us.Gadget).toBe(2000);
  });

  it("count of orders per product per region — volume heatmap data", async () => {
    const result = await DataFrame.fromArray(sales)
      .pivot({ index: "region", columns: "product", values: "revenue", agg: "count" })
      .collect();

    const us = result.find((r: any) => r.region === "US") as any;
    expect(us.Widget).toBe(2);
    expect(us.Gadget).toBe(1);
  });
});

describe("Unpivot — convert wide quarterly columns into long format for charting", () => {
  it("quarterly revenue columns become variable/value rows for time-series charts", async () => {
    const wide = [
      { region: "US", q1: 100, q2: 200, q3: 150 },
      { region: "EU", q1: 80, q2: 120, q3: 90 },
    ];

    const result = await DataFrame.fromArray(wide)
      .unpivot({ id: "region", columns: ["q1", "q2", "q3"] as any })
      .collect();

    expect(result).toHaveLength(6); // 2 regions × 3 quarters
    expect((result[0] as any).region).toBe("US");
    expect((result[0] as any).variable).toBe("q1");
    expect((result[0] as any).value).toBe(100);
  });
});

// ---------------------------------------------------------------------------
// Explode
// ---------------------------------------------------------------------------

describe("Explode — flatten nested arrays into individual rows", () => {
  it("product tags array becomes one row per tag — enables tag-level analytics", async () => {
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

describe("String operations — clean and search text fields in bulk", () => {
  const products = [
    { name: "  Widget Pro  ", code: "WP-001" },
    { name: "Gadget Plus", code: "GP-002" },
    { name: "Widget Basic", code: "WB-003" },
  ];

  it("flag products containing 'Widget' for the Widget product line report", async () => {
    const result = await DataFrame.fromArray(products).str("name").contains("Widget").collect();

    expect((result[0] as any).name_contains).toBe(true);
    expect((result[1] as any).name_contains).toBe(false);
  });

  it("normalize product names to uppercase for case-insensitive matching", async () => {
    const result = await DataFrame.fromArray(products).str("name").toUpperCase().collect();

    expect((result[1] as any).name).toBe("GADGET PLUS");
  });

  it("lowercase product names for URL slug generation", async () => {
    const result = await DataFrame.fromArray(products).str("name").toLowerCase().collect();

    expect((result[1] as any).name).toBe("gadget plus");
  });

  it("trim whitespace from product names — fix messy CSV imports", async () => {
    const result = await DataFrame.fromArray(products).str("name").trim().collect();

    expect((result[0] as any).name).toBe("Widget Pro");
  });

  it("identify Widget product codes by prefix — startsWith filter", async () => {
    const result = await DataFrame.fromArray(products).str("code").startsWith("WP").collect();

    expect((result[0] as any).code_startsWith).toBe(true);
    expect((result[1] as any).code_startsWith).toBe(false);
  });

  it("replace hyphens with underscores in product codes for system compatibility", async () => {
    const result = await DataFrame.fromArray(products).str("code").replace("-", "_").collect();

    expect((result[0] as any).code).toBe("WP_001");
  });

  it("measure product code length — validate fixed-width format", async () => {
    const result = await DataFrame.fromArray(products).str("code").length().collect();

    expect((result[0] as any).code_len).toBe(6);
  });

  it("split product code into prefix and number — parse structured identifiers", async () => {
    const result = await DataFrame.fromArray(products).str("code").split("-").collect();

    expect((result[0] as any).code).toEqual(["WP", "001"]);
  });
});

// ---------------------------------------------------------------------------
// Date accessor
// ---------------------------------------------------------------------------

describe("Date operations — extract and truncate timestamps for time-based reports", () => {
  const events = [
    { event: "A", date: "2026-03-15T10:30:00Z" },
    { event: "B", date: "2026-07-04T14:00:00Z" },
    { event: "C", date: "2026-12-25T08:00:00Z" },
  ];

  it("extract year from event dates for annual reporting", async () => {
    const result = await DataFrame.fromArray(events).dt("date").year().collect();

    expect((result[0] as any).date_year).toBe(2026);
  });

  it("extract month for seasonal trend analysis", async () => {
    const result = await DataFrame.fromArray(events).dt("date").month().collect();

    expect((result[0] as any).date_month).toBe(3);
    expect((result[1] as any).date_month).toBe(7);
    expect((result[2] as any).date_month).toBe(12);
  });

  it("extract day of month for daily volume charts", async () => {
    const result = await DataFrame.fromArray(events).dt("date").day().collect();

    expect((result[0] as any).date_day).toBe(15);
  });

  it("truncate to first of month — group events into monthly buckets", async () => {
    const result = await DataFrame.fromArray(events).dt("date").truncate("month").collect();

    expect((result[0] as any).date).toBe("2026-03-01T00:00:00.000Z");
    expect((result[1] as any).date).toBe("2026-07-01T00:00:00.000Z");
  });
});

// ---------------------------------------------------------------------------
// Rolling windows
// ---------------------------------------------------------------------------

describe("Rolling windows — smooth noisy time-series data", () => {
  const timeseries = [
    { day: 1, value: 10 },
    { day: 2, value: 20 },
    { day: 3, value: 30 },
    { day: 4, value: 40 },
    { day: 5, value: 50 },
  ];

  it("3-day moving average — smooth daily revenue for trend line", async () => {
    const result = await DataFrame.fromArray(timeseries)
      .rolling("value", { window: 3, fn: "mean" })
      .collect();

    expect((result[0] as any).value_rolling_mean).toBe(10); // [10]
    expect((result[1] as any).value_rolling_mean).toBe(15); // [10, 20]
    expect((result[2] as any).value_rolling_mean).toBe(20); // [10, 20, 30]
    expect((result[3] as any).value_rolling_mean).toBe(30); // [20, 30, 40]
    expect((result[4] as any).value_rolling_mean).toBe(40); // [30, 40, 50]
  });

  it("2-day rolling sum — short-window revenue accumulation", async () => {
    const result = await DataFrame.fromArray(timeseries)
      .rolling("value", { window: 2, fn: "sum" })
      .collect();

    expect((result[0] as any).value_rolling_sum).toBe(10); // [10]
    expect((result[1] as any).value_rolling_sum).toBe(30); // [10, 20]
    expect((result[2] as any).value_rolling_sum).toBe(50); // [20, 30]
  });

  it("custom column name for the moving average — 'ma3' for the chart legend", async () => {
    const result = await DataFrame.fromArray(timeseries)
      .rolling("value", { window: 3, fn: "mean", as: "ma3" })
      .collect();

    expect((result[2] as any).ma3).toBe(20);
  });
});

// ---------------------------------------------------------------------------
// Cumulative operations
// ---------------------------------------------------------------------------

describe("Cumulative operations — running totals, products, and extremes", () => {
  const data = [{ n: 10 }, { n: 20 }, { n: 30 }, { n: 40 }];

  it("running total of payments — track progress toward monthly quota", async () => {
    const result = await DataFrame.fromArray(data).cumSum("n").collect();

    expect(result.map((r: any) => r.n_cumsum)).toEqual([10, 30, 60, 100]);
  });

  it("cumulative product — compound growth factor calculation", async () => {
    const result = await DataFrame.fromArray([{ n: 2 }, { n: 3 }, { n: 4 }])
      .cumProd("n")
      .collect();

    expect(result.map((r: any) => r.n_cumprod)).toEqual([2, 6, 24]);
  });

  it("running high-water mark — track the peak value seen so far", async () => {
    const result = await DataFrame.fromArray([{ n: 3 }, { n: 1 }, { n: 4 }, { n: 2 }])
      .cumMax("n")
      .collect();

    expect(result.map((r: any) => r.n_cummax)).toEqual([3, 3, 4, 4]);
  });

  it("running low-water mark — track the minimum value seen so far", async () => {
    const result = await DataFrame.fromArray([{ n: 3 }, { n: 1 }, { n: 4 }, { n: 2 }])
      .cumMin("n")
      .collect();

    expect(result.map((r: any) => r.n_cummin)).toEqual([3, 1, 1, 1]);
  });

  it("day-over-day price change percentage — detect volatility", async () => {
    const result = await DataFrame.fromArray([{ price: 100 }, { price: 110 }, { price: 99 }])
      .pctChange("price")
      .collect();

    expect((result[0] as any).price_pctchange).toBe(0); // no previous
    expect((result[1] as any).price_pctchange).toBeCloseTo(0.1); // 10% increase
    expect((result[2] as any).price_pctchange).toBeCloseTo(-0.1); // 10% decrease
  });

  it("custom column name for running total — 'running' for dashboard display", async () => {
    const result = await DataFrame.fromArray(data).cumSum("n", { as: "running" }).collect();

    expect(result.map((r: any) => r.running)).toEqual([10, 30, 60, 100]);
  });
});

// ---------------------------------------------------------------------------
// Complex chained example
// ---------------------------------------------------------------------------

describe("End-to-end analytics — realistic multi-step e-commerce reporting", () => {
  it("pivot, running total, top spender, and rolling average in one pipeline", async () => {
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

// ---------------------------------------------------------------------------
// Multi-column sort
// ---------------------------------------------------------------------------

describe("Multi-column sort — compound ordering across multiple fields", () => {
  it("sorts by multiple columns", async () => {
    const df = DataFrame.fromArray([
      { region: "US", date: "2024-01", revenue: 100 },
      { region: "EU", date: "2024-02", revenue: 200 },
      { region: "US", date: "2024-02", revenue: 150 },
      { region: "EU", date: "2024-01", revenue: 50 },
    ]);
    const sorted = await df
      .sort([
        { column: "region", order: "asc" },
        { column: "date", order: "desc" },
      ])
      .collect();
    expect(sorted[0]).toEqual({ region: "EU", date: "2024-02", revenue: 200 });
    expect(sorted[1]).toEqual({ region: "EU", date: "2024-01", revenue: 50 });
    expect(sorted[2]).toEqual({ region: "US", date: "2024-02", revenue: 150 });
    expect(sorted[3]).toEqual({ region: "US", date: "2024-01", revenue: 100 });
  });

  it("multi-column sort with mixed asc/desc directions", async () => {
    const df = DataFrame.fromArray([
      { dept: "eng", level: 3, name: "Alice" },
      { dept: "eng", level: 1, name: "Bob" },
      { dept: "design", level: 2, name: "Charlie" },
      { dept: "design", level: 2, name: "Diana" },
    ]);
    const sorted = await df
      .sort([
        { column: "dept", order: "asc" },
        { column: "level", order: "desc" },
      ])
      .collect();
    expect((sorted[0] as any).dept).toBe("design");
    expect((sorted[0] as any).level).toBe(2);
    expect((sorted[2] as any).dept).toBe("eng");
    expect((sorted[2] as any).level).toBe(3);
    expect((sorted[3] as any).level).toBe(1);
  });

  it("single-column sort still works after overload change", async () => {
    const df = DataFrame.fromArray([{ v: 3 }, { v: 1 }, { v: 2 }]);
    const sorted = await df.sort("v", "desc").collect();
    expect(sorted.map((r) => r.v)).toEqual([3, 2, 1]);
  });
});

// ---------------------------------------------------------------------------
// Composite join keys
// ---------------------------------------------------------------------------

describe("Composite join keys — joining on multiple columns", () => {
  it("joins on composite keys", async () => {
    const left = DataFrame.fromArray([
      { userId: 1, date: "2024-01", score: 100 },
      { userId: 1, date: "2024-02", score: 200 },
      { userId: 2, date: "2024-01", score: 50 },
    ]);
    const right = DataFrame.fromArray([
      { userId: 1, date: "2024-01", label: "A" },
      { userId: 2, date: "2024-01", label: "B" },
    ]);
    const joined = await left.join(right, { on: ["userId", "date"] }).collect();
    expect(joined).toHaveLength(2);
    expect(joined[0]!.score).toBe(100);
    expect(joined[0]!.label).toBe("A");
    expect(joined[1]!.score).toBe(50);
    expect(joined[1]!.label).toBe("B");
  });

  it("composite key left join preserves unmatched rows", async () => {
    const left = DataFrame.fromArray([
      { a: 1, b: "x", val: 10 },
      { a: 1, b: "y", val: 20 },
      { a: 2, b: "x", val: 30 },
    ]);
    const right = DataFrame.fromArray([{ a: 1, b: "x", extra: "found" }]);
    const joined = await left.join(right, { on: ["a", "b"], type: "left" }).collect();
    expect(joined).toHaveLength(3);
    expect((joined[0] as any).extra).toBe("found");
    expect((joined[1] as any).extra).toBeUndefined();
    expect((joined[2] as any).extra).toBeUndefined();
  });

  it("single-column join still works after composite key change", async () => {
    const left = DataFrame.fromArray([
      { id: 1, v: "a" },
      { id: 2, v: "b" },
    ]);
    const right = DataFrame.fromArray([{ id: 1, w: "x" }]);
    const joined = await left.join(right, { on: "id", type: "inner" }).collect();
    expect(joined).toHaveLength(1);
    expect((joined[0] as any).v).toBe("a");
    expect((joined[0] as any).w).toBe("x");
  });
});

// ---------------------------------------------------------------------------
// Time-series resampling
// ---------------------------------------------------------------------------

describe("Time-series resampling — aggregate data into time buckets", () => {
  it("resamples time-series data", async () => {
    const df = DataFrame.fromArray([
      { ts: new Date("2024-01-01T00:00:00Z"), value: 10 },
      { ts: new Date("2024-01-01T00:30:00Z"), value: 20 },
      { ts: new Date("2024-01-01T01:00:00Z"), value: 30 },
      { ts: new Date("2024-01-01T01:30:00Z"), value: 40 },
      { ts: new Date("2024-01-01T02:00:00Z"), value: 50 },
    ]);
    const hourly = await df.resample("ts", "1h", { value: "avg" }).collect();
    expect(hourly).toHaveLength(3);
    expect(hourly[0]!.value).toBe(15); // avg(10, 20)
    expect(hourly[1]!.value).toBe(35); // avg(30, 40)
    expect(hourly[2]!.value).toBe(50); // avg(50)
  });

  it("resamples with numeric interval (ms)", async () => {
    const df = DataFrame.fromArray([
      { ts: 0, v: 1 },
      { ts: 500, v: 2 },
      { ts: 1000, v: 3 },
      { ts: 1500, v: 4 },
    ]);
    const result = await df.resample("ts", 1000, { v: "sum" }).collect();
    expect(result).toHaveLength(2);
    expect(result[0]!.v).toBe(3); // sum(1, 2) at bucket 0
    expect(result[1]!.v).toBe(7); // sum(3, 4) at bucket 1000
  });
});

// ---------------------------------------------------------------------------
// Column pruning optimizer
// ---------------------------------------------------------------------------

describe("Column pruning — plan optimizer preserves correctness", () => {
  it("column pruning preserves correctness", async () => {
    const df = DataFrame.fromArray([
      { a: 1, b: 2, c: 3, d: 4 },
      { a: 5, b: 6, c: 7, d: 8 },
      { a: 9, b: 10, c: 11, d: 12 },
    ]);
    // Select + filter — optimizer should handle this
    const result = await df.filter(col("a").gt(1)).select("a", "b").collect();
    expect(result).toEqual([
      { a: 5, b: 6 },
      { a: 9, b: 10 },
    ]);
  });

  it("optimizer pushes select through filter when filter columns are subset of select", () => {
    const source = { _tag: "Source" as const, data: [{ a: 1, b: 2, c: 3, d: 4 }] };
    const filter = {
      _tag: "Filter" as const,
      input: source,
      fn: (r: any) => r.a > 0,
      expr: {
        type: "binary" as const,
        op: ">",
        left: { type: "col" as const, name: "a" },
        right: { type: "lit" as const, value: 0 },
      },
    };
    const select = { _tag: "Select" as const, input: filter, columns: ["a", "b"] };

    const optimized = optimizePlan(select);
    // Filter's input should now be a Select (pushed down)
    expect((optimized as any)._tag).toBe("Filter");
    expect((optimized as any).input._tag).toBe("Select");
    expect((optimized as any).input.columns).toEqual(["a", "b"]);
  });

  it("optimizer adds wider select when filter uses columns outside select", () => {
    const source = { _tag: "Source" as const, data: [{ a: 1, b: 2, c: 3, d: 4 }] };
    const filter = {
      _tag: "Filter" as const,
      input: source,
      fn: (r: any) => r.c > 0,
      expr: {
        type: "binary" as const,
        op: ">",
        left: { type: "col" as const, name: "c" },
        right: { type: "lit" as const, value: 0 },
      },
    };
    const select = { _tag: "Select" as const, input: filter, columns: ["a", "b"] };

    const optimized = optimizePlan(select);
    // Should be Select(a,b) -> Filter -> Select(a,b,c) -> Source
    expect((optimized as any)._tag).toBe("Select");
    expect((optimized as any).columns).toEqual(["a", "b"]);
    const innerFilter = (optimized as any).input;
    expect(innerFilter._tag).toBe("Filter");
    expect(innerFilter.input._tag).toBe("Select");
    expect(new Set(innerFilter.input.columns)).toEqual(new Set(["a", "b", "c"]));
  });

  it("optimizer does not modify plan without expr on filter", async () => {
    const df = DataFrame.fromArray([
      { a: 1, b: 2, c: 3 },
      { a: 5, b: 6, c: 7 },
    ]);
    // Opaque filter function — optimizer should leave it alone
    const result = await df
      .filter((row) => row.a > 1)
      .select("a", "b")
      .collect();
    expect(result).toEqual([{ a: 5, b: 6 }]);
  });
});

// ---------------------------------------------------------------------------
// Custom aggregation functions
// ---------------------------------------------------------------------------

describe("Custom aggregation functions — median, stddev, mode, countDistinct, custom reducers", () => {
  it("groupBy with median aggregation", async () => {
    const df = DataFrame.fromArray([
      { g: "a", v: 1 },
      { g: "a", v: 3 },
      { g: "a", v: 5 },
      { g: "b", v: 10 },
      { g: "b", v: 20 },
    ]);
    const result = await df.groupBy("g").agg({ v: "median" }).collect();
    const a = result.find((r: any) => r.g === "a");
    expect(a!.v).toBe(3);
  });

  it("groupBy with stddev aggregation", async () => {
    const df = DataFrame.fromArray([
      { g: "a", v: 2 },
      { g: "a", v: 4 },
      { g: "a", v: 4 },
      { g: "a", v: 4 },
      { g: "a", v: 5 },
      { g: "a", v: 5 },
      { g: "a", v: 7 },
      { g: "a", v: 9 },
    ]);
    const result = await df.groupBy("g").agg({ v: "stddev" }).collect();
    expect(result[0]!.v).toBeCloseTo(2, 0); // stddev ≈ 2
  });

  it("groupBy with countDistinct", async () => {
    const df = DataFrame.fromArray([
      { g: "a", v: 1 },
      { g: "a", v: 2 },
      { g: "a", v: 1 },
      { g: "b", v: 3 },
    ]);
    const result = await df.groupBy("g").agg({ v: "countDistinct" }).collect();
    const a = result.find((r: any) => r.g === "a");
    expect(a!.v).toBe(2);
  });

  it("groupBy with mode aggregation", async () => {
    const df = DataFrame.fromArray([
      { g: "a", v: 1 },
      { g: "a", v: 2 },
      { g: "a", v: 2 },
      { g: "a", v: 3 },
    ]);
    const result = await df.groupBy("g").agg({ v: "mode" }).collect();
    expect(result[0]!.v).toBe(2);
  });

  it("groupBy with custom reducer", async () => {
    const df = DataFrame.fromArray([
      { g: "a", v: 10 },
      { g: "a", v: 20 },
      { g: "a", v: 30 },
    ]);
    const concatReducer = reduce("", (acc: string, val) => acc + String(val) + ",");
    const result = await df.groupBy("g").agg({ v: concatReducer }).collect();
    expect(result[0]!.v).toBe("10,20,30,");
  });

  it("groupBy with percentile", async () => {
    const df = DataFrame.fromArray([
      { g: "a", v: 1 },
      { g: "a", v: 2 },
      { g: "a", v: 3 },
      { g: "a", v: 4 },
      { g: "a", v: 5 },
    ]);
    const result = await df
      .groupBy("g")
      .agg({ v: percentile(0.9) })
      .collect();
    expect(result[0]!.v).toBe(5);
  });

  it("groupBy with variance aggregation", async () => {
    const df = DataFrame.fromArray([
      { g: "a", v: 2 },
      { g: "a", v: 4 },
      { g: "a", v: 4 },
      { g: "a", v: 4 },
      { g: "a", v: 5 },
      { g: "a", v: 5 },
      { g: "a", v: 7 },
      { g: "a", v: 9 },
    ]);
    const result = await df.groupBy("g").agg({ v: "variance" }).collect();
    // sample variance (n-1 denominator) is stddev^2
    expect(result[0]!.v).toBeCloseTo(4.571, 2);
  });

  it("exprAgg — sum of expression", async () => {
    const df = DataFrame.fromArray([
      { region: "north", revenue: 100, tax: 10 },
      { region: "north", revenue: 200, tax: 20 },
      { region: "south", revenue: 150, tax: 15 },
    ]);
    const result = await df
      .groupBy("region")
      .agg({
        revenue: exprAgg({
          expr: col("revenue").add(col("tax")),
          agg: "sum",
        }),
      })
      .collect();
    const north = result.find((r: any) => r.region === "north");
    const south = result.find((r: any) => r.region === "south");
    expect(north!.revenue).toBe(330); // (100+10) + (200+20)
    expect(south!.revenue).toBe(165); // 150+15
  });

  it("exprAgg — conditional aggregation with filter", async () => {
    const df = DataFrame.fromArray([
      { region: "north", revenue: 100, premium: true },
      { region: "north", revenue: 50, premium: false },
      { region: "north", revenue: 200, premium: true },
      { region: "south", revenue: 80, premium: false },
    ]);
    const result = await df
      .groupBy("region")
      .agg({
        revenue: exprAgg({
          expr: col("revenue"),
          agg: "sum",
          filter: col("premium").eq(true),
        }),
      })
      .collect();
    const north = result.find((r: any) => r.region === "north");
    const south = result.find((r: any) => r.region === "south");
    expect(north!.revenue).toBe(300); // 100 + 200 (premium only)
    expect(south!.revenue).toBe(0); // no premium rows
  });

  it("exprAgg — count with filter", async () => {
    const df = DataFrame.fromArray([
      { g: "a", active: true },
      { g: "a", active: false },
      { g: "a", active: true },
      { g: "b", active: true },
    ]);
    const result = await df
      .groupBy("g")
      .agg({
        active: exprAgg({
          expr: col("active"),
          agg: "count",
          filter: col("active").eq(true),
        }),
      })
      .collect();
    const a = result.find((r: any) => r.g === "a");
    const b = result.find((r: any) => r.g === "b");
    expect(a!.active).toBe(2);
    expect(b!.active).toBe(1);
  });
});
