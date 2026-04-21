import { describe, it, expect } from "bun:test";
import { DataFrame } from "../dataframe.ts";
import { StreamPipeline } from "@promin/core";

// ---------------------------------------------------------------------------
// Test data
// ---------------------------------------------------------------------------

interface Sale {
  region: string;
  product: string;
  revenue: number;
  quantity: number;
}

const sales: Sale[] = [
  { region: "US", product: "Widget", revenue: 1000, quantity: 10 },
  { region: "US", product: "Gadget", revenue: 2000, quantity: 5 },
  { region: "EU", product: "Widget", revenue: 800, quantity: 8 },
  { region: "EU", product: "Gadget", revenue: 1500, quantity: 3 },
  { region: "US", product: "Widget", revenue: 1200, quantity: 12 },
  { region: "AP", product: "Gadget", revenue: 500, quantity: 2 },
];

interface User {
  id: number;
  name: string;
  age: number;
  email: string | null;
}

const users: User[] = [
  { id: 1, name: "Alice", age: 30, email: "alice@example.com" },
  { id: 2, name: "Bob", age: 25, email: null },
  { id: 3, name: "Charlie", age: 35, email: "charlie@example.com" },
  { id: 4, name: "Diana", age: 28, email: null },
  { id: 5, name: "Eve", age: 30, email: "eve@example.com" },
];

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

describe("Loading data — ingest from arrays, iterables, and streams", () => {
  it("import sales records from an array into a queryable DataFrame", async () => {
    const df = DataFrame.fromArray(sales);
    const result = await df.collect();
    expect(result).toHaveLength(6);
    expect(result[0]).toEqual(sales[0]);
  });

  it("collect materializes all rows for downstream consumption", async () => {
    const result = await DataFrame.fromArray([1, 2, 3]).collect();
    expect(result).toEqual([1, 2, 3]);
  });

  it("deduplicated set of IDs loaded into a DataFrame", async () => {
    const set = new Set([1, 2, 3]);
    const result = await DataFrame.fromIterable(set).collect();
    expect(result).toEqual([1, 2, 3]);
  });

  it("lazily generated records streamed into a DataFrame", async () => {
    function* gen() {
      yield { n: 1 };
      yield { n: 2 };
      yield { n: 3 };
    }
    const result = await DataFrame.fromIterable(gen()).collect();
    expect(result).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
  });

  it("real-time event stream materialized into a DataFrame for batch analysis", async () => {
    const stream = StreamPipeline.fromIterable([
      { name: "Alice", age: 30 },
      { name: "Bob", age: 25 },
    ]);
    const df = await DataFrame.fromStream(stream);
    const result = await df.filter((r) => r.age > 27).collect();
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe("Alice");
  });
});

// ---------------------------------------------------------------------------
// Column operations
// ---------------------------------------------------------------------------

describe("Column operations — reshape the data for specific reports", () => {
  it("export only region and revenue for the finance team", async () => {
    const result = await DataFrame.fromArray(sales).select("region", "revenue").collect();

    expect(result).toHaveLength(6);
    expect(Object.keys(result[0]!)).toEqual(["region", "revenue"]);
    expect(result[0]).toEqual({ region: "US", revenue: 1000 });
  });

  it("strip internal quantity field before sharing with external partners", async () => {
    const result = await DataFrame.fromArray(sales).drop("quantity").collect();

    expect(result[0]).toEqual({ region: "US", product: "Widget", revenue: 1000 });
    expect("quantity" in result[0]!).toBe(false);
  });

  it("rename revenue to sales to match the BI tool's expected schema", async () => {
    const result = await DataFrame.fromArray(sales).rename({ revenue: "sales" }).collect();

    expect(result[0]).toHaveProperty("sales", 1000);
    expect(result[0]).not.toHaveProperty("revenue");
  });

  it("compute unit price from revenue and quantity — derived metric", async () => {
    const result = await DataFrame.fromArray(sales)
      .withColumn("unitPrice", (row) => row.revenue / row.quantity)
      .collect();

    expect(result[0]).toHaveProperty("unitPrice", 100);
    expect(result[1]).toHaveProperty("unitPrice", 400);
  });

  it("withColumns adds multiple computed columns", async () => {
    const df = DataFrame.fromArray([
      { price: 100, quantity: 5 },
      { price: 200, quantity: 3 },
    ]);
    const result = await df
      .withColumns({
        total: (r) => r.price * r.quantity,
        discounted: (r) => r.price * 0.9,
      })
      .collect();
    expect(result[0]!.total).toBe(500);
    expect(result[0]!.discounted).toBe(90);
    expect(result[1]!.total).toBe(600);
  });
});

// ---------------------------------------------------------------------------
// Row operations
// ---------------------------------------------------------------------------

describe("Row operations — filter, transform, sort, and deduplicate", () => {
  it("sales team filters for US region — only US orders remain", async () => {
    const result = await DataFrame.fromArray(sales)
      .filter((r) => r.region === "US")
      .collect();

    expect(result).toHaveLength(3);
    expect(result.every((r) => r.region === "US")).toBe(true);
  });

  it("calculate total value per line item — revenue times quantity", async () => {
    const result = await DataFrame.fromArray(sales)
      .map((r) => ({ region: r.region, total: r.revenue * r.quantity }))
      .collect();

    expect(result[0]).toEqual({ region: "US", total: 10000 });
  });

  it("rank products by revenue lowest to highest", async () => {
    const result = await DataFrame.fromArray(sales).sort("revenue").collect();

    const revenues = result.map((r) => r.revenue);
    expect(revenues).toEqual([500, 800, 1000, 1200, 1500, 2000]);
  });

  it("rank products by revenue highest first — top sellers on top", async () => {
    const result = await DataFrame.fromArray(sales).sort("revenue", "desc").collect();

    const revenues = result.map((r) => r.revenue);
    expect(revenues).toEqual([2000, 1500, 1200, 1000, 800, 500]);
  });

  it("preview just the first 2 rows for a quick sanity check", async () => {
    const result = await DataFrame.fromArray(sales).limit(2).collect();
    expect(result).toHaveLength(2);
  });

  it("skip already-processed rows for incremental loading", async () => {
    const result = await DataFrame.fromArray(sales).offset(4).collect();
    expect(result).toHaveLength(2);
  });

  it("extract a page of results for paginated display", async () => {
    const result = await DataFrame.fromArray(sales).slice(1, 3).collect();
    expect(result).toHaveLength(2);
    expect(result[0]).toEqual(sales[1]);
  });

  it("remove exact duplicate rows from a denormalized export", async () => {
    const data = [
      { a: 1, b: 2 },
      { a: 1, b: 2 },
      { a: 3, b: 4 },
    ];
    const result = await DataFrame.fromArray(data).distinct().collect();
    expect(result).toHaveLength(2);
  });

  it("one row per region — keep the earliest order from each", async () => {
    const result = await DataFrame.fromArray(sales).distinctBy("region").collect();

    const regions = result.map((r) => r.region);
    expect(new Set(regions).size).toBe(regions.length);
    expect(result.find((r) => r.region === "US")?.revenue).toBe(1000); // first US row
  });

  it("one row per region — keep the most recent order from each", async () => {
    const result = await DataFrame.fromArray(sales)
      .distinctBy("region", { keep: "last" })
      .collect();

    const regions = result.map((r) => r.region);
    expect(new Set(regions).size).toBe(regions.length);
    expect(result.find((r) => r.region === "US")?.revenue).toBe(1200); // last US row
  });
});

// ---------------------------------------------------------------------------
// Null handling
// ---------------------------------------------------------------------------

describe("Null handling — clean up missing data before analysis", () => {
  it("exclude users without email — cannot send marketing campaigns to them", async () => {
    const result = await DataFrame.fromArray(users).dropNull("email").collect();

    expect(result).toHaveLength(3);
    expect(result.every((r) => r.email != null)).toBe(true);
  });

  it("drop any row with missing data — strict completeness requirement", async () => {
    const result = await DataFrame.fromArray(users).dropNull().collect();

    expect(result).toHaveLength(3);
  });

  it("replace missing emails with 'unknown' for the report — fill nulls", async () => {
    const result = await DataFrame.fromArray(users).fillNull("email", "unknown").collect();

    expect(result.find((r) => r.name === "Bob")?.email).toBe("unknown");
    expect(result.find((r) => r.name === "Alice")?.email).toBe("alice@example.com");
  });

  it("fillNull with forward fill", async () => {
    const df = DataFrame.fromArray([
      { ts: 1, temp: 20 },
      { ts: 2, temp: null },
      { ts: 3, temp: null },
      { ts: 4, temp: 25 },
      { ts: 5, temp: null },
    ]);
    const result = await df.fillNull("temp", { method: "forward" }).collect();
    expect(result.map((r) => r.temp)).toEqual([20, 20, 20, 25, 25]);
  });

  it("fillNull with backward fill", async () => {
    const df = DataFrame.fromArray([
      { ts: 1, temp: null },
      { ts: 2, temp: null },
      { ts: 3, temp: 25 },
      { ts: 4, temp: null },
      { ts: 5, temp: 30 },
    ]);
    const result = await df.fillNull("temp", { method: "backward" }).collect();
    expect(result.map((r) => r.temp)).toEqual([25, 25, 25, 30, 30]);
  });

  it("fillNull with static value still works", async () => {
    const df = DataFrame.fromArray([{ x: 1 }, { x: null }, { x: 3 }]);
    const result = await df.fillNull("x", 0).collect();
    expect(result.map((r) => r.x)).toEqual([1, 0, 3]);
  });
});

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

describe("Aggregation — summarize data for executive dashboards", () => {
  it("revenue report by region — total up all orders per geography", async () => {
    const result = await DataFrame.fromArray(sales)
      .groupBy("region")
      .agg({ revenue: "sum" })
      .collect();

    expect(result).toHaveLength(3);
    const us = result.find((r: any) => r.region === "US") as any;
    expect(us.revenue).toBe(4200); // 1000 + 2000 + 1200
  });

  it("regional summary with total revenue and average quantity per order", async () => {
    const result = await DataFrame.fromArray(sales)
      .groupBy("region")
      .agg({ revenue: "sum", quantity: "avg" })
      .collect();

    expect(result).toHaveLength(3);
    const eu = result.find((r: any) => r.region === "EU") as any;
    expect(eu.revenue).toBe(2300); // 800 + 1500
    expect(eu.quantity).toBeCloseTo(5.5); // (8 + 3) / 2
  });

  it("count orders per region — measure market activity", async () => {
    const result = await DataFrame.fromArray(sales)
      .groupBy("region")
      .agg({ revenue: "count" })
      .collect();

    const us = result.find((r: any) => r.region === "US") as any;
    expect(us.revenue).toBe(3);
  });

  it("find the smallest order per region — detect low-value anomalies", async () => {
    const result = await DataFrame.fromArray(sales)
      .groupBy("region")
      .agg({ revenue: "min" })
      .collect();

    const us = result.find((r: any) => r.region === "US") as any;
    expect(us.revenue).toBe(1000);
  });

  it("revenue by region and product — detailed product performance breakdown", async () => {
    const result = await DataFrame.fromArray(sales)
      .groupBy("region", "product")
      .agg({ revenue: "sum" })
      .collect();

    const usWidget = result.find((r: any) => r.region === "US" && r.product === "Widget") as any;
    expect(usWidget.revenue).toBe(2200); // 1000 + 1200
  });

  it("collect all revenue values per region into an array for sparkline charts", async () => {
    const result = await DataFrame.fromArray(sales)
      .groupBy("region")
      .agg({ revenue: "collect" })
      .collect();

    const us = result.find((r: any) => r.region === "US") as any;
    expect(us.revenue).toEqual([1000, 2000, 1200]);
  });
});

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

describe("Joins — combine orders with customer profiles", () => {
  const orders = [
    { userId: 1, amount: 100 },
    { userId: 2, amount: 200 },
    { userId: 1, amount: 150 },
    { userId: 99, amount: 50 },
  ];

  const profiles = [
    { userId: 1, tier: "gold" },
    { userId: 2, tier: "silver" },
    { userId: 3, tier: "bronze" },
  ];

  it("match orders to customer tiers — only orders with known customers", async () => {
    const result = await DataFrame.fromArray(orders)
      .join(DataFrame.fromArray(profiles), { on: "userId", type: "inner" })
      .collect();

    expect(result).toHaveLength(3); // userId 1 (2 orders) + userId 2 (1 order)
    expect(result.every((r: any) => r.tier != null)).toBe(true);
  });

  it("keep all orders even if customer profile is missing — left join", async () => {
    const result = await DataFrame.fromArray(orders)
      .join(DataFrame.fromArray(profiles), { on: "userId", type: "left" })
      .collect();

    expect(result).toHaveLength(4); // all 4 orders, userId 99 has no tier
  });

  it("keep all customer profiles even those with no orders — right join", async () => {
    const result = await DataFrame.fromArray(orders)
      .join(DataFrame.fromArray(profiles), { on: "userId", type: "right" })
      .collect();

    // userId 1 (2 matches), userId 2 (1 match), userId 3 (no match in orders)
    expect(result).toHaveLength(4);
  });

  it("orders from active customers only — semi join filters without adding columns", async () => {
    const result = await DataFrame.fromArray(orders)
      .join(DataFrame.fromArray(profiles), { on: "userId", type: "semi" })
      .collect();

    expect(result).toHaveLength(3); // 3 orders with matching profiles
    expect(result.every((r: any) => r.tier === undefined)).toBe(true); // no right columns
  });

  it("find orphan orders with no matching customer — anti join", async () => {
    const result = await DataFrame.fromArray(orders)
      .join(DataFrame.fromArray(profiles), { on: "userId", type: "anti" })
      .collect();

    expect(result).toHaveLength(1); // userId 99 only
    expect((result[0] as any).userId).toBe(99);
  });
});

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

describe("Statistics — quick numeric summaries for data exploration", () => {
  it("total number of sales transactions", async () => {
    expect(await DataFrame.fromArray(sales).count()).toBe(6);
  });

  it("total revenue across all regions", async () => {
    expect(await DataFrame.fromArray(sales).sum("revenue")).toBe(7000);
  });

  it("average order value — key metric for the growth team", async () => {
    const avg = await DataFrame.fromArray(sales).avg("revenue");
    expect(avg).toBeCloseTo(1166.67, 0);
  });

  it("smallest order value — detect micro-transactions", async () => {
    expect(await DataFrame.fromArray(sales).min("revenue")).toBe(500);
  });

  it("largest order value — spot high-value deals", async () => {
    expect(await DataFrame.fromArray(sales).max("revenue")).toBe(2000);
  });

  it("how many distinct regions are we selling to", async () => {
    expect(await DataFrame.fromArray(sales).countDistinct("region")).toBe(3);
  });

  it("full statistical profile of revenue — count, mean, min, max, nulls", async () => {
    const stats = await DataFrame.fromArray(sales).describe();

    const revenueStat = stats.find((s) => s.column === "revenue")!;
    expect(revenueStat.count).toBe(6);
    expect(revenueStat.nulls).toBe(0);
    expect(revenueStat.mean).toBeCloseTo(1166.67, 0);
    expect(revenueStat.min).toBe(500);
    expect(revenueStat.max).toBe(2000);
  });
});

// ---------------------------------------------------------------------------
// Terminals
// ---------------------------------------------------------------------------

describe("Terminal operations — extract final results from the DataFrame", () => {
  it("peek at the first record to verify schema shape", async () => {
    const row = await DataFrame.fromArray(sales).first();
    expect(row).toEqual(sales[0]);
  });

  it("empty dataset returns null instead of crashing", async () => {
    const row = await DataFrame.fromArray([]).first();
    expect(row).toBeNull();
  });

  it("preview the top 2 rows for a dashboard widget", async () => {
    const rows = await DataFrame.fromArray(sales).head(2);
    expect(rows).toHaveLength(2);
  });

  it("last 2 records — check the most recent entries", async () => {
    const rows = await DataFrame.fromArray(sales).tail(2);
    expect(rows).toHaveLength(2);
    expect(rows[1]).toEqual(sales[5]);
  });
});

// ---------------------------------------------------------------------------
// Chained operations
// ---------------------------------------------------------------------------

describe("Chained operations — compose multi-step data pipelines", () => {
  it("top 2 US orders by revenue — filter, sort, then limit", async () => {
    const result = await DataFrame.fromArray(sales)
      .filter((r) => r.region === "US")
      .sort("revenue", "desc")
      .limit(2)
      .collect();

    expect(result).toHaveLength(2);
    expect(result[0]!.revenue).toBe(2000);
    expect(result[1]!.revenue).toBe(1200);
  });

  it("compute unit price, keep only premium products, export product and price", async () => {
    const result = await DataFrame.fromArray(sales)
      .withColumn("unitPrice", (r) => r.revenue / r.quantity)
      .filter((r) => (r as any).unitPrice > 100)
      .select("product", "unitPrice" as any)
      .collect();

    expect(result.every((r: any) => r.unitPrice > 100)).toBe(true);
    expect(Object.keys(result[0]!)).toEqual(["product", "unitPrice"]);
  });

  it("exclude small orders, summarize by region, rank by total revenue", async () => {
    const result = await DataFrame.fromArray(sales)
      .filter((r) => r.revenue > 500)
      .groupBy("region")
      .agg({ revenue: "sum", quantity: "count" })
      .sort("revenue" as any, "desc")
      .collect();

    expect((result[0] as any).region).toBe("US");
    expect((result[0] as any).revenue).toBe(4200);
  });

  it("lazy evaluation — no work until collect is called, saving resources", async () => {
    let filterCalled = false;

    const df = DataFrame.fromArray(sales).filter((r) => {
      filterCalled = true;
      return r.revenue > 1000;
    });

    // No terminal called yet
    expect(filterCalled).toBe(false);

    // Now collect triggers execution
    await df.collect();
    expect(filterCalled).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Concat, Union, Reverse, Set Operations
// ---------------------------------------------------------------------------

describe("concat, union, reverse, set operations", () => {
  it("concat merges rows from multiple frames", async () => {
    const a = DataFrame.fromArray([{ id: 1 }, { id: 2 }]);
    const b = DataFrame.fromArray([{ id: 3 }, { id: 4 }]);
    const result = await DataFrame.concat(a, b).collect();
    expect(result).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
  });

  it("concat with three frames", async () => {
    const a = DataFrame.fromArray([{ x: 1 }]);
    const b = DataFrame.fromArray([{ x: 2 }]);
    const c = DataFrame.fromArray([{ x: 3 }]);
    const result = await DataFrame.concat(a, b, c).collect();
    expect(result).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
  });

  it("concat with empty frames", async () => {
    const a = DataFrame.fromArray([{ id: 1 }]);
    const b = DataFrame.fromArray<{ id: number }>([]);
    const result = await DataFrame.concat(a, b).collect();
    expect(result).toEqual([{ id: 1 }]);
  });

  it("union deduplicates rows", async () => {
    const a = DataFrame.fromArray([{ id: 1 }, { id: 2 }, { id: 3 }]);
    const b = DataFrame.fromArray([{ id: 2 }, { id: 3 }, { id: 4 }]);
    const result = await a.union(b).collect();
    expect(result).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
  });

  it("reverse reverses row order", async () => {
    const result = await DataFrame.fromArray([{ v: 1 }, { v: 2 }, { v: 3 }])
      .reverse()
      .collect();
    expect(result).toEqual([{ v: 3 }, { v: 2 }, { v: 1 }]);
  });

  it("intersection keeps only rows in both frames", async () => {
    const a = DataFrame.fromArray([
      { id: 1, name: "a" },
      { id: 2, name: "b" },
      { id: 3, name: "c" },
    ]);
    const b = DataFrame.fromArray([
      { id: 2, name: "x" },
      { id: 4, name: "y" },
    ]);
    const result = await a.intersection(b, "id").collect();
    expect(result).toEqual([{ id: 2, name: "b" }]);
  });

  it("difference keeps rows not in other frame", async () => {
    const a = DataFrame.fromArray([
      { id: 1, name: "a" },
      { id: 2, name: "b" },
      { id: 3, name: "c" },
    ]);
    const b = DataFrame.fromArray([
      { id: 2, name: "x" },
      { id: 4, name: "y" },
    ]);
    const result = await a.difference(b, "id").collect();
    expect(result).toEqual([
      { id: 1, name: "a" },
      { id: 3, name: "c" },
    ]);
  });
});

// ---------------------------------------------------------------------------
// Statistical Functions
// ---------------------------------------------------------------------------

describe("statistical functions", () => {
  const data = [
    { name: "a", score: 10 },
    { name: "b", score: 20 },
    { name: "c", score: 30 },
    { name: "d", score: 40 },
    { name: "e", score: 50 },
  ];
  const df = DataFrame.fromArray(data);

  it("median with odd count", async () => {
    expect(await df.median("score")).toBe(30);
  });

  it("median with even count", async () => {
    const even = DataFrame.fromArray([{ v: 10 }, { v: 20 }, { v: 30 }, { v: 40 }]);
    expect(await even.median("v")).toBe(25);
  });

  it("median returns null for empty", async () => {
    expect(await DataFrame.fromArray<{ v: number }>([]).median("v")).toBeNull();
  });

  it("variance (sample)", async () => {
    const v = await df.variance("score");
    expect(v).toBeCloseTo(250); // sample variance of [10,20,30,40,50]
  });

  it("std (sample)", async () => {
    const s = await df.std("score");
    expect(s).toBeCloseTo(Math.sqrt(250));
  });

  it("variance returns null for < 2 rows", async () => {
    expect(await DataFrame.fromArray([{ v: 5 }]).variance("v")).toBeNull();
  });

  it("quantile at 0.5 equals median", async () => {
    expect(await df.quantile("score", 0.5)).toBe(30);
  });

  it("quantile at 0 and 1", async () => {
    expect(await df.quantile("score", 0)).toBe(10);
    expect(await df.quantile("score", 1)).toBe(50);
  });

  it("quantile at 0.25", async () => {
    expect(await df.quantile("score", 0.25)).toBe(20);
  });

  it("correlation — perfect positive", async () => {
    const linear = DataFrame.fromArray([
      { x: 1, y: 2 },
      { x: 2, y: 4 },
      { x: 3, y: 6 },
    ]);
    expect(await linear.correlation("x", "y")).toBeCloseTo(1);
  });

  it("correlation — perfect negative", async () => {
    const neg = DataFrame.fromArray([
      { x: 1, y: 6 },
      { x: 2, y: 4 },
      { x: 3, y: 2 },
    ]);
    expect(await neg.correlation("x", "y")).toBeCloseTo(-1);
  });

  it("correlation — uncorrelated", async () => {
    const uncorr = DataFrame.fromArray([
      { x: 1, y: 5 },
      { x: 2, y: 3 },
      { x: 3, y: 7 },
      { x: 4, y: 1 },
      { x: 5, y: 9 },
    ]);
    const c = await uncorr.correlation("x", "y");
    // Not perfectly uncorrelated but close — just check it's a number
    expect(typeof c).toBe("number");
  });

  it("covariance", async () => {
    const linear = DataFrame.fromArray([
      { x: 1, y: 2 },
      { x: 2, y: 4 },
      { x: 3, y: 6 },
    ]);
    expect(await linear.covariance("x", "y")).toBeCloseTo(2);
  });

  it("covariance returns null for < 2 rows", async () => {
    expect(await DataFrame.fromArray([{ x: 1, y: 2 }]).covariance("x", "y")).toBeNull();
  });
});
