import { describe, it, expect } from "bun:test";
import { DataFrame } from "./dataframe.ts";

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

describe("DataFrame sources", () => {
  it("fromArray creates a DataFrame", async () => {
    const df = DataFrame.fromArray(sales);
    const result = await df.collect();
    expect(result).toHaveLength(6);
    expect(result[0]).toEqual(sales[0]);
  });

  it("collect returns all rows", async () => {
    const result = await DataFrame.fromArray([1, 2, 3]).collect();
    expect(result).toEqual([1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// Column operations
// ---------------------------------------------------------------------------

describe("Column operations", () => {
  it("select picks specific columns", async () => {
    const result = await DataFrame.fromArray(sales).select("region", "revenue").collect();

    expect(result).toHaveLength(6);
    expect(Object.keys(result[0]!)).toEqual(["region", "revenue"]);
    expect(result[0]).toEqual({ region: "US", revenue: 1000 });
  });

  it("drop removes columns", async () => {
    const result = await DataFrame.fromArray(sales).drop("quantity").collect();

    expect(result[0]).toEqual({ region: "US", product: "Widget", revenue: 1000 });
    expect("quantity" in result[0]!).toBe(false);
  });

  it("rename renames columns", async () => {
    const result = await DataFrame.fromArray(sales).rename({ revenue: "sales" }).collect();

    expect(result[0]).toHaveProperty("sales", 1000);
    expect(result[0]).not.toHaveProperty("revenue");
  });

  it("withColumn adds a computed column", async () => {
    const result = await DataFrame.fromArray(sales)
      .withColumn("unitPrice", (row) => row.revenue / row.quantity)
      .collect();

    expect(result[0]).toHaveProperty("unitPrice", 100);
    expect(result[1]).toHaveProperty("unitPrice", 400);
  });
});

// ---------------------------------------------------------------------------
// Row operations
// ---------------------------------------------------------------------------

describe("Row operations", () => {
  it("filter keeps matching rows", async () => {
    const result = await DataFrame.fromArray(sales)
      .filter((r) => r.region === "US")
      .collect();

    expect(result).toHaveLength(3);
    expect(result.every((r) => r.region === "US")).toBe(true);
  });

  it("map transforms rows", async () => {
    const result = await DataFrame.fromArray(sales)
      .map((r) => ({ region: r.region, total: r.revenue * r.quantity }))
      .collect();

    expect(result[0]).toEqual({ region: "US", total: 10000 });
  });

  it("sort ascending", async () => {
    const result = await DataFrame.fromArray(sales).sort("revenue").collect();

    const revenues = result.map((r) => r.revenue);
    expect(revenues).toEqual([500, 800, 1000, 1200, 1500, 2000]);
  });

  it("sort descending", async () => {
    const result = await DataFrame.fromArray(sales).sort("revenue", "desc").collect();

    const revenues = result.map((r) => r.revenue);
    expect(revenues).toEqual([2000, 1500, 1200, 1000, 800, 500]);
  });

  it("limit takes first N rows", async () => {
    const result = await DataFrame.fromArray(sales).limit(2).collect();
    expect(result).toHaveLength(2);
  });

  it("offset skips first N rows", async () => {
    const result = await DataFrame.fromArray(sales).offset(4).collect();
    expect(result).toHaveLength(2);
  });

  it("slice extracts range", async () => {
    const result = await DataFrame.fromArray(sales).slice(1, 3).collect();
    expect(result).toHaveLength(2);
    expect(result[0]).toEqual(sales[1]);
  });

  it("distinct removes duplicate rows", async () => {
    const data = [
      { a: 1, b: 2 },
      { a: 1, b: 2 },
      { a: 3, b: 4 },
    ];
    const result = await DataFrame.fromArray(data).distinct().collect();
    expect(result).toHaveLength(2);
  });

  it("distinctBy keeps first by column", async () => {
    const result = await DataFrame.fromArray(sales).distinctBy("region").collect();

    const regions = result.map((r) => r.region);
    expect(new Set(regions).size).toBe(regions.length);
    expect(result.find((r) => r.region === "US")?.revenue).toBe(1000); // first US row
  });

  it("distinctBy keeps last by column", async () => {
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

describe("Null handling", () => {
  it("dropNull removes rows with null in column", async () => {
    const result = await DataFrame.fromArray(users).dropNull("email").collect();

    expect(result).toHaveLength(3);
    expect(result.every((r) => r.email != null)).toBe(true);
  });

  it("dropNull() removes rows with any null", async () => {
    const result = await DataFrame.fromArray(users).dropNull().collect();

    expect(result).toHaveLength(3);
  });

  it("fillNull replaces null with default", async () => {
    const result = await DataFrame.fromArray(users).fillNull("email", "unknown").collect();

    expect(result.find((r) => r.name === "Bob")?.email).toBe("unknown");
    expect(result.find((r) => r.name === "Alice")?.email).toBe("alice@example.com");
  });
});

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

describe("Aggregation", () => {
  it("groupBy + agg with sum", async () => {
    const result = await DataFrame.fromArray(sales)
      .groupBy("region")
      .agg({ revenue: "sum" })
      .collect();

    expect(result).toHaveLength(3);
    const us = result.find((r: any) => r.region === "US") as any;
    expect(us.revenue).toBe(4200); // 1000 + 2000 + 1200
  });

  it("groupBy + agg with multiple aggregations", async () => {
    const result = await DataFrame.fromArray(sales)
      .groupBy("region")
      .agg({ revenue: "sum", quantity: "avg" })
      .collect();

    expect(result).toHaveLength(3);
    const eu = result.find((r: any) => r.region === "EU") as any;
    expect(eu.revenue).toBe(2300); // 800 + 1500
    expect(eu.quantity).toBeCloseTo(5.5); // (8 + 3) / 2
  });

  it("groupBy + agg with count", async () => {
    const result = await DataFrame.fromArray(sales)
      .groupBy("region")
      .agg({ revenue: "count" })
      .collect();

    const us = result.find((r: any) => r.region === "US") as any;
    expect(us.revenue).toBe(3);
  });

  it("groupBy + agg with min/max", async () => {
    const result = await DataFrame.fromArray(sales)
      .groupBy("region")
      .agg({ revenue: "min" })
      .collect();

    const us = result.find((r: any) => r.region === "US") as any;
    expect(us.revenue).toBe(1000);
  });

  it("groupBy multiple columns", async () => {
    const result = await DataFrame.fromArray(sales)
      .groupBy("region", "product")
      .agg({ revenue: "sum" })
      .collect();

    const usWidget = result.find((r: any) => r.region === "US" && r.product === "Widget") as any;
    expect(usWidget.revenue).toBe(2200); // 1000 + 1200
  });

  it("groupBy + agg with collect", async () => {
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

describe("Joins", () => {
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

  it("inner join", async () => {
    const result = await DataFrame.fromArray(orders)
      .join(DataFrame.fromArray(profiles), { on: "userId", type: "inner" })
      .collect();

    expect(result).toHaveLength(3); // userId 1 (2 orders) + userId 2 (1 order)
    expect(result.every((r: any) => r.tier != null)).toBe(true);
  });

  it("left join preserves all left rows", async () => {
    const result = await DataFrame.fromArray(orders)
      .join(DataFrame.fromArray(profiles), { on: "userId", type: "left" })
      .collect();

    expect(result).toHaveLength(4); // all 4 orders, userId 99 has no tier
  });

  it("right join preserves all right rows", async () => {
    const result = await DataFrame.fromArray(orders)
      .join(DataFrame.fromArray(profiles), { on: "userId", type: "right" })
      .collect();

    // userId 1 (2 matches), userId 2 (1 match), userId 3 (no match in orders)
    expect(result).toHaveLength(4);
  });

  it("semi join keeps left rows with match", async () => {
    const result = await DataFrame.fromArray(orders)
      .join(DataFrame.fromArray(profiles), { on: "userId", type: "semi" })
      .collect();

    expect(result).toHaveLength(3); // 3 orders with matching profiles
    expect(result.every((r: any) => r.tier === undefined)).toBe(true); // no right columns
  });

  it("anti join keeps left rows without match", async () => {
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

describe("Statistics", () => {
  it("count returns row count", async () => {
    expect(await DataFrame.fromArray(sales).count()).toBe(6);
  });

  it("sum computes column sum", async () => {
    expect(await DataFrame.fromArray(sales).sum("revenue")).toBe(7000);
  });

  it("avg computes column average", async () => {
    const avg = await DataFrame.fromArray(sales).avg("revenue");
    expect(avg).toBeCloseTo(1166.67, 0);
  });

  it("min returns minimum value", async () => {
    expect(await DataFrame.fromArray(sales).min("revenue")).toBe(500);
  });

  it("max returns maximum value", async () => {
    expect(await DataFrame.fromArray(sales).max("revenue")).toBe(2000);
  });

  it("countDistinct counts unique values", async () => {
    expect(await DataFrame.fromArray(sales).countDistinct("region")).toBe(3);
  });

  it("describe returns column statistics", async () => {
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

describe("Terminals", () => {
  it("first returns first row", async () => {
    const row = await DataFrame.fromArray(sales).first();
    expect(row).toEqual(sales[0]);
  });

  it("first returns null for empty", async () => {
    const row = await DataFrame.fromArray([]).first();
    expect(row).toBeNull();
  });

  it("head returns first N rows", async () => {
    const rows = await DataFrame.fromArray(sales).head(2);
    expect(rows).toHaveLength(2);
  });

  it("tail returns last N rows", async () => {
    const rows = await DataFrame.fromArray(sales).tail(2);
    expect(rows).toHaveLength(2);
    expect(rows[1]).toEqual(sales[5]);
  });
});

// ---------------------------------------------------------------------------
// Chained operations
// ---------------------------------------------------------------------------

describe("Chained operations", () => {
  it("filter → sort → limit", async () => {
    const result = await DataFrame.fromArray(sales)
      .filter((r) => r.region === "US")
      .sort("revenue", "desc")
      .limit(2)
      .collect();

    expect(result).toHaveLength(2);
    expect(result[0]!.revenue).toBe(2000);
    expect(result[1]!.revenue).toBe(1200);
  });

  it("withColumn → filter → select", async () => {
    const result = await DataFrame.fromArray(sales)
      .withColumn("unitPrice", (r) => r.revenue / r.quantity)
      .filter((r) => (r as any).unitPrice > 100)
      .select("product", "unitPrice" as any)
      .collect();

    expect(result.every((r: any) => r.unitPrice > 100)).toBe(true);
    expect(Object.keys(result[0]!)).toEqual(["product", "unitPrice"]);
  });

  it("filter → groupBy → agg → sort", async () => {
    const result = await DataFrame.fromArray(sales)
      .filter((r) => r.revenue > 500)
      .groupBy("region")
      .agg({ revenue: "sum", quantity: "count" })
      .sort("revenue" as any, "desc")
      .collect();

    expect((result[0] as any).region).toBe("US");
    expect((result[0] as any).revenue).toBe(4200);
  });

  it("lazy execution — nothing runs until terminal", async () => {
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
